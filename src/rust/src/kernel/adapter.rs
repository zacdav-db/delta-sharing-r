//! Isolation layer for concrete Delta Kernel APIs.
//!
//! No other package module should depend on concrete Delta Kernel types. This
//! adapter accepts only an already-prepared local table location and compact
//! scan controls, then exposes logical Arrow record batches.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef as ArrowSchemaRef};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::{DeltaResult, Engine, EngineData, Snapshot, SnapshotRef};
use delta_kernel_default_engine::DefaultEngineBuilder;
use url::Url;

const MAX_BATCH_SIZE: usize = 1_000_000;
const MIN_SOURCE_BATCH_SIZE: usize = 1_000;
const MAX_SOURCE_BATCH_SIZE: usize = 65_536;
const MAX_PROJECTION_COLUMNS: usize = 10_000;
const MAX_TABLE_LOCATION_BYTES: usize = 32_768;

#[derive(Debug, Clone)]
pub(crate) struct SnapshotReadOptions {
    table_location: String,
    columns: Option<Vec<String>>,
    limit: Option<u64>,
    batch_size: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct CdfReadOptions {
    table_location: String,
    columns: Option<Vec<String>>,
    start_version: u64,
    end_version: u64,
    batch_size: usize,
}

impl SnapshotReadOptions {
    pub(crate) fn try_new(
        table_location: String,
        columns: Option<Vec<String>>,
        limit: Option<u64>,
        batch_size: usize,
    ) -> Result<Self, String> {
        validate_table_location(&table_location)?;
        validate_batch_size(batch_size)?;
        validate_projection(columns.as_ref())?;

        Ok(Self {
            table_location,
            columns,
            limit,
            batch_size,
        })
    }
}

impl CdfReadOptions {
    pub(crate) fn try_new(
        table_location: String,
        columns: Option<Vec<String>>,
        start_version: u64,
        end_version: u64,
        batch_size: usize,
    ) -> Result<Self, String> {
        validate_table_location(&table_location)?;
        validate_batch_size(batch_size)?;
        validate_projection(columns.as_ref())?;
        if end_version < start_version {
            return Err(
                "`end_version` must be greater than or equal to `start_version`".to_string(),
            );
        }
        Ok(Self {
            table_location,
            columns,
            start_version,
            end_version,
            batch_size,
        })
    }
}

fn validate_batch_size(batch_size: usize) -> Result<(), String> {
    if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
        return Err(format!(
            "`batch_size` must be between 1 and {MAX_BATCH_SIZE}"
        ));
    }
    Ok(())
}

fn validate_projection(columns: Option<&Vec<String>>) -> Result<(), String> {
    let Some(columns) = columns else {
        return Ok(());
    };
    if columns.is_empty() {
        return Err("`columns` must be NULL or contain at least one name".to_string());
    }
    if columns.len() > MAX_PROJECTION_COLUMNS {
        return Err(format!(
            "`columns` must contain at most {MAX_PROJECTION_COLUMNS} names"
        ));
    }

    let mut seen = HashSet::with_capacity(columns.len());
    for column in columns {
        if column.is_empty() {
            return Err("`columns` must not contain empty names".to_string());
        }
        if column.as_bytes().contains(&0) {
            return Err("`columns` must not contain NUL bytes".to_string());
        }
        if !seen.insert(column.to_lowercase()) {
            return Err(format!(
                "`columns` contains the duplicate Delta column name `{column}`"
            ));
        }
    }
    Ok(())
}

fn validate_table_location(table_location: &str) -> Result<(), String> {
    if table_location.is_empty() {
        return Err("`table_location` must not be empty".to_string());
    }
    if table_location.len() > MAX_TABLE_LOCATION_BYTES {
        return Err(format!(
            "`table_location` must be at most {MAX_TABLE_LOCATION_BYTES} bytes"
        ));
    }
    if table_location.as_bytes().contains(&0) {
        return Err("`table_location` must not contain NUL bytes".to_string());
    }

    if table_location.starts_with("file://") {
        return Ok(());
    }
    if table_location.contains("://") {
        return Err(
            "only an absolute local path or `file://` URI is accepted by the native scan"
                .to_string(),
        );
    }
    if !Path::new(table_location).is_absolute() {
        return Err("`table_location` must be an absolute local path or `file://` URI".to_string());
    }

    Ok(())
}

fn source_batch_size(batch_size: usize) -> NonZeroUsize {
    NonZeroUsize::new(batch_size.clamp(MIN_SOURCE_BATCH_SIZE, MAX_SOURCE_BATCH_SIZE))
        .expect("source batch size is always non-zero")
}

fn default_engine(batch_size: usize) -> Arc<dyn Engine> {
    Arc::new(
        DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
            .with_batch_size(source_batch_size(batch_size))
            .build(),
    )
}

pub(crate) fn snapshot_reader(
    options: SnapshotReadOptions,
) -> Result<Box<dyn RecordBatchReader + Send>, String> {
    let engine = default_engine(options.batch_size);
    let snapshot = Snapshot::builder_for(&options.table_location)
        .build(engine.as_ref())
        .map_err(|_| "Delta Kernel snapshot preparation failed".to_string())?;

    let output_logical_schema = match options.columns.as_ref() {
        Some(columns) => snapshot
            .schema()
            .project(columns)
            .map_err(|_| "Delta Kernel projection validation failed".to_string())?,
        None => snapshot.schema(),
    };

    let mut scan = snapshot
        .clone()
        .scan_builder()
        .with_schema(output_logical_schema.clone())
        .build()
        .map_err(|_| "Delta Kernel scan planning failed".to_string())?;
    let mut output_projection = None;

    // Delta Kernel 0.26's default Parquet engine cannot currently decode a
    // zero-field physical schema. A projection containing only partition
    // columns produces exactly that shape because those values come from the
    // Delta log rather than Parquet. Add one hidden data column only in this
    // case, then project it away after Kernel reconstructs the logical batch.
    if scan.physical_schema().num_fields() == 0 {
        let columns = options.columns.as_ref().ok_or_else(|| {
            "Delta Kernel cannot scan a table with no physical data columns".to_string()
        })?;
        let selected: HashSet<String> =
            columns.iter().map(|column| column.to_lowercase()).collect();
        let mut replacement = None;
        for field in snapshot.schema().fields() {
            if selected.contains(&field.name().to_lowercase()) {
                continue;
            }
            let mut physical_columns = columns.clone();
            physical_columns.push(field.name().to_string());
            let physical_logical_schema = snapshot
                .schema()
                .project(&physical_columns)
                .map_err(|_| "Delta Kernel projection validation failed".to_string())?;
            let candidate = snapshot
                .clone()
                .scan_builder()
                .with_schema(physical_logical_schema)
                .build()
                .map_err(|_| "Delta Kernel scan planning failed".to_string())?;
            if candidate.physical_schema().num_fields() > 0 {
                replacement = Some(candidate);
                break;
            }
        }
        scan = replacement.ok_or_else(|| {
            "Delta Kernel partition-only projection requires one physical data column".to_string()
        })?;
        output_projection = Some((0..columns.len()).collect::<Vec<_>>());
    }

    let physical_arrow_schema: Schema = scan
        .logical_schema()
        .as_ref()
        .try_into_arrow()
        .map_err(|_| "Delta Kernel logical schema conversion failed".to_string())?;
    let output_arrow_schema: Schema = output_logical_schema
        .as_ref()
        .try_into_arrow()
        .map_err(|_| "Delta Kernel logical schema conversion failed".to_string())?;
    let source = scan
        .execute(engine.clone())
        .map_err(|_| "Delta Kernel scan initialization failed".to_string())?;
    let source: KernelDataIterator = Box::new(source);

    Ok(Box::new(KernelRecordBatchReader {
        schema: Arc::new(output_arrow_schema),
        physical_schema: output_projection
            .as_ref()
            .map(|_| Arc::new(physical_arrow_schema)),
        output_projection,
        source,
        // These fields intentionally retain the engine and snapshot until the
        // Arrow stream is exhausted or released early.
        _engine: engine,
        _owner: KernelReadOwner::Snapshot {
            _snapshot: snapshot,
        },
        pending: None,
        pending_offset: 0,
        remaining: options.limit,
        batch_size: options.batch_size,
        terminal: false,
    }))
}

fn local_table_url(table_location: &str) -> Result<Url, String> {
    if table_location.starts_with("file://") {
        let url = Url::parse(table_location)
            .map_err(|_| "`table_location` is not a valid local file URI".to_string())?;
        if url.scheme() != "file" {
            return Err("`table_location` must use the local file scheme".to_string());
        }
        Ok(url)
    } else {
        Url::from_directory_path(table_location)
            .map_err(|_| "`table_location` is not a valid absolute local path".to_string())
    }
}

pub(crate) fn cdf_reader(
    options: CdfReadOptions,
) -> Result<Box<dyn RecordBatchReader + Send>, String> {
    const CDF_COLUMNS: [&str; 3] = ["_change_type", "_commit_version", "_commit_timestamp"];

    let engine = default_engine(options.batch_size);
    let table_root = local_table_url(&options.table_location)?;
    let changes = Arc::new(
        TableChanges::try_new(
            table_root,
            engine.as_ref(),
            options.start_version,
            Some(options.end_version),
        )
        .map_err(|_| "Delta Kernel CDF preparation failed".to_string())?,
    );

    let output_logical_schema = match options.columns.as_ref() {
        Some(columns) => changes
            .schema()
            .project(columns)
            .map_err(|_| "Delta Kernel CDF projection validation failed".to_string())?,
        None => Arc::new(changes.schema().clone()),
    };
    let mut physical_logical_schema = output_logical_schema.clone();
    let mut output_projection = None;
    if let Some(columns) = options.columns.as_ref() {
        let mut physical_columns = columns.clone();
        for cdf_column in CDF_COLUMNS {
            if !physical_columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(cdf_column))
            {
                physical_columns.push(cdf_column.to_string());
            }
        }
        let metadata_only = columns
            .iter()
            .all(|column| CDF_COLUMNS.contains(&column.to_lowercase().as_str()));
        if metadata_only {
            let hidden = changes
                .schema()
                .fields()
                .find(|field| !CDF_COLUMNS.contains(&field.name().to_lowercase().as_str()))
                .ok_or_else(|| {
                    "Delta Kernel CDF metadata-only projection requires one data column".to_string()
                })?;
            physical_columns.push(hidden.name().to_string());
        }
        physical_logical_schema = changes
            .schema()
            .project(&physical_columns)
            .map_err(|_| "Delta Kernel CDF projection validation failed".to_string())?;
        output_projection = (physical_columns.len() != columns.len())
            .then(|| (0..columns.len()).collect::<Vec<_>>());
    }

    let scan = changes
        .clone()
        .scan_builder()
        .with_schema(physical_logical_schema)
        .build()
        .map_err(|_| "Delta Kernel CDF scan planning failed".to_string())?;
    let physical_arrow_schema: Schema = scan
        .logical_schema()
        .as_ref()
        .try_into_arrow()
        .map_err(|_| "Delta Kernel CDF physical schema conversion failed".to_string())?;
    let output_arrow_schema: Schema = output_logical_schema
        .as_ref()
        .try_into_arrow()
        .map_err(|_| "Delta Kernel CDF logical schema conversion failed".to_string())?;
    let source = scan
        .execute(engine.clone())
        .map_err(|_| "Delta Kernel CDF scan initialization failed".to_string())?;
    let source: KernelDataIterator = Box::new(source);

    Ok(Box::new(KernelRecordBatchReader {
        schema: Arc::new(output_arrow_schema),
        physical_schema: output_projection
            .as_ref()
            .map(|_| Arc::new(physical_arrow_schema)),
        output_projection,
        source,
        _engine: engine,
        _owner: KernelReadOwner::TableChanges { _changes: changes },
        pending: None,
        pending_offset: 0,
        remaining: None,
        batch_size: options.batch_size,
        terminal: false,
    }))
}

type KernelDataIterator = Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>;

enum KernelReadOwner {
    Snapshot { _snapshot: SnapshotRef },
    TableChanges { _changes: Arc<TableChanges> },
}

struct KernelRecordBatchReader {
    schema: ArrowSchemaRef,
    physical_schema: Option<ArrowSchemaRef>,
    output_projection: Option<Vec<usize>>,
    source: KernelDataIterator,
    _engine: Arc<dyn Engine>,
    _owner: KernelReadOwner,
    pending: Option<RecordBatch>,
    pending_offset: usize,
    remaining: Option<u64>,
    batch_size: usize,
    terminal: bool,
}

impl KernelRecordBatchReader {
    fn load_next_non_empty_batch(&mut self) -> Option<Result<(), ArrowError>> {
        loop {
            let data = match self.source.next()? {
                Ok(data) => data,
                Err(_) => {
                    return Some(Err(ArrowError::ComputeError(
                        "Delta Kernel data scan failed".to_string(),
                    )));
                }
            };
            let batch = match data.try_into_record_batch() {
                Ok(batch) => batch,
                Err(_) => {
                    return Some(Err(ArrowError::ComputeError(
                        "Delta Kernel Arrow conversion failed".to_string(),
                    )));
                }
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let batch_schema = self
                .physical_schema
                .clone()
                .unwrap_or_else(|| self.schema.clone());
            let batch = match batch.with_schema(batch_schema) {
                Ok(batch) => batch,
                Err(_) => {
                    return Some(Err(ArrowError::SchemaError(
                        "Delta Kernel returned a batch outside its logical schema".to_string(),
                    )));
                }
            };
            let batch = match self.output_projection.as_ref() {
                Some(projection) => match batch.project(projection) {
                    Ok(batch) => match batch.with_schema(self.schema.clone()) {
                        Ok(batch) => batch,
                        Err(_) => {
                            return Some(Err(ArrowError::SchemaError(
                                "Delta Kernel returned a batch outside its projected schema"
                                    .to_string(),
                            )));
                        }
                    },
                    Err(_) => {
                        return Some(Err(ArrowError::SchemaError(
                            "Delta Kernel output projection failed".to_string(),
                        )));
                    }
                },
                None => batch,
            };

            self.pending = Some(batch);
            self.pending_offset = 0;
            return Some(Ok(()));
        }
    }
}

impl Iterator for KernelRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal || self.remaining == Some(0) {
            self.terminal = true;
            return None;
        }

        let pending_rows = self.pending.as_ref().map_or(0, RecordBatch::num_rows);
        if self.pending_offset >= pending_rows {
            self.pending = None;
            match self.load_next_non_empty_batch()? {
                Ok(()) => {}
                Err(error) => {
                    self.terminal = true;
                    return Some(Err(error));
                }
            }
        }

        let Some(batch) = self.pending.as_ref() else {
            self.terminal = true;
            return Some(Err(ArrowError::ComputeError(
                "Delta Kernel batch state is invalid".to_string(),
            )));
        };
        let available = batch.num_rows() - self.pending_offset;
        let limit_rows = self.remaining.map_or(available, |remaining| {
            remaining.min(available as u64) as usize
        });
        let emit_rows = available.min(self.batch_size).min(limit_rows);

        if emit_rows == 0 {
            self.terminal = true;
            return None;
        }

        let output = batch.slice(self.pending_offset, emit_rows);
        self.pending_offset += emit_rows;
        if let Some(remaining) = self.remaining.as_mut() {
            *remaining -= emit_rows as u64;
        }
        Some(Ok(output))
    }
}

impl RecordBatchReader for KernelRecordBatchReader {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}
