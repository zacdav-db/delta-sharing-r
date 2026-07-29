//! Isolation layer for concrete Delta Kernel APIs.
//!
//! No other package module should depend on concrete Delta Kernel types. This
//! adapter accepts only an already-prepared local table location and compact
//! scan controls, then exposes logical Arrow record batches.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef as ArrowSchemaRef};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::{DeltaResult, Engine, EngineData, Snapshot, SnapshotRef};
use url::Url;

const MAX_BATCH_SIZE: usize = 1_000_000;
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

pub(crate) fn snapshot_reader(
    options: SnapshotReadOptions,
) -> Result<Box<dyn RecordBatchReader + Send>, String> {
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(&options.table_location)
        .build(engine.as_ref())
        .map_err(|_| "Delta Kernel snapshot preparation failed".to_string())?;

    let logical_schema = match options.columns.as_ref() {
        Some(columns) => snapshot
            .schema()
            .project(columns)
            .map_err(|_| "Delta Kernel projection validation failed".to_string())?,
        None => snapshot.schema(),
    };

    let scan = snapshot
        .clone()
        .scan_builder()
        .with_schema(logical_schema)
        .build()
        .map_err(|_| "Delta Kernel scan planning failed".to_string())?;
    let arrow_schema: Schema = scan
        .logical_schema()
        .as_ref()
        .try_into_arrow()
        .map_err(|_| "Delta Kernel logical schema conversion failed".to_string())?;
    let source = scan
        .execute(engine.clone())
        .map_err(|_| "Delta Kernel scan initialization failed".to_string())?;
    let source: KernelDataIterator = Box::new(source);

    Ok(Box::new(KernelRecordBatchReader {
        schema: Arc::new(arrow_schema),
        physical_schema: None,
        output_projection: None,
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

    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let table_root = local_table_url(&options.table_location)?;
    let changes = Arc::new(
        TableChanges::try_new(
            table_root,
            engine.as_ref(),
            options.start_version,
            Some(options.end_version),
        )
        .map_err(|e| format!("Delta Kernel CDF preparation failed: {e}"))?,
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
                            "Delta Kernel CDF output projection failed".to_string(),
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::FileTimes;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use delta_kernel::table_changes::TableChanges;

    use super::*;

    static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("test clock must follow the Unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "delta-sharing-r-{label}-{}-{nanos}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(path.join("_delta_log"))
                .expect("test Delta log directory must be created");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn fixture_table() -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/testthat/fixtures/delta/local-table");
        fs::canonicalize(path)
            .expect("committed Delta fixture must exist")
            .to_string_lossy()
            .into_owned()
    }

    fn fixture_parquet() -> &'static [u8] {
        include_bytes!("../../../../tests/testthat/fixtures/delta/local-table/part-00000.parquet")
    }

    fn bounded_cdf_fixture() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/testthat/fixtures/delta/cdf")
    }

    fn copy_fixture_directory(source: &Path, target: &Path) {
        fs::create_dir_all(target).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let source_path = entry.path();
            let target_path = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_fixture_directory(&source_path, &target_path);
            } else {
                fs::copy(source_path, target_path).unwrap();
            }
        }
    }

    fn set_modified_millis(path: &Path, millis: u64) {
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_millis(millis)))
            .unwrap();
    }

    fn prepared_bounded_cdf_table() -> TestDirectory {
        let table = TestDirectory::new("bounded-cdf");
        copy_fixture_directory(&bounded_cdf_fixture(), table.path());
        set_modified_millis(
            &table.path().join("_delta_log/00000000000000000001.json"),
            1_734_480_105_872,
        );
        set_modified_millis(
            &table.path().join("_delta_log/00000000000000000002.json"),
            1_734_480_106_177,
        );
        table
    }

    fn write_commit(table: &Path, action_path: Option<&str>, protocol_version: u32) {
        let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"group\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":false,\"metadata\":{}}]}"#;
        let mut lines = vec![
            format!(
                r#"{{"protocol":{{"minReaderVersion":{protocol_version},"minWriterVersion":2}}}}"#
            ),
            format!(
                r#"{{"metaData":{{"id":"1a29bce9-7213-47de-bfb4-bf2f8ab80796","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{schema}","partitionColumns":[],"configuration":{{}},"createdTime":0}}}}"#
            ),
        ];
        if let Some(action_path) = action_path {
            lines.push(format!(
                r#"{{"add":{{"path":"{action_path}","partitionValues":{{}},"size":{},"modificationTime":0,"dataChange":true}}}}"#,
                fixture_parquet().len()
            ));
        }
        fs::write(
            table.join("_delta_log").join("00000000000000000000.json"),
            format!("{}\n", lines.join("\n")),
        )
        .expect("test Delta commit must be written");
    }

    #[test]
    fn pinned_kernel_default_engine_constructs() {
        // Guards against pre-1.0 kernel API drift in the pinned version: the
        // default engine builder and snapshot builder must still construct.
        let store = Arc::new(delta_kernel::object_store::memory::InMemory::new());
        let _engine = DefaultEngineBuilder::new(store).build();
        let _snapshot_builder = Snapshot::builder_for("memory:///delta-sharing-r-smoke");
    }

    #[test]
    fn bounded_true_version_log_preserves_cdf_metadata() {
        const VERSION_1_MILLIS: u64 = 1_734_480_105_872;
        const VERSION_2_MILLIS: u64 = 1_734_480_106_177;

        let table = prepared_bounded_cdf_table();

        let engine: Arc<dyn Engine> =
            Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
        let table_root = url::Url::from_directory_path(table.path()).unwrap();
        let changes = TableChanges::try_new(table_root, engine.as_ref(), 1, Some(2)).unwrap();
        let logical_schema = changes
            .schema()
            .project(&["id", "_change_type", "_commit_version", "_commit_timestamp"])
            .unwrap();
        let scan = changes
            .into_scan_builder()
            .with_schema(logical_schema)
            .build()
            .unwrap();
        let source = scan.execute(engine).unwrap();

        let mut observed = Vec::new();
        for data in source {
            let batch = data.unwrap().try_into_record_batch().unwrap();
            let change_type = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let commit_version = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let commit_timestamp = batch
                .column(3)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                observed.push((
                    change_type.value(row).to_string(),
                    commit_version.value(row),
                    commit_timestamp.value(row),
                ));
            }
        }

        assert!(!observed.is_empty());
        assert!(observed.iter().any(|row| row.1 == 1));
        assert!(observed.iter().any(|row| row.1 == 2));
        for (change_type, version, timestamp) in observed {
            match version {
                1 => {
                    assert_eq!(change_type, "delete");
                    assert_eq!(timestamp, VERSION_1_MILLIS as i64 * 1_000);
                }
                2 => {
                    assert_eq!(change_type, "insert");
                    assert_eq!(timestamp, VERSION_2_MILLIS as i64 * 1_000);
                }
                _ => panic!("provider version was rebased to {version}"),
            }
        }
    }

    #[test]
    fn cdf_reader_metadata_only_projection_is_exact_and_panic_free() {
        let table = prepared_bounded_cdf_table();
        let columns = vec![
            "_change_type".to_string(),
            "_commit_version".to_string(),
            "_commit_timestamp".to_string(),
        ];
        let reader = cdf_reader(
            CdfReadOptions::try_new(
                table.path().to_string_lossy().into_owned(),
                Some(columns.clone()),
                1,
                2,
                3,
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            reader
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            columns
        );
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(!batches.is_empty());
        assert!(batches.iter().all(|batch| batch.num_columns() == 3));
        assert!(batches.iter().all(|batch| batch.num_rows() <= 3));
    }

    #[test]
    fn cdf_reader_uses_inclusive_provider_bounds() {
        let table = prepared_bounded_cdf_table();
        let versions_for = |end_version| {
            let reader = cdf_reader(
                CdfReadOptions::try_new(
                    table.path().to_string_lossy().into_owned(),
                    Some(vec!["id".to_string(), "_commit_version".to_string()]),
                    1,
                    end_version,
                    64,
                )
                .unwrap(),
            )
            .unwrap();
            reader
                .flat_map(|batch| {
                    let batch = batch.unwrap();
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>()
        };

        let through_two = versions_for(2);
        assert!(through_two.contains(&1));
        assert!(through_two.contains(&2));
        let through_one = versions_for(1);
        assert!(!through_one.is_empty());
        assert!(through_one.iter().all(|version| *version == 1));
    }

    #[test]
    fn cdf_reader_supports_a_zero_start_without_a_checkpoint() {
        let table = TestDirectory::new("cdf-zero-start");
        fs::write(table.path().join("part-00000.parquet"), fixture_parquet()).unwrap();
        write_commit(table.path(), Some("part-00000.parquet"), 1);
        let commit = table.path().join("_delta_log/00000000000000000000.json");
        let contents = fs::read_to_string(&commit)
            .unwrap()
            .replace(
                r#""minWriterVersion":2"#,
                r#""minWriterVersion":7,"writerFeatures":["changeDataFeed"]"#,
            )
            .replace(
                r#""configuration":{}"#,
                r#""configuration":{"delta.enableChangeDataFeed":"true"}"#,
            );
        fs::write(&commit, contents).unwrap();
        set_modified_millis(&commit, 1_734_480_100_000);

        let reader = cdf_reader(
            CdfReadOptions::try_new(
                table.path().to_string_lossy().into_owned(),
                Some(vec![
                    "id".to_string(),
                    "_change_type".to_string(),
                    "_commit_version".to_string(),
                ]),
                0,
                0,
                64,
            )
            .unwrap(),
        )
        .unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(!batches.is_empty());
        for batch in batches {
            let change_type = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let commit_version = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(change_type.iter().flatten().all(|value| value == "insert"));
            assert!(commit_version.iter().flatten().all(|value| value == 0));
        }
    }

    #[test]
    fn bounded_cdf_range_with_no_files_returns_schema_and_no_rows() {
        let table = prepared_bounded_cdf_table();
        let version_one = table.path().join("_delta_log/00000000000000000001.json");
        let head = fs::read_to_string(&version_one)
            .unwrap()
            .lines()
            .take(2)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&version_one, format!("{head}\n")).unwrap();
        fs::write(
            table.path().join("_delta_log/00000000000000000002.json"),
            "",
        )
        .unwrap();
        set_modified_millis(&version_one, 1_734_480_105_872);

        let reader = cdf_reader(
            CdfReadOptions::try_new(
                table.path().to_string_lossy().into_owned(),
                Some(vec![
                    "_change_type".to_string(),
                    "_commit_version".to_string(),
                    "_commit_timestamp".to_string(),
                ]),
                1,
                2,
                64,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(reader.schema().fields().len(), 3);
        assert!(reader.collect::<Result<Vec<_>, _>>().unwrap().is_empty());
    }

    #[test]
    fn compact_options_reject_non_local_or_ambiguous_inputs() {
        for location in ["", "relative/table", "https://example.com/table"] {
            assert!(SnapshotReadOptions::try_new(location.to_string(), None, None, 1024).is_err());
        }
        assert!(SnapshotReadOptions::try_new(
            "/tmp/table".to_string(),
            Some(vec!["id".to_string(), "ID".to_string()]),
            None,
            1024,
        )
        .is_err());
        assert!(SnapshotReadOptions::try_new("/tmp/table".to_string(), None, None, 0).is_err());
    }

    #[test]
    fn real_snapshot_preserves_projection_and_exact_batch_limit() {
        let options = SnapshotReadOptions::try_new(
            fixture_table(),
            Some(vec!["group".to_string(), "id".to_string()]),
            Some(5),
            2,
        )
        .unwrap();
        let reader = snapshot_reader(options).unwrap();
        assert_eq!(reader.schema().fields().len(), 2);
        assert_eq!(reader.schema().field(0).name(), "group");
        assert_eq!(reader.schema().field(1).name(), "id");

        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let batch_rows = batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>();
        assert_eq!(batch_rows.iter().sum::<usize>(), 5);
        assert!(batch_rows.iter().all(|rows| *rows <= 2));
        assert!(batch_rows.len() >= 3);
        assert!(batches
            .iter()
            .all(|batch| batch.schema() == batches[0].schema()));
    }

    #[test]
    fn real_snapshot_supports_zero_and_one_batch_without_losing_schema() {
        let zero = SnapshotReadOptions::try_new(fixture_table(), None, Some(0), 2).unwrap();
        let zero = snapshot_reader(zero).unwrap();
        assert_eq!(zero.schema().fields().len(), 4);
        assert!(zero.collect::<Result<Vec<_>, _>>().unwrap().is_empty());

        let one = SnapshotReadOptions::try_new(fixture_table(), None, Some(1), 8).unwrap();
        let one = snapshot_reader(one)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].num_rows(), 1);
    }

    #[test]
    fn malformed_and_unsupported_tables_return_fixed_stage_messages() {
        let malformed = TestDirectory::new("malformed-url-secret");
        fs::write(
            malformed
                .path()
                .join("_delta_log")
                .join("00000000000000000000.json"),
            "not valid Delta JSON\n",
        )
        .unwrap();
        let error = snapshot_reader(
            SnapshotReadOptions::try_new(
                malformed.path().to_string_lossy().into_owned(),
                None,
                None,
                10,
            )
            .unwrap(),
        )
        .err()
        .expect("malformed table must fail");
        assert_eq!(error, "Delta Kernel snapshot preparation failed");
        assert!(!error.contains("url-secret"));

        let unsupported = TestDirectory::new("unsupported-url-secret");
        write_commit(unsupported.path(), None, 999);
        let error = snapshot_reader(
            SnapshotReadOptions::try_new(
                unsupported.path().to_string_lossy().into_owned(),
                None,
                None,
                10,
            )
            .unwrap(),
        )
        .err()
        .expect("unsupported protocol must fail");
        assert_eq!(error, "Delta Kernel scan planning failed");
        assert!(!error.contains("url-secret"));
    }

    #[test]
    fn loopback_presigned_action_is_read_by_the_kernel_default_engine() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let body = fixture_parquet().to_vec();
        let server = thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                match listener.accept() {
                    Ok((mut connection, _)) => {
                        connection.set_nonblocking(false).unwrap();
                        connection
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let mut request = vec![0_u8; 8192];
                        let bytes_read = connection.read(&mut request).unwrap();
                        let request = String::from_utf8_lossy(&request[..bytes_read]);
                        assert!(request.starts_with("GET "));
                        assert!(!request.starts_with("PROPFIND "));
                        assert!(request.contains("X-Amz-Signature=url-secret"));
                        write!(
                            connection,
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .unwrap();
                        connection.write_all(&body).unwrap();
                        return;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "Kernel did not request the loopback presigned action"
                        );
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("loopback server failed: {error}"),
                }
            }
        });

        let table = TestDirectory::new("presigned-success");
        let action = format!("http://{address}/part-00000.parquet?X-Amz-Signature=url-secret");
        write_commit(table.path(), Some(&action), 1);
        let reader = snapshot_reader(
            SnapshotReadOptions::try_new(
                table.path().to_string_lossy().into_owned(),
                None,
                None,
                2,
            )
            .unwrap(),
        )
        .unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        server.join().unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[test]
    fn downstream_presigned_errors_never_expose_the_url_or_query() {
        let table = TestDirectory::new("presigned-error");
        let secret = "super-secret-query-value";
        let action = format!("http://127.0.0.1:1/data.parquet?X-Amz-Signature={secret}");
        write_commit(table.path(), Some(&action), 1);
        let mut reader = snapshot_reader(
            SnapshotReadOptions::try_new(
                table.path().to_string_lossy().into_owned(),
                None,
                None,
                10,
            )
            .unwrap(),
        )
        .unwrap();

        let error = reader
            .next()
            .expect("failed presigned request must return one stream error")
            .unwrap_err()
            .to_string();
        assert_eq!(error, "Compute error: Delta Kernel data scan failed");
        assert!(!error.contains(secret));
        assert!(!error.contains("127.0.0.1"));
        assert!(reader.next().is_none());
    }
}
