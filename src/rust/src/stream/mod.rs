//! Arrow C Stream export and lifecycle ownership.
//!
//! nanoarrow owns the outer `FFI_ArrowArrayStream` allocation. Rust moves an
//! initialized stream into that allocation. The release callback drops the
//! reader, cancellation state, and all resources tied to the active stream.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{
    ArrayRef, Decimal128Array, Int32Array, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use same_file::Handle;

static GLOBAL_METRICS: LazyLock<Arc<StreamMetrics>> =
    LazyLock::new(|| Arc::new(StreamMetrics::default()));
static PENDING_CLEANUPS: LazyLock<Mutex<Vec<PendingCleanup>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

#[derive(Debug, Default)]
struct StreamMetrics {
    active_streams: AtomicU64,
    cancelled_streams: AtomicU64,
    emitted_batches: AtomicU64,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamMetricsSnapshot {
    pub(crate) active_streams: u64,
    pub(crate) cancelled_streams: u64,
    pub(crate) emitted_batches: u64,
}

impl StreamMetrics {
    #[cfg(test)]
    fn snapshot(&self) -> StreamMetricsSnapshot {
        StreamMetricsSnapshot {
            active_streams: self.active_streams.load(Ordering::Acquire),
            cancelled_streams: self.cancelled_streams.load(Ordering::Acquire),
            emitted_batches: self.emitted_batches.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Resources whose lifetime must exactly match the stream lifetime.
pub(crate) struct StreamOwner {
    cancellation: CancellationToken,
    metrics: Arc<StreamMetrics>,
    _resources: Vec<Box<dyn Any + Send>>,
    released: bool,
}

impl StreamOwner {
    fn new(metrics: Arc<StreamMetrics>) -> Self {
        metrics.active_streams.fetch_add(1, Ordering::AcqRel);
        Self {
            cancellation: CancellationToken::default(),
            metrics,
            _resources: Vec::new(),
            released: false,
        }
    }

    fn keep_alive<T: Any + Send>(&mut self, resource: T) {
        self._resources.push(Box::new(resource));
    }

    fn release(&mut self) {
        if self.released {
            return;
        }

        self.cancellation.cancel();
        self.metrics
            .cancelled_streams
            .fetch_add(1, Ordering::AcqRel);
        self.metrics.active_streams.fetch_sub(1, Ordering::AcqRel);
        self.released = true;
    }

    fn finish(&mut self) {
        self.release();
        self._resources.clear();
    }
}

impl Drop for StreamOwner {
    fn drop(&mut self) {
        // Cancellation happens before retained resources are dropped.
        self.release();
    }
}

/// Prevent a reader panic from unwinding through Arrow's `extern "C"` callback.
struct PanicBoundaryReader {
    schema: SchemaRef,
    inner: Option<Box<dyn RecordBatchReader + Send>>,
    owner: StreamOwner,
    terminal: bool,
}

impl PanicBoundaryReader {
    fn new(inner: Box<dyn RecordBatchReader + Send>, owner: StreamOwner) -> Self {
        let schema = inner.schema();
        Self {
            schema,
            inner: Some(inner),
            owner,
            terminal: false,
        }
    }

    fn finish(&mut self) {
        self.terminal = true;
        // Kernel scan/source/engine resources must be dropped before a
        // prepared-log cleanup capability can remove the local log.
        drop(self.inner.take());
        self.owner.finish();
    }
}

impl Iterator for PanicBoundaryReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }

        if self.owner.cancellation.is_cancelled() {
            self.terminal = true;
            return Some(Err(ArrowError::ComputeError(
                "delta-sharing stream cancelled".to_string(),
            )));
        }

        match catch_unwind(AssertUnwindSafe(|| {
            self.inner.as_mut().and_then(|reader| reader.next())
        })) {
            Ok(Some(Ok(batch))) => {
                self.owner
                    .metrics
                    .emitted_batches
                    .fetch_add(1, Ordering::AcqRel);
                Some(Ok(batch))
            }
            Ok(Some(Err(error))) => {
                self.finish();
                let message = error.to_string();
                if message.contains('\0') {
                    Some(Err(ArrowError::ComputeError(message.replace('\0', "\\0"))))
                } else {
                    Some(Err(error))
                }
            }
            Ok(None) => {
                self.finish();
                None
            }
            Err(_) => {
                self.finish();
                Some(Err(ArrowError::ComputeError(
                    "panic contained at Arrow stream boundary".to_string(),
                )))
            }
        }
    }
}

impl RecordBatchReader for PanicBoundaryReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for PanicBoundaryReader {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.owner.release();
    }
}

fn export_reader(
    reader: Box<dyn RecordBatchReader + Send>,
    owner: StreamOwner,
) -> FFI_ArrowArrayStream {
    FFI_ArrowArrayStream::new(Box::new(PanicBoundaryReader::new(reader, owner)))
}

pub(crate) fn record_batch_stream(
    reader: Box<dyn RecordBatchReader + Send>,
) -> FFI_ArrowArrayStream {
    export_reader(reader, StreamOwner::new(GLOBAL_METRICS.clone()))
}

pub(crate) fn record_batch_stream_with_resource<T: Any + Send>(
    reader: Box<dyn RecordBatchReader + Send>,
    resource: T,
) -> FFI_ArrowArrayStream {
    let mut owner = StreamOwner::new(GLOBAL_METRICS.clone());
    owner.keep_alive(resource);
    export_reader(reader, owner)
}

/// Cleanup token for an R-prepared synthetic log.
///
/// Construction proves that the supplied table is exactly the `table` child
/// of a private `.delta-sharing-snapshot-*` directory. The token performs no
/// synthetic-log interpretation; it only couples cleanup to native stream
/// release after R transfers ownership.
pub(crate) struct PreparedLogCleanup {
    root: PathBuf,
    identity: FileIdentity,
    log_entries: Vec<String>,
    #[cfg(test)]
    full_log_shape_checks: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    injected_failures: Arc<std::sync::atomic::AtomicUsize>,
}

impl PreparedLogCleanup {
    pub(crate) fn try_new(root: &str, table_location: &str) -> Result<Self, String> {
        Self::try_new_with_log_entries(
            root,
            table_location,
            vec!["00000000000000000000.json".to_string()],
        )
    }

    pub(crate) fn try_new_cdf(
        root: &str,
        table_location: &str,
        start_version: u64,
        end_version: u64,
    ) -> Result<Self, String> {
        const MAX_CDF_VERSIONS: u64 = 1_000_000;

        let version_count = end_version
            .checked_sub(start_version)
            .and_then(|difference| difference.checked_add(1))
            .filter(|count| *count <= MAX_CDF_VERSIONS)
            .ok_or_else(|| "prepared CDF cleanup range is invalid".to_string())?;
        let bootstrap_entries = if start_version > 0 { 2 } else { 0 };
        let mut log_entries = Vec::with_capacity(version_count as usize + bootstrap_entries);
        if let Some(checkpoint_version) = start_version.checked_sub(1) {
            log_entries.push(format!("{checkpoint_version:020}.checkpoint.parquet"));
            log_entries.push("_last_checkpoint".to_string());
        }
        for version in start_version..=end_version {
            log_entries.push(format!("{version:020}.json"));
        }
        Self::try_new_with_log_entries(root, table_location, log_entries)
    }

    fn try_new_with_log_entries(
        root: &str,
        table_location: &str,
        mut log_entries: Vec<String>,
    ) -> Result<Self, String> {
        let root_path = Path::new(root);
        let table_path = Path::new(table_location);
        if !root_path.is_absolute() || !table_path.is_absolute() {
            return Err("prepared-log cleanup paths must be absolute".to_string());
        }

        log_entries.sort();
        let canonical_root = validate_prepared_root(root_path, table_path, &log_entries)?;
        let identity = file_identity(&canonical_root)?;
        Ok(Self {
            root: canonical_root,
            identity,
            log_entries,
            #[cfg(test)]
            full_log_shape_checks: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            injected_failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    fn pending_cleanup(&self) -> PendingCleanup {
        PendingCleanup {
            root: self.root.clone(),
            identity: self.identity.clone(),
            log_entries: self.log_entries.clone(),
            next_log_entry: 0,
            stage: CleanupStage::LogEntries,
            #[cfg(test)]
            full_log_shape_checks: self.full_log_shape_checks.clone(),
            #[cfg(test)]
            injected_failures: self.injected_failures.clone(),
        }
    }

    #[cfg(test)]
    fn inject_removal_failures(&self, failures: usize) {
        self.injected_failures
            .store(failures, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    fn injected_failure_controller(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.injected_failures.clone()
    }

    #[cfg(test)]
    fn full_log_shape_checks_controller(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.full_log_shape_checks.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity(Vec<u8>);

#[derive(Default)]
struct IdentityCollector {
    bytes: Vec<u8>,
}

impl Hasher for IdentityCollector {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.bytes
            .extend_from_slice(&(bytes.len() as u64).to_ne_bytes());
        self.bytes.extend_from_slice(bytes);
    }
}

fn file_identity(path: &Path) -> Result<FileIdentity, String> {
    let handle = Handle::from_path(path)
        .map_err(|_| "prepared-log cleanup identity is unavailable".to_string())?;
    let mut collector = IdentityCollector::default();
    handle.hash(&mut collector);
    Ok(FileIdentity(collector.bytes))
}

fn validate_prepared_root(
    root_path: &Path,
    table_path: &Path,
    expected_log_entries: &[String],
) -> Result<PathBuf, String> {
    let root_metadata = require_plain_directory(
        root_path,
        "prepared-log cleanup root is not a private directory",
    )?;
    let safe_name = root_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(".delta-sharing-snapshot-"));
    if !safe_name {
        return Err("prepared-log cleanup root has an invalid name".to_string());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if root_metadata.permissions().mode() & 0o077 != 0 {
            return Err("prepared-log cleanup root is not private".to_string());
        }
    }

    let canonical_root = std::fs::canonicalize(root_path)
        .map_err(|_| "prepared-log cleanup root is unavailable".to_string())?;

    require_exact_entries(&canonical_root, &[".delta-sharing-r-prepared-log", "table"])?;
    let marker = canonical_root.join(".delta-sharing-r-prepared-log");
    require_plain_file(&marker, "prepared-log ownership marker is invalid")?;
    let marker_value = std::fs::read_to_string(&marker)
        .map_err(|_| "prepared-log ownership marker is invalid".to_string())?;
    if marker_value != "delta-sharing-r:vnext\n" {
        return Err("prepared-log ownership marker is invalid".to_string());
    }

    let owned_table = canonical_root.join("table");
    require_plain_directory(&owned_table, "prepared local table is invalid")?;
    require_exact_entries(&owned_table, &["_delta_log"])?;
    let log_directory = owned_table.join("_delta_log");
    require_plain_directory(&log_directory, "prepared local table log is invalid")?;
    require_exact_entry_names(&log_directory, expected_log_entries)?;
    for entry in expected_log_entries {
        require_plain_file(
            &log_directory.join(entry),
            "prepared local table log entry is invalid",
        )?;
    }

    let canonical_table = std::fs::canonicalize(table_path)
        .map_err(|_| "prepared local table is unavailable".to_string())?;
    let expected_table = std::fs::canonicalize(owned_table)
        .map_err(|_| "prepared local table is unavailable".to_string())?;
    if canonical_table != expected_table {
        return Err("prepared-log cleanup root does not own the local table".to_string());
    }

    Ok(canonical_root)
}

fn require_plain_directory(path: &Path, message: &str) -> Result<std::fs::Metadata, String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| message.to_string())?;
    if !metadata.is_dir() || metadata_is_link_like(&metadata) {
        return Err(message.to_string());
    }
    Ok(metadata)
}

fn require_plain_file(path: &Path, message: &str) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|_| message.to_string())?;
    if !metadata.is_file() || metadata_is_link_like(&metadata) {
        return Err(message.to_string());
    }
    Ok(())
}

fn metadata_is_link_like(metadata: &std::fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return true;
        }
    }
    false
}

fn require_exact_entries(path: &Path, expected: &[&str]) -> Result<(), String> {
    let expected = expected
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    require_exact_entry_names(path, &expected)
}

fn require_exact_entry_names(path: &Path, expected: &[String]) -> Result<(), String> {
    let mut actual = std::fs::read_dir(path)
        .map_err(|_| "prepared-log directory shape is invalid".to_string())?
        .map(|entry| {
            entry
                .map_err(|_| "prepared-log directory shape is invalid".to_string())?
                .file_name()
                .into_string()
                .map_err(|_| "prepared-log directory shape is invalid".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    actual.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    if actual != expected {
        return Err("prepared-log directory shape is invalid".to_string());
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupStage {
    LogEntries,
    LogDirectory,
    Table,
    Marker,
    Root,
}

impl CleanupStage {
    fn next(self) -> Option<Self> {
        match self {
            Self::LogEntries => Some(Self::LogDirectory),
            Self::LogDirectory => Some(Self::Table),
            Self::Table => Some(Self::Marker),
            Self::Marker => Some(Self::Root),
            Self::Root => None,
        }
    }
}

struct PendingCleanup {
    root: PathBuf,
    identity: FileIdentity,
    log_entries: Vec<String>,
    next_log_entry: usize,
    stage: CleanupStage,
    #[cfg(test)]
    full_log_shape_checks: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    injected_failures: Arc<std::sync::atomic::AtomicUsize>,
}

enum CleanupOutcome {
    Complete,
    Retry(PendingCleanup),
    Abandon,
}

impl PendingCleanup {
    fn run(mut self) -> CleanupOutcome {
        loop {
            let mut removed = false;
            for _ in 0..3 {
                if !self.is_valid_for_stage() {
                    return CleanupOutcome::Abandon;
                }
                if self.remove_current_target().is_ok() {
                    removed = true;
                    break;
                }
            }
            if !removed {
                return CleanupOutcome::Retry(self);
            }

            if self.stage == CleanupStage::LogEntries {
                self.next_log_entry += 1;
                if self.next_log_entry < self.log_entries.len() {
                    continue;
                }
            }
            match self.stage.next() {
                Some(next) => self.stage = next,
                None => return CleanupOutcome::Complete,
            }
        }
    }

    fn is_valid_for_stage(&self) -> bool {
        let root_metadata = match require_plain_directory(
            &self.root,
            "prepared-log cleanup root is not a private directory",
        ) {
            Ok(metadata) => metadata,
            Err(_) => return false,
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if root_metadata.permissions().mode() & 0o077 != 0 {
                return false;
            }
        }
        #[cfg(not(unix))]
        let _ = root_metadata;
        if file_identity(&self.root).ok().as_ref() != Some(&self.identity) {
            return false;
        }
        if std::fs::canonicalize(&self.root).ok().as_ref() != Some(&self.root) {
            return false;
        }

        let marker = self.root.join(".delta-sharing-r-prepared-log");
        let table = self.root.join("table");
        let log = table.join("_delta_log");
        let marker_is_valid = || {
            require_plain_file(&marker, "invalid").is_ok()
                && std::fs::read_to_string(&marker).ok().as_deref()
                    == Some("delta-sharing-r:vnext\n")
        };

        match self.stage {
            CleanupStage::LogEntries => {
                let Some(current) = self.log_entries.get(self.next_log_entry) else {
                    return false;
                };
                require_exact_entries(&self.root, &[".delta-sharing-r-prepared-log", "table"])
                    .is_ok()
                    && marker_is_valid()
                    && require_plain_directory(&table, "invalid").is_ok()
                    && require_exact_entries(&table, &["_delta_log"]).is_ok()
                    && require_plain_directory(&log, "invalid").is_ok()
                    && require_plain_file(&log.join(current), "invalid").is_ok()
            }
            CleanupStage::LogDirectory => {
                #[cfg(test)]
                self.full_log_shape_checks
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                require_exact_entries(&self.root, &[".delta-sharing-r-prepared-log", "table"])
                    .is_ok()
                    && marker_is_valid()
                    && require_plain_directory(&table, "invalid").is_ok()
                    && require_exact_entries(&table, &["_delta_log"]).is_ok()
                    && require_plain_directory(&log, "invalid").is_ok()
                    && require_exact_entries(&log, &[]).is_ok()
            }
            CleanupStage::Table => {
                require_exact_entries(&self.root, &[".delta-sharing-r-prepared-log", "table"])
                    .is_ok()
                    && marker_is_valid()
                    && require_plain_directory(&table, "invalid").is_ok()
                    && require_exact_entries(&table, &[]).is_ok()
            }
            CleanupStage::Marker => {
                require_exact_entries(&self.root, &[".delta-sharing-r-prepared-log"]).is_ok()
                    && marker_is_valid()
            }
            CleanupStage::Root => require_exact_entries(&self.root, &[]).is_ok(),
        }
    }

    fn remove_current_target(&self) -> std::io::Result<()> {
        #[cfg(test)]
        if self
            .injected_failures
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected cleanup failure",
            ));
        }

        let table = self.root.join("table");
        let log = table.join("_delta_log");
        match self.stage {
            CleanupStage::LogEntries => {
                std::fs::remove_file(log.join(&self.log_entries[self.next_log_entry]))
            }
            CleanupStage::LogDirectory => std::fs::remove_dir(log),
            CleanupStage::Table => std::fs::remove_dir(table),
            CleanupStage::Marker => {
                std::fs::remove_file(self.root.join(".delta-sharing-r-prepared-log"))
            }
            CleanupStage::Root => std::fs::remove_dir(&self.root),
        }
    }
}

fn enqueue_pending_cleanup(cleanup: PendingCleanup) {
    if let Ok(mut pending) = PENDING_CLEANUPS.lock() {
        pending.push(cleanup);
    }
}

pub(crate) fn reap_pending_cleanups() {
    let pending = match PENDING_CLEANUPS.lock() {
        Ok(mut pending) => std::mem::take(&mut *pending),
        Err(_) => return,
    };
    let mut retry = Vec::new();
    for cleanup in pending {
        if let CleanupOutcome::Retry(cleanup) = cleanup.run() {
            retry.push(cleanup);
        }
    }
    if retry.is_empty() {
        return;
    }
    if let Ok(mut pending) = PENDING_CLEANUPS.lock() {
        pending.extend(retry);
    }
}

pub(crate) fn pending_cleanup_count() -> u64 {
    PENDING_CLEANUPS
        .lock()
        .map_or(0, |pending| pending.len() as u64)
}

impl Drop for PreparedLogCleanup {
    fn drop(&mut self) {
        match self.pending_cleanup().run() {
            CleanupOutcome::Retry(cleanup) => enqueue_pending_cleanup(cleanup),
            CleanupOutcome::Complete | CleanupOutcome::Abandon => {}
        }
    }
}

/// Populate a nanoarrow-owned stream shell exactly once.
pub(crate) fn populate_stream<F>(
    destination: NonNull<FFI_ArrowArrayStream>,
    make_stream: F,
) -> Result<(), String>
where
    F: FnOnce() -> Result<FFI_ArrowArrayStream, String>,
{
    // nanoarrow initializes `release` to NULL for an output stream shell.
    // SAFETY: the C shim validated that this pointer came from a nanoarrow
    // external pointer and remains alive for the duration of this call.
    if unsafe { destination.as_ref().release.is_some() } {
        return Err("nanoarrow stream output is already initialized".to_string());
    }

    let stream = catch_unwind(AssertUnwindSafe(make_stream))
        .map_err(|_| "panic contained while creating Arrow stream".to_string())??;

    // SAFETY: destination is aligned, non-null, and owned by nanoarrow. Only
    // its NULL release slot has been initialized; this moves in the stream.
    unsafe { destination.as_ptr().write(stream) };
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FixtureStreamConfig {
    batches: usize,
    rows_per_batch: usize,
    error_after: Option<usize>,
    panic_after: Option<usize>,
}

impl FixtureStreamConfig {
    pub(crate) fn try_from_raw(
        batches: i32,
        rows_per_batch: i32,
        error_after: i32,
        panic_after: i32,
    ) -> Result<Self, String> {
        if !(0..=10_000).contains(&batches) {
            return Err("`batches` must be between 0 and 10000".to_string());
        }
        if !(0..=1_000_000).contains(&rows_per_batch) {
            return Err("`rows_per_batch` must be between 0 and 1000000".to_string());
        }
        if error_after < -1 || panic_after < -1 {
            return Err("error/panic batch positions must be -1 or non-negative".to_string());
        }

        Ok(Self {
            batches: batches as usize,
            rows_per_batch: rows_per_batch as usize,
            error_after: (error_after >= 0).then_some(error_after as usize),
            panic_after: (panic_after >= 0).then_some(panic_after as usize),
        })
    }
}

pub(crate) fn fixture_stream(config: FixtureStreamConfig) -> Result<FFI_ArrowArrayStream, String> {
    let reader = FixtureReader::new(config);
    Ok(record_batch_stream(Box::new(reader)))
}

struct FixtureReader {
    schema: SchemaRef,
    config: FixtureStreamConfig,
    next_batch: usize,
}

impl FixtureReader {
    fn new(config: FixtureStreamConfig) -> Self {
        Self {
            schema: fixture_schema(),
            config,
            next_batch: 0,
        }
    }
}

impl Iterator for FixtureReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch_index = self.next_batch;

        if self.config.panic_after == Some(batch_index) {
            std::panic::resume_unwind(Box::new(format!(
                "synthetic reader panic after {batch_index} batches"
            )));
        }
        if self.config.error_after == Some(batch_index) {
            return Some(Err(ArrowError::ComputeError(format!(
                "synthetic reader error after {batch_index} batches"
            ))));
        }
        if batch_index >= self.config.batches {
            return None;
        }

        self.next_batch += 1;
        Some(make_fixture_batch(
            self.schema.clone(),
            batch_index,
            self.config.rows_per_batch,
        ))
    }
}

impl RecordBatchReader for FixtureReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("batch_index", DataType::Int32, false),
        Field::new("row_index", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(20, 4), false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ),
    ]))
}

fn make_fixture_batch(
    schema: SchemaRef,
    batch_index: usize,
    rows: usize,
) -> Result<RecordBatch, ArrowError> {
    let base = (batch_index as i64) * (rows as i64);
    let batch_indices = Int32Array::from(vec![batch_index as i32; rows]);
    let row_indices = Int64Array::from_iter_values(base..base + rows as i64);
    let labels = StringArray::from_iter_values(
        (0..rows).map(|row| format!("batch-{batch_index}-row-{row}")),
    );
    let amounts = Decimal128Array::from_iter_values(
        (0..rows).map(|row| (base as i128 + row as i128) * 10_000),
    )
    .with_precision_and_scale(20, 4)?;
    let event_times = TimestampMicrosecondArray::from_iter_values(
        (0..rows).map(|row| 1_700_000_000_000_000_i64 + (base + row as i64) * 1_000),
    )
    .with_timezone("UTC");

    let mut values_builder = ListBuilder::new(Int32Builder::new());
    for row in 0..rows {
        values_builder.values().append_value(row as i32);
        values_builder
            .values()
            .append_value(row.saturating_add(1) as i32);
        values_builder.append(true);
    }
    let values = values_builder.finish();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(batch_indices) as ArrayRef,
            Arc::new(row_indices) as ArrayRef,
            Arc::new(labels) as ArrayRef,
            Arc::new(amounts) as ArrayRef,
            Arc::new(event_times) as ArrayRef,
            Arc::new(values) as ArrayRef,
        ],
    )
}
