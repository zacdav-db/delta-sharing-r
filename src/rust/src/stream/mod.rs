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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamMetricsSnapshot {
    pub(crate) active_streams: u64,
    pub(crate) cancelled_streams: u64,
    pub(crate) emitted_batches: u64,
}

impl StreamMetrics {
    fn snapshot(&self) -> StreamMetricsSnapshot {
        StreamMetricsSnapshot {
            active_streams: self.active_streams.load(Ordering::Acquire),
            cancelled_streams: self.cancelled_streams.load(Ordering::Acquire),
            emitted_batches: self.emitted_batches.load(Ordering::Acquire),
        }
    }
}

pub(crate) fn global_metrics_snapshot() -> StreamMetricsSnapshot {
    GLOBAL_METRICS.snapshot()
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow_array::ffi_stream::ArrowArrayStreamReader;

    use super::*;

    fn config(batches: i32, error_after: i32, panic_after: i32) -> FixtureStreamConfig {
        FixtureStreamConfig::try_from_raw(batches, 3, error_after, panic_after).unwrap()
    }

    fn isolated_stream(
        config: FixtureStreamConfig,
        metrics: Arc<StreamMetrics>,
    ) -> (FFI_ArrowArrayStream, Arc<StreamMetrics>) {
        let reader = FixtureReader::new(config);
        let owner = StreamOwner::new(metrics.clone());
        (export_reader(Box::new(reader), owner), metrics)
    }

    fn prepared_root(label: &str) -> (PathBuf, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            ".delta-sharing-snapshot-{label}-{}-{nanos}",
            std::process::id()
        ));
        populate_prepared_root(&root);
        let root = fs::canonicalize(root).unwrap();
        let table = root.join("table");
        (root, table)
    }

    fn populate_prepared_root(root: &Path) {
        let table = root.join("table");
        let log = table.join("_delta_log");
        fs::create_dir_all(&log).unwrap();
        fs::write(
            root.join(".delta-sharing-r-prepared-log"),
            "delta-sharing-r:vnext\n",
        )
        .unwrap();
        fs::write(log.join("00000000000000000000.json"), "{}\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    fn prepared_cdf_root(label: &str, start_version: u64, end_version: u64) -> (PathBuf, PathBuf) {
        let (root, table) = prepared_root(label);
        let log = table.join("_delta_log");
        fs::remove_file(log.join("00000000000000000000.json")).unwrap();
        if let Some(checkpoint_version) = start_version.checked_sub(1) {
            fs::write(
                log.join(format!("{checkpoint_version:020}.checkpoint.parquet")),
                "checkpoint",
            )
            .unwrap();
            fs::write(log.join("_last_checkpoint"), "{}\n").unwrap();
        }
        for version in start_version..=end_version {
            fs::write(log.join(format!("{version:020}.json")), "{}\n").unwrap();
        }
        (root, table)
    }

    #[test]
    fn empty_one_and_many_batches_round_trip() {
        for expected_batches in [0, 1, 4] {
            let metrics = Arc::new(StreamMetrics::default());
            let (stream, metrics) = isolated_stream(config(expected_batches, -1, -1), metrics);
            let reader = ArrowArrayStreamReader::try_new(stream).unwrap();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

            assert_eq!(batches.len(), expected_batches as usize);
            assert!(batches.iter().all(|batch| batch.num_rows() == 3));
            assert_eq!(
                metrics.snapshot(),
                StreamMetricsSnapshot {
                    active_streams: 0,
                    cancelled_streams: 1,
                    emitted_batches: expected_batches as u64,
                }
            );
        }
    }

    #[test]
    fn invalid_fixture_controls_are_rejected() {
        for arguments in [
            (-1, 1, -1, -1),
            (10_001, 1, -1, -1),
            (1, -1, -1, -1),
            (1, 1_000_001, -1, -1),
            (1, 1, -2, -1),
            (1, 1, -1, -2),
        ] {
            assert!(FixtureStreamConfig::try_from_raw(
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3
            )
            .is_err());
        }
    }

    #[test]
    fn error_after_one_batch_is_terminal_and_nul_safe() {
        let metrics = Arc::new(StreamMetrics::default());
        let (stream, _) = isolated_stream(config(3, 1, -1), metrics);
        let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 3);
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("synthetic reader error after 1 batches"));
        assert!(reader.next().is_none());
    }

    #[test]
    fn errors_and_panics_before_the_first_batch_are_contained() {
        let metrics = Arc::new(StreamMetrics::default());
        let (error_stream, _) = isolated_stream(config(3, 0, -1), metrics.clone());
        let mut error_reader = ArrowArrayStreamReader::try_new(error_stream).unwrap();
        let error = error_reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("synthetic reader error after 0 batches"));
        assert!(error_reader.next().is_none());

        let (panic_stream, _) = isolated_stream(config(3, -1, 0), metrics);
        let mut panic_reader = ArrowArrayStreamReader::try_new(panic_stream).unwrap();
        let error = panic_reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("panic contained at Arrow stream boundary"));
        assert!(!error.contains("synthetic reader panic after 0 batches"));
        assert!(panic_reader.next().is_none());
    }

    #[test]
    fn reader_panic_becomes_stream_error() {
        let metrics = Arc::new(StreamMetrics::default());
        let (stream, _) = isolated_stream(config(3, -1, 1), metrics);
        let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();

        assert!(reader.next().unwrap().is_ok());
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("panic contained at Arrow stream boundary"));
        assert!(!error.contains("synthetic reader panic after 1 batches"));
    }

    #[test]
    fn early_release_cancels_and_drops_owned_resources() {
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let metrics = Arc::new(StreamMetrics::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let reader = FixtureReader::new(config(4, -1, -1));
        let mut owner = StreamOwner::new(metrics.clone());
        owner.keep_alive(DropProbe(drops.clone()));
        let mut stream = export_reader(Box::new(reader), owner);

        let get_next = stream.get_next.unwrap();
        let mut array = arrow_array::ffi::FFI_ArrowArray::empty();
        assert_eq!(unsafe { get_next(&mut stream, &mut array) }, 0);
        drop(array);

        let release = stream.release.unwrap();
        unsafe { release(&mut stream) };
        assert!(stream.release.is_none());
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(
            metrics.snapshot(),
            StreamMetricsSnapshot {
                active_streams: 0,
                cancelled_streams: 1,
                emitted_batches: 1,
            }
        );
    }

    #[test]
    fn emitted_array_buffers_outlive_stream_release() {
        let metrics = Arc::new(StreamMetrics::default());
        let (mut stream, _) = isolated_stream(config(2, -1, -1), metrics);

        let mut schema = arrow_schema::ffi::FFI_ArrowSchema::empty();
        let get_schema = stream.get_schema.unwrap();
        assert_eq!(unsafe { get_schema(&mut stream, &mut schema) }, 0);

        let mut array = arrow_array::ffi::FFI_ArrowArray::empty();
        let get_next = stream.get_next.unwrap();
        assert_eq!(unsafe { get_next(&mut stream, &mut array) }, 0);

        let release = stream.release.unwrap();
        unsafe { release(&mut stream) };

        let data_type = DataType::Struct(fixture_schema().fields().clone());
        let data = unsafe { arrow_array::ffi::from_ffi_and_data_type(array, data_type) }.unwrap();
        assert_eq!(data.len(), 3);
    }

    #[test]
    fn populate_rejects_initialized_destination_and_contains_panics() {
        let metrics = Arc::new(StreamMetrics::default());
        let (mut initialized, _) = isolated_stream(config(1, -1, -1), metrics);
        let destination = NonNull::from(&mut initialized);

        assert!(populate_stream(destination, || unreachable!())
            .unwrap_err()
            .contains("already initialized"));

        let mut empty = FFI_ArrowArrayStream::empty();
        let destination = NonNull::from(&mut empty);
        let error = populate_stream(destination, || {
            panic!("constructor panic X-Amz-Signature=super-secret")
        })
        .unwrap_err();
        assert!(error.contains("panic contained while creating Arrow stream"));
        assert!(!error.contains("super-secret"));
        assert!(empty.release.is_none());
    }

    #[test]
    fn prepared_log_cleanup_requires_exact_private_capability_shape() {
        let (root, table) = prepared_root("valid");
        let cleanup =
            PreparedLogCleanup::try_new(root.to_str().unwrap(), table.to_str().unwrap()).unwrap();
        drop(cleanup);
        assert!(!root.exists());

        let (root, table) = prepared_root("tampered");
        fs::write(root.join("unexpected"), "not package owned").unwrap();
        assert!(
            PreparedLogCleanup::try_new(root.to_str().unwrap(), table.to_str().unwrap()).is_err()
        );
        assert!(root.exists());
        fs::remove_dir_all(root).unwrap();

        let (root, _table) = prepared_root("mismatch");
        let unrelated =
            std::env::temp_dir().join(format!("delta-sharing-r-unrelated-{}", std::process::id()));
        fs::create_dir_all(&unrelated).unwrap();
        assert!(
            PreparedLogCleanup::try_new(root.to_str().unwrap(), unrelated.to_str().unwrap())
                .is_err()
        );
        assert!(root.exists());
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(unrelated).unwrap();
    }

    #[test]
    fn prepared_cdf_cleanup_requires_the_exact_bounded_log_shape() {
        let (root, table) = prepared_cdf_root("cdf-valid", 1, 2);
        let cleanup =
            PreparedLogCleanup::try_new_cdf(root.to_str().unwrap(), table.to_str().unwrap(), 1, 2)
                .unwrap();
        drop(cleanup);
        assert!(!root.exists());

        let (root, table) = prepared_cdf_root("cdf-extra", 1, 2);
        fs::write(
            table.join("_delta_log").join("00000000000000000003.json"),
            "{}\n",
        )
        .unwrap();
        assert!(PreparedLogCleanup::try_new_cdf(
            root.to_str().unwrap(),
            table.to_str().unwrap(),
            1,
            2,
        )
        .is_err());
        assert!(root.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepared_cdf_cleanup_accepts_start_zero_without_a_bootstrap_checkpoint() {
        let (root, table) = prepared_cdf_root("cdf-zero", 0, 2);
        let cleanup =
            PreparedLogCleanup::try_new_cdf(root.to_str().unwrap(), table.to_str().unwrap(), 0, 2)
                .unwrap();
        drop(cleanup);
        assert!(!root.exists());
    }

    #[test]
    fn prepared_cdf_cleanup_does_not_rescan_the_remaining_range_per_file() {
        const END_VERSION: u64 = 2_047;

        let (root, table) = prepared_cdf_root("cdf-linear", 0, END_VERSION);
        let cleanup = PreparedLogCleanup::try_new_cdf(
            root.to_str().unwrap(),
            table.to_str().unwrap(),
            0,
            END_VERSION,
        )
        .unwrap();
        let full_log_shape_checks = cleanup.full_log_shape_checks_controller();
        drop(cleanup);

        assert!(!root.exists());
        assert_eq!(
            full_log_shape_checks.load(std::sync::atomic::Ordering::Acquire),
            1,
            "cleanup must enumerate the whole log only once after deleting owned entries"
        );
    }

    #[test]
    fn prepared_log_cleanup_accepts_equivalent_noncanonical_spelling() {
        let (root, _) = prepared_root("normalized-spelling");
        let alias_root = root
            .parent()
            .unwrap()
            .join(".")
            .join(root.file_name().unwrap());
        let alias_table = alias_root.join("table");
        let cleanup = PreparedLogCleanup::try_new(
            alias_root.to_str().unwrap(),
            alias_table.to_str().unwrap(),
        )
        .unwrap();

        drop(cleanup);
        assert!(!root.exists());
    }

    #[test]
    fn prepared_log_cleanup_revalidates_mutations_before_removal() {
        let (root, table) = prepared_root("mutated-after-handoff");
        let cleanup =
            PreparedLogCleanup::try_new(root.to_str().unwrap(), table.to_str().unwrap()).unwrap();
        let unexpected = root.join("unexpected-user-content");
        fs::write(&unexpected, "must survive fail-closed cleanup").unwrap();

        drop(cleanup);
        assert!(root.exists());
        assert!(unexpected.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pending_cleanup_reaper_recovers_after_bounded_removal_failures() {
        let (root, table) = prepared_root("retry");
        let cleanup =
            PreparedLogCleanup::try_new(root.to_str().unwrap(), table.to_str().unwrap()).unwrap();
        let failures = cleanup.injected_failure_controller();
        cleanup.inject_removal_failures(1_000_000);

        drop(cleanup);
        assert!(root.exists());

        failures.store(0, std::sync::atomic::Ordering::Release);
        reap_pending_cleanups();
        assert!(!root.exists());
    }

    #[cfg(unix)]
    #[test]
    fn prepared_log_cleanup_rejects_a_replaced_root_identity() {
        let (root, table) = prepared_root("replaced-after-handoff");
        let cleanup =
            PreparedLogCleanup::try_new(root.to_str().unwrap(), table.to_str().unwrap()).unwrap();
        let original = root.with_file_name(format!(
            "{}-original",
            root.file_name().unwrap().to_string_lossy()
        ));
        fs::rename(&root, &original).unwrap();
        populate_prepared_root(&root);

        drop(cleanup);
        assert!(root.exists(), "replacement root must not be deleted");
        assert!(original.exists(), "original root must not be followed");
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(original).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn prepared_log_cleanup_rejects_symlinked_shape_without_following_it() {
        use std::os::unix::fs::symlink;

        let (root, table) = prepared_root("symlink");
        let marker = root.join(".delta-sharing-r-prepared-log");
        let target = root.join("marker-target");
        fs::remove_file(&marker).unwrap();
        fs::write(&target, "delta-sharing-r:vnext\n").unwrap();
        symlink(&target, &marker).unwrap();

        assert!(
            PreparedLogCleanup::try_new(root.to_str().unwrap(), table.to_str().unwrap()).is_err()
        );
        assert!(root.exists());
        assert!(target.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn terminalization_drops_the_reader_before_owned_cleanup_resources() {
        struct ReaderDropProbe(Arc<AtomicBool>);
        impl Iterator for ReaderDropProbe {
            type Item = Result<RecordBatch, ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                None
            }
        }
        impl RecordBatchReader for ReaderDropProbe {
            fn schema(&self) -> SchemaRef {
                fixture_schema()
            }
        }
        impl Drop for ReaderDropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        struct CleanupOrderProbe {
            reader_dropped: Arc<AtomicBool>,
            cleanup_dropped: Arc<AtomicBool>,
        }
        impl Drop for CleanupOrderProbe {
            fn drop(&mut self) {
                assert!(
                    self.reader_dropped.load(Ordering::Acquire),
                    "reader must drop before cleanup resources"
                );
                self.cleanup_dropped.store(true, Ordering::Release);
            }
        }

        let reader_dropped = Arc::new(AtomicBool::new(false));
        let cleanup_dropped = Arc::new(AtomicBool::new(false));
        let stream = record_batch_stream_with_resource(
            Box::new(ReaderDropProbe(reader_dropped.clone())),
            CleanupOrderProbe {
                reader_dropped: reader_dropped.clone(),
                cleanup_dropped: cleanup_dropped.clone(),
            },
        );
        let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        assert!(reader.next().is_none());
        assert!(reader_dropped.load(Ordering::Acquire));
        assert!(cleanup_dropped.load(Ordering::Acquire));
    }
}
