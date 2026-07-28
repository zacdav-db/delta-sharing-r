//! Arrow C Stream export and lifecycle ownership.
//!
//! nanoarrow owns the outer `FFI_ArrowArrayStream` allocation. Rust moves an
//! initialized stream into that allocation. The release callback drops the
//! reader, cancellation state, and all resources tied to the active stream.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{
    ArrayRef, Decimal128Array, Int32Array, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};

static GLOBAL_METRICS: LazyLock<Arc<StreamMetrics>> =
    LazyLock::new(|| Arc::new(StreamMetrics::default()));

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

    #[cfg(test)]
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
    inner: Box<dyn RecordBatchReader + Send>,
    owner: StreamOwner,
    terminal: bool,
}

impl PanicBoundaryReader {
    fn new(inner: Box<dyn RecordBatchReader + Send>, owner: StreamOwner) -> Self {
        let schema = inner.schema();
        Self {
            schema,
            inner,
            owner,
            terminal: false,
        }
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

        match catch_unwind(AssertUnwindSafe(|| self.inner.next())) {
            Ok(Some(Ok(batch))) => {
                self.owner
                    .metrics
                    .emitted_batches
                    .fetch_add(1, Ordering::AcqRel);
                Some(Ok(batch))
            }
            Ok(Some(Err(error))) => {
                self.terminal = true;
                let message = error.to_string();
                if message.contains('\0') {
                    Some(Err(ArrowError::ComputeError(message.replace('\0', "\\0"))))
                } else {
                    Some(Err(error))
                }
            }
            Ok(None) => {
                self.terminal = true;
                None
            }
            Err(payload) => {
                self.terminal = true;
                Some(Err(ArrowError::ComputeError(format!(
                    "panic contained at Arrow stream boundary: {}",
                    panic_message(payload.as_ref())
                ))))
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
        self.owner.release();
    }
}

pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    let message = if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string Rust panic".to_string()
    };

    // Arrow stores callback errors in a CString.
    message.replace('\0', "\\0")
}

fn export_reader(
    reader: Box<dyn RecordBatchReader + Send>,
    owner: StreamOwner,
) -> FFI_ArrowArrayStream {
    FFI_ArrowArrayStream::new(Box::new(PanicBoundaryReader::new(reader, owner)))
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

    let stream = catch_unwind(AssertUnwindSafe(make_stream)).map_err(|payload| {
        format!(
            "panic contained while creating Arrow stream: {}",
            panic_message(payload.as_ref())
        )
    })??;

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
    let owner = StreamOwner::new(GLOBAL_METRICS.clone());
    Ok(export_reader(Box::new(reader), owner))
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
    use std::sync::atomic::AtomicUsize;

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

        let message = panic_message(&String::from("embedded\0nul"));
        assert_eq!(message, "embedded\\0nul");
        assert_eq!(panic_message(&123_i32), "non-string Rust panic");
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
        assert!(error.contains("synthetic reader panic after 0 batches"));
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
        assert!(error.contains("synthetic reader panic after 1 batches"));
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
        let error = populate_stream(destination, || panic!("constructor panic")).unwrap_err();
        assert!(error.contains("panic contained while creating Arrow stream"));
        assert!(empty.release.is_none());
    }
}
