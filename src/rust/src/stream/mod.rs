//! Arrow C Stream export and lifecycle ownership.
//!
//! nanoarrow owns the outer `FFI_ArrowArrayStream` allocation. Rust moves an
//! initialized stream into that allocation. The release callback drops the
//! reader and all resources tied to the active stream.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
#[cfg(test)]
use arrow_array::{
    ArrayRef, Decimal128Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
#[cfg(test)]
use arrow_schema::{DataType, Field, Schema, TimeUnit};

/// Prevent a reader panic from unwinding through Arrow's `extern "C"` callback.
struct PanicBoundaryReader {
    schema: SchemaRef,
    inner: Option<Box<dyn RecordBatchReader + Send>>,
    terminal: bool,
}

impl PanicBoundaryReader {
    fn new(inner: Box<dyn RecordBatchReader + Send>) -> Self {
        let schema = inner.schema();
        Self {
            schema,
            inner: Some(inner),
            terminal: false,
        }
    }

    fn finish(&mut self) {
        self.terminal = true;
        drop(self.inner.take());
    }
}

impl Iterator for PanicBoundaryReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }

        match catch_unwind(AssertUnwindSafe(|| {
            self.inner.as_mut().and_then(|reader| reader.next())
        })) {
            Ok(Some(Ok(batch))) => Some(Ok(batch)),
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
    }
}

fn export_reader(reader: Box<dyn RecordBatchReader + Send>) -> FFI_ArrowArrayStream {
    FFI_ArrowArrayStream::new(Box::new(PanicBoundaryReader::new(reader)))
}

pub(crate) fn record_batch_stream(
    reader: Box<dyn RecordBatchReader + Send>,
) -> FFI_ArrowArrayStream {
    export_reader(reader)
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
#[cfg(test)]
pub(crate) struct FixtureStreamConfig {
    batches: usize,
    rows_per_batch: usize,
    error_after: Option<usize>,
    panic_after: Option<usize>,
}

#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn fixture_stream(config: FixtureStreamConfig) -> Result<FFI_ArrowArrayStream, String> {
    let reader = FixtureReader::new(config);
    Ok(record_batch_stream(Box::new(reader)))
}

#[cfg(test)]
struct FixtureReader {
    schema: SchemaRef,
    config: FixtureStreamConfig,
    next_batch: usize,
}

#[cfg(test)]
impl FixtureReader {
    fn new(config: FixtureStreamConfig) -> Self {
        Self {
            schema: fixture_schema(),
            config,
            next_batch: 0,
        }
    }
}

#[cfg(test)]
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

#[cfg(test)]
impl RecordBatchReader for FixtureReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
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

#[cfg(test)]
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
    use arrow_array::ffi_stream::ArrowArrayStreamReader;

    use super::*;

    fn config(batches: i32, error_after: i32, panic_after: i32) -> FixtureStreamConfig {
        FixtureStreamConfig::try_from_raw(batches, 3, error_after, panic_after).unwrap()
    }

    #[test]
    fn empty_one_and_many_batches_round_trip() {
        for expected_batches in [0, 1, 4] {
            let stream = fixture_stream(config(expected_batches, -1, -1)).unwrap();
            let reader = ArrowArrayStreamReader::try_new(stream).unwrap();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

            assert_eq!(batches.len(), expected_batches as usize);
            assert!(batches.iter().all(|batch| batch.num_rows() == 3));
        }
    }

    #[test]
    fn reader_errors_and_panics_are_terminal_and_sanitized() {
        let stream = fixture_stream(config(3, 1, -1)).unwrap();
        let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 3);
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("synthetic reader error after 1 batches"));
        assert!(reader.next().is_none());

        let stream = fixture_stream(config(3, -1, 0)).unwrap();
        let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("panic contained at Arrow stream boundary"));
        assert!(!error.contains("synthetic reader panic"));
        assert!(reader.next().is_none());
    }

    #[test]
    fn emitted_array_buffers_outlive_stream_release() {
        let mut stream = fixture_stream(config(2, -1, -1)).unwrap();
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
    fn populate_stream_rejects_reuse_and_contains_panics() {
        let mut initialized = fixture_stream(config(1, -1, -1)).unwrap();
        let destination = NonNull::from(&mut initialized);
        assert!(populate_stream(destination, || unreachable!())
            .unwrap_err()
            .contains("already initialized"));

        let mut empty = FFI_ArrowArrayStream::empty();
        let destination = NonNull::from(&mut empty);
        let error = populate_stream(destination, || panic!("constructor panic must-not-escape"))
            .unwrap_err();
        assert!(error.contains("panic contained while creating Arrow stream"));
        assert!(!error.contains("must-not-escape"));
        assert!(empty.release.is_none());
    }
}
