//! Background collection for eager reads with live R progress.
//!
//! R cannot repaint a progress indicator while a synchronous Arrow callback is
//! blocked on I/O. This module moves an already-created Arrow C stream to one
//! native worker, records only atomic counters while it drains, and returns the
//! collected batches as another Arrow C stream. Lazy and progress-free reads
//! never enter this path.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::SchemaRef;

use crate::stream;

static ACTIVE_JOBS: AtomicU64 = AtomicU64::new(0);
static LIBRARY_PINNED: AtomicBool = AtomicBool::new(false);

struct ActiveJob;

impl Drop for ActiveJob {
    fn drop(&mut self) {
        ACTIVE_JOBS.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CollectedBatches {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

struct CollectState {
    cancelled: AtomicBool,
    rows: AtomicU64,
    batches: AtomicU64,
    done: AtomicBool,
    outcome: Mutex<Option<Result<CollectedBatches, String>>>,
}

impl Default for CollectState {
    fn default() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            rows: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            done: AtomicBool::new(false),
            outcome: Mutex::new(None),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CollectStatus {
    pub(crate) rows: u64,
    pub(crate) batches: u64,
    pub(crate) done: bool,
}

pub(crate) struct CollectJob {
    state: Arc<CollectState>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl CollectJob {
    pub(crate) fn start(source: FFI_ArrowArrayStream) -> Result<Self, String> {
        let reader = ArrowArrayStreamReader::try_new(source)
            .map_err(|_| "Arrow stream collection could not read the schema".to_string())?;
        let state = Arc::new(CollectState::default());
        let worker_state = state.clone();

        ACTIVE_JOBS.fetch_add(1, Ordering::AcqRel);
        let worker = thread::Builder::new()
            .name("delta-sharing-read".to_string())
            .spawn(move || run_worker(reader, worker_state))
            .map_err(|_| {
                ACTIVE_JOBS.fetch_sub(1, Ordering::AcqRel);
                "Arrow stream collection worker could not start".to_string()
            })?;

        Ok(Self {
            state,
            worker: Mutex::new(Some(worker)),
        })
    }

    pub(crate) fn status(&self) -> CollectStatus {
        CollectStatus {
            rows: self.state.rows.load(Ordering::Acquire),
            batches: self.state.batches.load(Ordering::Acquire),
            done: self.state.done.load(Ordering::Acquire),
        }
    }

    pub(crate) fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
    }

    pub(crate) fn finish_stream(&self) -> Result<FFI_ArrowArrayStream, String> {
        if !self.state.done.load(Ordering::Acquire) {
            return Err("Arrow stream collection is not complete".to_string());
        }
        self.join_worker()?;

        let outcome = self
            .state
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| "Arrow stream collection result is unavailable".to_string())??;
        let reader = RecordBatchIterator::new(outcome.batches.into_iter().map(Ok), outcome.schema);
        Ok(stream::record_batch_stream(Box::new(reader)))
    }

    fn join_worker(&self) -> Result<(), String> {
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            worker
                .join()
                .map_err(|_| "Arrow stream collection worker did not exit cleanly".to_string())?;
        }
        Ok(())
    }
}

pub(crate) fn active_job_count() -> u64 {
    let active = ACTIVE_JOBS.load(Ordering::Acquire);
    if LIBRARY_PINNED.load(Ordering::Acquire) {
        active.max(1)
    } else {
        active
    }
}

impl Drop for CollectJob {
    fn drop(&mut self) {
        self.cancel();
        let worker = self
            .worker
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            if worker.is_finished() {
                let _ = worker.join();
            } else {
                // Dropping a live JoinHandle detaches the worker. Keep the DLL
                // resident for the rest of the R process so its eventual
                // completion can never execute through an unloaded library.
                LIBRARY_PINNED.store(true, Ordering::Release);
            }
        }
    }
}

fn run_worker(reader: ArrowArrayStreamReader, state: Arc<CollectState>) {
    let _active = ActiveJob;
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let mut reader = reader;
        let outcome = collect_batches(&mut reader, &state);
        // Release the source stream and its prepared-log ownership before
        // publishing completion to R.
        drop(reader);
        outcome
    }))
    .unwrap_or_else(|_| Err("panic contained at asynchronous Arrow boundary".to_string()));

    *state
        .outcome
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome);
    state.done.store(true, Ordering::Release);
}

fn collect_batches(
    reader: &mut ArrowArrayStreamReader,
    state: &CollectState,
) -> Result<CollectedBatches, String> {
    let schema = reader.schema();
    let mut batches = Vec::new();

    loop {
        if state.cancelled.load(Ordering::Acquire) {
            return Err("Arrow stream collection was cancelled".to_string());
        }

        let Some(batch) = reader.next() else {
            return Ok(CollectedBatches { schema, batches });
        };
        let batch = batch.map_err(|_| "Arrow stream collection failed".to_string())?;

        if state.cancelled.load(Ordering::Acquire) {
            return Err("Arrow stream collection was cancelled".to_string());
        }
        state
            .rows
            .fetch_add(batch.num_rows() as u64, Ordering::AcqRel);
        state.batches.fetch_add(1, Ordering::AcqRel);
        batches.push(batch);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Condvar, Mutex};
    use std::time::{Duration, Instant};

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    struct GatedReader {
        schema: SchemaRef,
        gate: Arc<(Mutex<bool>, Condvar)>,
        emitted: bool,
    }

    impl Iterator for GatedReader {
        type Item = Result<RecordBatch, arrow_schema::ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.emitted {
                return None;
            }
            let (lock, wake) = &*self.gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = wake.wait(open).unwrap();
            }
            self.emitted = true;
            Some(RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            ))
        }
    }

    impl RecordBatchReader for GatedReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    fn gated_job() -> (CollectJob, Arc<(Mutex<bool>, Condvar)>) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let source = FFI_ArrowArrayStream::new(Box::new(GatedReader {
            schema,
            gate: gate.clone(),
            emitted: false,
        }));
        (CollectJob::start(source).unwrap(), gate)
    }

    fn open_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (lock, wake) = &**gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
    }

    fn wait_until_done(job: &CollectJob) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !job.status().done && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(job.status().done);
    }

    #[test]
    fn status_remains_live_while_the_next_batch_is_blocked() {
        let (job, gate) = gated_job();
        thread::sleep(Duration::from_millis(20));

        assert_eq!(
            job.status(),
            CollectStatus {
                rows: 0,
                batches: 0,
                done: false,
            }
        );

        open_gate(&gate);
        wait_until_done(&job);
        assert_eq!(job.status().rows, 3);
        assert_eq!(job.status().batches, 1);

        let output = job.finish_stream().unwrap();
        assert!(job.worker.lock().unwrap().is_none());
        let batches = ArrowArrayStreamReader::try_new(output)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches[0].num_rows(), 3);
        assert!(job.finish_stream().is_err());
    }

    #[test]
    fn cancellation_is_observed_after_a_blocked_pull_returns() {
        let (job, gate) = gated_job();
        job.cancel();
        open_gate(&gate);
        wait_until_done(&job);

        assert!(job.finish_stream().unwrap_err().contains("cancelled"));
    }

    #[test]
    fn detaching_a_blocked_worker_pins_the_native_library() {
        let (job, gate) = gated_job();
        drop(job);

        assert!(LIBRARY_PINNED.load(Ordering::Acquire));
        assert!(active_job_count() >= 1);
        open_gate(&gate);
    }
}
