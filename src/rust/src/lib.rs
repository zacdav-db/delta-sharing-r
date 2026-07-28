//! Minimal native core for Delta Kernel and Arrow C Stream ownership.
//!
//! The exported functions form a small C ABI. They never call the R API,
//! retain an R object, or unwind across the native boundary.

mod kernel;
mod stream;

use std::ffi::{c_char, c_int, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use crate::kernel::adapter::SnapshotReadOptions;
use crate::stream::{fixture_stream, FixtureStreamConfig};

const ABI_VERSION: u32 = 2;
const STATUS_OK: c_int = 0;
const STATUS_ERROR: c_int = 1;
const STATUS_PANIC: c_int = 2;

static DELTA_KERNEL_VERSION_C: &[u8] = b"0.22.0\0";
static ARROW_RS_VERSION_C: &[u8] = b"57.3.0\0";
static FFI_BACKEND_C: &[u8] = b"registered-c-shim\0";
static KERNEL_SMOKE_MESSAGE_C: &[u8] =
    b"Delta Kernel default engine and snapshot builder constructed\0";

#[repr(C)]
pub struct DeltaSharingNativeInfo {
    abi_version: u32,
    kernel_smoke_ok: c_int,
    delta_kernel_version: *const c_char,
    arrow_rs_version: *const c_char,
    ffi_backend: *const c_char,
    kernel_smoke_message: *const c_char,
    active_streams: u64,
    cancelled_streams: u64,
    emitted_batches: u64,
    pending_cleanups: u64,
}

fn write_error(error_buffer: *mut c_char, error_capacity: usize, message: &str) {
    if error_buffer.is_null() || error_capacity == 0 {
        return;
    }

    let sanitized = message.replace('\0', "\\0");
    let bytes = sanitized.as_bytes();
    let copy_length = bytes.len().min(error_capacity - 1);

    // SAFETY: callers provide writable storage of `error_capacity` bytes.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), error_buffer.cast(), copy_length);
        *error_buffer.add(copy_length) = 0;
    }
}

fn clear_error(error_buffer: *mut c_char, error_capacity: usize) {
    if !error_buffer.is_null() && error_capacity > 0 {
        // SAFETY: callers provide writable storage of `error_capacity` bytes.
        unsafe {
            *error_buffer = 0;
        }
    }
}

fn ffi_boundary<F>(error_buffer: *mut c_char, error_capacity: usize, operation: F) -> c_int
where
    F: FnOnce() -> Result<(), String>,
{
    clear_error(error_buffer, error_capacity);
    match catch_unwind(AssertUnwindSafe(|| {
        stream::reap_pending_cleanups();
        operation()
    })) {
        Ok(Ok(())) => STATUS_OK,
        Ok(Err(error)) => {
            write_error(error_buffer, error_capacity, &error);
            STATUS_ERROR
        }
        Err(_) => {
            write_error(
                error_buffer,
                error_capacity,
                "panic contained at native boundary",
            );
            STATUS_PANIC
        }
    }
}

/// Populate a nanoarrow-owned ArrowArrayStream with deterministic test data.
///
/// # Safety
///
/// `destination` must point to writable, aligned storage for an uninitialized
/// Arrow C Stream owned by nanoarrow. `error_buffer`, when non-null, must point
/// to `error_capacity` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn delta_sharing_native_populate_test_stream(
    destination: *mut FFI_ArrowArrayStream,
    batches: i32,
    rows_per_batch: i32,
    error_after: i32,
    panic_after: i32,
    error_buffer: *mut c_char,
    error_capacity: usize,
) -> c_int {
    ffi_boundary(error_buffer, error_capacity, || {
        let destination = NonNull::new(destination)
            .ok_or_else(|| "nanoarrow stream pointer is NULL".to_string())?;
        let config =
            FixtureStreamConfig::try_from_raw(batches, rows_per_batch, error_after, panic_after)?;
        stream::populate_stream(destination, || fixture_stream(config))
    })
}

/// Populate a nanoarrow-owned ArrowArrayStream from a prepared local Delta table.
///
/// This is the complete native reader boundary: R owns all control-plane work
/// and passes only a local table location plus projection, exact limit, and
/// output batch-size controls.
///
/// # Safety
///
/// `destination` must point to writable, aligned storage for an uninitialized
/// Arrow C Stream owned by nanoarrow. `table_location` and every entry in
/// `columns` must be valid NUL-terminated strings for the duration of the call.
/// `columns` may be NULL only when `column_count` is zero. `error_buffer`, when
/// non-null, must point to `error_capacity` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn delta_sharing_native_populate_snapshot_stream(
    destination: *mut FFI_ArrowArrayStream,
    table_location: *const c_char,
    cleanup_root: *const c_char,
    columns: *const *const c_char,
    column_count: usize,
    has_limit: c_int,
    limit: u64,
    batch_size: u32,
    error_buffer: *mut c_char,
    error_capacity: usize,
) -> c_int {
    ffi_boundary(error_buffer, error_capacity, || {
        let destination = NonNull::new(destination)
            .ok_or_else(|| "nanoarrow stream pointer is NULL".to_string())?;
        if table_location.is_null() {
            return Err("`table_location` pointer is NULL".to_string());
        }
        if column_count > 10_000 {
            return Err("`column_count` must be at most 10000".to_string());
        }
        if column_count > 0 && columns.is_null() {
            return Err("`columns` pointer is NULL for a non-empty projection".to_string());
        }
        if !matches!(has_limit, 0 | 1) {
            return Err("`has_limit` must be 0 or 1".to_string());
        }

        // SAFETY: the C shim promises NUL-terminated input strings that remain
        // alive for this synchronous construction call.
        let table_location = unsafe { CStr::from_ptr(table_location) }
            .to_str()
            .map_err(|_| "`table_location` must be valid UTF-8".to_string())?
            .to_string();
        let cleanup_root = if cleanup_root.is_null() {
            None
        } else {
            // SAFETY: the C shim promises a NUL-terminated string that
            // remains alive for this synchronous construction call.
            Some(
                unsafe { CStr::from_ptr(cleanup_root) }
                    .to_str()
                    .map_err(|_| "`cleanup_root` must be valid UTF-8".to_string())?
                    .to_string(),
            )
        };

        let projected_columns = if column_count == 0 {
            None
        } else {
            // SAFETY: non-null and length bounds were checked above; the C
            // shim owns this pointer array throughout the Rust call.
            let raw_columns = unsafe { std::slice::from_raw_parts(columns, column_count) };
            let mut projected = Vec::with_capacity(column_count);
            for (index, column) in raw_columns.iter().copied().enumerate() {
                if column.is_null() {
                    return Err(format!("`columns[[{}]]` pointer is NULL", index + 1));
                }
                // SAFETY: every pointer is promised to reference a
                // NUL-terminated string alive for this call.
                let column = unsafe { CStr::from_ptr(column) }
                    .to_str()
                    .map_err(|_| format!("`columns[[{}]]` must be valid UTF-8", index + 1))?;
                projected.push(column.to_string());
            }
            Some(projected)
        };

        let cleanup_table_location = table_location.clone();
        let options = SnapshotReadOptions::try_new(
            table_location,
            projected_columns,
            (has_limit == 1).then_some(limit),
            batch_size as usize,
        )?;
        stream::populate_stream(destination, || {
            let reader = kernel::adapter::snapshot_reader(options)?;
            match cleanup_root {
                Some(root) => {
                    let cleanup =
                        stream::PreparedLogCleanup::try_new(&root, &cleanup_table_location)?;
                    Ok(stream::record_batch_stream_with_resource(reader, cleanup))
                }
                None => Ok(stream::record_batch_stream(reader)),
            }
        })
    })
}

/// Fill dependency and lifecycle diagnostics for the registered C shim.
///
/// # Safety
///
/// `output` must point to writable storage for `DeltaSharingNativeInfo`.
/// `error_buffer`, when non-null, must point to `error_capacity` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn delta_sharing_native_info(
    output: *mut DeltaSharingNativeInfo,
    error_buffer: *mut c_char,
    error_capacity: usize,
) -> c_int {
    ffi_boundary(error_buffer, error_capacity, || {
        let output =
            NonNull::new(output).ok_or_else(|| "native info output pointer is NULL".to_string())?;
        kernel::adapter::smoke()?;
        let metrics = stream::global_metrics_snapshot();

        let info = DeltaSharingNativeInfo {
            abi_version: ABI_VERSION,
            kernel_smoke_ok: 1,
            delta_kernel_version: DELTA_KERNEL_VERSION_C.as_ptr().cast(),
            arrow_rs_version: ARROW_RS_VERSION_C.as_ptr().cast(),
            ffi_backend: FFI_BACKEND_C.as_ptr().cast(),
            kernel_smoke_message: KERNEL_SMOKE_MESSAGE_C.as_ptr().cast(),
            active_streams: metrics.active_streams,
            cancelled_streams: metrics.cancelled_streams,
            emitted_batches: metrics.emitted_batches,
            pending_cleanups: stream::pending_cleanup_count(),
        };

        // SAFETY: `output` was checked non-null and the caller promises the
        // correctly aligned `DeltaSharingNativeInfo` allocation.
        unsafe {
            output.as_ptr().write(info);
        }
        Ok(())
    })
}

/// Retry capability-checked prepared-log cleanups retained after transient
/// filesystem failures.
///
/// # Safety
///
/// `pending` must point to writable storage for one `u64`. `error_buffer`,
/// when non-null, must point to `error_capacity` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn delta_sharing_native_reap_pending(
    pending: *mut u64,
    error_buffer: *mut c_char,
    error_capacity: usize,
) -> c_int {
    ffi_boundary(error_buffer, error_capacity, || {
        let pending =
            NonNull::new(pending).ok_or_else(|| "pending cleanup output is NULL".to_string())?;
        stream::reap_pending_cleanups();
        // SAFETY: the caller supplied writable storage for one `u64`.
        unsafe {
            pending.as_ptr().write(stream::pending_cleanup_count());
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use arrow_array::ffi_stream::ArrowArrayStreamReader;

    use super::*;

    fn error_text(buffer: &[c_char]) -> String {
        // SAFETY: every FFI boundary call NUL-terminates this buffer.
        unsafe { CStr::from_ptr(buffer.as_ptr()) }
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn ffi_populates_a_readable_stream() {
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut error = [0 as c_char; 256];
        let status = unsafe {
            delta_sharing_native_populate_test_stream(
                &mut stream,
                2,
                3,
                -1,
                -1,
                error.as_mut_ptr(),
                error.len(),
            )
        };

        assert_eq!(status, STATUS_OK, "{}", error_text(&error));
        let reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn ffi_rejects_null_and_invalid_inputs_without_unwinding() {
        let mut error = [0 as c_char; 32];
        let status = unsafe {
            delta_sharing_native_populate_test_stream(
                std::ptr::null_mut(),
                1,
                1,
                -1,
                -1,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        assert_eq!(status, STATUS_ERROR);
        assert!(error_text(&error).contains("nanoarrow stream pointer"));

        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe {
            delta_sharing_native_populate_test_stream(
                &mut stream,
                -1,
                1,
                -1,
                -1,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        assert_eq!(status, STATUS_ERROR);
        assert!(error_text(&error).contains("batches"));
    }

    #[test]
    fn ffi_error_buffer_is_bounded_and_nul_safe() {
        let mut short = [b'X' as c_char; 5];
        write_error(short.as_mut_ptr(), short.len(), "long\0message");
        assert_eq!(short[4], 0);
        assert_eq!(error_text(&short), "long");

        write_error(std::ptr::null_mut(), 0, "ignored");
        clear_error(std::ptr::null_mut(), 0);
    }

    #[test]
    fn ffi_boundary_contains_panics() {
        let mut error = [0 as c_char; 128];
        let status = ffi_boundary(error.as_mut_ptr(), error.len(), || {
            panic!("boundary panic https://example.test/data?X-Amz-Signature=super-secret");
        });

        assert_eq!(status, STATUS_PANIC);
        assert!(error_text(&error).contains("panic contained"));
        assert!(!error_text(&error).contains("boundary panic"));
        assert!(!error_text(&error).contains("super-secret"));
    }

    #[test]
    fn native_info_reports_pins_and_c_backend() {
        let mut output = std::mem::MaybeUninit::<DeltaSharingNativeInfo>::uninit();
        let mut error = [0 as c_char; 256];
        let status = unsafe {
            delta_sharing_native_info(output.as_mut_ptr(), error.as_mut_ptr(), error.len())
        };
        assert_eq!(status, STATUS_OK, "{}", error_text(&error));

        // SAFETY: a successful call initialized the full output structure.
        let info = unsafe { output.assume_init() };
        assert_eq!(info.abi_version, ABI_VERSION);
        assert_eq!(info.kernel_smoke_ok, 1);
        // SAFETY: diagnostic string pointers refer to static NUL-terminated data.
        assert_eq!(
            unsafe { CStr::from_ptr(info.delta_kernel_version) }
                .to_str()
                .unwrap(),
            "0.22.0"
        );
        assert_eq!(
            unsafe { CStr::from_ptr(info.arrow_rs_version) }
                .to_str()
                .unwrap(),
            "57.3.0"
        );
        assert_eq!(
            unsafe { CStr::from_ptr(info.ffi_backend) }
                .to_str()
                .unwrap(),
            "registered-c-shim"
        );
    }

    #[test]
    fn native_info_rejects_null_output() {
        let mut error = [0 as c_char; 128];
        let status = unsafe {
            delta_sharing_native_info(std::ptr::null_mut(), error.as_mut_ptr(), error.len())
        };

        assert_eq!(status, STATUS_ERROR);
        assert!(error_text(&error).contains("NULL"));
    }
}
