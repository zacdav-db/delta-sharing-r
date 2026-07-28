//! Minimal native core for Delta Kernel and Arrow C Stream ownership.
//!
//! The exported functions form a small C ABI. They never call the R API,
//! retain an R object, or unwind across the native boundary.

mod kernel;
mod stream;

use std::ffi::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use crate::stream::{fixture_stream, FixtureStreamConfig};

const ABI_VERSION: u32 = 1;
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
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => STATUS_OK,
        Ok(Err(error)) => {
            write_error(error_buffer, error_capacity, &error);
            STATUS_ERROR
        }
        Err(payload) => {
            write_error(
                error_buffer,
                error_capacity,
                &format!(
                    "panic contained at native boundary: {}",
                    stream::panic_message(payload.as_ref())
                ),
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
        };

        // SAFETY: `output` was checked non-null and the caller promises the
        // correctly aligned `DeltaSharingNativeInfo` allocation.
        unsafe {
            output.as_ptr().write(info);
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
            panic!("boundary panic");
        });

        assert_eq!(status, STATUS_PANIC);
        assert!(error_text(&error).contains("panic contained"));
        assert!(error_text(&error).contains("boundary panic"));
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
