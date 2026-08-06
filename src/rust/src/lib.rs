//! Minimal native core for Delta Kernel and Arrow C Stream ownership.
//!
//! The exported functions form a small C ABI. They never call the R API,
//! retain an R object, or unwind across the native boundary.
//!
//! `unsafe` here is confined to the unavoidable FFI surface: the `extern "C"`
//! entry points, reading raw pointers R passes in, and the Arrow C Stream ABI.
//! Every `unsafe` block is individually scoped and carries a `// SAFETY:` note;
//! `unsafe_op_in_unsafe_fn` is denied so even the entry points must justify each
//! pointer operation rather than leaning on the function-level `unsafe`.
#![deny(unsafe_op_in_unsafe_fn)]

mod kernel;
mod stream;

use std::ffi::{c_char, c_int, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;

use crate::kernel::adapter::{CdfReadOptions, SnapshotReadOptions};
use crate::stream::{fixture_stream, FixtureStreamConfig};

const STATUS_OK: c_int = 0;
const STATUS_ERROR: c_int = 1;
const STATUS_PANIC: c_int = 2;

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

/// Populate a nanoarrow-owned ArrowArrayStream from a prepared versioned CDF log.
///
/// R has already resolved and validated the inclusive provider bounds and
/// constructed the private local log. This boundary owns only Kernel
/// `TableChanges`, Arrow streaming, and prepared-root cleanup.
///
/// # Safety
///
/// The pointer and string requirements match
/// `delta_sharing_native_populate_snapshot_stream`.
#[no_mangle]
pub unsafe extern "C" fn delta_sharing_native_populate_cdf_stream(
    destination: *mut FFI_ArrowArrayStream,
    table_location: *const c_char,
    cleanup_root: *const c_char,
    columns: *const *const c_char,
    column_count: usize,
    start_version: u64,
    end_version: u64,
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

        // SAFETY: the C shim retains all strings for this synchronous call.
        let table_location = unsafe { CStr::from_ptr(table_location) }
            .to_str()
            .map_err(|_| "`table_location` must be valid UTF-8".to_string())?
            .to_string();
        let cleanup_root = if cleanup_root.is_null() {
            None
        } else {
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
            // SAFETY: the pointer-array bounds were validated above.
            let raw_columns = unsafe { std::slice::from_raw_parts(columns, column_count) };
            let mut projected = Vec::with_capacity(column_count);
            for (index, column) in raw_columns.iter().copied().enumerate() {
                if column.is_null() {
                    return Err(format!("`columns[[{}]]` pointer is NULL", index + 1));
                }
                let column = unsafe { CStr::from_ptr(column) }
                    .to_str()
                    .map_err(|_| format!("`columns[[{}]]` must be valid UTF-8", index + 1))?;
                projected.push(column.to_string());
            }
            Some(projected)
        };

        let cleanup_table_location = table_location.clone();
        let options = CdfReadOptions::try_new(
            table_location,
            projected_columns,
            start_version,
            end_version,
            batch_size as usize,
        )?;
        stream::populate_stream(destination, || {
            let reader = kernel::adapter::cdf_reader(options)?;
            match cleanup_root {
                Some(root) => {
                    let cleanup = stream::PreparedLogCleanup::try_new_cdf(
                        &root,
                        &cleanup_table_location,
                        start_version,
                        end_version,
                    )?;
                    Ok(stream::record_batch_stream_with_resource(reader, cleanup))
                }
                None => Ok(stream::record_batch_stream(reader)),
            }
        })
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
