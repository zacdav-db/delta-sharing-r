# Internal native lifecycle and compact Kernel invocation. R completes all
# control-plane and synthetic-log work, then supplies a prepared local table
# path (plus an optional temp `cleanup_root` whose lifetime the native stream
# takes over) to the Delta Kernel scan.

release_materializer_stream <- function(stream) {
  if (inherits(stream, "nanoarrow_array_stream")) {
    try(stream$release(), silent = TRUE)
  }
  invisible(NULL)
}

# Default and maximum Arrow output batch size (rows), shared by the reader
# surface and the native scan validation.
DEFAULT_BATCH_SIZE <- 65536L
MAX_BATCH_SIZE <- 1000000L

# Shared native-scan argument validation. The native functions accept only a
# prepared local table path (or file:// URI); everything else is an R bug or a
# caller error, so all failures are validation conditions.
validate_native_location <- function(table_location) {
  if (!is_scalar_character(table_location)) {
    abort(
      "`table_location` must be one non-empty local path or `file://` URI.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  if (startsWith(table_location, "file://")) {
    return(table_location)
  }
  if (grepl("^[A-Za-z][A-Za-z0-9+.-]*://", table_location)) {
    abort(
      "The native scan accepts only a local path or `file://` URI.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  if (!fs::dir_exists(table_location)) {
    abort(
      "The prepared local table does not exist.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  as.character(fs::path_abs(table_location))
}

validate_native_columns <- function(columns) {
  columns <- normalize_columns(columns)
  if (!is.null(columns) && anyDuplicated(tolower(columns))) {
    abort(
      "`columns` must not contain duplicate Delta names ignoring case.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  columns
}

validate_native_batch_size <- function(batch_size) {
  if (
    !rlang::is_scalar_integerish(batch_size, finite = TRUE) ||
      batch_size < 1 ||
      batch_size > MAX_BATCH_SIZE
  ) {
    abort(
      "{.arg batch_size} must be one whole number between 1 and \\
       {MAX_BATCH_SIZE}.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  as.integer(batch_size)
}

cdf_whole_version <- function(value, label) {
  if (!rlang::is_scalar_integerish(value, finite = TRUE) || value < 0) {
    abort(
      "{.arg {label}} must be a supported provider version.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  as.double(value)
}

native_test_stream <- function(
  batches = 1L,
  rows_per_batch = 3L,
  error_after = -1L,
  panic_after = -1L
) {
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  .Call(
    C_delta_sharing_stream_from_test_data,
    stream,
    as.integer(batches),
    as.integer(rows_per_batch),
    as.integer(error_after),
    as.integer(panic_after)
  )
  interruptible_native_stream(stream)
}

native_stream_interrupt_message <- "delta-sharing stream interrupted"

native_stream_was_interrupted <- function(condition) {
  inherits(condition, "error") &&
    grepl(
      native_stream_interrupt_message,
      conditionMessage(condition),
      fixed = TRUE
    )
}

abort_native_stream_interrupt <- function(operation) {
  abort(
    "The Delta Sharing read was interrupted.",
    type = "cancelled",
    operation = operation
  )
}

abort_native_stream_failure <- function(operation) {
  abort(
    "Delta Kernel could not produce the requested Arrow data.",
    type = "kernel",
    operation = operation,
    kernel_category = "data_scan"
  )
}

with_native_stream_conditions <- function(code, operation, stream = NULL) {
  tryCatch(
    code,
    error = function(condition) {
      if (inherits(condition, "delta_sharing_error")) {
        if (!is.null(stream)) {
          release_materializer_stream(stream)
        }
        stop(condition)
      }
      if (native_stream_was_interrupted(condition)) {
        if (!is.null(stream)) {
          release_materializer_stream(stream)
        }
        abort_native_stream_interrupt(operation)
      }
      if (is.null(stream)) {
        stop(condition)
      }
      # At this point a live stream has entered its consumer. Hook-shape
      # validation and Arrow reader construction happen before this guard;
      # production code inside it only performs schema and data pulls.
      # Injected internal consumer hooks intentionally share this public
      # redaction boundary so their implementation detail cannot escape.
      release_materializer_stream(stream)
      abort_native_stream_failure(operation)
    },
    interrupt = function(condition) {
      if (!is.null(stream)) {
        release_materializer_stream(stream)
      }
      abort_native_stream_interrupt(operation)
    }
  )
}

interruptible_native_stream <- function(stream) {
  class(stream) <- c(
    "delta_sharing_interruptible_stream",
    setdiff(class(stream), "delta_sharing_interruptible_stream")
  )
  stream
}

#' @export
`$.delta_sharing_interruptible_stream` <- function(x, name) {
  method <- NextMethod("$")
  if (!identical(name, "get_next")) {
    return(method)
  }

  function(...) {
    with_native_stream_conditions(
      method(...),
      operation = "read_arrow_stream",
      stream = x
    )
  }
}

native_snapshot_stream <- function(
  table_location,
  columns = NULL,
  limit = NULL,
  batch_size = DEFAULT_BATCH_SIZE,
  cleanup_root = NULL
) {
  table_location <- validate_native_location(table_location)
  columns <- validate_native_columns(columns)
  limit <- normalize_limit(limit)
  batch_size <- validate_native_batch_size(batch_size)

  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  # Successful construction transfers `cleanup_root` ownership to Rust, which
  # deletes it when the native stream is released. Rust retains no R object and
  # performs no log interpretation.
  .Call(
    C_delta_sharing_stream_from_snapshot,
    stream,
    table_location,
    cleanup_root,
    columns,
    limit,
    batch_size
  )
  interruptible_native_stream(stream)
}

native_cdf_stream <- function(
  table_location,
  start_version,
  end_version,
  columns = NULL,
  batch_size = DEFAULT_BATCH_SIZE,
  cleanup_root = NULL
) {
  table_location <- validate_native_location(table_location)
  columns <- validate_native_columns(columns)
  batch_size <- validate_native_batch_size(batch_size)
  start_version <- cdf_whole_version(start_version, "start_version")
  end_version <- cdf_whole_version(end_version, "end_version")
  if (end_version < start_version) {
    abort(
      "`end_version` must be greater than or equal to `start_version`.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }

  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  .Call(
    C_delta_sharing_stream_from_cdf,
    stream,
    table_location,
    cleanup_root,
    columns,
    start_version,
    end_version,
    batch_size
  )
  interruptible_native_stream(stream)
}

native_collect_start <- function(stream) {
  with_native_stream_conditions(
    .Call(C_delta_sharing_collect_start, stream),
    operation = "read_arrow_stream",
    stream = stream
  )
}

native_collect_status <- function(job) {
  .Call(C_delta_sharing_collect_status, job)
}

native_collect_finish <- function(job) {
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  with_native_stream_conditions(
    .Call(C_delta_sharing_collect_finish, job, stream),
    operation = "read_arrow_stream",
    stream = stream
  )
  interruptible_native_stream(stream)
}

native_collect_cancel <- function(job) {
  .Call(C_delta_sharing_collect_cancel, job)
  invisible(NULL)
}

native_collect_active <- function() {
  .Call(C_delta_sharing_collect_active)
}

native_reap_pending_cleanups <- function() {
  .Call(C_delta_sharing_reap_pending_cleanups)
}
