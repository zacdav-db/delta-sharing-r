# Internal native lifecycle and compact Kernel invocation. Public reads remain
# routed through the R execution interface; it supplies only a prepared local
# table after its control-plane and synthetic-log work is complete.

.native_test_stream <- function(batches = 1L,
                                rows_per_batch = 3L,
                                error_after = -1L,
                                panic_after = -1L) {
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  .Call(
    C_delta_sharing_stream_from_test_data,
    stream,
    as.integer(batches),
    as.integer(rows_per_batch),
    as.integer(error_after),
    as.integer(panic_after)
  )
  stream
}

.native_snapshot_stream <- function(table_location,
                                    columns = NULL,
                                    limit = NULL,
                                    batch_size = 65536L) {
  guard <- NULL
  cleanup_root <- NULL
  if (inherits(table_location, "delta_sharing_snapshot_log")) {
    guard <- table_location
    state <- .validate_snapshot_log_guard(guard)
    table_location <- .snapshot_log_path(guard)
    cleanup_root <- state$root
  } else {
    if (!.is_scalar_character(table_location)) {
      .abort_delta_sharing(
        "`table_location` must be one non-empty local path, `file://` URI, or prepared snapshot log.",
        type = "validation",
        operation = "read_arrow_stream"
      )
    }
    if (!startsWith(table_location, "file://")) {
      if (grepl("^[A-Za-z][A-Za-z0-9+.-]*://", table_location)) {
        .abort_delta_sharing(
          "The native scan accepts only a local path or `file://` URI.",
          type = "validation",
          operation = "read_arrow_stream"
        )
      }
      if (!dir.exists(table_location)) {
        .abort_delta_sharing(
          "The prepared local table does not exist.",
          type = "validation",
          operation = "read_arrow_stream"
        )
      }
      table_location <- normalizePath(
        table_location,
        winslash = "/",
        mustWork = FALSE
      )
    }
  }

  columns <- .normalize_columns(columns)
  if (!is.null(columns) && anyDuplicated(tolower(columns))) {
    .abort_delta_sharing(
      "`columns` must not contain duplicate Delta names ignoring case.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  limit <- .normalize_limit(limit)
  if (
    !is.numeric(batch_size) ||
      length(batch_size) != 1L ||
      is.na(batch_size) ||
      !is.finite(batch_size) ||
      batch_size < 1 ||
      batch_size > 1000000 ||
      batch_size != floor(batch_size)
  ) {
    .abort_delta_sharing(
      "`batch_size` must be one whole number between 1 and 1000000.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }

  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  .Call(
    C_delta_sharing_stream_from_snapshot,
    stream,
    table_location,
    cleanup_root,
    columns,
    limit,
    as.integer(batch_size)
  )
  if (!is.null(guard)) {
    # Successful construction transfers only this private root's cleanup
    # capability. Rust retains no R object and performs no log interpretation.
    state$released <- TRUE
  }
  stream
}

.native_diagnostics <- function() {
  .Call(C_delta_sharing_native_diagnostics)
}

.native_reap_pending_cleanups <- function() {
  .Call(C_delta_sharing_reap_pending_cleanups)
}
