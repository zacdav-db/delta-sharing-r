#!/usr/bin/env Rscript

worker_abort <- function(message) {
  stop(message, call. = FALSE)
}

worker_parse_cli <- function(args) {
  values <- list(
    workload = NULL,
    output = NULL,
    batches = NULL,
    rows_per_batch = NULL,
    table = NULL,
    batch_size = NULL,
    expected_rows = NULL
  )
  index <- 1L
  while (index <= length(args)) {
    argument <- args[[index]]
    if (!argument %in% c(
      "--workload",
      "--output",
      "--batches",
      "--rows-per-batch",
      "--table",
      "--batch-size",
      "--expected-rows"
    )) {
      worker_abort(sprintf("Unknown worker argument: %s", argument))
    }
    if (index == length(args)) {
      worker_abort(sprintf("Worker argument %s requires a value.", argument))
    }
    name <- sub("^--", "", argument)
    name <- gsub("-", "_", name, fixed = TRUE)
    values[[name]] <- args[[index + 1L]]
    index <- index + 2L
  }

  if (is.null(values$workload) ||
      !values$workload %in% c("baseline", "synthetic", "kernel")) {
    worker_abort("`--workload` must be `baseline`, `synthetic`, or `kernel`.")
  }
  if (is.null(values$output) || !nzchar(values$output)) {
    worker_abort("`--output` is required.")
  }
  integer_field <- function(name, minimum = 1L) {
    value <- suppressWarnings(as.integer(values[[name]]))
    if (is.na(value) || value < minimum) {
      worker_abort(sprintf(
        "`--%s` must be an integer of at least %d.",
        gsub("_", "-", name, fixed = TRUE),
        minimum
      ))
    }
    value
  }
  if (identical(values$workload, "synthetic")) {
    values$batches <- integer_field("batches")
    values$rows_per_batch <- integer_field("rows_per_batch")
  }
  if (identical(values$workload, "kernel")) {
    if (is.null(values$table) || !nzchar(values$table)) {
      worker_abort("`--table` is required for the Kernel workload.")
    }
    values$table <- normalizePath(
      values$table,
      winslash = "/",
      mustWork = TRUE
    )
    values$batch_size <- integer_field("batch_size")
    values$expected_rows <- integer_field("expected_rows", minimum = 0L)
  }
  values
}

worker_consume <- function(stream) {
  on.exit(try(stream$release(), silent = TRUE), add = TRUE)
  rows <- 0
  batches <- 0L
  maximum_batch_rows <- 0L
  repeat {
    batch <- stream$get_next()
    if (is.null(batch)) {
      break
    }
    rows <- rows + batch$length
    batches <- batches + 1L
    maximum_batch_rows <- max(maximum_batch_rows, batch$length)
  }
  stream$release()
  list(
    rows = as.double(rows),
    batches = batches,
    maximum_batch_rows = maximum_batch_rows
  )
}

worker_run <- function(options) {
  suppressPackageStartupMessages(library(delta.sharing))
  diagnostics_before <- delta.sharing:::.native_diagnostics()
  started <- as.numeric(Sys.time())
  result <- switch(
    options$workload,
    baseline = list(
      rows = 0,
      batches = 0L,
      maximum_batch_rows = 0L
    ),
    synthetic = worker_consume(delta.sharing:::.native_test_stream(
      batches = options$batches,
      rows_per_batch = options$rows_per_batch
    )),
    kernel = worker_consume(delta.sharing:::.native_snapshot_stream(
      options$table,
      limit = NULL,
      batch_size = options$batch_size
    ))
  )
  elapsed_seconds <- as.numeric(Sys.time()) - started
  diagnostics_after <- delta.sharing:::.native_diagnostics()

  if (identical(options$workload, "synthetic")) {
    expected <- as.double(options$batches) *
      as.double(options$rows_per_batch)
    if (!identical(result$rows, expected)) {
      worker_abort("Synthetic RSS workload returned an unexpected row count.")
    }
    emitted <- as.double(
      diagnostics_after$emitted_batches -
        diagnostics_before$emitted_batches
    )
    if (!identical(emitted, as.double(options$batches))) {
      worker_abort("Synthetic RSS workload emitted an unexpected batch count.")
    }
  }
  if (identical(options$workload, "kernel") &&
      !identical(result$rows, as.double(options$expected_rows))) {
    worker_abort("Kernel RSS workload returned an unexpected row count.")
  }
  if (!identical(
    as.double(
      diagnostics_after$active_streams -
        diagnostics_before$active_streams
    ),
    0
  )) {
    worker_abort("RSS workload leaked an active native stream.")
  }
  if (!identical(
    as.double(
      diagnostics_after$pending_cleanups -
        diagnostics_before$pending_cleanups
    ),
    0
  )) {
    worker_abort("RSS workload leaked a pending native cleanup.")
  }

  list(
    schema_version = 1L,
    workload = options$workload,
    parameters = list(
      batches = options$batches,
      rows_per_batch = options$rows_per_batch,
      table = options$table,
      batch_size = options$batch_size,
      expected_rows = options$expected_rows
    ),
    result = c(
      result,
      list(
        elapsed_seconds = elapsed_seconds,
        active_streams_delta = as.double(
          diagnostics_after$active_streams -
            diagnostics_before$active_streams
        ),
        pending_cleanups_delta = as.double(
          diagnostics_after$pending_cleanups -
            diagnostics_before$pending_cleanups
        ),
        emitted_batches_delta = as.double(
          diagnostics_after$emitted_batches -
            diagnostics_before$emitted_batches
        )
      )
    )
  )
}

worker_write <- function(value, path) {
  parent <- dirname(path)
  if (!dir.exists(parent) && !dir.create(parent, recursive = TRUE)) {
    worker_abort("Could not create worker output directory.")
  }
  temporary <- tempfile(paste0(".", basename(path), "-"), tmpdir = parent)
  on.exit(unlink(temporary, force = TRUE), add = TRUE)
  jsonlite::write_json(
    value,
    temporary,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null",
    na = "null",
    digits = NA
  )
  if (!file.rename(temporary, path)) {
    worker_abort("Could not publish worker output.")
  }
  invisible(path)
}

if (sys.nframe() == 0L) {
  options <- worker_parse_cli(commandArgs(trailingOnly = TRUE))
  worker_write(worker_run(options), options$output)
}
