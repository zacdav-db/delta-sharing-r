#!/usr/bin/env Rscript

mmw_abort <- function(message) {
  stop(message, call. = FALSE)
}

mmw_parse_cli <- function(args) {
  values <- list(
    files = NULL,
    chunk_files = "256",
    outcome = "explicit_release",
    output = NULL
  )
  index <- 1L
  while (index <= length(args)) {
    argument <- args[[index]]
    if (!argument %in% c(
      "--files",
      "--chunk-files",
      "--outcome",
      "--output"
    )) {
      mmw_abort(sprintf("Unknown worker argument: %s", argument))
    }
    if (index == length(args)) {
      mmw_abort(sprintf("Worker argument %s requires a value.", argument))
    }
    name <- gsub("-", "_", sub("^--", "", argument), fixed = TRUE)
    values[[name]] <- args[[index + 1L]]
    index <- index + 2L
  }

  integer_value <- function(value, name, minimum) {
    parsed <- suppressWarnings(as.integer(value))
    if (is.na(parsed) || parsed < minimum) {
      mmw_abort(sprintf("`--%s` must be an integer of at least %d.", name, minimum))
    }
    parsed
  }
  values$files <- integer_value(values$files, "files", 0L)
  values$chunk_files <- integer_value(
    values$chunk_files,
    "chunk-files",
    1L
  )
  if (values$files > 1000000L) {
    mmw_abort("`--files` cannot exceed the production one-million-action limit.")
  }
  if (!values$outcome %in% c(
    "explicit_release",
    "write_error",
    "finalizer"
  )) {
    mmw_abort(
      "`--outcome` must be `explicit_release`, `write_error`, or `finalizer`."
    )
  }
  if (is.null(values$output) || !nzchar(values$output)) {
    mmw_abort("`--output` is required.")
  }
  values
}

mmw_protocol_line <- paste0(
  '{"protocol":{"deltaProtocol":',
  '{"minReaderVersion":1,"minWriterVersion":2}}}'
)

mmw_metadata_line <- paste0(
  '{"metaData":{"version":42,"size":0,"numFiles":0,',
  '"deltaMetadata":{"id":"manifest-memory-table",',
  '"format":{"provider":"parquet","options":{}},',
  '"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[',
  '{\\"name\\":\\"value\\",\\"type\\":\\"long\\",',
  '\\"nullable\\":false,\\"metadata\\":{}}]}",',
  '"partitionColumns":[],"configuration":{}}}}'
)

mmw_file_line <- function(index) {
  id <- sprintf("%08d", index)
  paste0(
    '{"file":{"id":"file-', id,
    '","expirationTimestamp":4102444800000,',
    '"deltaSingleAction":{"add":{"path":',
    '"https://benchmark.invalid/part-', id,
    '.parquet?fixture=manifest-memory",',
    '"partitionValues":{},"size":128,',
    '"modificationTime":1700000000000,"dataChange":true}}}}'
  )
}

mmw_headers <- function() {
  c(
    "Content-Type" = "application/x-ndjson",
    "Delta-Table-Version" = "42",
    fileidhash = "delta",
    "delta-sharing-capabilities" =
      "responseformat=delta;includeendstreamaction=true"
  )
}

mmw_response <- function(file_count, chunk_files, recorder) {
  next_file <- 1L
  started <- FALSE
  finished <- FALSE
  recorder$pulls <- 0L
  recorder$closes <- 0L
  recorder$wire_bytes <- 0

  delta.sharing:::.new_snapshot_pull_response(
    status = 200L,
    headers = mmw_headers(),
    pull = function() {
      recorder$pulls <- recorder$pulls + 1L
      if (finished) {
        return(NULL)
      }
      lines <- character()
      if (!started) {
        lines <- c(mmw_protocol_line, mmw_metadata_line)
        started <<- TRUE
      }
      if (next_file <= file_count) {
        last_file <- min(file_count, next_file + chunk_files - 1L)
        lines <- c(
          lines,
          vapply(
            seq.int(next_file, last_file),
            mmw_file_line,
            character(1),
            USE.NAMES = FALSE
          )
        )
        next_file <<- last_file + 1L
      }
      if (next_file > file_count) {
        lines <- c(lines, '{"endStreamAction":{}}')
        finished <<- TRUE
      }
      bytes <- charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
      recorder$wire_bytes <- recorder$wire_bytes + length(bytes)
      bytes
    },
    close = function() {
      recorder$closes <- recorder$closes + 1L
      invisible(NULL)
    }
  )
}

mmw_read <- function() {
  profile <- list(
    shareCredentialsVersion = 1,
    endpoint = "https://sharing.example.invalid/api",
    bearerToken = "benchmark-only-token",
    expirationTime = "2099-01-01T00:00:00Z"
  )
  sharing_read(
    sharing_table(
      sharing_client(profile),
      share = "benchmark",
      schema = "default",
      table = "manifest"
    ),
    version = 42,
    response_format = "delta"
  )
}

mmw_entries <- function(parent) {
  list.files(parent, all.files = TRUE, no.. = TRUE)
}

mmw_prepare <- function(options, parent, recorder, write_commit = NULL) {
  fetch <- function(request) {
    if (!identical(request$page_number, 1L)) {
      mmw_abort("The worker unexpectedly requested a second page.")
    }
    mmw_response(options$files, options$chunk_files, recorder)
  }
  arguments <- list(
    read = mmw_read(),
    fetch = fetch,
    temp_parent = parent,
    max_files_per_page = max(1L, options$files)
  )
  if (!is.null(write_commit)) {
    arguments$write_commit <- write_commit
  }
  do.call(delta.sharing:::.prepare_snapshot_read, arguments)
}

mmw_run <- function(options) {
  suppressPackageStartupMessages(library(delta.sharing))
  parent <- tempfile("delta-sharing-manifest-memory-")
  if (!dir.create(parent, mode = "0700")) {
    mmw_abort("Could not create the worker temporary parent.")
  }
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  recorder <- new.env(parent = emptyenv())
  started <- proc.time()[["elapsed"]]
  result <- switch(
    options$outcome,
    explicit_release = {
      prepared <- mmw_prepare(options, parent, recorder)
      state <- delta.sharing:::.prepared_snapshot_state(prepared)
      root <- state$guard$state$root
      commit <- file.path(
        state$guard$state$table_path,
        "_delta_log",
        "00000000000000000000.json"
      )
      if (!dir.exists(root) || !file.exists(commit)) {
        mmw_abort("Successful preparation did not publish its synthetic log.")
      }
      commit_bytes <- unname(file.info(commit)$size)
      live_roots <- length(mmw_entries(parent))
      delta.sharing:::.release_prepared_snapshot(prepared)
      if (length(mmw_entries(parent)) != 0L) {
        mmw_abort("Explicit release left a temporary root behind.")
      }
      list(
        status = "pass",
        commit_bytes = commit_bytes,
        live_roots_before_release = live_roots,
        roots_after_cleanup = 0L
      )
    },
    write_error = {
      condition <- tryCatch(
        {
          mmw_prepare(
            options,
            parent,
            recorder,
            write_commit = function(path, lines) {
              stop("intentional manifest-memory write failure", call. = FALSE)
            }
          )
          NULL
        },
        error = identity
      )
      if (!inherits(condition, "delta_sharing_protocol_error")) {
        mmw_abort("The injected write failure did not return the typed error.")
      }
      if (length(mmw_entries(parent)) != 0L) {
        mmw_abort("Failed preparation left a temporary root behind.")
      }
      list(
        status = "pass",
        condition_class = class(condition)[[1L]],
        roots_after_cleanup = 0L
      )
    },
    finalizer = {
      prepared <- mmw_prepare(options, parent, recorder)
      state <- delta.sharing:::.prepared_snapshot_state(prepared)
      root <- state$guard$state$root
      rm(state, prepared)
      attempts <- 0L
      while (dir.exists(root) && attempts < 10L) {
        gc()
        attempts <- attempts + 1L
      }
      if (dir.exists(root) || length(mmw_entries(parent)) != 0L) {
        mmw_abort("Finalization left a temporary root behind.")
      }
      list(
        status = "pass",
        gc_attempts = attempts,
        roots_after_cleanup = 0L
      )
    }
  )
  elapsed_seconds <- proc.time()[["elapsed"]] - started
  if (!identical(recorder$closes, 1L)) {
    mmw_abort("The production response was not closed exactly once.")
  }
  list(
    schema_version = 1L,
    workload = "snapshot_manifest_planning",
    files = options$files,
    chunk_files = options$chunk_files,
    outcome = options$outcome,
    elapsed_seconds = elapsed_seconds,
    wire_bytes = recorder$wire_bytes,
    pulls = recorder$pulls,
    closes = recorder$closes,
    result = result
  )
}

mmw_write <- function(value, path) {
  parent <- dirname(path)
  if (!dir.exists(parent) && !dir.create(parent, recursive = TRUE)) {
    mmw_abort("Could not create the worker output directory.")
  }
  temporary <- tempfile(paste0(".", basename(path), "-"), tmpdir = parent)
  on.exit(unlink(temporary, force = TRUE), add = TRUE)
  jsonlite::write_json(
    value,
    temporary,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null",
    digits = NA
  )
  if (!file.rename(temporary, path)) {
    mmw_abort("Could not publish the worker output.")
  }
  invisible(path)
}

if (sys.nframe() == 0L) {
  options <- mmw_parse_cli(commandArgs(trailingOnly = TRUE))
  mmw_write(mmw_run(options), options$output)
}
