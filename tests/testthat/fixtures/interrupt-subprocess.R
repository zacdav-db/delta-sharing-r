args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 4L) {
  stop("Expected kind, ready, result, and fixture paths.", call. = FALSE)
}

kind <- args[[1L]]
ready <- args[[2L]]
result_path <- args[[3L]]
fixtures <- args[[4L]]
suppressPackageStartupMessages(library(delta.sharing))

fixture_uri <- function(path) {
  paste0(
    "file://",
    utils::URLencode(
      normalizePath(path, winslash = "/", mustWork = TRUE),
      reserved = FALSE,
      repeated = TRUE
    )
  )
}

linked_files <- function(source, count, prefix) {
  directory <- tempfile(prefix)
  dir.create(directory)
  paths <- file.path(
    directory,
    sprintf("part-%05d.parquet", seq_len(count))
  )
  linked <- vapply(
    paths,
    function(path) file.link(source, path),
    logical(1),
    USE.NAMES = FALSE
  )
  if (!all(linked)) {
    unlink(paths[linked], force = TRUE)
    copied <- file.copy(
      rep(source, length(paths)),
      paths,
      copy.mode = FALSE,
      copy.date = FALSE
    )
    stopifnot(all(copied))
  }
  list(directory = directory, paths = paths)
}

private_log <- function(lines, file_count) {
  root <- tempfile(".delta-sharing-snapshot-", tmpdir = tempdir())
  dir.create(root, mode = "0700")
  writeLines(
    "delta-sharing-r:vnext",
    file.path(root, ".delta-sharing-r-prepared-log"),
    useBytes = TRUE
  )
  table <- file.path(root, "table")
  log <- file.path(table, "_delta_log")
  dir.create(log, recursive = TRUE)
  writeLines(
    lines,
    file.path(log, "00000000000000000000.json"),
    useBytes = TRUE
  )
  list(
    guard = delta.sharing:::.new_snapshot_log_guard(
      root,
      table,
      file_count
    ),
    root = root,
    log = log
  )
}

snapshot_stream <- function(file_count = 2048L) {
  fixture <- file.path(fixtures, "delta", "local-table")
  source <- file.path(fixture, "part-00000.parquet")
  files <- linked_files(
    source,
    file_count,
    "delta-sharing-interrupt-snapshot-"
  )
  source_log <- readLines(
    file.path(fixture, "_delta_log", "00000000000000000000.json"),
    warn = FALSE
  )
  size <- unname(file.info(source)$size)
  additions <- vapply(
    files$paths,
    function(path) {
      jsonlite::toJSON(
        list(add = list(
          path = fixture_uri(path),
          partitionValues = structure(list(), names = character()),
          size = size,
          modificationTime = 0,
          dataChange = TRUE
        )),
        auto_unbox = TRUE,
        null = "null"
      )
    },
    character(1),
    USE.NAMES = FALSE
  )
  prepared <- private_log(c(source_log[1:2], additions), file_count)
  list(
    stream = delta.sharing:::.native_snapshot_stream(
      prepared$guard,
      batch_size = 1L
    ),
    root = prepared$root,
    data_root = files$directory
  )
}

cdf_stream <- function(file_count = 2048L) {
  fixture <- file.path(fixtures, "delta", "cdf")
  source <- file.path(fixture, "b.parquet")
  files <- linked_files(
    source,
    file_count,
    "delta-sharing-interrupt-cdf-"
  )
  source_commit <- readLines(
    file.path(fixture, "_delta_log", "00000000000000000001.json"),
    warn = FALSE
  )
  size <- unname(file.info(source)$size)
  actions <- vapply(
    files$paths,
    function(path) {
      jsonlite::toJSON(
        list(cdc = list(
          path = fixture_uri(path),
          partitionValues = structure(list(), names = character()),
          size = size,
          dataChange = FALSE
        )),
        auto_unbox = TRUE,
        null = "null"
      )
    },
    character(1),
    USE.NAMES = FALSE
  )

  prepared <- private_log(character(), file_count)
  unlink(file.path(prepared$log, "00000000000000000000.json"))
  copied <- file.copy(
    file.path(
      fixture,
      "_delta_log",
      c("00000000000000000000.checkpoint.parquet", "_last_checkpoint")
    ),
    prepared$log,
    copy.mode = FALSE,
    copy.date = FALSE
  )
  stopifnot(all(copied))
  commit <- file.path(prepared$log, "00000000000000000001.json")
  writeLines(c(source_commit[1:2], actions), commit, useBytes = TRUE)
  Sys.setFileTime(
    commit,
    as.POSIXct(1734480105.872, origin = "1970-01-01", tz = "UTC")
  )
  guard_state <- prepared$guard$state
  guard_state$read_kind <- "cdf"
  guard_state$start_version <- 1
  guard_state$end_version <- 1

  list(
    stream = delta.sharing:::.native_cdf_stream(
      prepared$guard,
      start_version = 1,
      end_version = 1,
      batch_size = 1L
    ),
    root = prepared$root,
    data_root = files$directory
  )
}

start <- delta.sharing:::.native_diagnostics()
bundle <- switch(
  kind,
  synthetic = list(
    stream = delta.sharing:::.native_test_stream(
      batches = 10000L,
      rows_per_batch = 100L
    ),
    root = NULL,
    data_root = NULL
  ),
  direct = list(
    stream = delta.sharing:::.native_test_stream(
      batches = 10000L,
      rows_per_batch = 1000000L
    ),
    root = NULL,
    data_root = NULL
  ),
  arrow = list(
    stream = delta.sharing:::.native_test_stream(
      batches = 10000L,
      rows_per_batch = 100L
    ),
    root = NULL,
    data_root = NULL
  ),
  snapshot = snapshot_stream(),
  cdf = cdf_stream(),
  stop("Unknown interrupt fixture kind.", call. = FALSE)
)
stream <- bundle$stream
writeLines("ready", ready, useBytes = TRUE)
started <- proc.time()[["elapsed"]]
condition <- tryCatch(
  if (identical(kind, "direct")) {
    repeat {
      batch <- stream$get_next()
      if (is.null(batch)) {
        stop("Direct stream exhausted before interruption.", call. = FALSE)
      }
      nanoarrow::nanoarrow_pointer_release(batch)
    }
  } else if (identical(kind, "arrow")) {
    delta.sharing:::.materialize_arrow_stream(stream)
  } else {
    delta.sharing:::.materialize_data_frame_stream(stream)
  },
  delta_sharing_cancelled = identity,
  error = identity,
  interrupt = identity
)
elapsed <- proc.time()[["elapsed"]] - started
after <- delta.sharing:::.native_diagnostics()
root_exists <- !is.null(bundle$root) && file.exists(bundle$root)
pointer_valid <- nanoarrow::nanoarrow_pointer_is_valid(stream)
try(stream$release(), silent = TRUE)
final <- delta.sharing:::.native_diagnostics()
if (!is.null(bundle$data_root)) {
  unlink(bundle$data_root, recursive = TRUE, force = TRUE)
}

condition_is_condition <- inherits(condition, "condition")
saveRDS(
  list(
    classes = class(condition),
    message = if (condition_is_condition) {
      conditionMessage(condition)
    } else {
      "Materialization completed before interruption."
    },
    operation = if (condition_is_condition) condition$operation else NULL,
    active_delta = after$active_streams - start$active_streams,
    cancelled_delta =
      after$cancelled_streams - start$cancelled_streams,
    pending = after$pending_cleanups,
    final_active_delta =
      final$active_streams - start$active_streams,
    final_cancelled_delta =
      final$cancelled_streams - start$cancelled_streams,
    root_exists = root_exists,
    pointer_valid = pointer_valid,
    elapsed = elapsed
  ),
  result_path
)
