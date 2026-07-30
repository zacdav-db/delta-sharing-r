# Measure one public read path in a fresh process. Use `/usr/bin/time -l`
# around this worker to capture the process peak RSS alongside its sampled RSS.
#
# Usage:
#   Rscript tools/profile_live_read_path.R \
#     PROFILE TABLE LIMIT METHOD OUTPUT
#
# METHOD is baseline, stream, reader, arrow, data-frame, or duckdb.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 5L) {
  stop(
    paste(
      "Usage: profile_live_read_path.R",
      "PROFILE TABLE LIMIT METHOD OUTPUT"
    ),
    call. = FALSE
  )
}

profile_path <- fs::path_expand(args[[1L]])
table_name <- args[[2L]]
limit <- suppressWarnings(as.numeric(args[[3L]]))
method <- args[[4L]]
output_path <- fs::path_abs(args[[5L]])
methods <- c("baseline", "stream", "reader", "arrow", "data-frame", "duckdb")

if (
  !fs::file_exists(profile_path) ||
    !nzchar(table_name) ||
    !rlang::is_scalar_integerish(limit, finite = TRUE) ||
    limit < 0 ||
    !method %in% methods
) {
  stop("The profile, table, limit, or method is invalid.", call. = FALSE)
}

benchmark_library <- Sys.getenv(
  "DELTA_SHARING_BENCHMARK_LIBRARY",
  unset = NA_character_
)
if (is.na(benchmark_library)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  benchmark_library <- as.character(
    fs::path_real(fs::path_expand(benchmark_library))
  )
  .libPaths(c(benchmark_library, .libPaths()))
  suppressPackageStartupMessages(library(delta.sharing))
  installed_path <- as.character(fs::path_real(find.package("delta.sharing")))
  if (!identical(as.character(fs::path_dir(installed_path)), benchmark_library)) {
    stop("The benchmark did not load delta.sharing from its test library.")
  }
}

elapsed <- function(code) {
  started <- proc.time()[["elapsed"]]
  value <- force(code)
  list(
    value = value,
    seconds = unname(proc.time()[["elapsed"]] - started)
  )
}

# Sample the current process RSS without reporting command output or paths.
current_rss_bytes <- function() {
  rss_kib <- suppressWarnings(as.numeric(trimws(system2(
    "/bin/ps",
    c("-o", "rss=", "-p", as.character(Sys.getpid())),
    stdout = TRUE
  ))))
  if (length(rss_kib) != 1L || !is.finite(rss_kib)) {
    return(NA_real_)
  }
  rss_kib * 1024
}

drain_stream <- function(stream, first = NULL) {
  rows <- if (is.null(first)) 0 else first$length
  batches <- if (is.null(first)) 0L else 1L
  repeat {
    batch <- stream$get_next()
    if (is.null(batch)) {
      break
    }
    rows <- rows + batch$length
    batches <- batches + 1L
  }
  list(rows = rows, batches = batches)
}

result <- local({
  rss_loaded <- current_rss_bytes()
  if (identical(method, "baseline")) {
    return(list(
      rows = 0,
      batches = 0L,
      total_seconds = 0,
      rss = list(package_loaded = rss_loaded)
    ))
  }

  table <- sharing_client(profile_path)$table(table_name)
  snapshot <- table$snapshot(limit = limit, response_format = "delta")
  rss_descriptor <- current_rss_bytes()
  started <- proc.time()[["elapsed"]]

  if (identical(method, "stream")) {
    construction <- elapsed(snapshot$to_arrow_stream())
    stream <- construction$value
    withr::defer(delta.sharing:::release_materializer_stream(stream))
    rss_constructed <- current_rss_bytes()
    Sys.sleep(1)
    rss_idle <- current_rss_bytes()
    first <- elapsed(stream$get_next())
    rss_first_batch <- current_rss_bytes()
    drained <- drain_stream(stream, first$value)
    delta.sharing:::release_materializer_stream(stream)
    return(c(
      drained,
      list(
        construction_seconds = construction$seconds,
        first_batch_seconds = first$seconds,
        total_seconds = unname(proc.time()[["elapsed"]] - started),
        rss = list(
          package_loaded = rss_loaded,
          descriptor = rss_descriptor,
          stream_constructed = rss_constructed,
          after_one_second_idle = rss_idle,
          first_batch = rss_first_batch,
          exhausted = current_rss_bytes()
        )
      )
    ))
  }

  if (identical(method, "reader")) {
    construction <- elapsed(snapshot$to_arrow_reader())
    reader <- construction$value
    withr::defer(try(reader$Close(), silent = TRUE))
    first <- elapsed(reader$read_next_batch())
    remaining <- elapsed(reader$read_table())
    first_rows <- if (is.null(first$value)) 0 else first$value$num_rows
    reader$Close()
    return(list(
      rows = first_rows + remaining$value$num_rows,
      batches = NULL,
      construction_seconds = construction$seconds,
      first_batch_seconds = first$seconds,
      remaining_seconds = remaining$seconds,
      total_seconds = unname(proc.time()[["elapsed"]] - started),
      rss = list(
        package_loaded = rss_loaded,
        descriptor = rss_descriptor,
        exhausted = current_rss_bytes()
      )
    ))
  }

  if (identical(method, "arrow")) {
    materialized <- elapsed(snapshot$to_arrow())
    return(list(
      rows = materialized$value$num_rows,
      batches = NULL,
      total_seconds = materialized$seconds,
      rss = list(
        package_loaded = rss_loaded,
        descriptor = rss_descriptor,
        materialized = current_rss_bytes()
      )
    ))
  }

  if (identical(method, "data-frame")) {
    materialized <- elapsed(snapshot$to_data_frame())
    return(list(
      rows = nrow(materialized$value),
      batches = NULL,
      total_seconds = materialized$seconds,
      rss = list(
        package_loaded = rss_loaded,
        descriptor = rss_descriptor,
        materialized = current_rss_bytes()
      )
    ))
  }

  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE)) {
    stop("The duckdb method requires the DBI and duckdb packages.")
  }
  construction <- elapsed(snapshot$to_arrow_reader())
  reader <- construction$value
  withr::defer(try(reader$Close(), silent = TRUE))
  connection <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))
  DBI::dbExecute(connection, "SET threads = 1")
  duckdb::duckdb_register_arrow(connection, "shared_rows", reader)
  withr::defer(
    duckdb::duckdb_unregister_arrow(connection, "shared_rows")
  )
  query <- elapsed(DBI::dbGetQuery(
    connection,
    "select count(*) as rows from shared_rows"
  ))
  rows <- as.numeric(query$value$rows[[1L]])
  list(
    rows = rows,
    batches = NULL,
    construction_seconds = construction$seconds,
    query_seconds = query$seconds,
    total_seconds = unname(proc.time()[["elapsed"]] - started),
    rss = list(
      package_loaded = rss_loaded,
      descriptor = rss_descriptor,
      queried = current_rss_bytes()
    )
  )
})

pending_cleanups <- delta.sharing:::native_reap_pending_cleanups()
if (
  !identical(method, "baseline") &&
    !identical(as.numeric(result$rows), as.numeric(limit))
) {
  stop("The profiled read did not return the exact requested limit.")
}
if (!identical(as.numeric(pending_cleanups), 0)) {
  stop("The profiled read left a native cleanup pending.")
}

artifact <- list(
  schema_version = 1L,
  package_version = as.character(packageVersion("delta.sharing")),
  table = table_name,
  method = method,
  limit = limit,
  result = result,
  pending_cleanups = pending_cleanups
)
fs::dir_create(fs::path_dir(output_path))
jsonlite::write_json(
  artifact,
  output_path,
  auto_unbox = TRUE,
  null = "null",
  pretty = TRUE
)
cat(jsonlite::toJSON(artifact, auto_unbox = TRUE, null = "null"), "\n")
