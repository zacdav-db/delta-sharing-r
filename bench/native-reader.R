# Rscript bench/native-reader.R mode local_table [repetitions] [batch_size]
# Modes: stream, tibble, arrow-close. Use a separate process for each mode.
args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) >= 2L)
mode <- match.arg(args[[1]], c("stream", "tibble", "arrow-close"))
table <- normalizePath(args[[2]], mustWork = TRUE)
repetitions <- if (length(args) >= 3L) as.integer(args[[3]]) else 10L
batch_size <- if (length(args) >= 4L) as.integer(args[[4]]) else 65536L
stopifnot(!is.na(repetitions), repetitions > 0L)
open <- getFromNamespace("native_snapshot_stream", "delta.sharing")
materialize <- getFromNamespace("sharing_stream_to_tibble", "delta.sharing")
release <- getFromNamespace("release_materializer_stream", "delta.sharing")
if (mode == "arrow-close") {
  stopifnot(requireNamespace("arrow", quietly = TRUE))
  options(arrow.use_threads = TRUE)
}
rss <- function() {
  if (.Platform$OS.type != "unix") return(NA_real_)
  as.numeric(system2(
    "ps",
    c("-o", "rss=", "-p", Sys.getpid()),
    stdout = TRUE
  )) /
    1024
}
clock <- function() unname(proc.time()[["elapsed"]])
results <- lapply(seq_len(repetitions), function(iteration) {
  gc()
  before <- rss()
  start <- clock()
  stream <- open(table, batch_size = batch_size)
  opened <- clock()
  first <- NA_real_
  rows <- 0
  if (mode == "stream") {
    batch <- stream$get_next()
    first <- clock() - start
    while (!is.null(batch)) {
      rows <- rows + batch$length
      batch <- stream$get_next()
    }
    release(stream)
  } else if (mode == "tibble") {
    result <- materialize(stream)
    rows <- nrow(result)
    rm(result)
  } else {
    reader <- arrow::RecordBatchReader$import_from_c(stream)
    batch <- reader$read_next_batch()
    first <- clock() - start
    rows <- if (is.null(batch)) 0 else batch$num_rows
    reader$Close()
    rm(reader, batch)
  }
  finished <- clock()
  consumed <- rss()
  rm(stream)
  gc()
  data.frame(
    iteration = iteration,
    mode = mode,
    rows = rows,
    open_seconds = opened - start,
    first_batch_seconds = first,
    total_seconds = finished - start,
    rss_before_MiB = before,
    rss_consumed_MiB = consumed,
    rss_after_gc_MiB = rss()
  )
})
write.csv(do.call(rbind, results), stdout(), row.names = FALSE)
