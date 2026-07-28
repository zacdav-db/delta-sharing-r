library(delta.sharing)

pull_once <- function() {
  stream <- delta.sharing:::.native_test_stream(1000L, 1024L)
  rows <- 0
  elapsed <- system.time(repeat {
    batch <- stream$get_next()
    if (is.null(batch)) {
      break
    }
    rows <- rows + batch$length
  })[["elapsed"]]
  stream$release()

  c(
    rows = rows,
    seconds = elapsed,
    rows_per_second = rows / elapsed,
    batches_per_second = 1000 / elapsed
  )
}

materialize_once <- function() {
  stream <- delta.sharing:::.native_test_stream(100L, 4096L)
  elapsed <- system.time({
    reader <- arrow::as_record_batch_reader(stream)
    table <- reader$read_table()
  })[["elapsed"]]
  rows <- table$num_rows
  rm(table, reader, stream)
  gc()

  c(
    rows = rows,
    seconds = elapsed,
    rows_per_second = rows / elapsed,
    batches_per_second = 100 / elapsed
  )
}

cat("nanoarrow_pull\n")
print(t(replicate(3, pull_once())))
cat("arrow_materialize\n")
print(t(replicate(3, materialize_once())))
