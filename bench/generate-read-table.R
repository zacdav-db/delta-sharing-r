# Rscript bench/generate-read-table.R new_directory [rows]
# Creates a disposable, uncompressed eight-double-column Delta table.
args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) >= 1L, !dir.exists(args[[1]]))
root <- args[[1]]
rows <- if (length(args) >= 2L) as.integer(args[[2]]) else 1000000L
stopifnot(!is.na(rows), rows > 0L, requireNamespace("arrow", quietly = TRUE))
dir.create(file.path(root, "_delta_log"), recursive = TRUE)
values <- setNames(lapply(1:8, function(i) seq_len(rows) / i), paste0("x", 1:8))
arrow::write_parquet(
  arrow::Table$create(as.data.frame(values)),
  file.path(root, "part.parquet"),
  compression = "uncompressed",
  use_dictionary = FALSE,
  write_statistics = FALSE,
  chunk_size = 65536L
)
empty <- structure(list(), names = character())
fields <- lapply(names(values), function(name) {
  list(name = name, type = "double", nullable = TRUE, metadata = empty)
})
schema <- as.character(jsonlite::toJSON(
  list(type = "struct", fields = fields),
  auto_unbox = TRUE
))
actions <- list(
  list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
  list(
    metaData = list(
      id = "read-phase-benchmark",
      format = list(provider = "parquet", options = empty),
      schemaString = schema,
      partitionColumns = list(),
      configuration = empty
    )
  ),
  list(
    add = list(
      path = "part.parquet",
      size = file.info(file.path(root, "part.parquet"))$size,
      partitionValues = empty,
      dataChange = TRUE,
      modificationTime = 0
    )
  )
)
writeLines(
  vapply(
    actions,
    function(action) {
      as.character(jsonlite::toJSON(action, auto_unbox = TRUE))
    },
    character(1)
  ),
  file.path(root, "_delta_log", "00000000000000000000.json")
)
cat(normalizePath(root), "\n")
