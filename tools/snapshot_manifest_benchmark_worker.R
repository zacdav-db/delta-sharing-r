# Compare whole-manifest retention with bounded snapshot action staging.
#
# Run each mode in a fresh process under the platform RSS tool, for example:
#   /usr/bin/time -l Rscript tools/snapshot_manifest_benchmark_worker.R \
#     retained 100000
#   /usr/bin/time -l Rscript tools/snapshot_manifest_benchmark_worker.R \
#     staged 100000

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L || !args[[1L]] %in% c("retained", "staged")) {
  stop("Usage: snapshot_manifest_benchmark_worker.R MODE FILES", call. = FALSE)
}
mode <- args[[1L]]
file_count <- suppressWarnings(as.integer(args[[2L]]))
if (is.na(file_count) || file_count < 0L) {
  stop("FILES must be a non-negative integer.", call. = FALSE)
}

json_line <- function(value) {
  as.character(jsonlite::toJSON(
    value,
    auto_unbox = TRUE,
    null = "null",
    digits = NA,
    pretty = FALSE
  ))
}

snapshot_file_action <- function(index) {
  add <- list(
    path = sprintf(
      "https://storage.example.test/part-%08d.parquet?signature=benchmark",
      index
    ),
    partitionValues = list(day = sprintf("%02d", index %% 31L + 1L)),
    size = 1048576,
    modificationTime = 0,
    dataChange = TRUE,
    stats = sprintf('{"numRecords":1000,"minValues":{"id":%d}}', index)
  )
  if (index %% 10L == 0L) {
    add$deletionVector <- list(
      storageType = "i",
      pathOrInlineDv = "benchmark-inline-vector",
      offset = 1,
      sizeInBytes = 64,
      cardinality = 2
    )
  }
  list(add = add)
}

header <- c(
  json_line(list(
    protocol = list(
      minReaderVersion = 3,
      minWriterVersion = 7,
      readerFeatures = list("deletionVectors")
    )
  )),
  json_line(list(
    metaData = list(
      id = "benchmark",
      format = list(
        provider = "parquet",
        options = structure(
          list(),
          names = character()
        )
      ),
      schemaString = "{\"type\":\"struct\",\"fields\":[]}",
      partitionColumns = list("day"),
      configuration = structure(list(), names = character())
    )
  ))
)

commit <- fs::file_temp(pattern = "delta-sharing-manifest-", ext = ".json")
on.exit(
  {
    if (fs::file_exists(commit)) {
      fs::file_delete(commit)
    }
  },
  add = TRUE
)

started <- proc.time()[["elapsed"]]
if (identical(mode, "retained")) {
  actions <- purrr::map(seq_len(file_count), snapshot_file_action)
  lines <- c(header, purrr::map_chr(actions, json_line))
  writeLines(lines, commit, useBytes = TRUE)
} else {
  stage <- fs::file_temp(pattern = "delta-sharing-manifest-stage-")
  on.exit(
    {
      if (fs::file_exists(stage)) {
        fs::file_delete(stage)
      }
    },
    add = TRUE
  )
  local({
    output <- file(stage, open = "wb")
    on.exit(close(output), add = TRUE)
    if (file_count > 0L) {
      starts <- seq.int(1L, file_count, by = 256L)
      purrr::walk(starts, function(start) {
        end <- min(start + 255L, file_count)
        lines <- purrr::map_chr(
          seq.int(start, end),
          function(index) json_line(snapshot_file_action(index))
        )
        writeLines(lines, output, useBytes = TRUE)
      })
    }
  })
  local({
    output <- file(commit, open = "wb")
    on.exit(close(output), add = TRUE)
    input <- file(stage, open = "rb")
    on.exit(close(input), add = TRUE)
    writeLines(header, output, useBytes = TRUE)
    repeat {
      bytes <- readBin(input, what = "raw", n = 1024 * 1024)
      if (length(bytes) == 0L) {
        break
      }
      writeBin(bytes, output)
    }
  })
}
elapsed <- proc.time()[["elapsed"]] - started

result <- list(
  mode = mode,
  files = file_count,
  elapsed_seconds = unname(elapsed),
  commit_bytes = as.numeric(fs::file_size(commit)),
  commit_md5 = unname(tools::md5sum(commit))
)
cat(jsonlite::toJSON(result, auto_unbox = TRUE, pretty = TRUE), "\n")
