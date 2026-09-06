test_that("R materializers accept lossless character prototypes for exact integers", {
  skip_if_not_installed("arrow")
  values <- c(
    "9007199254740992",
    "9007199254740993",
    NA_character_,
    "-9007199254740992",
    "-9007199254740993",
    "9223372036854775807",
    "-9223372036854775808"
  )
  exact <- arrow::Array$create(
    values,
    type = arrow::utf8()
  )$cast(arrow::int64())
  LocalReader <- R6::R6Class(
    "Integer64Reader",
    inherit = SharingReader,
    cloneable = FALSE,
    private = list(
      open_stream = function(batch_size) {
        batches <- lapply(seq_along(values), function(i) {
          nanoarrow::as_nanoarrow_array(
            arrow::RecordBatch$create(id = exact$Slice(i - 1L, 1L))
          )
        })
        nanoarrow::basic_array_stream(batches)
      }
    )
  )
  reader <- LocalReader$new()
  to <- data.frame(id = character())
  for (method in c("to_tibble", "to_data_frame")) {
    result <- reader[[method]](batch_size = 1L, to = to)
    expect_type(result$id, "character")
    expect_identical(as.character(result$id), values)
    expect_identical(inherits(result, "tbl_df"), method == "to_tibble")
  }
  result <- reader$to_tibble(to = function(schema, default) to)
  expect_identical(as.character(result$id), values)

  # The default is deliberately unchanged; callers choose exact conversion.
  default <- suppressWarnings(reader$to_tibble())
  expect_type(default$id, "double")
  expect_identical(default$id[[1]], default$id[[2]])
})

test_that("conversion prototypes work for empty results and release on errors", {
  to <- data.frame(id = character())
  stream <- nanoarrow::basic_array_stream(list(to))
  result <- sharing_stream_to_tibble(stream, to = to)
  expect_type(result$id, "character")
  expect_equal(nrow(result), 0L)
  expect_match(capture.output(print(stream)), "invalid pointer")

  stream <- nanoarrow::basic_array_stream(list(data.frame(id = 1)))
  expect_error(
    sharing_stream_to_tibble(stream, to = function(schema, default) {
      stop("invalid prototype")
    }),
    "invalid prototype",
    fixed = TRUE
  )
  expect_match(capture.output(print(stream)), "invalid pointer")
})


test_that("exact character conversion works through the native materializer", {
  skip_if_not_installed("arrow")
  values <- c(
    "9007199254740992",
    "9007199254740993",
    NA_character_,
    "-9007199254740992",
    "-9007199254740993",
    "9223372036854775807",
    "-9223372036854775808"
  )
  root <- withr::local_tempdir()
  data_path <- fs::path(root, "part.parquet")
  exact <- arrow::Array$create(
    values,
    type = arrow::utf8()
  )$cast(arrow::int64())
  arrow::write_parquet(arrow::Table$create(id = exact), data_path)
  metadata <- fixture_commit_actions("local-table", 0L)[[2]]
  schema <- jsonlite::fromJSON(
    metadata$metaData$schemaString,
    simplifyVector = FALSE
  )
  schema$fields <- Filter(function(field) field$name == "id", schema$fields)
  schema$fields[[1]]$nullable <- TRUE
  metadata$metaData$schemaString <- log_json_line(schema)
  fs::dir_create(fs::path(root, "_delta_log"))
  writeLines(
    ndjson_body(list(
      list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
      metadata,
      list(
        add = list(
          path = "part.parquet",
          partitionValues = as_json_map(NULL),
          size = as.numeric(fs::file_size(data_path)),
          modificationTime = 0,
          dataChange = TRUE
        )
      )
    )),
    fs::path(root, "_delta_log", "00000000000000000000.json")
  )
  NativeReader <- R6::R6Class(
    "ExactNativeReader",
    inherit = SharingReader,
    cloneable = FALSE,
    private = list(open_stream = function(batch_size) {
      native_snapshot_stream(root, columns = "id", batch_size = batch_size)
    })
  )
  for (batch_size in c(1L, 65536L)) {
    for (method in c("to_tibble", "to_data_frame")) {
      result <- NativeReader$new()[[method]](
        batch_size = batch_size,
        to = data.frame(id = character())
      )
      expect_identical(result$id, values)
    }
  }
})
