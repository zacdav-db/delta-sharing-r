# End-to-end read behaviour through the real Delta Kernel native scan. These
# use local Delta table fixtures (a real `_delta_log` + parquet files), so they
# exercise the native reader, Arrow C stream, and nanoarrow conversion without a
# network mock. This is the layer unit tests with httr2 mocks cannot cover.

fixture_table <- function(name) {
  path <- test_path("fixtures", "delta", name)
  as.character(fs::path_real(path))
}

test_that("kernel reads a local table to a data frame", {
  stream <- native_snapshot_stream(fixture_table("local-table"))
  df <- sharing_stream_to_data_frame(stream)

  expect_s3_class(df, "data.frame")
  expect_equal(names(df), c("id", "group", "value", "active"))
  # two parquet files (3 + 4 rows) read as one table
  expect_equal(nrow(df), 7L)
  expect_type(df$id, "double")
  expect_type(df$group, "character")
  expect_type(df$active, "logical")
  # row content survives the round trip
  expect_true(1 %in% df$id)
  expect_true("alpha" %in% df$group)
})

test_that("data-frame materialization can report read progress", {
  withr::local_options(list(cli.progress_show_after = Inf))
  stream <- native_test_stream(batches = 3L, rows_per_batch = 2L)

  df <- sharing_stream_to_data_frame(stream, progress = TRUE)

  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 6L)
})

test_that("read progress preserves typed stream failures", {
  withr::local_options(list(cli.progress_show_after = Inf))
  stream <- native_test_stream(
    batches = 3L,
    rows_per_batch = 2L,
    error_after = 1L
  )

  expect_error(
    sharing_stream_to_data_frame(stream, progress = TRUE),
    class = "delta_sharing_kernel_error"
  )
})

test_that("a reader exposes the kernel stream as an Arrow reader", {
  skip_if_not_installed("arrow")

  LocalSharingReader <- R6::R6Class(
    "LocalSharingReader",
    inherit = SharingReader,
    cloneable = FALSE,
    private = list(
      open_stream = function(batch_size) {
        native_snapshot_stream(
          fixture_table("local-table"),
          batch_size = batch_size
        )
      }
    )
  )

  reader <- LocalSharingReader$new()$to_arrow_reader(batch_size = 2L)
  withr::defer(reader$Close())

  expect_s3_class(reader, "RecordBatchReader")
  expect_equal(nrow(reader$read_table()), 7L)
})

test_that("Arrow materialization can report read progress", {
  skip_if_not_installed("arrow")
  withr::local_options(list(cli.progress_show_after = Inf))
  stream <- native_snapshot_stream(
    fixture_table("local-table"),
    batch_size = 2L
  )

  table <- sharing_stream_to_arrow(stream, progress = TRUE)

  expect_s3_class(table, "Table")
  expect_equal(nrow(table), 7L)
})

test_that("projection selects and orders columns", {
  stream <- native_snapshot_stream(
    fixture_table("local-table"),
    columns = c("active", "id")
  )
  df <- sharing_stream_to_data_frame(stream)
  expect_equal(names(df), c("active", "id"))
})

test_that("limit is enforced exactly by the kernel scan", {
  stream <- native_snapshot_stream(fixture_table("local-table"), limit = 4)
  df <- sharing_stream_to_data_frame(stream)
  expect_equal(nrow(df), 4L)
})

test_that("logical types round-trip through the kernel", {
  stream <- native_snapshot_stream(fixture_table("logical-types"))
  df <- sharing_stream_to_data_frame(stream)
  expect_s3_class(df, "data.frame")
  expect_gt(nrow(df), 0L)
})

test_that("an invalid table location raises a typed condition", {
  expect_error(
    native_snapshot_stream(fs::file_temp()),
    class = "delta_sharing_validation_error"
  )
})
