# End-to-end read behaviour through the real Delta Kernel native scan. These
# use local Delta table fixtures (a real `_delta_log` + parquet files), so they
# exercise the native reader, Arrow C stream, and nanoarrow conversion without a
# network mock. This is the layer unit tests with httr2 mocks cannot cover.

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
  expect_match(capture.output(print(stream)), "invalid pointer")
})

test_that("data-frame materialization exhausts its native stream", {
  stream <- native_test_stream(batches = 3L, rows_per_batch = 2L)

  df <- sharing_stream_to_data_frame(stream)

  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 6L)
  expect_match(capture.output(print(stream)), "invalid pointer")
})

test_that("data-frame materialization preserves typed stream failures", {
  stream <- native_test_stream(
    batches = 3L,
    rows_per_batch = 2L,
    error_after = 1L
  )

  expect_error(
    sharing_stream_to_data_frame(stream),
    class = "delta_sharing_kernel_error"
  )
})

test_that("the native stream boundary translates user interrupts", {
  stream <- native_test_stream()
  interrupt <- structure(
    list(message = "simulated user interrupt"),
    class = c("interrupt", "condition")
  )

  expect_error(
    with_native_stream_conditions(
      signalCondition(interrupt),
      operation = "read_arrow_stream",
      stream = stream
    ),
    class = "delta_sharing_cancelled"
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

test_that("Arrow materialization exhausts its native stream", {
  skip_if_not_installed("arrow")
  stream <- native_snapshot_stream(
    fixture_table("local-table"),
    batch_size = 2L
  )

  table <- sharing_stream_to_arrow(stream)

  expect_s3_class(table, "Table")
  expect_equal(nrow(table), 7L)
  expect_match(capture.output(print(stream)), "invalid pointer")
})

test_that("Arrow materialization preserves typed stream failures", {
  skip_if_not_installed("arrow")
  stream <- native_test_stream(
    batches = 3L,
    rows_per_batch = 2L,
    error_after = 1L
  )

  expect_error(
    sharing_stream_to_arrow(stream),
    class = "delta_sharing_kernel_error"
  )
})

test_that("projection selects and orders columns", {
  stream <- native_snapshot_stream(
    fixture_table("local-table"),
    columns = c("active", "id")
  )
  df <- sharing_stream_to_data_frame(stream)
  expect_equal(names(df), c("active", "id"))
})

test_that("partition-only projection does not require a visible data column", {
  stream <- native_snapshot_stream(
    fixture_table("timestamp-ntz"),
    columns = "region"
  )
  df <- sharing_stream_to_data_frame(stream)

  expect_identical(names(df), "region")
  expect_gt(nrow(df), 0L)
  expect_true(all(df$region == "emea"))
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

test_that("native scan validation rejects unsafe or ambiguous inputs", {
  purrr::walk(
    list(NULL, character(), 42),
    function(location) {
      expect_error(
        validate_native_location(location),
        class = "delta_sharing_validation_error"
      )
    }
  )
  expect_error(
    validate_native_location("https://storage.example.test/table"),
    class = "delta_sharing_validation_error"
  )
  expect_equal(
    validate_native_location(paste0("file://", fixture_table("local-table"))),
    paste0("file://", fixture_table("local-table"))
  )

  expect_error(
    validate_native_columns(c("ID", "id")),
    class = "delta_sharing_validation_error"
  )
  purrr::walk(
    list(0, MAX_BATCH_SIZE + 1, 1.5, Inf),
    function(size) {
      expect_error(
        validate_native_batch_size(size),
        class = "delta_sharing_validation_error"
      )
    }
  )
  expect_identical(validate_native_batch_size(1024), 1024L)
})

test_that("native CDF versions and ranges are validated", {
  purrr::walk(
    list(-1, 1.5, Inf, NA_real_),
    function(version) {
      expect_error(
        cdf_whole_version(version, "version"),
        class = "delta_sharing_validation_error"
      )
    }
  )
  expect_identical(cdf_whole_version(2L, "version"), 2)
  expect_error(
    native_cdf_stream(fixture_table("cdf"), 2, 1),
    class = "delta_sharing_validation_error"
  )
})

test_that("native CDF reads the local change fixture", {
  stream <- native_cdf_stream(
    fixture_table("cdf"),
    start_version = 1,
    end_version = 2,
    columns = c("id", "_change_type")
  )
  changes <- sharing_stream_to_data_frame(stream)

  expect_gt(nrow(changes), 0L)
  expect_identical(names(changes), c("id", "_change_type"))
  expect_setequal(unique(changes$`_change_type`), c("delete", "insert"))
})

test_that("native condition translation releases streams and redacts failures", {
  typed_stream <- native_test_stream()
  expect_error(
    with_native_stream_conditions(
      abort("bad protocol", type = "protocol"),
      operation = "read_arrow_stream",
      stream = typed_stream
    ),
    class = "delta_sharing_protocol_error"
  )

  interrupted_stream <- native_test_stream()
  expect_error(
    with_native_stream_conditions(
      stop(native_stream_interrupt_message),
      operation = "read_arrow_stream",
      stream = interrupted_stream
    ),
    class = "delta_sharing_cancelled"
  )

  failed_stream <- native_test_stream()
  expect_error(
    with_native_stream_conditions(
      stop("internal implementation detail"),
      operation = "read_arrow_stream",
      stream = failed_stream
    ),
    class = "delta_sharing_kernel_error"
  )

  original <- simpleError("construction failed")
  expect_error(
    with_native_stream_conditions(
      stop(original),
      operation = "read_arrow_stream"
    ),
    "construction failed",
    fixed = TRUE
  )
})

test_that("interruptible streams preserve non-pull methods", {
  stream <- native_test_stream()
  withr::defer(release_materializer_stream(stream))

  expect_type(stream$get_schema, "closure")
  expect_true(native_stream_was_interrupted(
    simpleError(native_stream_interrupt_message)
  ))
  expect_false(native_stream_was_interrupted(simpleError("other")))
  expect_no_error(native_reap_pending_cleanups())
})

test_that("public snapshot readers materialize through mocked local files", {
  httr2::local_mocked_responses(function(req) {
    httr2::response(
      200,
      headers = list(`content-type` = "application/x-ndjson"),
      body = charToRaw(ndjson_body(local_snapshot_actions()))
    )
  })
  table <- test_client()$table("sales.default.local")

  data <- table$snapshot(
    limit = 4,
    response_format = "delta"
  )$to_data_frame(batch_size = 2L)
  arrow_table <- table$snapshot(
    limit = 3,
    response_format = "delta"
  )$to_arrow(batch_size = 2L)

  expect_equal(nrow(data), 4L)
  expect_s3_class(arrow_table, "Table")
  expect_equal(nrow(arrow_table), 3L)
})

test_that("public CDF readers paginate and materialize local change files", {
  actions <- local_cdf_actions()
  page <- 0L
  httr2::local_mocked_responses(function(req) {
    page <<- page + 1L
    page_actions <- if (page == 1L) {
      c(actions[seq_len(3L)], list(list(nextPageToken = "second")))
    } else {
      actions[-seq_len(3L)]
    }
    httr2::response(
      200,
      headers = list(`content-type` = "application/x-ndjson"),
      body = charToRaw(ndjson_body(page_actions))
    )
  })
  changes <- test_client()$
    table("sales.default.changes")$
    changes(
      starting_version = 1,
      ending_version = 2,
      response_format = "delta"
    )$
    to_data_frame()

  expect_identical(page, 2L)
  expect_gt(nrow(changes), 0L)
  expect_true(all(c(
    "_change_type",
    "_commit_version",
    "_commit_timestamp"
  ) %in% names(changes)))
})

test_that("parquet CDF is rejected before any request", {
  changes <- test_client()$
    table("sales.default.changes")$
    changes(starting_version = 1, response_format = "parquet")

  expect_error(
    changes$to_arrow_stream(),
    class = "delta_sharing_unsupported_error"
  )
})
