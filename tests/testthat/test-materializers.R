materializer_delta_fixture <- function() {
  normalizePath(
    test_path("fixtures", "delta", "local-table"),
    winslash = "/",
    mustWork = TRUE
  )
}

materializer_execution_interface <- function(recorder) {
  delta.sharing:::.new_execution_interface(list(
    read_arrow_stream = function(
      specification,
      batch_size = NULL,
      concurrency = NULL
    ) {
      recorder$scans <- if (is.null(recorder$scans)) {
        1L
      } else {
        recorder$scans + 1L
      }
      recorder$options <- c(
        recorder$options,
        list(list(
          batch_size = batch_size,
          concurrency = concurrency
        ))
      )
      delta.sharing:::.native_snapshot_stream(
        materializer_delta_fixture(),
        columns = specification@columns,
        limit = specification@limit,
        batch_size = if (is.null(batch_size)) 65536L else batch_size
      )
    },
    arrow_from_stream = delta.sharing:::.materialize_arrow_stream,
    data_frame_from_stream =
      delta.sharing:::.materialize_data_frame_stream
  ))
}

materializer_read <- function(columns = NULL, limit = NULL) {
  sharing_read(
    test_table(),
    columns = columns,
    limit = limit
  )
}

test_that("eager adapters each consume exactly one real Kernel stream", {
  skip_if_not_installed("arrow")
  gc()
  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  recorder <- new.env(parent = emptyenv())
  interface <- materializer_execution_interface(recorder)
  read <- materializer_read(columns = c("group", "id"))

  delta.sharing:::.with_execution_interface(interface, {
    arrow_table <- read_arrow(read, batch_size = 2L)
    data <- read_data_frame(read, batch_size = 2L)
    via_base <- as.data.frame(read, batch_size = 2L)
  })

  expect_identical(recorder$scans, 3L)
  expect_identical(
    vapply(recorder$options, `[[`, integer(1), "batch_size"),
    c(2L, 2L, 2L)
  )
  expect_s3_class(arrow_table, "Table")
  expect_identical(arrow_table$schema$names, c("group", "id"))
  expect_identical(names(data), c("group", "id"))
  expect_identical(names(via_base), c("group", "id"))
  expect_identical(nrow(data), 7L)
  expect_identical(nrow(via_base), 7L)

  arrow_data <- as.data.frame(arrow_table)
  normalize_rows <- function(x) {
    x$id <- as.numeric(x$id)
    x[order(x$id), c("group", "id"), drop = FALSE]
  }
  expect_equal(
    unname(normalize_rows(arrow_data)),
    unname(normalize_rows(data))
  )
  expect_equal(
    unname(normalize_rows(via_base)),
    unname(normalize_rows(data))
  )
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before
  )
})

test_that("data-frame materialization handles empty and multiple batches", {
  gc()
  start <- delta.sharing:::.native_diagnostics()$active_streams

  empty <- delta.sharing:::.native_snapshot_stream(
    materializer_delta_fixture(),
    columns = c("id", "active"),
    limit = 0,
    batch_size = 2L
  )
  empty_data <- delta.sharing:::.materialize_data_frame_stream(empty)
  expect_s3_class(empty_data, "data.frame")
  expect_identical(names(empty_data), c("id", "active"))
  expect_identical(nrow(empty_data), 0L)
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(empty))

  multiple <- delta.sharing:::.native_snapshot_stream(
    materializer_delta_fixture(),
    columns = c("id", "active"),
    batch_size = 2L
  )
  multiple_data <- delta.sharing:::.materialize_data_frame_stream(multiple)
  expect_identical(names(multiple_data), c("id", "active"))
  expect_identical(nrow(multiple_data), 7L)
  expect_identical(
    multiple_data$active,
    c(TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, TRUE)
  )
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(multiple))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    start
  )
})

test_that("Arrow materialization preserves rich Arrow schema without IPC", {
  skip_if_not_installed("arrow")
  decimal <- arrow::Array$create(
    c(12.34, NA),
    type = arrow::decimal128(10, 2)
  )
  event_time <- arrow::Array$create(
    as.POSIXct(c("2026-01-01", "2026-01-02"), tz = "UTC"),
    type = arrow::timestamp("us", "UTC")
  )
  nested <- arrow::Array$create(
    data.frame(label = c("a", "b"), score = 1:2),
    type = arrow::struct(
      label = arrow::utf8(),
      score = arrow::int32()
    )
  )
  batch <- arrow::record_batch(
    decimal = decimal,
    event_time = event_time,
    nested = nested
  )
  reader <- arrow::RecordBatchReader$create(batches = list(batch))
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  reader$export_to_c(stream)

  table <- delta.sharing:::.materialize_arrow_stream(stream)
  schema <- table$schema$ToString()

  expect_identical(table$num_rows, 2L)
  expect_identical(
    table$schema$names,
    c("decimal", "event_time", "nested")
  )
  expect_match(schema, "decimal128\\(10, 2\\)")
  expect_match(schema, "timestamp\\[us, tz=UTC\\]")
  expect_match(schema, "struct<label: string, score: int32>")
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
})

test_that("adapter failures deterministically release stream ownership", {
  gc()
  start <- delta.sharing:::.native_diagnostics()$active_streams

  data_stream <- delta.sharing:::.native_test_stream(batches = 3L)
  expect_error(
    delta.sharing:::.materialize_data_frame_stream(
      data_stream,
      converter = function(stream) stop("data adapter failed")
    ),
    "data adapter failed",
    fixed = TRUE
  )
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(data_stream))

  arrow_stream <- delta.sharing:::.native_test_stream(batches = 3L)
  expect_error(
    delta.sharing:::.materialize_arrow_stream(
      arrow_stream,
      arrow_available = function() TRUE,
      reader_factory = function(stream) stop("arrow adapter failed")
    ),
    "arrow adapter failed",
    fixed = TRUE
  )
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(arrow_stream))

  public_stream <- NULL
  secret <- "private-adapter-error-detail"
  interface <- delta.sharing:::.new_execution_interface(list(
    read_arrow_stream = function(specification, ...) {
      public_stream <<- delta.sharing:::.native_test_stream(batches = 3L)
      public_stream
    },
    data_frame_from_stream = function(stream) {
      delta.sharing:::.materialize_data_frame_stream(
        stream,
        converter = function(stream) stop(secret)
      )
    }
  ))
  condition <- expect_error(
    delta.sharing:::.with_execution_interface(interface, {
      read_data_frame(materializer_read())
    }),
    class = "delta_sharing_protocol_error"
  )
  expect_false(grepl(secret, conditionMessage(condition), fixed = TRUE))
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(public_stream))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    start
  )
})

test_that("Arrow reader failures close transferred stream ownership", {
  skip_if_not_installed("arrow")
  gc()
  start <- delta.sharing:::.native_diagnostics()$active_streams
  stream <- delta.sharing:::.native_test_stream(
    batches = 3L,
    error_after = 1L
  )

  expect_error(
    delta.sharing:::.materialize_arrow_stream(stream),
    "synthetic reader error after 1 batches",
    fixed = TRUE
  )
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    start
  )
})

test_that("missing optional Arrow dependency is typed and releases the stream", {
  gc()
  start <- delta.sharing:::.native_diagnostics()$active_streams
  stream <- delta.sharing:::.native_test_stream(batches = 3L)

  condition <- expect_error(
    delta.sharing:::.materialize_arrow_stream(
      stream,
      arrow_available = function() FALSE
    ),
    class = "delta_sharing_unsupported_error"
  )

  expect_match(conditionMessage(condition), "optional package.*arrow")
  expect_identical(condition$operation, "read_arrow")
  expect_identical(condition$feature, "arrow_package")
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    start
  )
})

test_that("public Arrow preflight fails before scan or HTTP without Arrow", {
  recorder <- new.env(parent = emptyenv())
  recorder$auth_requests <- 0L
  recorder$snapshot_opens <- 0L
  snapshot_transport <- list(
    open = function(request) {
      recorder$snapshot_opens <- recorder$snapshot_opens + 1L
      stop("snapshot transport must not open")
    },
    status = function(response) 500L,
    headers = function(response) character(),
    pull = function(response) NULL,
    close = function(response) invisible(NULL),
    retry_after = function(response) NULL
  )
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      recorder$auth_requests <- recorder$auth_requests + 1L
      stop("auth transport must not run")
    }),
    snapshot_transport = snapshot_transport,
    arrow_available = function() FALSE
  )
  interface <- delta.sharing:::.new_execution_interface(callbacks)

  condition <- expect_error(
    delta.sharing:::.with_execution_interface(interface, {
      read_arrow(materializer_read())
    }),
    class = "delta_sharing_unsupported_error"
  )

  expect_identical(condition$operation, "read_arrow")
  expect_identical(condition$feature, "arrow_package")
  expect_identical(recorder$snapshot_opens, 0L)
  expect_identical(recorder$auth_requests, 0L)
})

test_that("as.data.frame forwards stream options to the one eager adapter", {
  recorder <- new.env(parent = emptyenv())
  interface <- materializer_execution_interface(recorder)
  read <- materializer_read(columns = c("id", "group"), limit = 3)

  result <- delta.sharing:::.with_execution_interface(interface, {
    as.data.frame(read, batch_size = 1L)
  })

  expect_identical(recorder$scans, 1L)
  expect_identical(recorder$options[[1L]]$batch_size, 1L)
  expect_identical(names(result), c("id", "group"))
  expect_identical(nrow(result), 3L)
})
