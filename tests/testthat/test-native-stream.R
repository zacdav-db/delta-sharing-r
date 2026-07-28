test_that("native streams expose empty, one, and multiple batches", {
  empty <- delta.sharing:::.native_test_stream(batches = 0L)
  expect_s3_class(empty, "nanoarrow_array_stream")
  expect_named(
    empty$get_schema()$children,
    c("batch_index", "row_index", "label", "amount", "event_time", "values")
  )
  expect_null(empty$get_next())
  empty$release()

  one <- delta.sharing:::.native_test_stream(
    batches = 1L,
    rows_per_batch = 4L
  )
  expect_equal(one$get_next()$length, 4L)
  expect_null(one$get_next())
  one$release()

  many <- delta.sharing:::.native_test_stream(
    batches = 3L,
    rows_per_batch = 2L
  )
  batches <- list(many$get_next(), many$get_next(), many$get_next())
  expect_equal(vapply(batches, `[[`, integer(1), "length"), c(2L, 2L, 2L))
  expect_null(many$get_next())
  many$release()
})

test_that("native streams import into arrow without IPC", {
  skip_if_not_installed("arrow")

  stream <- delta.sharing:::.native_test_stream(
    batches = 3L,
    rows_per_batch = 5L
  )
  reader <- arrow::as_record_batch_reader(stream)
  table <- reader$read_table()

  expect_equal(table$num_rows, 15)
  expect_identical(
    table$schema$names,
    c("batch_index", "row_index", "label", "amount", "event_time", "values")
  )
})

test_that("release and garbage collection drop native ownership", {
  gc()
  start <- delta.sharing:::.native_diagnostics()

  stream <- delta.sharing:::.native_test_stream(batches = 10L)
  created <- delta.sharing:::.native_diagnostics()
  expect_equal(created$active_streams, start$active_streams + 1)

  stream$release()
  released <- delta.sharing:::.native_diagnostics()
  expect_equal(released$active_streams, start$active_streams)
  expect_equal(released$cancelled_streams, start$cancelled_streams + 1)

  local({
    abandoned <- delta.sharing:::.native_test_stream(batches = 10L)
    expect_true(nanoarrow::nanoarrow_pointer_is_valid(abandoned))
  })
  gc()

  collected <- delta.sharing:::.native_diagnostics()
  expect_equal(collected$active_streams, start$active_streams)
  expect_equal(collected$cancelled_streams, start$cancelled_streams + 2)
})

test_that("reader errors and panics are contained as R errors", {
  error_before_first <- delta.sharing:::.native_test_stream(
    batches = 3L,
    error_after = 0L
  )
  expect_error(
    error_before_first$get_next(),
    "synthetic reader error after 0 batches",
    fixed = TRUE
  )
  error_before_first$release()

  panic_before_first <- delta.sharing:::.native_test_stream(
    batches = 3L,
    panic_after = 0L
  )
  expect_error(
    panic_before_first$get_next(),
    "panic contained at Arrow stream boundary",
    fixed = TRUE
  )
  panic_before_first$release()

  error_stream <- delta.sharing:::.native_test_stream(
    batches = 3L,
    error_after = 1L
  )
  expect_s3_class(error_stream$get_next(), "nanoarrow_array")
  expect_error(
    error_stream$get_next(),
    "synthetic reader error after 1 batches",
    fixed = TRUE
  )
  error_stream$release()

  panic_stream <- delta.sharing:::.native_test_stream(
    batches = 3L,
    panic_after = 1L
  )
  expect_s3_class(panic_stream$get_next(), "nanoarrow_array")
  expect_error(
    panic_stream$get_next(),
    "panic contained at Arrow stream boundary",
    fixed = TRUE
  )
  panic_stream$release()
})

test_that("the C control boundary rejects invalid or reused pointers", {
  expect_error(
    .Call(
      delta.sharing:::C_delta_sharing_stream_from_test_data,
      structure(1L, class = "nanoarrow_array_stream"),
      1L,
      1L,
      -1L,
      -1L
    ),
    "must be an R external pointer",
    fixed = TRUE
  )

  stream <- delta.sharing:::.native_test_stream()
  expect_error(
    .Call(
      delta.sharing:::C_delta_sharing_stream_from_test_data,
      stream,
      1L,
      1L,
      -1L,
      -1L
    ),
    "already initialized",
    fixed = TRUE
  )
  stream$release()
})

test_that("native diagnostics prove the slim pinned foundation", {
  info <- delta.sharing:::.native_diagnostics()

  expect_identical(info$abi_version, 2L)
  expect_true(info$kernel_smoke_ok, info$kernel_smoke_message)
  expect_identical(info$delta_kernel_version, "0.22.0")
  expect_identical(info$arrow_rs_version, "57.3.0")
  expect_identical(info$ffi_backend, "registered-c-shim")
  expect_equal(info$pending_cleanups, 0)
})
