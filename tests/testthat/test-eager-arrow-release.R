test_that("eager Arrow materialization closes the imported reader", {
  skip_if_not_installed("arrow")
  state <- new.env(parent = emptyenv())
  state$released <- FALSE
  input <- data.frame(id = 1:3, value = c(1.5, NA, 3.5))
  stream <- nanoarrow::array_stream_set_finalizer(
    nanoarrow::basic_array_stream(list(input)),
    function() state$released <- TRUE
  )
  result <- sharing_stream_to_arrow(stream)
  expect_true(state$released)
  # Closing the reader must not invalidate the returned table's buffers.
  expect_equal(as.data.frame(result), input)
})

test_that("eager Arrow read failures close the imported reader", {
  skip_if_not_installed("arrow")
  table <- fs::path(withr::local_tempdir(), "table")
  fs::dir_copy(fixture_table("local-table"), table)
  writeBin(charToRaw("invalid parquet"), fs::path(table, "part-00000.parquet"))
  state <- new.env(parent = emptyenv())
  state$released <- FALSE
  stream <- nanoarrow::array_stream_set_finalizer(
    native_snapshot_stream(table),
    function() state$released <- TRUE
  )
  expect_error(
    sharing_stream_to_arrow(stream),
    "Delta Kernel data scan failed",
    fixed = TRUE
  )
  expect_true(state$released)
})
