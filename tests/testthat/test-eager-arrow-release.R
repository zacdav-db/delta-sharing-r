test_that("eager Arrow materialization closes the imported reader", {
  state <- new.env(parent = emptyenv())
  state$released <- 0L
  input <- data.frame(
    id = seq_len(100000L),
    value = rep(c(1.5, NA, 3.5), length.out = 100000L)
  )
  stream <- nanoarrow::array_stream_set_finalizer(
    nanoarrow::basic_array_stream(list(input)),
    function() state$released <- state$released + 1L
  )
  result <- sharing_stream_to_arrow(stream)
  expect_identical(state$released, 1L)
  rm(stream)
  gc()
  expect_identical(state$released, 1L)
  # Closing the reader must not invalidate the returned table's buffers.
  expect_equal(as.data.frame(result), input)
})

test_that("eager Arrow read failures close the imported reader", {
  table <- fs::path(withr::local_tempdir(), "table")
  fs::dir_copy(fixture_table("local-table"), table)
  writeBin(charToRaw("invalid parquet"), fs::path(table, "part-00000.parquet"))
  state <- new.env(parent = emptyenv())
  state$released <- 0L
  stream <- nanoarrow::array_stream_set_finalizer(
    native_snapshot_stream(table),
    function() state$released <- state$released + 1L
  )
  expect_error(
    sharing_stream_to_arrow(stream),
    "Delta Kernel data scan failed",
    fixed = TRUE
  )
  expect_identical(state$released, 1L)
  rm(stream)
  gc()
  expect_identical(state$released, 1L)
})

test_that("reader cleanup errors do not replace the original read error", {
  state <- new.env(parent = emptyenv())
  state$closed <- 0L
  state$released <- 0L
  stream <- nanoarrow::array_stream_set_finalizer(
    nanoarrow::basic_array_stream(list(data.frame(id = 1L))),
    function() state$released <- state$released + 1L
  )
  imported <- sharing_stream_to_arrow_reader
  read_error <- simpleError("read failed")
  testthat::local_mocked_bindings(
    sharing_stream_to_arrow_reader = function(stream) {
      reader <- imported(stream)
      list(
        read_table = function() stop(read_error),
        Close = function() {
          state$closed <- state$closed + 1L
          reader$Close()
          stop("close failed")
        }
      )
    },
    .package = "delta.sharing"
  )

  caught <- tryCatch(sharing_stream_to_arrow(stream), error = identity)
  expect_identical(caught, read_error)
  expect_identical(state$closed, 1L)
  expect_identical(state$released, 1L)
})
