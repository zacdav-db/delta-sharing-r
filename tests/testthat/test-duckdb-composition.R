duckdb_composition_fixture <- function() {
  normalizePath(
    test_path("fixtures", "delta", "local-table"),
    winslash = "/",
    mustWork = TRUE
  )
}

duckdb_composition_interface <- function(recorder) {
  delta.sharing:::.new_execution_interface(list(
    read_arrow_stream = function(
      specification,
      batch_size = NULL,
      concurrency = NULL
    ) {
      recorder$scans <- recorder$scans + 1L
      delta.sharing:::.native_snapshot_stream(
        duckdb_composition_fixture(),
        columns = specification@columns,
        limit = specification@limit,
        batch_size = if (is.null(batch_size)) 65536L else batch_size
      )
    },
    data_frame_from_stream = function(stream) {
      recorder$data_frames <- recorder$data_frames + 1L
      stop("DuckDB composition must not use the data-frame adapter.")
    }
  ))
}

duckdb_composition_connection <- function() {
  DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE),
    dbdir = ":memory:",
    config = list(threads = "1")
  )
}

test_that("DuckDB scans the one Kernel Arrow stream without a data frame", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("DBI")
  skip_if_not_installed("duckdb")

  gc()
  before <- delta.sharing:::.native_diagnostics()
  recorder <- new.env(parent = emptyenv())
  recorder$scans <- 0L
  recorder$data_frames <- 0L
  interface <- duckdb_composition_interface(recorder)
  read <- sharing_read(
    test_table(),
    columns = c("group", "id")
  )

  stream <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(read, batch_size = 2L)
  })
  expect_identical(recorder$scans, 1L)
  expect_identical(recorder$data_frames, 0L)
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    before$active_streams + 1L
  )

  reader <- arrow::as_record_batch_reader(stream)
  expect_s3_class(reader, "RecordBatchReader")
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))

  con <- duckdb_composition_connection()
  cleaned <- FALSE
  on.exit({
    if (!cleaned) {
      try(
        duckdb::duckdb_unregister_arrow(con, "shared_rows"),
        silent = TRUE
      )
      try(reader$Close(), silent = TRUE)
      try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    }
  }, add = TRUE)

  duckdb::duckdb_register_arrow(con, "shared_rows", reader)
  expect_identical(duckdb::duckdb_list_arrow(con), "shared_rows")

  schema <- DBI::dbGetQuery(con, "describe shared_rows")
  result <- DBI::dbGetQuery(
    con,
    "select \"group\", count(*) as n, sum(id) as id_sum
       from shared_rows
      group by \"group\"
      order by \"group\" nulls last"
  )

  expect_identical(schema$column_name, c("group", "id"))
  expect_identical(schema$column_type, c("VARCHAR", "BIGINT"))
  expect_identical(result$group, c("alpha", "beta", "gamma", NA_character_))
  expect_equal(as.numeric(result$n), c(2, 2, 2, 1))
  expect_equal(as.numeric(result$id_sum), c(5, 9, 11, 3))
  expect_identical(recorder$scans, 1L)
  expect_identical(recorder$data_frames, 0L)
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    before$active_streams
  )
  expect_identical(
    delta.sharing:::.native_diagnostics()$pending_cleanups,
    before$pending_cleanups
  )

  duckdb::duckdb_unregister_arrow(con, "shared_rows")
  reader$Close()
  DBI::dbDisconnect(con, shutdown = TRUE)
  cleaned <- TRUE
  gc()

  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    before$active_streams
  )
})

test_that("DuckDB query errors release native stream ownership", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("DBI")
  skip_if_not_installed("duckdb")

  gc()
  before <- delta.sharing:::.native_diagnostics()
  stream <- delta.sharing:::.native_test_stream(
    batches = 3L,
    rows_per_batch = 2L,
    error_after = 1L
  )
  reader <- arrow::as_record_batch_reader(stream)
  con <- duckdb_composition_connection()
  cleaned <- FALSE
  on.exit({
    if (!cleaned) {
      try(
        duckdb::duckdb_unregister_arrow(con, "broken_rows"),
        silent = TRUE
      )
      try(reader$Close(), silent = TRUE)
      try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    }
  }, add = TRUE)

  duckdb::duckdb_register_arrow(con, "broken_rows", reader)
  expect_error(
    DBI::dbGetQuery(con, "select count(*) from broken_rows"),
    "synthetic reader error after 1 batches",
    fixed = TRUE
  )
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    before$active_streams
  )

  duckdb::duckdb_unregister_arrow(con, "broken_rows")
  reader$Close()
  DBI::dbDisconnect(con, shutdown = TRUE)
  cleaned <- TRUE
  gc()

  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    before$active_streams
  )
  expect_identical(
    delta.sharing:::.native_diagnostics()$pending_cleanups,
    before$pending_cleanups
  )
})
