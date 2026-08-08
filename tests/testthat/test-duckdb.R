test_that("DuckDB queries lazy snapshot readers", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("DBI")
  skip_if_not_installed("duckdb")

  # DuckDB wraps lazy readers in an Arrow scanner. Arrow C streams are not
  # assumed to support concurrent pulls, so keep this scanner serialized.
  withr::local_options(arrow.use_threads = FALSE)

  stream <- native_snapshot_stream(
    fixture_table("local-table"),
    batch_size = 2L
  )
  reader <- sharing_stream_to_arrow_reader(stream)
  withr::defer(reader$Close())
  connection <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))
  duckdb::duckdb_register_arrow(connection, "shared_orders", reader)
  withr::defer(
    duckdb::duckdb_unregister_arrow(connection, "shared_orders")
  )

  result <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT \"group\", SUM(value) AS total",
      "FROM shared_orders",
      "GROUP BY \"group\"",
      "ORDER BY \"group\""
    )
  )

  expect_identical(names(result), c("group", "total"))
  expect_gt(nrow(result), 0L)
  expect_equal(sum(result$total, na.rm = TRUE), 28)
})

test_that("DuckDB queries eager Arrow tables", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("DBI")
  skip_if_not_installed("duckdb")

  table <- sharing_stream_to_arrow(
    native_snapshot_stream(fixture_table("local-table"))
  )
  connection <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))
  duckdb::duckdb_register_arrow(connection, "shared_orders", table)
  withr::defer(
    duckdb::duckdb_unregister_arrow(connection, "shared_orders")
  )

  result <- DBI::dbGetQuery(
    connection,
    "SELECT id, active FROM shared_orders WHERE active ORDER BY id"
  )

  expect_identical(names(result), c("id", "active"))
  expect_true(all(result$active))
  expect_gt(nrow(result), 0L)
})

test_that("DuckDB early completion releases the prepared snapshot root", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("DBI")
  skip_if_not_installed("duckdb")

  withr::local_options(arrow.use_threads = FALSE)

  httr2::local_mocked_responses(function(req) {
    httr2::response(
      200,
      headers = list(`content-type` = "application/x-ndjson"),
      body = charToRaw(ndjson_body(local_snapshot_actions()))
    )
  })
  profile <- test_profile()
  log <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    sharing_table_identifier("sales.default.local"),
    list(
      predicate = NULL,
      limit = NULL,
      version = NULL,
      timestamp = NULL
    ),
    "delta"
  )
  root <- log$root
  result <- local({
    stream <- native_snapshot_stream(
      table_location = log$path,
      batch_size = 2L,
      cleanup_root = root
    )
    reader <- sharing_stream_to_arrow_reader(stream)
    withr::defer(reader$Close())
    connection <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
    withr::defer(
      if (DBI::dbIsValid(connection)) {
        DBI::dbDisconnect(connection, shutdown = TRUE)
      }
    )
    duckdb::duckdb_register_arrow(connection, "shared_orders", reader)
    withr::defer(
      if (DBI::dbIsValid(connection)) {
        duckdb::duckdb_unregister_arrow(connection, "shared_orders")
      }
    )

    result <- DBI::dbGetQuery(
      connection,
      "SELECT * FROM shared_orders LIMIT 1"
    )
    duckdb::duckdb_unregister_arrow(connection, "shared_orders")
    DBI::dbDisconnect(connection, shutdown = TRUE)
    result
  })

  expect_identical(nrow(result), 1L)
  expect_false(fs::dir_exists(root))
  expect_identical(native_reap_pending_cleanups(), 0)
})

test_that("DuckDB queries CDF metadata columns", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("DBI")
  skip_if_not_installed("duckdb")

  withr::local_options(arrow.use_threads = FALSE)

  stream <- native_cdf_stream(
    fixture_table("cdf"),
    start_version = 1,
    end_version = 2
  )
  reader <- sharing_stream_to_arrow_reader(stream)
  withr::defer(reader$Close())
  connection <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
  withr::defer(DBI::dbDisconnect(connection, shutdown = TRUE))
  duckdb::duckdb_register_arrow(connection, "shared_changes", reader)
  withr::defer(
    duckdb::duckdb_unregister_arrow(connection, "shared_changes")
  )

  result <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT _change_type, COUNT(*) AS changes",
      "FROM shared_changes",
      "GROUP BY _change_type",
      "ORDER BY _change_type"
    )
  )

  expect_setequal(result$`_change_type`, c("delete", "insert"))
  expect_equal(sum(result$changes), 15)
})
