# Real end-to-end coverage against the public open datasets endpoint. Opt in
# with DELTA_SHARING_RUN_INTEGRATION=1; skipped otherwise.

test_that("discovery lists the open share, schemas, and tables", {
  skip_if_no_integration()
  client <- open_datasets_client()

  shares <- client$list_shares()
  expect_s3_class(shares, "tbl_df")
  expect_true("delta_sharing" %in% shares$name)

  schemas <- client$list_schemas(share = "delta_sharing")
  expect_true("default" %in% schemas$name)

  tables <- client$list_tables(share = "delta_sharing", schema = "default")
  expect_true(all(c("share", "schema", "name") %in% names(tables)))
  expect_true("boston-housing" %in% tables$name)
})

test_that("table metadata queries hit the real control plane", {
  skip_if_no_integration()
  tbl <- open_datasets_client()$table("delta_sharing.default.boston-housing")

  expect_type(tbl$version(), "double")
  expect_gte(tbl$version(), 0)

  protocol <- tbl$protocol()
  expect_true(protocol$response_format %in% c("delta", "parquet"))

  schema <- tbl$schema()
  expect_equal(schema$type, "struct")
  expect_gt(length(schema$fields), 0L)
})

test_that("a snapshot reads real rows through Delta Kernel", {
  skip_if_no_integration()
  tbl <- open_datasets_client()$table("delta_sharing.default.boston-housing")

  df <- tbl$snapshot(limit = 5)$to_data_frame()
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 5L)
  expect_gt(ncol(df), 1L)
})

test_that("an unbounded snapshot (no limit) reads the whole table", {
  skip_if_no_integration()
  tbl <- open_datasets_client()$table("delta_sharing.default.boston-housing")

  df <- tbl$snapshot()$to_data_frame()
  expect_s3_class(df, "data.frame")
  expect_gt(nrow(df), 5L)
})

test_that("projection selects columns end to end", {
  skip_if_no_integration()
  tbl <- open_datasets_client()$table("delta_sharing.default.boston-housing")

  df <- tbl$snapshot(columns = c("ID", "medv"), limit = 3)$to_data_frame()
  expect_equal(names(df), c("ID", "medv"))
  expect_equal(nrow(df), 3L)
})

test_that("the lazy Arrow stream yields the same data", {
  skip_if_no_integration()
  tbl <- open_datasets_client()$table("delta_sharing.default.boston-housing")

  stream <- tbl$snapshot(limit = 4)$to_arrow_stream()
  df <- as.data.frame(nanoarrow::convert_array_stream(stream))
  expect_equal(nrow(df), 4L)
})
