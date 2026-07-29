test_that("table version uses a bodyless HEAD on the table path", {
  tbl <- test_client()$table("sales.default.orders")
  seen <- list()
  mock <- function(req) {
    seen <<- list(path = httr2::url_parse(req$url)$path, method = req$method)
    httr2::response(
      200,
      headers = list(`delta-table-version` = "42"),
      body = raw()
    )
  }
  httr2::local_mocked_responses(mock)
  v <- tbl$version()
  expect_equal(v, 42)
  expect_equal(seen$method, "HEAD")
  expect_match(seen$path, "/tables/orders$")
})

test_that("wire field coercions handle present and absent values", {
  expect_identical(wire_character(42), "42")
  expect_identical(wire_character(NULL), NA_character_)
  expect_identical(wire_character(character()), NA_character_)

  expect_identical(wire_integer(3), 3L)
  expect_identical(wire_integer(NULL), NA_integer_)
  expect_identical(wire_integer(integer()), NA_integer_)

  expect_identical(wire_number("3000000000"), 3000000000)
  expect_identical(wire_number(NULL), NA_real_)
  expect_identical(wire_number("not-a-number"), NA_real_)

  expect_identical(
    wire_character_vector(list("columnMapping", "timestampNtz")),
    c("columnMapping", "timestampNtz")
  )
  expect_identical(wire_character_vector(NULL), character())
})

test_that("table protocol projects delta reader features", {
  tbl <- test_client()$table("sales.default.orders")
  httr2::local_mocked_responses(function(req) delta_metadata_response())
  proto <- tbl$protocol()
  expect_equal(proto$response_format, "delta")
  expect_equal(proto$min_reader_version, 3L)
  expect_true("columnMapping" %in% proto$reader_features)
})

test_that("table metadata exposes safe fields", {
  tbl <- test_client()$table("sales.default.orders")
  httr2::local_mocked_responses(function(req) delta_metadata_response())
  meta <- tbl$metadata()
  expect_equal(meta$name, "orders")
  expect_equal(meta$table_version, 42)
  expect_equal(meta$num_files, 5)
  expect_equal(meta$size, 3000000000)
  expect_equal(meta$created_time, 1720000000000)
  expect_null(meta$location)
})

test_that("table schema parses the struct schema", {
  tbl <- test_client()$table("sales.default.orders")
  httr2::local_mocked_responses(function(req) delta_metadata_response())
  schema <- tbl$schema()
  expect_equal(schema$type, "struct")
  expect_equal(schema$fields[[1]]$name, "id")
})
