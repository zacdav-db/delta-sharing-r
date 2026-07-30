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

test_that("automatic response format is cached within one client", {
  profile <- test_profile()
  auth <- sharing_auth_context(profile)
  identifier <- sharing_table_identifier("sales.default.orders")
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    delta_metadata_response()
  })

  first <- resolve_query_format(profile, auth, identifier, "auto")
  second <- resolve_query_format(profile, auth, identifier, "auto")

  expect_identical(first, "delta")
  expect_identical(second, "delta")
  expect_identical(requests, 1L)
})

test_that("response format cache is isolated by table and client", {
  profile <- test_profile()
  first_auth <- sharing_auth_context(profile)
  second_auth <- sharing_auth_context(profile)
  orders <- sharing_table_identifier("sales.default.orders")
  events <- sharing_table_identifier("sales.default.events")
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    delta_metadata_response()
  })

  expect_identical(
    resolve_query_format(profile, first_auth, orders, "auto"),
    "delta"
  )
  expect_identical(
    resolve_query_format(profile, first_auth, events, "auto"),
    "delta"
  )
  expect_identical(
    resolve_query_format(profile, second_auth, orders, "auto"),
    "delta"
  )
  expect_identical(requests, 3L)
})

test_that("explicit response formats bypass the negotiation cache", {
  profile <- test_profile()
  auth <- sharing_auth_context(profile)
  identifier <- sharing_table_identifier("sales.default.orders")
  httr2::local_mocked_responses(function(req) {
    stop("explicit response formats must not perform metadata I/O")
  })

  expect_identical(
    resolve_query_format(profile, auth, identifier, "delta"),
    "delta"
  )
  expect_identical(
    resolve_query_format(profile, auth, identifier, "parquet"),
    "parquet"
  )
})

test_that("failed format negotiation is not cached", {
  profile <- test_profile()
  auth <- sharing_auth_context(profile)
  identifier <- sharing_table_identifier("sales.default.orders")
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    if (requests == 1L) {
      return(httr2::response(
        400,
        headers = list(`content-type` = "application/json"),
        body = charToRaw('{"message":"invalid request"}')
      ))
    }
    delta_metadata_response()
  })

  expect_error(
    resolve_query_format(profile, auth, identifier, "auto"),
    class = "delta_sharing_http_error"
  )
  expect_identical(
    resolve_query_format(profile, auth, identifier, "auto"),
    "delta"
  )
  expect_identical(requests, 2L)
})

test_that("metadata parsing warms automatic format negotiation", {
  profile <- test_profile()
  auth <- sharing_auth_context(profile)
  identifier <- sharing_table_identifier("sales.default.orders")
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    delta_metadata_response()
  })

  sharing_table_metadata(profile, auth, identifier)
  resolved <- resolve_query_format(profile, auth, identifier, "auto")

  expect_identical(resolved, "delta")
  expect_identical(requests, 1L)
})
