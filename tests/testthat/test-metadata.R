test_that("table version uses a bodyless HEAD on the table path", {
  tbl <- test_client()$table("sales.default.orders")
  seen <- new.env(parent = emptyenv())
  mock <- function(req) {
    seen$path <- httr2::url_parse(req$url)$path
    seen$method <- req$method
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

test_that("wire projections do not silently coerce or truncate values", {
  expect_identical(wire_integer(3), 3L)
  expect_identical(wire_integer(NULL), NA_integer_)
  expect_identical(wire_integer(c(3, 4)), c(3L, 4L))

  expect_identical(
    wire_character_vector(list("columnMapping", "timestampNtz")),
    c("columnMapping", "timestampNtz")
  )
  expect_identical(wire_character_vector(NULL), character())
  expect_error(
    wire_character_vector(list("columnMapping", 7)),
    class = "vctrs_error_cast"
  )
})

test_that("metadata fields retain their wire types", {
  body <- ndjson_body(list(list(
    metadata = list(
      id = 42,
      name = "events",
      schemaString = "{\"type\":\"struct\",\"fields\":[]}",
      partitionColumns = list(),
      numFiles = "5",
      size = "not-a-number",
      createdTime = "yesterday"
    )
  )))
  response <- httr2::response(200, body = charToRaw(body))

  metadata <- parse_table_actions(response, "metadata")$metadata

  expect_identical(metadata$id, 42L)
  expect_identical(metadata$num_files, "5")
  expect_identical(metadata$size, "not-a-number")
  expect_identical(metadata$created_time, "yesterday")
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
  state <- new.env(parent = emptyenv())
  state$requests <- 0L
  httr2::local_mocked_responses(function(req) {
    state$requests <- state$requests + 1L
    delta_metadata_response()
  })

  first <- resolve_query_format(profile, auth, identifier, "auto")
  second <- resolve_query_format(profile, auth, identifier, "auto")

  expect_identical(first, "delta")
  expect_identical(second, "delta")
  expect_identical(state$requests, 1L)
})

test_that("response format cache is isolated by table and client", {
  profile <- test_profile()
  first_auth <- sharing_auth_context(profile)
  second_auth <- sharing_auth_context(profile)
  orders <- sharing_table_identifier("sales.default.orders")
  events <- sharing_table_identifier("sales.default.events")
  state <- new.env(parent = emptyenv())
  state$requests <- 0L
  httr2::local_mocked_responses(function(req) {
    state$requests <- state$requests + 1L
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
  expect_identical(state$requests, 3L)
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
  state <- new.env(parent = emptyenv())
  state$requests <- 0L
  httr2::local_mocked_responses(function(req) {
    state$requests <- state$requests + 1L
    if (state$requests == 1L) {
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
  expect_identical(state$requests, 2L)
})

test_that("metadata parsing warms automatic format negotiation", {
  profile <- test_profile()
  auth <- sharing_auth_context(profile)
  identifier <- sharing_table_identifier("sales.default.orders")
  state <- new.env(parent = emptyenv())
  state$requests <- 0L
  httr2::local_mocked_responses(function(req) {
    state$requests <- state$requests + 1L
    delta_metadata_response()
  })

  sharing_table_metadata(profile, auth, identifier)
  resolved <- resolve_query_format(profile, auth, identifier, "auto")

  expect_identical(resolved, "delta")
  expect_identical(state$requests, 1L)
})

test_that("capability headers distinguish snapshot, CDF, and parquet", {
  expect_match(capability_header("auto"), "timestampntz", fixed = TRUE)
  expect_false(grepl(
    "timestampntz",
    capability_header("delta", for_cdf = TRUE),
    fixed = TRUE
  ))
  expect_identical(capability_header("parquet"), "responseformat=parquet")
})

test_that("format negotiation falls back to parquet and tolerates old contexts", {
  profile <- test_profile()
  auth <- sharing_auth_context(profile)
  identifier <- sharing_table_identifier("sales.default.orders")
  httr2::local_mocked_responses(function(req) {
    delta_metadata_response(capabilities = "responseformat=parquet")
  })

  expect_identical(
    resolve_query_format(profile, auth, identifier, "auto"),
    "parquet"
  )
  old_auth <- list(authenticate = auth$authenticate)
  expect_null(cached_response_format(old_auth, identifier))
  expect_identical(
    remember_response_format(old_auth, identifier, "delta"),
    "delta"
  )
})

test_that("metadata protocol validation rejects invalid wire responses", {
  invalid_version <- httr2::response(
    200,
    headers = list(`delta-table-version` = "-1")
  )
  expect_error(
    parse_version_header(invalid_version, "table_version"),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    parse_ndjson_lines("{not-json", "metadata"),
    class = "delta_sharing_protocol_error"
  )
  expect_length(parse_ndjson_lines("\n \n", "metadata"), 0L)
})

test_that("parquet metadata envelopes are projected safely", {
  body <- ndjson_body(list(
    list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
    list(
      metadata = list(
        id = "table",
        name = "events",
        schemaString = "{\"type\":\"struct\",\"fields\":[]}",
        partitionColumns = list("date")
      )
    )
  ))
  response <- httr2::response(
    200,
    headers = list(`content-type` = "application/x-ndjson"),
    body = charToRaw(body)
  )

  parsed <- parse_table_actions(response, "metadata")

  expect_identical(parsed$response_format, "parquet")
  expect_identical(parsed$protocol$min_reader_version, 1L)
  expect_identical(parsed$metadata$partition_columns, "date")
})

test_that("schema inspection rejects missing and malformed schemas", {
  missing_schema <- function(req) {
    body <- ndjson_body(list(
      list(protocol = list(minReaderVersion = 1L)),
      list(metadata = list(id = "table"))
    ))
    httr2::response(200, body = charToRaw(body))
  }
  httr2::local_mocked_responses(missing_schema)
  expect_error(
    test_client()$table("sales.default.events")$schema(),
    class = "delta_sharing_protocol_error"
  )

  malformed_schema <- function(req) {
    body <- ndjson_body(list(
      list(protocol = list(minReaderVersion = 1L)),
      list(
        metadata = list(
          id = "table",
          schemaString = "{\"type\":\"array\"}"
        )
      )
    ))
    httr2::response(200, body = charToRaw(body))
  }
  httr2::local_mocked_responses(malformed_schema)
  expect_error(
    test_client()$table("sales.default.events")$schema(),
    class = "delta_sharing_protocol_error"
  )

  invalid_json_schema <- function(req) {
    body <- ndjson_body(list(
      list(protocol = list(minReaderVersion = 1L)),
      list(
        metadata = list(
          id = "table",
          schemaString = "{not-json"
        )
      )
    ))
    httr2::response(200, body = charToRaw(body))
  }
  httr2::local_mocked_responses(invalid_json_schema)
  expect_error(
    test_client()$table("sales.default.events")$schema(),
    class = "delta_sharing_protocol_error"
  )
})
