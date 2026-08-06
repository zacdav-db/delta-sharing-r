# The Query Table request body must always be a JSON object. R's list() encodes
# as `[]`, which the server rejects, so an options-free snapshot needs a named
# empty list that encodes as `{}`.

encode <- function(body) {
  as.character(jsonlite::toJSON(body, auto_unbox = TRUE))
}

test_that("an empty query body encodes as a JSON object, not an array", {
  spec <- list()
  expect_equal(encode(query_body(spec, NULL)), "{}")
})

test_that("query options are carried into the body", {
  spec <- list(limit = 100, version = 3)
  body <- query_body(spec, NULL)
  expect_equal(body$limitHint, 100)
  expect_equal(body$version, 3)
  expect_match(encode(body), "^\\{")
})

test_that("structured predicate hints are encoded as one JSON string", {
  predicate <- list(
    op = "equal",
    children = list(
      list(op = "column", name = "id", valueType = "long"),
      list(op = "literal", value = "1", valueType = "long")
    )
  )
  body <- query_body(list(predicate = predicate), NULL)

  expect_type(body$jsonPredicateHints, "character")
  expect_equal(
    jsonlite::fromJSON(body$jsonPredicateHints, simplifyVector = FALSE),
    predicate
  )
  expect_type(
    jsonlite::fromJSON(encode(body), simplifyVector = FALSE)$jsonPredicateHints,
    "character"
  )
})

test_that("a page token is included when present", {
  expect_equal(query_body(list(), "tok")$pageToken, "tok")
})

test_that("snapshot timestamps are formatted for the wire", {
  timestamp <- as.POSIXct("2026-01-01", tz = "UTC")

  expect_identical(
    query_body(list(timestamp = timestamp), NULL)$timestamp,
    "2026-01-01T00:00:00Z"
  )
})

test_that("parquet action helpers validate required wire fields", {
  expect_equal(as_json_map(NULL), rlang::set_names(list(), character()))
  expect_equal(as_json_map(c(a = 1)), list(a = 1))
  expect_equal(
    parquet_protocol_action(list()),
    list(minReaderVersion = 1L, minWriterVersion = 2L)
  )
  expect_error(
    parquet_metadata_action(list(), "read"),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    parquet_add_action(list(), "read"),
    class = "delta_sharing_protocol_error"
  )

  add <- parquet_add_action(
    list(
      url = "https://storage.example.test/file.parquet",
      partitionValues = list(region = "emea"),
      size = 42,
      stats = "{\"numRecords\":2}"
    ),
    "read"
  )
  expect_equal(add$partitionValues$region, "emea")
  expect_equal(add$stats, "{\"numRecords\":2}")
})
