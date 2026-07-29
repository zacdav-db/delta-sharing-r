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

test_that("snapshot progress totals account for deletion vectors and limits", {
  files <- list(
    list(
      deltaSingleAction = list(
        add = list(stats = '{"numRecords":10}')
      )
    ),
    list(
      deltaSingleAction = list(
        add = list(
          stats = '{"numRecords":7}',
          deletionVector = list(cardinality = 2)
        )
      )
    )
  )

  expect_equal(snapshot_total_rows(files, "delta"), 15)
  expect_equal(snapshot_total_rows(files, "delta", limit = 12), 12)
})

test_that("parquet snapshot progress uses file statistics", {
  files <- list(
    list(url = "https://example.test/a", stats = '{"numRecords":4}'),
    list(url = "https://example.test/b", stats = list(numRecords = 6))
  )

  expect_equal(snapshot_total_rows(files, "parquet"), 10)
})

test_that("snapshot progress stays indeterminate without trustworthy stats", {
  expect_null(snapshot_total_rows(list(list()), "delta"))
  expect_null(snapshot_total_rows(
    list(list(deltaSingleAction = list(
      add = list(stats = '{"numRecords":"unknown"}')
    ))),
    "delta"
  ))
  expect_null(snapshot_total_rows(
    list(list(deltaSingleAction = list(
      add = list(
        stats = '{"numRecords":3}',
        deletionVector = list(cardinality = 4)
      )
    ))),
    "delta"
  ))
})
