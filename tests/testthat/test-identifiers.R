test_that("parses a compact three-part name", {
  id <- sharing_table_identifier("sales.default.orders")
  expect_equal(id$share, "sales")
  expect_equal(id$schema, "default")
  expect_equal(id$table, "orders")
})

test_that("explicit components preserve dots in names", {
  id <- sharing_table_identifier(
    share = "sales",
    schema = "default",
    name = "orders.v2"
  )
  expect_equal(id$share, "sales")
  expect_equal(id$schema, "default")
  expect_equal(id$table, "orders.v2")
})

test_that("rejects malformed compact names", {
  expect_error(
    sharing_table_identifier("only.two"),
    class = "delta_sharing_validation_error"
  )
})

test_that("changes validation accepts a version range", {
  spec <- sharing_changes_validate(120, 125, NULL, NULL, NULL, "auto")
  expect_equal(spec$starting_version, 120)
  expect_equal(spec$ending_version, 125)
})

test_that("changes validation accepts protocol-native timestamp strings", {
  spec <- sharing_changes_validate(
    NULL,
    NULL,
    "2024-01-01T00:00:00.123Z",
    "2024-01-02T00:00:00.123Z",
    NULL,
    "auto"
  )

  expect_equal(spec$starting_timestamp, "2024-01-01T00:00:00.123Z")
  expect_equal(spec$ending_timestamp, "2024-01-02T00:00:00.123Z")
})

test_that("changes validation rejects mixed bounds", {
  expect_error(
    sharing_changes_validate(
      1,
      NULL,
      as.POSIXct("2020-01-01"),
      NULL,
      NULL,
      "auto"
    ),
    class = "delta_sharing_validation_error"
  )
})

test_that("changes validation requires a starting bound", {
  expect_error(
    sharing_changes_validate(NULL, NULL, NULL, NULL, NULL, "auto"),
    class = "delta_sharing_validation_error"
  )
})

test_that("changes validation rejects an ending version before its start", {
  expect_error(
    sharing_changes_validate(125, 120, NULL, NULL, NULL, "auto"),
    class = "delta_sharing_validation_error"
  )
})

test_that("explicit identifiers require all three components", {
  expect_error(
    sharing_table_identifier(name = "orders", share = "sales"),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_table_identifier(name = "orders", schema = "default"),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_table_identifier(42),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_table_identifier("sales..orders"),
    class = "delta_sharing_validation_error"
  )
})

test_that("changes validation rejects ending-only ranges", {
  expect_error(
    sharing_changes_validate(NULL, 2, NULL, NULL, NULL, "auto"),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_changes_validate(
      NULL,
      NULL,
      NULL,
      as.POSIXct("2026-01-02", tz = "UTC"),
      NULL,
      "auto"
    ),
    class = "delta_sharing_validation_error"
  )
})

test_that("changes validation orders POSIXct timestamp bounds", {
  start <- as.POSIXct("2026-01-02", tz = "UTC")
  end <- as.POSIXct("2026-01-01", tz = "UTC")

  expect_error(
    sharing_changes_validate(NULL, NULL, start, end, NULL, "auto"),
    class = "delta_sharing_validation_error"
  )
})

test_that("shared validation helpers normalize supported values", {
  expect_null(normalize_count(NULL, "count"))
  expect_identical(normalize_count(3L, "count"), 3)
  expect_identical(
    normalize_timestamp("2026-01-01T00:00:00Z", "timestamp"),
    "2026-01-01T00:00:00Z"
  )
  timestamp <- as.POSIXct("2026-01-01", tz = "Australia/Sydney")
  normalized <- normalize_timestamp(timestamp, "timestamp")
  expect_s3_class(normalized, "POSIXct")
  expect_identical(attr(normalized, "tzone"), "UTC")
  expect_identical(normalize_columns(c("a", "b")), c("a", "b"))
  expect_equal(normalize_predicate(list(op = "equal")), list(op = "equal"))
})

test_that("shared validation helpers reject malformed values", {
  expect_error(
    normalize_count(NULL, "count", required = TRUE),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    normalize_timestamp(NULL, "timestamp", required = TRUE),
    class = "delta_sharing_validation_error"
  )
  purrr::walk(
    list(NA, Inf, as.POSIXct(c("2026-01-01", "2026-01-02"), tz = "UTC")),
    function(value) {
      expect_error(
        normalize_timestamp(value, "timestamp"),
        class = "delta_sharing_validation_error"
      )
    }
  )
  purrr::walk(
    list(character(), NA_character_, c("a", ""), c("a", "a"), 1),
    function(value) {
      expect_error(
        normalize_columns(value),
        class = "delta_sharing_validation_error"
      )
    }
  )
  expect_error(
    normalize_predicate("json"),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    normalize_response_format("csv"),
    class = "rlang_error"
  )
})
