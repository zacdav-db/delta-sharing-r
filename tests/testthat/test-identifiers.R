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
