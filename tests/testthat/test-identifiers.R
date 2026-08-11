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
