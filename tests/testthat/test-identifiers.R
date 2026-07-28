test_that("compact identifiers parse exactly three parts", {
  id <- table_identifier("sales.default.orders")

  expect_true(S7::S7_inherits(id, SharingTableIdentifier))
  expect_identical(id@share, "sales")
  expect_identical(id@schema, "default")
  expect_identical(id@table, "orders")

  expect_error(
    table_identifier("sales.orders"),
    "exactly three",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    table_identifier("sales.default.events.v2"),
    "Supply",
    class = "delta_sharing_validation_error"
  )
})

test_that("structured identifiers preserve names containing dots", {
  id <- table_identifier("sales.eu", "default", "events.v2")

  expect_identical(id@share, "sales.eu")
  expect_identical(id@schema, "default")
  expect_identical(id@table, "events.v2")
})

test_that("identifier components are non-empty scalar strings", {
  expect_error(
    table_identifier("", "default", "orders"),
    "`share`",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    table_identifier(c("a", "b"), "default", "orders"),
    "`share`",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    table_identifier("sales", NA_character_, "orders"),
    "`schema`",
    class = "delta_sharing_validation_error"
  )
})

test_that("sharing_table supports compact and structured identifiers", {
  client <- test_client()
  compact <- sharing_table(client, "sales.default.orders")
  structured <- sharing_table(
    client,
    share = "sales.eu",
    schema = "default",
    table = "events.v2"
  )

  expect_identical(table_identifier(compact)@table, "orders")
  expect_identical(table_identifier(structured)@share, "sales.eu")
  expect_identical(table_identifier(structured)@table, "events.v2")

  expect_error(
    sharing_table(client),
    "exactly one",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_table(
      client,
      "sales.default.orders",
      share = "sales",
      schema = "default",
      table = "orders"
    ),
    "exactly one",
    class = "delta_sharing_validation_error"
  )
})

test_that("table_identifier dispatch is introspectable", {
  table <- test_table()

  expect_true(is.function(S7::method(table_identifier, SharingTable)))
  expect_identical(table_identifier(table), table@identifier)
  expect_identical(table_identifier(table@identifier), table@identifier)
})
