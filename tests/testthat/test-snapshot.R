test_that("snapshot specifications represent latest, version, or timestamp", {
  table <- test_table()
  latest <- sharing_read(table)
  versioned <- sharing_read(table, version = 12L)
  at_time <- sharing_read(
    table,
    timestamp = as.POSIXct("2026-07-01 10:00:00", tz = "Australia/Sydney")
  )

  expect_true(S7::S7_inherits(latest, SharingRead))
  expect_null(latest@version)
  expect_null(latest@timestamp)
  expect_identical(versioned@version, 12)
  expect_identical(attr(at_time@timestamp, "tzone"), "UTC")
})

test_that("snapshot version and timestamp are mutually exclusive", {
  expect_error(
    sharing_read(
      test_table(),
      version = 1,
      timestamp = as.POSIXct("2026-07-01", tz = "UTC")
    ),
    "mutually exclusive",
    class = "delta_sharing_validation_error"
  )
})

test_that("versions are non-negative whole numbers", {
  table <- test_table()

  for (version in list(-1, 1.5, Inf, NA_real_, c(1, 2), "1")) {
    expect_error(
      sharing_read(table, version = version),
      "`version`",
      class = "delta_sharing_validation_error"
    )
  }
  expect_identical(sharing_read(table, version = 2^40)@version, 2^40)
})

test_that("timestamp must be one non-missing POSIXct value", {
  table <- test_table()

  expect_error(
    sharing_read(table, timestamp = "2026-07-01"),
    "POSIXct",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_read(table, timestamp = as.POSIXct(NA)),
    "POSIXct",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_read(
      table,
      timestamp = as.POSIXct(Inf, origin = "1970-01-01", tz = "UTC")
    ),
    "POSIXct",
    class = "delta_sharing_validation_error"
  )
})

test_that("projection preserves order and case", {
  table <- test_table()
  columns <- c("OrderID", "ordered_at", "Amount")
  snapshot <- sharing_read(table, columns = columns)

  expect_identical(snapshot@columns, columns)
  expect_null(sharing_read(table)@columns)

  expect_error(
    sharing_read(table, columns = character()),
    "`columns`",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_read(table, columns = c("id", "id")),
    "duplicate",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_read(table, columns = c("id", "")),
    "`columns`",
    class = "delta_sharing_validation_error"
  )
})

test_that("limit is a non-negative whole number", {
  table <- test_table()

  expect_identical(sharing_read(table, limit = 0)@limit, 0)
  expect_identical(sharing_read(table, limit = 1L)@limit, 1)
  expect_identical(sharing_read(table, limit = 2^40)@limit, 2^40)

  for (limit in list(-1, 1.5, Inf, NA_real_, c(1, 2), "10")) {
    expect_error(
      sharing_read(table, limit = limit),
      "`limit`",
      class = "delta_sharing_validation_error"
    )
  }
})

test_that("snapshot options create new descriptors without mutation", {
  table <- test_table()
  latest <- sharing_read(table)
  projected <- sharing_read(table, columns = "id", limit = 5)

  expect_null(latest@columns)
  expect_null(latest@limit)
  expect_identical(projected@columns, "id")
  expect_identical(projected@limit, 5)
  expect_identical(latest@table, projected@table)
})

test_that("predicate hints and response formats are validated", {
  table <- test_table()
  hints <- list(op = "equal", children = list("status", "open"))
  snapshot <- sharing_read(
    table,
    predicate = hints,
    response_format = "delta"
  )

  expect_identical(snapshot@predicate, hints)
  expect_identical(snapshot@response_format, "delta")
  expect_error(
    sharing_read(table, predicate = "status = 'open'"),
    "structured server hint",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_read(table, response_format = "csv"),
    "`response_format`",
    class = "delta_sharing_validation_error"
  )
})
