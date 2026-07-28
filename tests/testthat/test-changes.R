test_that("CDF accepts version ranges", {
  changes <- sharing_changes(
    test_table(),
    starting_version = 10L,
    ending_version = 15L,
    columns = c("id", "_change_type"),
    response_format = "delta"
  )

  expect_true(S7::S7_inherits(changes, SharingChanges))
  expect_identical(changes@starting_version, 10)
  expect_identical(changes@ending_version, 15)
  expect_null(changes@starting_timestamp)
  expect_identical(changes@columns, c("id", "_change_type"))
  expect_identical(changes@response_format, "delta")
})

test_that("CDF accepts timestamp ranges and normalizes to UTC", {
  start <- as.POSIXct("2026-07-01 10:00:00", tz = "Australia/Sydney")
  end <- start + 3600
  changes <- sharing_changes(
    test_table(),
    starting_timestamp = start,
    ending_timestamp = end
  )

  expect_identical(attr(changes@starting_timestamp, "tzone"), "UTC")
  expect_identical(attr(changes@ending_timestamp, "tzone"), "UTC")
  expect_equal(as.double(changes@starting_timestamp), as.double(start))
  expect_equal(as.double(changes@ending_timestamp), as.double(end))
})

test_that("CDF requires one starting bound", {
  table <- test_table()

  expect_error(
    sharing_changes(table),
    "starting_version.*starting_timestamp",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_changes(table, ending_version = 10),
    "`starting_version`",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_changes(
      table,
      ending_timestamp = as.POSIXct("2026-07-01", tz = "UTC")
    ),
    "`starting_timestamp`",
    class = "delta_sharing_validation_error"
  )
})

test_that("CDF cannot mix version and timestamp bounds", {
  timestamp <- as.POSIXct("2026-07-01", tz = "UTC")

  expect_error(
    sharing_changes(
      test_table(),
      starting_version = 1,
      ending_timestamp = timestamp
    ),
    "cannot be mixed",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_changes(
      test_table(),
      starting_timestamp = timestamp,
      ending_version = 2
    ),
    "cannot be mixed",
    class = "delta_sharing_validation_error"
  )
})

test_that("CDF ending bounds do not precede starting bounds", {
  timestamp <- as.POSIXct("2026-07-01", tz = "UTC")

  expect_error(
    sharing_changes(
      test_table(),
      starting_version = 10,
      ending_version = 9
    ),
    "greater than or equal",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_changes(
      test_table(),
      starting_timestamp = timestamp,
      ending_timestamp = timestamp - 1
    ),
    "greater than or equal",
    class = "delta_sharing_validation_error"
  )
})

test_that("CDF projection follows snapshot projection invariants", {
  table <- test_table()

  expect_identical(
    sharing_changes(
      table,
      starting_version = 1,
      columns = c("B", "a")
    )@columns,
    c("B", "a")
  )
  expect_error(
    sharing_changes(
      table,
      starting_version = 1,
      columns = c("id", "id")
    ),
    "duplicate",
    class = "delta_sharing_validation_error"
  )
})
