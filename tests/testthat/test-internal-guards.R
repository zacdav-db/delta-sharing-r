test_that("condition constructors reject unsafe internal inputs", {
  expect_error(
    delta.sharing:::.new_delta_sharing_condition("", type = "auth"),
    "`message` must be one non-empty string",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.new_delta_sharing_condition("Safe.", type = "unknown"),
    "Unknown Delta Sharing condition type",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.new_delta_sharing_condition(
      "Safe.",
      type = "auth",
      "unnamed"
    ),
    "Condition metadata must be named",
    fixed = TRUE
  )
})

test_that("execution interface rejects malformed callback registries", {
  invalid_callbacks <- list(
    NULL,
    list(function() NULL),
    structure(list(function() NULL), names = ""),
    structure(
      list(function() NULL, function() NULL),
      names = c("list_shares", "list_shares")
    )
  )
  for (callbacks in invalid_callbacks) {
    expect_error(
      delta.sharing:::.new_execution_interface(callbacks),
      "`callbacks` must be a uniquely named list",
      fixed = TRUE
    )
  }

  expect_error(
    delta.sharing:::.new_execution_interface(
      list(not_an_operation = function() NULL)
    ),
    "Unknown execution callback",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.new_execution_interface(list(list_shares = "no")),
    "Every execution callback must be a function",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.set_execution_interface(list()),
    "`interface` must be created by `.new_execution_interface()`",
    fixed = TRUE
  )
})

test_that("required scalar normalizers fail before execution", {
  expect_error(
    delta.sharing:::.normalize_version(
      NULL,
      "starting_version",
      required = TRUE
    ),
    "`starting_version` is required",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.normalize_timestamp(
      NULL,
      "starting_timestamp",
      required = TRUE
    ),
    "`starting_timestamp` is required",
    class = "delta_sharing_validation_error"
  )
})

test_that("direct S7 constructors validate descriptor classes", {
  identifier <- table_identifier("share.schema.table")

  expect_error(
    SharingTable("not a client", identifier),
    "`client` must be a SharingClient",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    SharingTable(test_client(), "not an identifier"),
    "`identifier` must be a SharingTableIdentifier",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    SharingRead("not a table"),
    "`table` must be a SharingTable",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    SharingChanges("not a table", starting_version = 1),
    "`table` must be a SharingTable",
    class = "delta_sharing_validation_error"
  )
})

test_that("identifier methods reject ambiguous arguments", {
  identifier <- table_identifier("share.schema.table")
  table <- test_table()

  expect_error(
    table_identifier(c("share", "schema", "table")),
    "`x` must be one three-part table name",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    table_identifier("share", schema = "schema"),
    "`schema` and `table` must either both be supplied",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    table_identifier(identifier, schema = "schema"),
    "must be omitted",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    table_identifier(table, schema = "schema"),
    "must be omitted",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_table(test_client()),
    "Supply exactly one",
    class = "delta_sharing_validation_error"
  )
})

test_that("timestamp change printing and base conversion dispatch", {
  changes <- sharing_changes(
    test_table(),
    starting_timestamp = as.POSIXct(
      "2026-07-29 00:00:00",
      tz = "UTC"
    ),
    ending_timestamp = as.POSIXct(
      "2026-07-30 00:00:00",
      tz = "UTC"
    )
  )
  printed <- capture.output(print(changes))
  expect_true(any(grepl(
    "2026-07-29T00:00:00Z",
    printed,
    fixed = TRUE
  )))
  expect_true(any(grepl(
    "2026-07-30T00:00:00Z",
    printed,
    fixed = TRUE
  )))

  interface <- delta.sharing:::.new_execution_interface(list(
    read_arrow_stream = function(specification, ...) "stream",
    data_frame_from_stream = function(stream) {
      data.frame(value = stream, stringsAsFactors = FALSE)
    }
  ))
  result <- delta.sharing:::.with_execution_interface(
    interface,
    as.data.frame(changes)
  )
  expect_identical(result$value, "stream")
})
