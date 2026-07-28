test_that("profile and client constructors create S7 descriptors", {
  profile <- SharingProfile("config.share")
  client <- SharingClient(profile)

  expect_true(S7::S7_inherits(profile, SharingProfile))
  expect_true(S7::S7_inherits(client, SharingClient))
  expect_identical(profile@source_type, "path")
  expect_identical(profile@label, "config.share")
  expect_identical(client@profile, profile)
  expect_identical(client@state, "descriptor")
})

test_that("sharing_client accepts supported profile source forms", {
  inline_json <- sharing_client('{"bearerToken":"secret"}')
  inline_raw <- sharing_client(charToRaw('{"bearerToken":"secret"}'))
  inline_list <- sharing_client(list(bearerToken = "secret"))
  connection <- textConnection('{"bearerToken":"secret"}')
  on.exit(close(connection), add = TRUE)
  inline_connection <- sharing_client(connection)

  expect_identical(inline_json@profile@source_type, "json")
  expect_identical(inline_raw@profile@source_type, "json")
  expect_identical(inline_list@profile@source_type, "list")
  expect_identical(inline_connection@profile@source_type, "connection")
  expect_identical(
    sharing_profile("config.share")@source_type,
    "path"
  )
})

test_that("profile source types are validated", {
  expect_error(
    SharingProfile("config.share", source_type = "url"),
    "`source_type`",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    SharingProfile(list(), source_type = "path"),
    "not valid",
    class = "delta_sharing_validation_error"
  )
})

test_that("descriptor properties are read-only", {
  table <- test_table()
  snapshot <- sharing_read(table, version = 3)
  changes <- sharing_changes(table, starting_version = 3)

  expect_read_only(table@client@profile, "source_type", "json")
  expect_read_only(table@client, "profile", SharingProfile("other.share"))
  expect_read_only(table@identifier, "table", "other")
  expect_read_only(table, "identifier", table_identifier("a", "b", "c"))
  expect_read_only(snapshot, "version", 4)
  expect_read_only(changes, "starting_version", 4)
})

test_that("S7 descriptors are compact and contain no execution results", {
  snapshot <- sharing_read(test_table(), columns = "id", limit = 10)
  properties <- S7::prop_names(snapshot)

  expect_setequal(
    properties,
    c(
      "table",
      "columns",
      "response_format",
      "version",
      "timestamp",
      "limit",
      "predicate"
    )
  )
  expect_false(any(c("files", "batches", "data", "metadata") %in% properties))
})
