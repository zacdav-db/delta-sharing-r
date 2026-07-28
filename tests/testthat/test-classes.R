test_that("profile and client constructors create S7 descriptors", {
  profile <- SharingProfile(test_profile())
  client <- SharingClient(profile)

  expect_true(S7::S7_inherits(profile, SharingProfile))
  expect_true(S7::S7_inherits(client, SharingClient))
  expect_identical(profile@source_type, "list")
  expect_identical(profile@label, "inline profile")
  expect_identical(profile@version, 1)
  expect_identical(profile@endpoint, "https://sharing.example.test/api")
  expect_identical(profile@auth_type, "bearer_token")
  expect_identical(client@profile, profile)
  expect_identical(
    delta.sharing:::.client_context(client)$state,
    "configured"
  )
})

test_that("sharing_client accepts supported profile source forms", {
  profile_json <- paste0(
    '{"shareCredentialsVersion":1,',
    '"endpoint":"https://sharing.example.test/api",',
    '"bearerToken":"secret"}'
  )
  inline_json <- sharing_client(profile_json)
  inline_raw <- sharing_client(charToRaw(profile_json))
  inline_list <- sharing_client(test_profile())
  connection <- rawConnection(charToRaw(profile_json), open = "rb")
  on.exit(close(connection), add = TRUE)
  inline_connection <- sharing_client(connection)
  path_profile <- sharing_profile(test_path(
    "fixtures",
    "profiles",
    "bearer-v1.json"
  ))

  expect_identical(inline_json@profile@source_type, "json")
  expect_identical(inline_raw@profile@source_type, "json")
  expect_identical(inline_list@profile@source_type, "list")
  expect_identical(inline_connection@profile@source_type, "connection")
  expect_identical(path_profile@source_type, "path")
  expect_identical(path_profile@label, "bearer-v1.json")
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
  expect_read_only(table@client, "profile", SharingProfile(test_profile()))
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
