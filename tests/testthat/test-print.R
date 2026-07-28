test_that("profile and client printing never expose credentials", {
  secret <- "SUPER-SECRET-BEARER-TOKEN"
  profile <- SharingProfile(list(
    shareCredentialsVersion = 1,
    endpoint = "https://sharing.example.test",
    bearerToken = secret
  ))
  client <- SharingClient(profile)

  profile_output <- capture.output(print(profile))
  client_output <- capture.output(print(client))

  expect_snapshot(cat(profile_output, sep = "\n"))
  expect_snapshot(cat(client_output, sep = "\n"))
  expect_false(any(grepl(secret, profile_output, fixed = TRUE)))
  expect_false(any(grepl(secret, client_output, fixed = TRUE)))
  expect_false(any(grepl("bearerToken", profile_output, fixed = TRUE)))
  expect_false(any(grepl("bearerToken", client_output, fixed = TRUE)))
})

test_that("identifier and table printing is unambiguous", {
  client <- test_client()
  identifier <- table_identifier("sales.eu", "default", "events.v2")
  table <- sharing_table(client, identifier)

  expect_snapshot(print(identifier))
  expect_snapshot(print(table))
})

test_that("snapshot printing is safe for every time-travel mode", {
  table <- test_table()

  expect_snapshot(print(sharing_read(table)))
  expect_snapshot(print(sharing_read(
    table,
    version = 42,
    columns = c("id", "Amount"),
    limit = 100,
    response_format = "delta"
  )))
  expect_snapshot(print(sharing_read(
    table,
    timestamp = as.POSIXct("2026-07-01", tz = "UTC")
  )))
})

test_that("CDF printing handles open and closed ranges", {
  table <- test_table()

  expect_snapshot(print(sharing_changes(
    table,
    starting_version = 10
  )))
  expect_snapshot(print(sharing_changes(
    table,
    starting_timestamp = as.POSIXct("2026-07-01", tz = "UTC"),
    ending_timestamp = as.POSIXct("2026-07-02", tz = "UTC"),
    columns = c("id", "_change_type"),
    response_format = "delta"
  )))
})
