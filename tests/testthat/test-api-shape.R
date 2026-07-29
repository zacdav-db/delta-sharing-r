test_that("sharing_client builds a SharingClient", {
  client <- sharing_client(list(
    shareCredentialsVersion = 2,
    type = "bearer_token",
    endpoint = "https://x.test/api",
    bearerToken = "tok"
  ))
  expect_s3_class(client, "SharingClient")
  expect_equal(client$endpoint(), "https://x.test/api")
})

test_that("the staged object graph composes", {
  client <- test_client()
  tbl <- client$table("sales.default.orders")
  expect_s3_class(tbl, "SharingTable")
  expect_equal(tbl$identifier()$table, "orders")

  snap <- tbl$snapshot(version = 42, columns = c("a", "b"), limit = 100)
  expect_s3_class(snap, "SharingSnapshot")

  chg <- tbl$changes(starting_version = 120, ending_version = 125)
  expect_s3_class(chg, "SharingChanges")
})

test_that("table accepts explicit share, schema, and name components", {
  tbl <- test_client()$table(
    share = "sales",
    schema = "default",
    name = "orders.v2"
  )

  expect_equal(
    tbl$identifier(),
    list(share = "sales", schema = "default", table = "orders.v2")
  )
})

test_that("snapshot rejects mutually exclusive version and timestamp", {
  tbl <- test_client()$table("sales.default.orders")
  expect_error(
    tbl$snapshot(version = 1, timestamp = as.POSIXct("2020-01-01", tz = "UTC")),
    class = "delta_sharing_validation_error"
  )
})

test_that("snapshot accepts protocol-native timestamp strings", {
  tbl <- test_client()$table("sales.default.orders")

  expect_no_error(
    tbl$snapshot(timestamp = "2024-01-01T00:00:00.123Z")
  )
})

test_that("numeric read options remain non-negative whole numbers", {
  tbl <- test_client()$table("sales.default.orders")

  purrr::walk(
    list(-1, 1.5, Inf, TRUE),
    function(value) {
      expect_error(
        tbl$snapshot(version = value),
        class = "delta_sharing_validation_error"
      )
    }
  )
})

test_that("print methods are stable", {
  client <- test_client()
  expect_output(print(client), "SharingClient")
  expect_output(print(client$table("s.sc.t")), "SharingTable")
  expect_output(print(client$table("s.sc.t")$snapshot()), "SharingSnapshot")
})

test_that("client printing redacts endpoint user information", {
  client <- sharing_client(list(
    shareCredentialsVersion = 1,
    endpoint = "https://user:secret@sharing.example.test/api",
    bearerToken = "tok"
  ))

  output <- capture.output(print(client))
  expect_match(output, "sharing.example.test", fixed = TRUE)
  expect_false(grepl("user", output, fixed = TRUE))
  expect_false(grepl("secret", output, fixed = TRUE))
})
