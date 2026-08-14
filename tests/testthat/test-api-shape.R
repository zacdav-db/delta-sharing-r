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
  tbl <- client$table("sales.default.orders", concurrency = 8)
  expect_s3_class(tbl, "SharingTable")
  expect_equal(tbl$identifier()$table, "orders")
  expect_true(fs::dir_exists(tbl$cache_path))

  snap <- tbl$snapshot(
    version = 42,
    columns = c("a", "b"),
    limit = 100
  )
  expect_s3_class(snap, "SharingSnapshot")

  chg <- tbl$changes(
    starting_version = 120,
    ending_version = 125
  )
  expect_s3_class(chg, "SharingChanges")
})

test_that("download concurrency belongs to the table", {
  snapshot <- test_client()$table("sales.default.orders")$snapshot()

  purrr::walk(
    c(
      "to_arrow",
      "to_arrow_reader",
      "to_data_frame",
      "to_tibble",
      "to_arrow_stream"
    ),
    function(method) {
      arguments <- formals(snapshot[[method]])
      expect_identical(names(arguments), "batch_size")
      expect_identical(arguments$batch_size, 65536L)
    }
  )
  expect_identical(formals(test_client()$table)$concurrency, 4L)
  expect_error(
    test_client()$table("sales.default.orders", concurrency = 0),
    class = "rlang_error"
  )
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

test_that("snapshot leaves version and timestamp validation to the server", {
  tbl <- test_client()$table("sales.default.orders")
  expect_no_error(
    tbl$snapshot(
      version = 1,
      timestamp = as.POSIXct("2020-01-01", tz = "UTC")
    )
  )
})

test_that("snapshot accepts protocol-native timestamp strings", {
  tbl <- test_client()$table("sales.default.orders")

  expect_no_error(
    tbl$snapshot(timestamp = "2024-01-01T00:00:00.123Z")
  )
})

test_that("limit remains a non-negative whole number", {
  tbl <- test_client()$table("sales.default.orders")

  purrr::walk(
    list(-1, 1.5, Inf, TRUE),
    function(value) {
      expect_error(
        tbl$snapshot(limit = value),
        class = "delta_sharing_validation_error"
      )
    }
  )
})

test_that("changes leaves bound validation to the server", {
  tbl <- test_client()$table("sales.default.orders")

  expect_no_error(tbl$changes(starting_version = 2, ending_version = 1))
  expect_no_error(tbl$changes(ending_version = 2))
  expect_no_error(tbl$changes())
})

test_that("readers validate options interpreted by the package", {
  tbl <- test_client()$table("sales.default.orders")

  expect_error(
    tbl$snapshot(columns = 1),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    tbl$snapshot(predicate = "json"),
    class = "delta_sharing_validation_error"
  )
  expect_error(tbl$snapshot(response_format = "csv"), class = "rlang_error")
  expect_error(
    tbl$changes(columns = 1),
    class = "delta_sharing_validation_error"
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
    endpoint = paste0(
      "https://user:secret@sharing.example.test/api",
      "?access_token=query-secret#private-fragment"
    ),
    bearerToken = "tok"
  ))

  output <- capture.output(print(client))
  expect_equal(client$endpoint(), paste0(
    "https://user:secret@sharing.example.test/api",
    "?access_token=query-secret#private-fragment"
  ))
  expect_match(output, "sharing.example.test", fixed = TRUE)
  expect_false(grepl("user", output, fixed = TRUE))
  expect_false(grepl("secret", output, fixed = TRUE))
  expect_false(grepl("access_token", output, fixed = TRUE))
  expect_false(grepl("fragment", output, fixed = TRUE))
})

test_that("base readers require a concrete stream implementation", {
  reader <- SharingReader$new()

  expect_error(
    reader$to_arrow_stream(),
    "must be implemented",
    fixed = TRUE
  )
})

test_that("change readers print their staged identity", {
  changes <- test_client()$
    table("sales.default.events")$
    changes(starting_version = 1)

  expect_output(print(changes), "SharingChanges")
})
