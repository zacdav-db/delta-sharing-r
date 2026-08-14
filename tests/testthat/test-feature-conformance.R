feature_snapshot_actions <- function(fixture) {
  root <- fixture_table(fixture)
  purrr::map(fixture_commit_actions(fixture, 0L), function(action) {
    if (!is.null(action$protocol)) {
      return(list(protocol = list(deltaProtocol = action$protocol)))
    }
    if (!is.null(action$metaData)) {
      return(list(metaData = list(deltaMetadata = action$metaData)))
    }

    add <- action$add
    source <- fs::path(root, add$path)
    add$path <- local_file_url(source)
    list(file = list(
      id = fixture_file_id(source),
      size = as.numeric(fs::file_size(source)),
      deltaSingleAction = list(add = add)
    ))
  })
}

feature_snapshot_table <- function(fixture, actions) {
  httr2::local_mocked_responses(function(req) {
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  }, env = parent.frame())
  shared_table <- test_client()$table(
    paste("fixtures", "default", fixture, sep = ".")
  )
  if (fs::dir_exists(shared_table$cache_path)) {
    fs::dir_delete(shared_table$cache_path)
  }
  withr::defer(
    if (fs::dir_exists(shared_table$cache_path)) {
      fs::dir_delete(shared_table$cache_path)
    },
    envir = parent.frame()
  )
  shared_table
}

test_that("column mapping restores logical data and partition names", {
  fixture <- "column-mapping"
  shared_table <- feature_snapshot_table(
    fixture,
    feature_snapshot_actions(fixture)
  )
  data <- shared_table$snapshot(
    columns = c("region", "id", "value"),
    response_format = "delta"
  )$to_tibble(batch_size = 2L)

  expect_named(data, c("region", "id", "value"))
  expect_identical(data$region, rep("apac", 3L))
  expect_equal(data$id, c(10, 11, 12))
  expect_identical(data$value, c("alpha", "beta", "gamma"))
})

test_that("column mapping by ID follows Parquet field IDs", {
  fixture <- "column-mapping-id"
  shared_table <- feature_snapshot_table(
    fixture,
    feature_snapshot_actions(fixture)
  )
  data <- shared_table$snapshot(
    columns = c("value", "region", "id"),
    response_format = "delta"
  )$to_tibble(batch_size = 2L)

  expect_named(data, c("value", "region", "id"))
  expect_identical(data$value, c("delta", "kernel", "arrow"))
  expect_identical(data$region, rep("latam", 3L))
  expect_equal(data$id, c(31, 32, 33))
})

test_that("deletion vectors remove rows before materialization", {
  fixture <- "deletion-vectors"
  shared_table <- feature_snapshot_table(
    fixture,
    feature_snapshot_actions(fixture)
  )
  data <- shared_table$snapshot(response_format = "delta")$to_tibble(
    batch_size = 2L
  )

  expect_equal(data$id, c(0, 2, 4))
  expect_identical(data$value, c("zero", "two", "four"))
  expect_false(any(data$id %in% c(1, 3)))
})

test_that("timestamp without timezone and its partition survive staging", {
  fixture <- "timestamp-ntz"
  shared_table <- feature_snapshot_table(
    fixture,
    feature_snapshot_actions(fixture)
  )
  snapshot <- shared_table$snapshot(
    columns = c("observed_at", "region", "id"),
    response_format = "delta"
  )
  stream <- snapshot$to_arrow_stream(batch_size = 1L)
  withr::defer(release_materializer_stream(stream))
  expect_identical(stream$get_schema()$children$observed_at$format, "tsu:")

  data <- snapshot$to_tibble(batch_size = 1L)
  expect_identical(data$region, rep("emea", 2L))
  expect_equal(data$id, c(21, 22))
  expect_equal(
    as.numeric(data$observed_at),
    c(1767323045.123456, 1780819750.654321),
    tolerance = 1e-6
  )
})

test_that("mapped logical types survive the complete Sharing read path", {
  fixture <- "logical-types"
  shared_table <- feature_snapshot_table(
    fixture,
    feature_snapshot_actions(fixture)
  )
  snapshot <- shared_table$snapshot(response_format = "delta")
  stream <- snapshot$to_arrow_stream(batch_size = 2L)
  withr::defer(release_materializer_stream(stream))
  schema <- stream$get_schema()

  expect_identical(schema$children$amount$format, "d:18,4")
  expect_identical(schema$children$metrics$format, "+m")
  expect_identical(schema$children$observed_at$format, "tsu:UTC")
  expect_identical(schema$children$local_at$format, "tsu:")

  data <- snapshot$to_tibble(batch_size = 2L)
  expect_named(
    data,
    c("id", "amount", "metrics", "observed_at", "local_at", "profile")
  )
  expect_equal(data$id, c(101, 102, 103))
  expect_equal(data$amount, c(12345.6789, -0.01, NA))
  expect_identical(data$metrics[[1L]]$key, c("alpha", "beta"))
  expect_equal(data$metrics[[1L]]$value, c(1.25, -2.5))
  expect_null(data$metrics[[3L]])
  expect_equal(data$profile$score, c(7.125, 8.5, NA))
  expect_identical(
    data$profile$contact$label,
    c("first", NA_character_, "third")
  )
  expect_identical(
    data$profile$events[[1L]]$code,
    c("open", "close")
  )
})

test_that("snapshot types survive the complete Sharing read path", {
  fixture <- "snapshot-types"
  shared_table <- feature_snapshot_table(
    fixture,
    feature_snapshot_actions(fixture)
  )
  data <- shared_table$snapshot(response_format = "delta")$to_tibble(
    batch_size = 2L
  )

  expect_equal(data$id, c(1, 2, 3))
  expect_identical(data$flag, c(TRUE, FALSE, NA))
  expect_identical(data$tiny, c(-128L, 0L, 127L))
  expect_identical(data$small, c(-32768L, 0L, 32767L))
  expect_identical(data$count, c(10L, 20L, 30L))
  expect_equal(data$ratio, c(1.25, NA, 3.5))
  expect_equal(data$measure, c(1.5, 2.5, NA))
  expect_identical(data$label, c("alpha", "beta", NA))
  expect_identical(data$payload[[1L]], charToRaw("A"))
  expect_identical(data$payload[[2L]], as.raw(c(0, 255)))
  expect_null(data$payload[[3L]])
  expect_identical(
    data$event_date,
    as.Date(c("2025-01-02", "2025-06-07", NA))
  )
  expect_identical(attr(data$event_at, "tzone"), "UTC")
  expect_identical(data$tags[[1L]], c("red", "blue"))
  expect_identical(data$tags[[2L]], character())
  expect_null(data$tags[[3L]])
  expect_identical(data$details$score, c(7L, 8L, NA))
  expect_identical(data$details$note, c("left", NA, "right"))
})
