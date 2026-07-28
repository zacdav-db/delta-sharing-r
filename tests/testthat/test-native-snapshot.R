native_delta_fixture <- function() {
  normalizePath(
    test_path("fixtures", "delta", "local-table"),
    winslash = "/",
    mustWork = TRUE
  )
}

native_snapshot_components <- function() {
  path <- test_path("fixtures", "protocol", "snapshot-delta.ndjson")
  bytes <- readBin(path, what = "raw", n = file.info(path)$size)
  decoder <- delta.sharing:::.new_ndjson_decoder("native snapshot fixture")
  actions <- c(
    delta.sharing:::.ndjson_decoder_push(decoder, bytes),
    delta.sharing:::.ndjson_decoder_finish(decoder)
  )
  list(
    protocol = actions[[1L]]$value,
    metadata = actions[[2L]]$value,
    files = lapply(actions[3:4], `[[`, "value")
  )
}

test_that("a real Kernel snapshot preserves schema, projection, and exact limits", {
  empty <- delta.sharing:::.native_snapshot_stream(
    native_delta_fixture(),
    limit = 0,
    batch_size = 2L
  )
  expect_named(empty$get_schema()$children, c("id", "group", "value", "active"))
  expect_null(empty$get_next())
  empty$release()

  stream <- delta.sharing:::.native_snapshot_stream(
    native_delta_fixture(),
    columns = c("group", "id"),
    limit = 5,
    batch_size = 2L
  )
  expect_named(stream$get_schema()$children, c("group", "id"))
  batches <- list(stream$get_next(), stream$get_next(), stream$get_next())
  batch_rows <- vapply(batches, `[[`, integer(1), "length")
  expect_equal(sum(batch_rows), 5L)
  expect_true(all(batch_rows <= 2L))
  expect_null(stream$get_next())
  stream$release()

  one <- delta.sharing:::.native_snapshot_stream(
    native_delta_fixture(),
    limit = 1,
    batch_size = 10L
  )
  expect_equal(one$get_next()$length, 1L)
  expect_null(one$get_next())
  one$release()
})

test_that("real Kernel output imports into arrow without IPC", {
  skip_if_not_installed("arrow")

  stream <- delta.sharing:::.native_snapshot_stream(
    native_delta_fixture(),
    columns = c("id", "value"),
    batch_size = 2L
  )
  reader <- arrow::as_record_batch_reader(stream)
  table <- reader$read_table()

  expect_equal(table$num_rows, 7)
  expect_identical(table$schema$names, c("id", "value"))
})

test_that("early release drops a live Kernel scan", {
  gc()
  start <- delta.sharing:::.native_diagnostics()
  stream <- delta.sharing:::.native_snapshot_stream(
    native_delta_fixture(),
    batch_size = 1L
  )
  expect_equal(
    delta.sharing:::.native_diagnostics()$active_streams,
    start$active_streams + 1
  )
  expect_equal(stream$get_next()$length, 1L)
  stream$release()
  expect_equal(
    delta.sharing:::.native_diagnostics()$active_streams,
    start$active_streams
  )
})

test_that("the compact native boundary rejects unsupported controls", {
  expect_error(
    delta.sharing:::.native_snapshot_stream("https://example.test/table"),
    "only a local path",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.native_snapshot_stream(
      native_delta_fixture(),
      columns = c("id", "ID")
    ),
    "ignoring case",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.native_snapshot_stream(
      native_delta_fixture(),
      batch_size = 0
    ),
    "between 1 and 1000000",
    class = "delta_sharing_validation_error"
  )
})

test_that("Kernel and object-store errors are fixed and secret-free", {
  malformed <- tempfile("delta-sharing-malformed-url-secret-")
  dir.create(file.path(malformed, "_delta_log"), recursive = TRUE)
  on.exit(unlink(malformed, recursive = TRUE, force = TRUE), add = TRUE)
  writeLines(
    "not valid Delta JSON",
    file.path(malformed, "_delta_log", "00000000000000000000.json"),
    useBytes = TRUE
  )

  condition <- expect_error(
    delta.sharing:::.native_snapshot_stream(malformed),
    "Delta Kernel snapshot preparation failed"
  )
  expect_false(grepl("url-secret", conditionMessage(condition), fixed = TRUE))
  expect_false(grepl(malformed, conditionMessage(condition), fixed = TRUE))

  signed <- tempfile("delta-sharing-signed-")
  dir.create(file.path(signed, "_delta_log"), recursive = TRUE)
  on.exit(unlink(signed, recursive = TRUE, force = TRUE), add = TRUE)
  lines <- readLines(
    file.path(
      native_delta_fixture(),
      "_delta_log",
      "00000000000000000000.json"
    ),
    warn = FALSE,
    encoding = "UTF-8"
  )
  secret <- "super-secret-query-value"
  lines <- sub(
    "\"path\":\"part-00000.parquet\"",
    paste0(
      "\"path\":\"http://127.0.0.1:1/data.parquet?",
      "X-Amz-Signature=",
      secret,
      "\""
    ),
    lines,
    fixed = TRUE
  )
  writeLines(
    lines[!grepl("part-00001.parquet", lines, fixed = TRUE)],
    file.path(signed, "_delta_log", "00000000000000000000.json"),
    useBytes = TRUE
  )

  stream <- delta.sharing:::.native_snapshot_stream(signed)
  condition <- expect_error(stream$get_next(), "Delta Kernel data scan failed")
  message <- conditionMessage(condition)
  expect_false(grepl(secret, message, fixed = TRUE))
  expect_false(grepl("127.0.0.1", message, fixed = TRUE))
  stream$release()
})

test_that("prepared-log cleanup follows explicit release, exhaustion, and GC", {
  make_guard <- function() {
    components <- native_snapshot_components()
    delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      components$files
    )
  }

  guard <- make_guard()
  root <- guard$state$root
  stream <- delta.sharing:::.native_snapshot_stream(guard, limit = 0)
  expect_true(guard$state$released)
  expect_true(dir.exists(root))
  stream$release()
  expect_false(file.exists(root))

  guard <- make_guard()
  root <- guard$state$root
  stream <- delta.sharing:::.native_snapshot_stream(guard, limit = 0)
  expect_null(stream$get_next())
  expect_false(file.exists(root))
  stream$release()

  root <- local({
    guard <- make_guard()
    stream <- delta.sharing:::.native_snapshot_stream(guard, limit = 0)
    guard$state$root
  })
  gc()
  expect_false(file.exists(root))
})
