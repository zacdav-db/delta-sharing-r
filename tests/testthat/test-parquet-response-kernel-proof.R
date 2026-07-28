parquet_response_proof_actions <- function() {
  path <- test_path(
    "fixtures",
    "protocol",
    "snapshot-parquet-kernel-proof.ndjson"
  )
  lapply(
    readLines(path, warn = FALSE, encoding = "UTF-8"),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
}

parquet_response_proof_delta_actions <- function(actions, local_file) {
  stopifnot(
    length(actions) == 3L,
    identical(actions[[1L]]$protocol$minReaderVersion, 1L),
    identical(actions[[2L]]$metaData$format$provider, "parquet"),
    grepl("^https://", actions[[3L]]$file$url)
  )

  metadata <- actions[[2L]]$metaData
  file <- actions[[3L]]$file
  empty_object <- structure(list(), names = character())
  list(
    list(protocol = list(
      minReaderVersion = 1L,
      minWriterVersion = 2L
    )),
    list(metaData = list(
      id = metadata$id,
      name = metadata$name,
      description = metadata$description,
      format = list(provider = "parquet", options = empty_object),
      schemaString = metadata$schemaString,
      partitionColumns = metadata$partitionColumns,
      # Parquet-response table configuration is not a Delta protocol
      # declaration. The production mapping must validate reader-sensitive
      # settings and omit benign Sharing-only configuration.
      configuration = empty_object
    )),
    list(add = list(
      # The committed fixture URL is deliberately unreachable. This test-only
      # substitution isolates log semantics; the existing native loopback test
      # separately proves that Kernel reads an absolute signed action URL.
      path = paste0("file://", local_file),
      partitionValues = file$partitionValues,
      size = file$size,
      # The wire timestamp identifies a table version, not file modification.
      modificationTime = 0L,
      dataChange = TRUE,
      stats = file$stats
    ))
  )
}

test_that("a Parquet response-shaped log uses the existing Kernel boundary", {
  actions <- parquet_response_proof_actions()
  fixture <- normalizePath(
    test_path(
      "fixtures",
      "delta",
      "local-table",
      "part-00000.parquet"
    ),
    winslash = "/",
    mustWork = TRUE
  )
  delta_actions <- parquet_response_proof_delta_actions(actions, fixture)

  table <- tempfile("parquet-response-kernel-proof-")
  dir.create(file.path(table, "_delta_log"), recursive = TRUE)
  on.exit(unlink(table, recursive = TRUE, force = TRUE), add = TRUE)
  commit <- file.path(
    table,
    "_delta_log",
    "00000000000000000000.json"
  )
  lines <- vapply(
    delta_actions,
    function(action) {
      unclass(jsonlite::toJSON(
        action,
        auto_unbox = TRUE,
        null = "null",
        digits = NA
      ))
    },
    character(1)
  )
  writeLines(lines, commit, useBytes = TRUE)

  encoded <- lapply(lines, jsonlite::fromJSON, simplifyVector = FALSE)
  expect_identical(encoded[[1L]]$protocol$minReaderVersion, 1L)
  expect_identical(encoded[[1L]]$protocol$minWriterVersion, 2L)
  expect_identical(
    unlist(encoded[[2L]]$metaData$partitionColumns, use.names = FALSE),
    "region"
  )
  expect_identical(encoded[[3L]]$add$partitionValues$region, "apac")
  expect_identical(encoded[[3L]]$add$modificationTime, 0L)
  expect_false("version" %in% names(encoded[[3L]]$add))
  expect_false("expirationTimestamp" %in% names(encoded[[3L]]$add))

  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  full_schema <- delta.sharing:::.native_snapshot_stream(table, limit = 0)
  expect_named(
    full_schema$get_schema()$children,
    c("value", "region", "id", "group", "active")
  )
  full_schema$release()
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before
  )

  stream <- delta.sharing:::.native_snapshot_stream(
    table,
    columns = c("region", "value", "id"),
    limit = 2,
    batch_size = 1L
  )
  expect_named(stream$get_schema()$children, c("region", "value", "id"))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before + 1
  )

  result <- delta.sharing:::.materialize_data_frame_stream(stream)
  expect_named(result, c("region", "value", "id"))
  expect_type(result$region, "character")
  expect_type(result$value, "double")
  expect_identical(result$region, c("apac", "apac"))
  expect_equal(result$value, c(1.5, 2.5))
  expect_equal(as.numeric(result$id), c(1, 2))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before
  )
})
