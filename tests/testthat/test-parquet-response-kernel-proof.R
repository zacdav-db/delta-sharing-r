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

parquet_response_proof_bytes <- function(actions) {
  charToRaw(paste0(
    paste(
      vapply(
        actions,
        function(action) {
          unclass(jsonlite::toJSON(
            action,
            auto_unbox = TRUE,
            null = "null",
            digits = NA
          ))
        },
        character(1)
      ),
      collapse = "\n"
    ),
    "\n"
  ))
}

parquet_response_proof_repoint <- function(prepared, local_file) {
  guard <- delta.sharing:::.prepared_snapshot_state(prepared)$guard
  commit <- file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log",
    "00000000000000000000.json"
  )
  lines <- readLines(commit, warn = FALSE, encoding = "UTF-8")
  delta_actions <- lapply(lines, jsonlite::fromJSON, simplifyVector = FALSE)

  # The committed signed URL is deliberately unreachable. This test-only
  # substitution occurs after the complete production planner/normalizer/log
  # path and isolates Kernel log semantics. Native loopback coverage separately
  # proves signed URL handling.
  delta_actions[[3L]]$add$path <- paste0("file://", local_file)
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
  Sys.chmod(commit, mode = "0600")
  lines
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
  prepared <- delta.sharing:::.prepare_snapshot_read(
    sharing_read(
      test_table(),
      columns = c("region", "value", "id"),
      limit = 2,
      response_format = "auto"
    ),
    fetch = function(request) {
      planned_pull_response(
        parquet_response_proof_bytes(actions),
        headers = planned_snapshot_headers(
          version = "42",
          capabilities = "responseformat=parquet"
        )
      )
    },
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    }
  )
  prepared_state <- delta.sharing:::.prepared_snapshot_state(prepared)
  guard_state <- delta.sharing:::.validate_snapshot_log_guard(
    prepared_state$guard
  )
  root <- guard_state$root
  on.exit(
    {
      if (!isTRUE(prepared_state$released)) {
        delta.sharing:::.release_prepared_snapshot(prepared)
      }
    },
    add = TRUE
  )

  diagnostics <- delta.sharing:::.prepared_snapshot_diagnostics(prepared)
  invocation <- delta.sharing:::.prepared_snapshot_invocation(prepared)
  expect_identical(diagnostics$response_format, "parquet")
  expect_identical(diagnostics$table_version, 42)
  expect_identical(diagnostics$file_count, 1L)
  expect_identical(invocation$projection, c("region", "value", "id"))
  expect_identical(invocation$exact_limit, 2)

  lines <- parquet_response_proof_repoint(prepared, fixture)

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
  stream <- delta.sharing:::.native_snapshot_stream(
    table_location = prepared_state$guard,
    columns = invocation$projection,
    limit = invocation$exact_limit,
    batch_size = 1L
  )
  expect_true(guard_state$released)
  prepared_state$released <- TRUE
  expect_named(stream$get_schema()$children, c("region", "value", "id"))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before + 1
  )
  expect_true(dir.exists(root))

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
  expect_false(dir.exists(root))
})
