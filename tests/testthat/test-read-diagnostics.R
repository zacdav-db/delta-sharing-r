diagnostics_snapshot_transport <- function(chunk_bytes = 13L) {
  pages <- list(
    planned_snapshot_bytes("snapshot-page-1.ndjson"),
    planned_snapshot_bytes("snapshot-page-2.ndjson")
  )

  list(
    open = function(request) {
      page <- if (is.null(request$body$pageToken)) 1L else 2L
      response <- new.env(parent = emptyenv())
      response$status <- 200L
      response$headers <- planned_snapshot_headers()
      response$bytes <- pages[[page]]
      response$offset <- 1L
      response
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    pull = function(response) {
      if (response$offset > length(response$bytes)) {
        return(NULL)
      }
      end <- min(
        length(response$bytes),
        response$offset + chunk_bytes - 1L
      )
      chunk <- response$bytes[seq.int(response$offset, end)]
      response$offset <- end + 1L
      chunk
    },
    close = function(response) invisible(NULL),
    retry_after = function(response) NULL
  )
}

diagnostics_native_stream <- function(table_location,
                                      columns,
                                      limit,
                                      batch_size) {
  delta.sharing:::.release_snapshot_log(table_location)
  delta.sharing:::.native_test_stream(
    batches = 2L,
    rows_per_batch = 1L
  )
}

diagnostics_execution_interface <- function() {
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("unexpected authentication request")
    }),
    snapshot_transport = diagnostics_snapshot_transport(),
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    },
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    snapshot_temp_parent = tempdir(),
    native_stream_factory = diagnostics_native_stream,
    arrow_available = function() FALSE
  )
  delta.sharing:::.new_execution_interface(callbacks)
}

test_that("snapshot diagnostics are immutable and lifecycle-stable", {
  predicate_secret <- "predicate-value-must-not-appear"
  specification <- sharing_read(
    test_table(),
    columns = c("id", "group"),
    limit = 4,
    predicate = list(
      op = "equal",
      column = "region",
      value = predicate_secret
    )
  )
  stream <- delta.sharing:::.with_execution_interface(
    diagnostics_execution_interface(),
    read_arrow_stream(specification, batch_size = 3)
  )
  on.exit(stream$release(), add = TRUE)

  before <- read_diagnostics(stream)
  expect_true(S7::S7_inherits(before, SharingReadDiagnostics))
  expect_identical(before@read_kind, "snapshot")
  expect_identical(before@response_format, "delta")
  expect_identical(before@table_version, 42)
  expect_identical(before@page_count, 2)
  expect_identical(before@file_count, 2)
  expect_identical(before@columns, c("id", "group"))
  expect_identical(before@limit, 4)
  expect_identical(before@batch_size, 3)
  expect_null(before@concurrency)
  expect_true(before@predicate_hint_sent)
  expect_identical(before@server_limit_hint, 4)
  expect_s3_class(before@min_url_expiration, "POSIXct")
  expect_gt(before@url_expires_in_seconds, 0)
  expect_false(any(c(
    "active",
    "released",
    "batches_emitted",
    "rows_emitted"
  ) %in% S7::prop_names(before)))
  expect_read_only(before, "file_count", 99)
  expect_read_only(before, "columns", "secret")

  expect_identical(stream$get_next()$length, 1L)
  expect_identical(stream$get_next()$length, 1L)
  expect_null(stream$get_next())
  expect_identical(read_diagnostics(stream), before)

  stream$release()
  expect_identical(read_diagnostics(stream), before)

  rendered <- paste(
    capture.output(print(before)),
    capture.output(str(before)),
    capture.output(str(attributes(stream))),
    collapse = "\n"
  )
  for (secret in c(
    predicate_secret,
    "test-only-bearer-token",
    "page-one-signed-url-secret",
    "page-two-signed-url-secret",
    "refresh-token-private-secret",
    "next-page-token-private-secret",
    "_delta_log"
  )) {
    expect_false(grepl(secret, rendered, fixed = TRUE))
  }
})

test_that("diagnostics are isolated across concurrent streams", {
  interface <- diagnostics_execution_interface()
  first <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(
      sharing_read(test_table(), columns = "id", limit = 1),
      batch_size = 2
    )
  })
  second <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(
      sharing_read(test_table(), columns = c("group", "id"), limit = 2),
      batch_size = 4
    )
  })
  on.exit(first$release(), add = TRUE)
  on.exit(second$release(), add = TRUE)

  first_diagnostics <- read_diagnostics(first)
  second_diagnostics <- read_diagnostics(second)
  expect_identical(first_diagnostics@columns, "id")
  expect_identical(first_diagnostics@limit, 1)
  expect_identical(first_diagnostics@batch_size, 2)
  expect_identical(second_diagnostics@columns, c("group", "id"))
  expect_identical(second_diagnostics@limit, 2)
  expect_identical(second_diagnostics@batch_size, 4)

  first$release()
  expect_identical(read_diagnostics(second), second_diagnostics)
  expect_identical(second$get_next()$length, 1L)
  expect_identical(read_diagnostics(first), first_diagnostics)
})

test_that("diagnostics reject unattached streams and invalid states", {
  unattached <- delta.sharing:::.native_test_stream()
  on.exit(unattached$release(), add = TRUE)
  expect_error(
    read_diagnostics(unattached),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    read_diagnostics(list()),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.new_execution_interface(list(
      read_diagnostics = function(stream) NULL
    )),
    "Unknown execution callback"
  )

  expect_error(
    SharingReadDiagnostics(
      read_kind = "snapshot",
      response_format = "delta",
      page_count = 1,
      file_count = 1,
      batch_size = 1024
    ),
    "require `table_version`",
    class = "delta_sharing_validation_error"
  )
  expect_error(
    SharingReadDiagnostics(
      read_kind = "cdf",
      response_format = "delta",
      starting_version = 5,
      ending_version = 4,
      page_count = 1,
      file_count = 1,
      batch_size = 1024
    ),
    "inclusive version range",
    class = "delta_sharing_validation_error"
  )

  cdf <- SharingReadDiagnostics(
    read_kind = "cdf",
    response_format = "delta",
    starting_version = 5,
    ending_version = 7,
    page_count = 3,
    file_count = 9,
    columns = c("id", "_change_type"),
    batch_size = 1024,
    predicate_hint_sent = FALSE
  )
  expect_identical(cdf@starting_version, 5)
  expect_identical(cdf@ending_version, 7)
  expect_null(cdf@table_version)
})

test_that("diagnostic constructor and attachment guards fail closed", {
  valid <- list(
    read_kind = "snapshot",
    response_format = "delta",
    table_version = 42,
    page_count = 1,
    file_count = 1,
    batch_size = 1024
  )
  invalid <- list(
    list(read_kind = "other"),
    list(response_format = "auto"),
    list(page_count = -1),
    list(batch_size = 0),
    list(concurrency = 0),
    list(predicate_hint_sent = 1),
    list(
      min_url_expiration = as.POSIXct(
        "2100-01-01 00:00:00",
        tz = "UTC"
      )
    ),
    list(
      min_url_expiration = as.POSIXct(
        "2100-01-01 00:00:00",
        tz = "UTC"
      ),
      url_expires_in_seconds = -1
    )
  )
  for (override in invalid) {
    expect_error(
      do.call(
        SharingReadDiagnostics,
        utils::modifyList(valid, override)
      ),
      class = "delta_sharing_validation_error"
    )
  }

  concurrent <- do.call(
    SharingReadDiagnostics,
    c(valid, list(concurrency = 2))
  )
  expect_identical(concurrent@concurrency, 2)
  expect_error(
    delta.sharing:::.new_snapshot_read_diagnostics(
      specification = list(),
      planning = list(),
      batch_size = 1024,
      concurrency = NULL
    ),
    class = "delta_sharing_native_error"
  )
  invalid_stream <- delta.sharing:::.native_test_stream()
  on.exit(invalid_stream$release(), add = TRUE)
  expect_error(
    delta.sharing:::.attach_read_diagnostics(
      invalid_stream,
      list()
    ),
    class = "delta_sharing_native_error"
  )
  expect_error(
    delta.sharing:::.attach_read_diagnostics(NULL, concurrent),
    class = "delta_sharing_native_error"
  )
})
