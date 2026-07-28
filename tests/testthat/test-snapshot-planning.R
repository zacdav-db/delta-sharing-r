snapshot_page_with_file_expiration <- function(expiration_timestamp) {
  lines <- readLines(
    test_path("fixtures", "protocol", "snapshot-page-2.ndjson"),
    warn = FALSE
  )
  file <- jsonlite::fromJSON(lines[[3L]], simplifyVector = FALSE)
  file$file$expirationTimestamp <- expiration_timestamp
  lines[[3L]] <- jsonlite::toJSON(
    file,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  lines[[4L]] <- '{"endStreamAction":{}}'
  charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
}

test_that("snapshot requests preserve raw identifiers and current query fields", {
  table <- sharing_table(
    test_client(),
    share = "sales/a",
    schema = "schema with space",
    table = "orders?"
  )
  predicate_secret <- "predicate-private-secret"
  read <- sharing_read(
    table,
    version = 42,
    columns = c("OrderID", "nested.value"),
    limit = 2^40,
    predicate = list(
      value = predicate_secret,
      op = "equal",
      column = "region"
    )
  )
  request <- delta.sharing:::.plan_snapshot_request(
    read,
    max_files_per_page = 123L
  )

  expect_identical(request$method, "POST")
  expect_identical(
    request$path_segments,
    c(
      "shares",
      "sales/a",
      "schemas",
      "schema with space",
      "tables",
      "orders?",
      "query"
    )
  )
  expect_identical(request$body$version, 42)
  expect_null(request$body$limitHint)
  expect_identical(request$body$maxFiles, 123L)
  expect_null(request$body$includeRefreshToken)
  expect_null(request$body$columns)
  expect_identical(
    jsonlite::fromJSON(
      request$body$jsonPredicateHints,
      simplifyVector = FALSE
    ),
    list(
      column = "region",
      op = "equal",
      value = predicate_secret
    )
  )
  expect_identical(request$headers$fileidhash, "delta")
  expect_match(
    request$headers[["delta-sharing-capabilities"]],
    "responseformat=delta,parquet",
    fixed = TRUE
  )
  expect_match(
    request$headers[["delta-sharing-capabilities"]],
    "includeendstreamaction=true",
    fixed = TRUE
  )

  printed <- paste(
    capture.output(print(request)),
    collapse = "\n"
  )
  expect_false(grepl(predicate_secret, printed, fixed = TRUE))
  expect_false(grepl("sales/a", printed, fixed = TRUE))

  int32_request <- delta.sharing:::.plan_snapshot_request(sharing_read(
    test_table(),
    limit = .Machine$integer.max
  ))
  expect_identical(
    int32_request$body$limitHint,
    as.double(.Machine$integer.max)
  )
})

test_that("latest and paginated requests follow the current provider wire model", {
  read <- sharing_read(test_table(), limit = 3)
  first <- delta.sharing:::.plan_snapshot_request(read)
  page_secret <- "opaque-page-token-private-secret"
  second <- delta.sharing:::.plan_snapshot_request(
    read,
    page_token = page_secret,
    page_number = 2L
  )

  expect_true(first$body$includeRefreshToken)
  expect_null(first$body$pageToken)
  expect_null(second$body$includeRefreshToken)
  expect_identical(second$body$pageToken, page_secret)
  expect_identical(second$body$limitHint, first$body$limitHint)
  expect_identical(second$page_number, 2L)
  expect_false(grepl(
    page_secret,
    paste(
      capture.output(print(second)),
      collapse = "\n"
    ),
    fixed = TRUE
  ))
})

test_that("snapshot planning rejects invalid hints and negotiates Parquet", {
  invalid_predicates <- list(
    list(1, 2),
    structure(list(op = "equal", op = "other"), names = c("op", "op")),
    list(op = Sys.time()),
    list(op = c("equal", "other")),
    list(op = NaN)
  )
  for (predicate in invalid_predicates) {
    expect_error(
      delta.sharing:::.plan_snapshot_request(
        sharing_read(test_table(), predicate = predicate)
      ),
      class = "delta_sharing_validation_error"
    )
  }

  parquet_request <- delta.sharing:::.plan_snapshot_request(
    sharing_read(test_table(), response_format = "parquet")
  )
  expect_match(
    parquet_request$headers[["delta-sharing-capabilities"]],
    "responseformat=parquet;",
    fixed = TRUE
  )
})

test_that("snapshot planning helpers enforce bounded canonical controls", {
  expect_error(
    delta.sharing:::.validate_snapshot_read(list()),
    class = "delta_sharing_validation_error"
  )
  for (value in list(0, -1, 1.5, NA_real_, Inf, c(1, 2), "1")) {
    expect_error(
      delta.sharing:::.snapshot_positive_integer(value, "page_number"),
      class = "delta_sharing_validation_error"
    )
  }

  expect_null(delta.sharing:::.canonical_snapshot_json(NULL))
  expect_error(
    delta.sharing:::.canonical_snapshot_json(
      structure(list(value = 1), class = "unsupported")
    ),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.canonical_snapshot_json(
      structure(list(1), names = "")
    ),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.snapshot_page_token("bad\r\nprivate-token"),
    class = "delta_sharing_protocol_error"
  )
  expect_match(
    delta.sharing:::.snapshot_query_capabilities("parquet"),
    "responseformat=parquet;",
    fixed = TRUE
  )
})

test_that("snapshot headers and capabilities reject ambiguous wire values", {
  expect_null(delta.sharing:::.snapshot_header(NULL, "optional"))
  expect_error(
    delta.sharing:::.snapshot_header(NULL, "required", required = TRUE),
    "missing",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.snapshot_header(
      c("X-Test" = "one", "x-test" = "two"),
      "x-test"
    ),
    "invalid",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.snapshot_header(
      c("X-Test" = "bad\nprivate-value"),
      "x-test"
    ),
    "invalid",
    class = "delta_sharing_protocol_error"
  )

  invalid_capabilities <- c(
    "responseformat=delta;;includeendstreamaction=true",
    "responseformat",
    "responseformat=delta;responseformat=delta",
    "responseformat=delta,parquet",
    "includeendstreamaction=maybe"
  )
  for (capability in invalid_capabilities) {
    expect_error(
      delta.sharing:::.parse_snapshot_capabilities(c(
        "delta-sharing-capabilities" = capability
      )),
      class = "delta_sharing_error"
    )
  }
  capabilities <- delta.sharing:::.parse_snapshot_capabilities(c(
    "delta-sharing-capabilities" =
      "responseformat=delta;readerfeatures=timestampntz,columnmapping"
  ))
  expect_identical(
    capabilities$readerfeatures,
    c("timestampntz", "columnmapping")
  )
})

test_that("snapshot clock, fetch, and prepared-state guards fail closed", {
  expect_error(
    delta.sharing:::.snapshot_now("not-a-function"),
    "`clock` must be a function",
    fixed = TRUE
  )
  for (clock in list(
    function() NA,
    function() as.POSIXct(NA),
    function() c(Sys.time(), Sys.time())
  )) {
    expect_error(
      delta.sharing:::.snapshot_now(clock),
      "must return one non-missing POSIXct"
    )
  }
  expect_error(
    delta.sharing:::.safe_snapshot_fetch("not-a-function", list()),
    "`fetch` must be a function",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.prepared_snapshot_state(list()),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) stop("must not be called"),
      write_commit = "not-a-function"
    ),
    "`write_commit` must be a function",
    fixed = TRUE
  )
})

test_that("a page is pulled incrementally and always closed", {
  recorder <- new.env(parent = emptyenv())
  bytes <- planned_snapshot_bytes("snapshot-page-1.ndjson")
  response <- planned_pull_response(
    bytes,
    chunk_bytes = 1L,
    recorder = recorder
  )
  page <- delta.sharing:::.consume_snapshot_page(response)

  expect_gt(recorder$pulls, 100L)
  expect_identical(recorder$closes, 1L)
  expect_identical(page$table_version, 42)
  expect_length(page$files, 1L)
  expect_identical(
    delta.sharing:::.snapshot_file_state(page$files[[1L]])$id,
    "file-a"
  )
  expect_identical(
    page$terminal$next_page_token,
    "next-page-token-private-secret"
  )
  expect_identical(
    page$terminal$min_url_expiration_timestamp,
    4102444800000
  )
})

test_that("pull response cleanup is retryable and disarms only after success", {
  recorder <- new.env(parent = emptyenv())
  recorder$closes <- 0L
  close_secret <- "first-close-private-secret"
  response <- planned_pull_response(
    planned_snapshot_bytes("snapshot-page-1.ndjson")
  )
  response$close <- function() {
    recorder$closes <- recorder$closes + 1L
    if (recorder$closes == 1L) {
      stop(close_secret)
    }
    invisible(NULL)
  }
  guard <- delta.sharing:::.new_snapshot_pull_close_guard(response)

  expect_false(delta.sharing:::.close_snapshot_pull_guard(
    guard,
    attempts = 1L
  ))
  expect_false(guard$closed)
  expect_true(delta.sharing:::.close_snapshot_pull_guard(
    guard,
    attempts = 1L
  ))
  expect_true(guard$closed)
  expect_true(delta.sharing:::.close_snapshot_pull_guard(guard))
  expect_identical(recorder$closes, 2L)
})

test_that("page cleanup retries close failures without exposing conditions", {
  bytes <- planned_snapshot_bytes("snapshot-page-1.ndjson")
  retry_recorder <- new.env(parent = emptyenv())
  retry_recorder$closes <- 0L
  retry_response <- planned_pull_response(bytes)
  retry_response$close <- function() {
    retry_recorder$closes <- retry_recorder$closes + 1L
    if (retry_recorder$closes == 1L) {
      stop("retry-close-private-secret")
    }
    invisible(NULL)
  }

  expect_silent(page <- delta.sharing:::.consume_snapshot_page(
    retry_response
  ))
  expect_identical(page$table_version, 42)
  expect_identical(retry_recorder$closes, 2L)

  permanent_recorder <- new.env(parent = emptyenv())
  permanent_recorder$closes <- 0L
  permanent_response <- planned_pull_response(bytes)
  permanent_response$close <- function() {
    permanent_recorder$closes <- permanent_recorder$closes + 1L
    stop("permanent-close-private-secret")
  }

  expect_silent(page <- delta.sharing:::.consume_snapshot_page(
    permanent_response
  ))
  expect_identical(page$table_version, 42)
  expect_identical(permanent_recorder$closes, 2L)
  gc()
  expect_identical(permanent_recorder$closes, 3L)
})

test_that("an armed pull response close guard retries during finalization", {
  recorder <- new.env(parent = emptyenv())
  recorder$closes <- 0L

  local({
    response <- planned_pull_response(
      planned_snapshot_bytes("snapshot-page-1.ndjson")
    )
    response$close <- function() {
      recorder$closes <- recorder$closes + 1L
      if (recorder$closes == 1L) {
        stop("finalizer-close-private-secret")
      }
      invisible(NULL)
    }
    guard <- delta.sharing:::.new_snapshot_pull_close_guard(response)
    expect_false(delta.sharing:::.close_snapshot_pull_guard(
      guard,
      attempts = 1L
    ))
    expect_false(guard$closed)
  })

  gc()
  expect_identical(recorder$closes, 2L)
})

test_that("paginated preparation hands a private atomic log to the native lane", {
  requests <- list()
  recorders <- list()
  page_one <- planned_snapshot_bytes("snapshot-page-1.ndjson")
  page_two <- planned_snapshot_bytes("snapshot-page-2.ndjson")
  fetch <- function(request) {
    requests[[length(requests) + 1L]] <<- request
    recorder <- new.env(parent = emptyenv())
    recorders[[length(recorders) + 1L]] <<- recorder
    if (request$page_number == 1L) {
      planned_pull_response(
        page_one,
        chunk_bytes = 13L,
        recorder = recorder
      )
    } else {
      expect_identical(
        request$body$pageToken,
        "next-page-token-private-secret"
      )
      planned_pull_response(
        page_two,
        chunk_bytes = 11L,
        recorder = recorder
      )
    }
  }
  predicate_secret <- "predicate-diagnostic-secret"
  read <- sharing_read(
    test_table(),
    columns = c("id"),
    limit = 2^40,
    predicate = list(op = "isNotNull", column = "id")
  )
  parent <- tempfile("snapshot-planning-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  prepared <- delta.sharing:::.prepare_snapshot_read(
    read,
    fetch = fetch,
    temp_parent = parent,
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    }
  )
  invocation <- delta.sharing:::.prepared_snapshot_invocation(prepared)
  diagnostics <- delta.sharing:::.prepared_snapshot_diagnostics(prepared)
  guard <- delta.sharing:::.prepared_snapshot_state(prepared)$guard
  table_path <- delta.sharing:::.snapshot_log_path(guard)
  commit <- readLines(
    file.path(table_path, "_delta_log", "00000000000000000000.json"),
    warn = FALSE
  )

  expect_length(requests, 2L)
  expect_true(all(vapply(
    recorders,
    function(recorder) identical(recorder$closes, 1L),
    logical(1)
  )))
  expect_identical(invocation$read_kind, "snapshot")
  expect_identical(invocation$version, 0)
  expect_identical(invocation$projection, "id")
  expect_identical(invocation$exact_limit, 2^40)
  expect_match(invocation$table_uri, "^file://")
  expect_identical(diagnostics$response_format, "delta")
  expect_identical(diagnostics$table_version, 42)
  expect_identical(diagnostics$page_count, 2L)
  expect_identical(diagnostics$file_count, 2L)
  expect_true(diagnostics$predicate_hint_sent)
  expect_null(diagnostics$server_limit_hint)
  expect_true(all(vapply(
    requests,
    function(request) is.null(request$body$limitHint),
    logical(1)
  )))
  expect_s3_class(diagnostics$min_url_expiration, "POSIXct")
  expect_identical(
    delta.sharing:::.prepared_snapshot_refresh_token(prepared),
    "refresh-token-private-secret"
  )
  expect_true(any(grepl(
    "page-one-signed-url-secret",
    commit,
    fixed = TRUE
  )))
  expect_true(any(grepl(
    "page-two-signed-url-secret",
    commit,
    fixed = TRUE
  )))

  safe_output <- paste(
    capture.output(print(prepared)),
    capture.output(str(diagnostics)),
    collapse = "\n"
  )
  for (secret in c(
    "refresh-token-private-secret",
    "next-page-token-private-secret",
    "page-one-signed-url-secret",
    "page-two-signed-url-secret",
    predicate_secret
  )) {
    expect_false(grepl(secret, safe_output, fixed = TRUE))
  }

  expect_true(delta.sharing:::.release_prepared_snapshot(prepared))
  expect_false(dir.exists(table_path))
  expect_true(delta.sharing:::.release_prepared_snapshot(prepared))
  expect_null(delta.sharing:::.prepared_snapshot_refresh_token(prepared))
  expect_error(
    delta.sharing:::.prepared_snapshot_invocation(prepared),
    class = "delta_sharing_validation_error"
  )
})

test_that("pagination cycles, ceilings, and page changes fail closed", {
  page_one <- planned_snapshot_bytes("snapshot-page-1.ndjson")
  page_two <- rawToChar(planned_snapshot_bytes("snapshot-page-2.ndjson"))
  repeated <- sub(
    '"minUrlExpirationTimestamp":4102444700000',
    paste0(
      '"nextPageToken":"next-page-token-private-secret",',
      '"minUrlExpirationTimestamp":4102444700000'
    ),
    page_two,
    fixed = TRUE
  )
  fetch_cycle <- function(request) {
    planned_pull_response(
      if (request$page_number == 1L) page_one else charToRaw(repeated)
    )
  }
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch_cycle
    ),
    "repeated",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      function(request) planned_pull_response(page_one),
      max_pages = 1L
    ),
    "page limit",
    class = "delta_sharing_protocol_error"
  )

  changed <- sub(
    '"id":"paged-table"',
    '"id":"changed-table"',
    page_two,
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      function(request) {
        planned_pull_response(
          if (request$page_number == 1L) page_one else charToRaw(changed)
        )
      }
    ),
    "changed across pages",
    class = "delta_sharing_protocol_error"
  )
})

test_that("headers, terminal position, and expiry are current and bounded", {
  bytes <- planned_snapshot_bytes("snapshot-page-1.ndjson")
  invalid_headers <- list(
    planned_snapshot_headers(content_type = "application/json"),
    planned_snapshot_headers(file_id_hash = "parquet"),
    planned_snapshot_headers(capabilities = "responseformat=parquet"),
    planned_snapshot_headers(capabilities = "readerfeatures=futurefeature")
  )
  for (headers in invalid_headers) {
    expect_error(
      delta.sharing:::.consume_snapshot_page(
        planned_pull_response(bytes, headers = headers)
      ),
      class = "delta_sharing_error"
    )
  }

  missing_terminal <- sub(
    '\n\\{"refreshToken".*',
    "",
    rawToChar(bytes)
  )
  expect_error(
    delta.sharing:::.consume_snapshot_page(
      planned_pull_response(charToRaw(missing_terminal))
    ),
    "terminal",
    class = "delta_sharing_protocol_error"
  )
  no_capability_terminal <- delta.sharing:::.consume_snapshot_page(
    planned_pull_response(
      charToRaw(missing_terminal),
      headers = planned_snapshot_headers(capabilities = NULL)
    )
  )
  expect_null(no_capability_terminal$terminal$next_page_token)
  false_capability_terminal <- delta.sharing:::.consume_snapshot_page(
    planned_pull_response(
      charToRaw(missing_terminal),
      headers = planned_snapshot_headers(
        capabilities = "includeendstreamaction=false"
      )
    )
  )
  expect_null(false_capability_terminal$terminal$refresh_token)
  after_terminal <- paste0(
    rawToChar(bytes),
    '{"futureAction":{"secret":"after-terminal-secret"}}\n'
  )
  condition <- expect_error(
    delta.sharing:::.consume_snapshot_page(
      planned_pull_response(charToRaw(after_terminal))
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_false(grepl(
    "after-terminal-secret",
    planned_condition_text(condition),
    fixed = TRUE
  ))

  expired <- gsub(
    "4102444800000",
    "1",
    rawToChar(bytes),
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      function(request) planned_pull_response(charToRaw(expired)),
      clock = function() {
        as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
      }
    ),
    "expired",
    class = "delta_sharing_http_error"
  )

  expect_error(
    delta.sharing:::.consume_snapshot_page(
      planned_pull_response(bytes, chunk_bytes = 1L),
      max_chunks = 2L
    ),
    "chunk limit",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.consume_snapshot_page(
      planned_pull_response(bytes, chunk_bytes = length(bytes)),
      max_line_bytes = 32L
    ),
    "size limit",
    class = "delta_sharing_protocol_error"
  )
})

test_that("stream errors and cancellation close responses without leaking", {
  secrets <- c(
    "stream-error-secret",
    "https://objects.example.test/file?sig=stream-secret"
  )
  failing_response <- function(failure) {
    recorder <- new.env(parent = emptyenv())
    recorder$closes <- 0L
    first <- TRUE
    list(
      recorder = recorder,
      response = delta.sharing:::.new_snapshot_pull_response(
        status = 200L,
        headers = planned_snapshot_headers(),
        pull = function() {
          if (first) {
            first <<- FALSE
            return(charToRaw(
              '{"protocol":{"deltaProtocol":{"minReaderVersion":1,"minWriterVersion":2}}}\n'
            ))
          }
          stop(failure)
        },
        close = function() {
          recorder$closes <- recorder$closes + 1L
        }
      )
    )
  }

  streamed <- failing_response(paste(secrets, collapse = " "))
  condition <- expect_error(
    delta.sharing:::.consume_snapshot_page(streamed$response),
    class = "delta_sharing_protocol_error"
  )
  expect_identical(streamed$recorder$closes, 1L)
  for (secret in secrets) {
    expect_false(grepl(
      secret,
      planned_condition_text(condition),
      fixed = TRUE
    ))
  }

  cancellation <- delta.sharing:::.new_delta_sharing_condition(
    "cancelled",
    type = "cancelled",
    operation = "query_table"
  )
  cancelled <- failing_response(cancellation)
  expect_error(
    delta.sharing:::.consume_snapshot_page(cancelled$response),
    class = "delta_sharing_cancelled"
  )
  expect_identical(cancelled$recorder$closes, 1L)

  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) stop(cancellation)
    ),
    class = "delta_sharing_cancelled"
  )
})

test_that("partial pagination and log publication failures leave no artifacts", {
  parent <- tempfile("snapshot-cleanup-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  page_one <- planned_snapshot_bytes("snapshot-page-1.ndjson")
  page_secret <- "second-page-transport-private-secret"
  condition <- expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      function(request) {
        if (request$page_number == 1L) {
          return(planned_pull_response(page_one))
        }
        stop(page_secret)
      },
      temp_parent = parent
    ),
    class = "delta_sharing_http_error"
  )
  expect_false(grepl(
    page_secret,
    planned_condition_text(condition),
    fixed = TRUE
  ))
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)

  condition <- expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      function(request) {
        planned_pull_response(
          planned_snapshot_bytes("snapshot-page-2.ndjson")
        )
      },
      temp_parent = parent,
      write_commit = function(path, lines) {
        stop("publication-private-secret")
      }
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_false(grepl(
    "publication-private-secret",
    planned_condition_text(condition),
    fixed = TRUE
  ))
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
})

test_that("URL expiry during publication removes the unpublished log", {
  parent <- tempfile("snapshot-expiry-publication-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  published <- FALSE
  prepared <- NULL
  expiration <- 4102444700

  condition <- expect_error(
    prepared <- delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) {
        planned_pull_response(
          planned_snapshot_bytes("snapshot-page-2.ndjson")
        )
      },
      temp_parent = parent,
      clock = function() {
        structure(
          if (published) expiration + 1 else expiration - 1,
          class = c("POSIXct", "POSIXt"),
          tzone = "UTC"
        )
      },
      write_commit = function(path, lines) {
        result <- delta.sharing:::.write_snapshot_commit(path, lines)
        published <<- TRUE
        result
      }
    ),
    class = "delta_sharing_http_error"
  )

  expect_true(published)
  expect_null(prepared)
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
})

test_that("private file expiry is enforced without a terminal minimum", {
  parent <- tempfile("snapshot-file-expiry-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  signed_url_secret <- "page-two-signed-url-secret"
  expired_bytes <- snapshot_page_with_file_expiration(1700000000000)

  condition <- expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) planned_pull_response(expired_bytes),
      temp_parent = parent,
      clock = function() {
        as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
      }
    ),
    class = "delta_sharing_http_error"
  )
  expect_false(grepl(
    signed_url_secret,
    planned_condition_text(condition),
    fixed = TRUE
  ))
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)

  live_expiration <- 4102444700000
  prepared <- delta.sharing:::.prepare_snapshot_read(
    sharing_read(test_table()),
    fetch = function(request) {
      planned_pull_response(
        snapshot_page_with_file_expiration(live_expiration)
      )
    },
    temp_parent = parent,
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    }
  )
  on.exit(
    delta.sharing:::.release_prepared_snapshot(prepared),
    add = TRUE
  )
  diagnostics <- delta.sharing:::.prepared_snapshot_diagnostics(prepared)
  expect_identical(
    as.double(diagnostics$min_url_expiration),
    live_expiration / 1000
  )
  expect_false(grepl(
    signed_url_secret,
    paste(capture.output(str(diagnostics)), collapse = "\n"),
    fixed = TRUE
  ))
})

test_that("internal snapshot print methods resolve and redact private state", {
  predicate_secret <- "print-predicate-private-secret"
  response_secret <- "print-response-private-secret"
  read <- sharing_read(
    test_table(),
    predicate = list(
      op = "equal",
      column = "region",
      value = predicate_secret
    )
  )
  request <- delta.sharing:::.plan_snapshot_request(read)
  http_request <- delta.sharing:::.new_snapshot_http_request(
    read@table@client,
    request
  )
  pull_response <- planned_pull_response(
    charToRaw(response_secret),
    headers = c(
      planned_snapshot_headers(),
      "X-Private-Test" = response_secret
    )
  )
  on.exit(pull_response$close(), add = TRUE)
  prepared <- delta.sharing:::.prepare_snapshot_read(
    read,
    fetch = function(request) {
      planned_pull_response(
        planned_snapshot_bytes("snapshot-page-2.ndjson")
      )
    },
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    }
  )
  on.exit(
    delta.sharing:::.release_prepared_snapshot(prepared),
    add = TRUE
  )
  snapshot_log <- delta.sharing:::.prepared_snapshot_state(prepared)$guard
  objects <- list(
    delta_sharing_prepared_snapshot = prepared,
    delta_sharing_snapshot_http_request = http_request,
    delta_sharing_snapshot_log = snapshot_log,
    delta_sharing_snapshot_pull_response = pull_response,
    delta_sharing_snapshot_request = request
  )

  for (class in names(objects)) {
    expect_true(is.function(utils::getS3method(
      "print",
      class,
      optional = TRUE
    )))
  }
  rendered <- paste(
    unlist(lapply(objects, function(object) {
      capture.output(print(object))
    })),
    collapse = "\n"
  )
  for (class in names(objects)) {
    expect_match(rendered, paste0("<", class, ">"), fixed = TRUE)
  }
  for (secret in c(
    predicate_secret,
    response_secret,
    "page-two-signed-url-secret"
  )) {
    expect_false(grepl(secret, rendered, fixed = TRUE))
  }
})

test_that("streamed end actions support the current wrapper and safe errors", {
  wrapped <- paste0(
    '{"endStreamAction":{"nextPageToken":"private-terminal-token",',
    '"minUrlExpirationTimestamp":4102444800000}}\n'
  )
  action <- delta.sharing:::.ndjson_decoder_push(
    delta.sharing:::.new_ndjson_decoder("query_table"),
    wrapped
  )[[1L]]
  expect_identical(action$type, "end_stream")
  expect_identical(
    delta.sharing:::.end_stream_state(action$value)$next_page_token,
    "private-terminal-token"
  )
  expect_false(grepl(
    "private-terminal-token",
    paste(capture.output(str(action$value)), collapse = "\n"),
    fixed = TRUE
  ))

  secret <- "server-error-message-private-secret"
  condition <- expect_error(
    delta.sharing:::.ndjson_decoder_push(
      delta.sharing:::.new_ndjson_decoder("query_table"),
      paste0(
        '{"endStreamAction":{"errorMessage":"',
        secret,
        '","httpStatusErrorCode":503}}\n'
      )
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_identical(condition$status, 503)
  expect_false(grepl(
    secret,
    planned_condition_text(condition),
    fixed = TRUE
  ))
})
