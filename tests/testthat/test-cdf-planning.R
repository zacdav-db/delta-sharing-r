cdf_fixture_bytes <- function(name) {
  path <- test_path("fixtures", "protocol", name)
  readBin(path, what = "raw", n = file.info(path)$size)
}

cdf_headers <- function(version = "1") {
  planned_snapshot_headers(
    version = version,
    capabilities = "responseformat=delta;includeendstreamaction=true"
  )
}

cdf_checkpoint_fixture <- function() {
  test_path(
    "fixtures",
    "delta",
    "cdf",
    "_delta_log",
    "00000000000000000000.checkpoint.parquet"
  )
}

test_that("CDF planner emits a redacted Delta-only GET request", {
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2,
    columns = c("id", "_change_type")
  )
  plan <- delta.sharing:::.plan_cdf_request(changes)
  rendered <- paste(capture.output(print(plan)), collapse = "\n")

  expect_identical(plan$method, "GET")
  expect_identical(tail(plan$path_segments, 1L), "changes")
  expect_identical(plan$query$startingVersion, "1")
  expect_identical(plan$query$endingVersion, "2")
  expect_identical(plan$query$includeHistoricalMetadata, "true")
  expect_identical(plan$query$includeHistoricalProtocol, "true")
  expect_match(
    plan$headers[["delta-sharing-capabilities"]],
    "responseformat=delta",
    fixed = TRUE
  )
  expect_false(grepl("startingVersion", rendered, fixed = TRUE))
  expect_false(grepl("cdf-page", rendered, fixed = TRUE))

  request <- delta.sharing:::.new_cdf_http_request(
    changes@table@client,
    plan
  )
  expect_identical(request$method, "GET")
  expect_identical(request$body_type, "none")
  expect_null(request$body)
  expect_false(any(grepl(
    "endingVersion",
    capture.output(print(request))
  )))
})

test_that("CDF pages retain required provider versions and timestamps", {
  page <- delta.sharing:::.consume_cdf_page(
    planned_pull_response(
      cdf_fixture_bytes("cdf-page-1.ndjson"),
      headers = cdf_headers(),
      chunk_bytes = 11L
    )
  )

  expect_identical(page$start_version, 1)
  expect_identical(page$protocol$version, 1)
  expect_identical(page$metadata$version, 1)
  expect_identical(
    delta.sharing:::.snapshot_file_version(page$files[[1L]]),
    1
  )
  expect_identical(
    delta.sharing:::.snapshot_file_timestamp(page$files[[1L]]),
    1734480105872
  )
  expect_identical(page$terminal$next_page_token, "cdf-page-two")
})

test_that("CDF preparation paginates and writes exact inclusive commits", {
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2,
    columns = c("_change_type", "_commit_version")
  )
  requests <- list()
  pages <- list(
    cdf_fixture_bytes("cdf-page-1.ndjson"),
    cdf_fixture_bytes("cdf-page-2.ndjson")
  )
  fetch <- function(request) {
    requests[[length(requests) + 1L]] <<- request
    planned_pull_response(
      pages[[length(requests)]],
      headers = cdf_headers(),
      chunk_bytes = 13L
    )
  }
  prepared <- delta.sharing:::.prepare_cdf_read(
    read = changes,
    fetch = fetch,
    checkpoint_asset = cdf_checkpoint_fixture(),
    clock = function() as.POSIXct("2026-07-29", tz = "UTC")
  )
  on.exit(
    delta.sharing:::.release_prepared_snapshot(prepared),
    add = TRUE
  )

  invocation <- delta.sharing:::.prepared_snapshot_invocation(prepared)
  diagnostics <- delta.sharing:::.prepared_snapshot_diagnostics(prepared)
  expect_identical(invocation$read_kind, "cdf")
  expect_identical(invocation$start_version, 1)
  expect_identical(invocation$end_version, 2)
  expect_identical(
    invocation$projection,
    c("_change_type", "_commit_version")
  )
  expect_identical(diagnostics$page_count, 2L)
  expect_identical(diagnostics$file_count, 2L)
  expect_identical(requests[[2L]]$query$pageToken, "cdf-page-two")

  guard <- delta.sharing:::.prepared_snapshot_state(prepared)$guard
  log_dir <- file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log"
  )
  expect_true(file.exists(file.path(
    log_dir,
    "00000000000000000000.checkpoint.parquet"
  )))
  expect_true(any(grepl(
    '"remove"',
    readLines(file.path(log_dir, "00000000000000000001.json")),
    fixed = TRUE
  )))
  expect_true(any(grepl(
    '"cdc"',
    readLines(file.path(log_dir, "00000000000000000002.json")),
    fixed = TRUE
  )))
})

test_that("executable CDF requires proven explicit version bounds", {
  called <- FALSE
  fetch <- function(request) {
    called <<- TRUE
    stop("must not fetch")
  }

  open_end <- expect_error(
    delta.sharing:::.prepare_cdf_read(
      sharing_changes(test_table(), starting_version = 1),
      fetch = fetch,
      checkpoint_asset = cdf_checkpoint_fixture()
    ),
    class = "delta_sharing_unsupported_error"
  )
  expect_identical(open_end$feature, "cdf_open_end")
  expect_false(called)

  timestamp_bounds <- expect_error(
    delta.sharing:::.prepare_cdf_read(
      sharing_changes(
        test_table(),
        starting_timestamp = as.POSIXct("2026-01-01", tz = "UTC"),
        ending_timestamp = as.POSIXct("2026-01-02", tz = "UTC")
      ),
      fetch = fetch,
      checkpoint_asset = cdf_checkpoint_fixture()
    ),
    class = "delta_sharing_unsupported_error"
  )
  expect_identical(timestamp_bounds$feature, "cdf_timestamp_bounds")
  expect_false(called)
})

test_that("empty bounded CDF ranges stay empty through their explicit end", {
  lines <- readLines(
    test_path("fixtures", "protocol", "cdf-page-1.ndjson"),
    warn = FALSE
  )
  empty <- charToRaw(paste0(
    paste(c(lines[1:2], '{"endStreamAction":{}}'), collapse = "\n"),
    "\n"
  ))
  prepared <- delta.sharing:::.prepare_cdf_read(
    read = sharing_changes(
      test_table(),
      starting_version = 1,
      ending_version = 2
    ),
    fetch = function(request) {
      planned_pull_response(empty, headers = cdf_headers())
    },
    checkpoint_asset = cdf_checkpoint_fixture()
  )
  on.exit(
    delta.sharing:::.release_prepared_snapshot(prepared),
    add = TRUE
  )
  invocation <- delta.sharing:::.prepared_snapshot_invocation(prepared)
  expect_identical(invocation$end_version, 2)
  log_dir <- file.path(
    delta.sharing:::.snapshot_log_path(
      delta.sharing:::.prepared_snapshot_state(prepared)$guard
    ),
    "_delta_log"
  )
  expect_identical(
    file.info(file.path(log_dir, "00000000000000000002.json"))$size,
    0
  )
})

test_that("CDF preparation rejects actions beyond the inclusive upper bound", {
  condition <- expect_error(
    delta.sharing:::.prepare_cdf_read(
      read = sharing_changes(
        test_table(),
        starting_version = 1,
        ending_version = 1
      ),
      fetch = function(request) {
        planned_pull_response(
          cdf_fixture_bytes("cdf-page-2.ndjson"),
          headers = cdf_headers()
        )
      },
      checkpoint_asset = cdf_checkpoint_fixture()
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_false(any(grepl(
    "storage.example",
    paste(conditionMessage(condition), capture.output(str(condition))),
    fixed = TRUE
  )))
})

cdf_stream_transport <- function(recorder) {
  recorder$requests <- list()
  list(
    open = function(request) {
      recorder$requests[[length(recorder$requests) + 1L]] <- request
      page <- if (is.null(request$query$pageToken)) {
        "cdf-page-1.ndjson"
      } else {
        expect_identical(request$query$pageToken, "cdf-page-two")
        "cdf-page-2.ndjson"
      }
      bytes <- cdf_fixture_bytes(page)
      response <- new.env(parent = emptyenv())
      response$status <- 200L
      response$headers <- cdf_headers()
      response$chunks <- split(
        bytes,
        ceiling(seq_along(bytes) / 17L)
      )
      response$offset <- 1L
      response
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    pull = function(response) {
      if (response$offset > length(response$chunks)) {
        return(NULL)
      }
      chunk <- response$chunks[[response$offset]]
      response$offset <- response$offset + 1L
      chunk
    },
    close = function(response) invisible(NULL),
    retry_after = function(response) NULL
  )
}

cdf_local_native_factory <- function(recorder) {
  force(recorder)
  function(table_location,
           start_version,
           end_version,
           columns,
           batch_size) {
    state <- delta.sharing:::.validate_snapshot_log_guard(table_location)
    recorder$prepared_root <- state$root
    fixture <- test_path("fixtures", "delta", "cdf")
    replacements <- c(
      remove = file.path(
        fixture,
        "a.parquet"
      ),
      cdc = file.path(
        fixture,
        "d.parquet"
      )
    )
    log_dir <- file.path(
      delta.sharing:::.snapshot_log_path(table_location),
      "_delta_log"
    )
    for (version in start_version:end_version) {
      commit <- file.path(
        log_dir,
        sprintf("%020.0f.json", version)
      )
      lines <- readLines(commit, warn = FALSE)
      rewritten <- vapply(lines, function(line) {
        action <- jsonlite::fromJSON(line, simplifyVector = FALSE)
        file_type <- intersect(c("remove", "cdc"), names(action))
        if (length(file_type) == 1L) {
          local_path <- normalizePath(
            replacements[[file_type]],
            winslash = "/",
            mustWork = TRUE
          )
          action[[file_type]]$path <- paste0(
            "file://",
            utils::URLencode(
              local_path,
              reserved = FALSE,
              repeated = TRUE
            )
          )
        }
        jsonlite::toJSON(action, auto_unbox = TRUE, null = "null")
      }, character(1))
      original_mtime <- file.info(commit)$mtime
      writeLines(rewritten, commit, useBytes = TRUE)
      Sys.setFileTime(commit, original_mtime)
    }
    delta.sharing:::.native_cdf_stream(
      table_location = table_location,
      start_version = start_version,
      end_version = end_version,
      columns = columns,
      batch_size = batch_size
    )
  }
}

test_that("public paginated CDF reaches Kernel and the eager materializer once", {
  recorder <- new.env(parent = emptyenv())
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("static bearer authentication must not perform auth HTTP")
    }),
    snapshot_transport = cdf_stream_transport(recorder),
    clock = function() as.POSIXct("2026-07-29", tz = "UTC"),
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    native_cdf_stream_factory = cdf_local_native_factory(recorder)
  )
  interface <- delta.sharing:::.new_execution_interface(callbacks)
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2,
    columns = c(
      "id",
      "_change_type",
      "_commit_version",
      "_commit_timestamp"
    )
  )
  data <- delta.sharing:::.with_execution_interface(interface, {
    read_data_frame(changes, batch_size = 2L)
  })

  expect_identical(length(recorder$requests), 2L)
  expect_identical(
    names(data),
    c("id", "_change_type", "_commit_version", "_commit_timestamp")
  )
  expect_equal(nrow(data), 8L)
  expect_setequal(unique(data$`_commit_version`), c(1, 2))
  expect_setequal(unique(data$`_change_type`), c("delete", "insert"))
  timestamps <- vapply(
    split(
      as.numeric(data$`_commit_timestamp`) * 1000,
      data$`_commit_version`
    ),
    function(value) unique(value),
    numeric(1)
  )
  expect_equal(unname(timestamps), c(1734480105872, 1734480106177))
  expect_false(file.exists(recorder$prepared_root))
})

test_that("public CDF attaches immutable stream-local diagnostics", {
  recorder <- new.env(parent = emptyenv())
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("static bearer authentication must not perform auth HTTP")
    }),
    snapshot_transport = cdf_stream_transport(recorder),
    clock = function() as.POSIXct("2026-07-29", tz = "UTC"),
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    native_cdf_stream_factory = cdf_local_native_factory(recorder)
  )
  interface <- delta.sharing:::.new_execution_interface(callbacks)
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2,
    columns = c("id", "_change_type")
  )
  stream <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(changes, batch_size = 2L)
  })
  on.exit(stream$release(), add = TRUE)

  diagnostics <- read_diagnostics(stream)
  expect_true(S7::S7_inherits(diagnostics, SharingReadDiagnostics))
  expect_identical(diagnostics@read_kind, "cdf")
  expect_identical(diagnostics@response_format, "delta")
  expect_null(diagnostics@table_version)
  expect_identical(diagnostics@starting_version, 1)
  expect_identical(diagnostics@ending_version, 2)
  expect_identical(diagnostics@page_count, 2)
  expect_identical(diagnostics@file_count, 2)
  expect_identical(diagnostics@columns, c("id", "_change_type"))
  expect_null(diagnostics@limit)
  expect_identical(diagnostics@batch_size, 2)
  expect_false(diagnostics@predicate_hint_sent)
  expect_null(diagnostics@server_limit_hint)

  stream$release()
  expect_identical(read_diagnostics(stream), diagnostics)
  expect_false(file.exists(recorder$prepared_root))
})

test_that("public CDF failure before native ownership removes the prepared root", {
  recorder <- new.env(parent = emptyenv())
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("static bearer authentication must not perform auth HTTP")
    }),
    snapshot_transport = cdf_stream_transport(recorder),
    clock = function() as.POSIXct("2026-07-29", tz = "UTC"),
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    native_cdf_stream_factory = function(table_location, ...) {
      recorder$prepared_root <-
        delta.sharing:::.validate_snapshot_log_guard(table_location)$root
      stop("contained native construction failure")
    }
  )
  interface <- delta.sharing:::.new_execution_interface(callbacks)
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2
  )

  expect_error(
    delta.sharing:::.with_execution_interface(interface, {
      read_arrow_stream(changes)
    }),
    class = "delta_sharing_native_error"
  )
  expect_false(file.exists(recorder$prepared_root))
})

cdf_http_transport <- function(specifications,
                               recorder = new.env(parent = emptyenv())) {
  recorder$opens <- 0L
  recorder$requests <- list()
  recorder$closed <- integer(length(specifications))
  list(
    open = function(request) {
      recorder$opens <- recorder$opens + 1L
      recorder$requests[[recorder$opens]] <- request
      specification <- specifications[[recorder$opens]]
      response <- new.env(parent = emptyenv())
      response$status <- specification$status
      response$headers <- specification$headers
      response$chunks <- specification$chunks
      if (is.null(response$chunks)) {
        response$chunks <- list()
      }
      response$offset <- 1L
      response$index <- recorder$opens
      response
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    pull = function(response) {
      if (response$offset > length(response$chunks)) {
        return(NULL)
      }
      chunk <- response$chunks[[response$offset]]
      response$offset <- response$offset + 1L
      chunk
    },
    close = function(response) {
      recorder$closed[[response$index]] <-
        recorder$closed[[response$index]] + 1L
      invisible(NULL)
    },
    retry_after = function(response) NULL
  )
}

test_that("CDF HTTP validates plans and replays OAuth 401 exactly once", {
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2
  )
  plan <- delta.sharing:::.plan_cdf_request(changes)
  expect_error(
    delta.sharing:::.new_cdf_http_request(changes@table@client, list()),
    class = "delta_sharing_validation_error"
  )
  post_plan <- plan
  post_plan$method <- "POST"
  expect_error(
    delta.sharing:::.new_cdf_http_request(
      changes@table@client,
      post_plan
    ),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.perform_authenticated_cdf_http(
      client = changes@table@client,
      plan = plan,
      stream_transport = cdf_http_transport(list()),
      auth_transport = delta.sharing:::.fake_http_transport(
        function(request) stop("not reached")
      ),
      clock = 1
    ),
    "CDF HTTP control hooks must be functions",
    fixed = TRUE
  )

  bearer_recorder <- new.env(parent = emptyenv())
  bearer_transport <- cdf_http_transport(
    list(list(status = 401L, headers = cdf_headers(), chunks = list())),
    bearer_recorder
  )
  expect_error(
    delta.sharing:::.perform_authenticated_cdf_http(
      client = changes@table@client,
      plan = plan,
      stream_transport = bearer_transport,
      auth_transport = delta.sharing:::.fake_http_transport(
        function(request) stop("bearer authentication must not fetch")
      )
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(bearer_recorder$closed, 1L)

  client <- sharing_client(test_path(
    "fixtures",
    "profiles",
    "oauth-client-v2.json"
  ))
  oauth_changes <- sharing_changes(
    sharing_table(client, "sales.default.orders"),
    starting_version = 1,
    ending_version = 2
  )
  token_calls <- 0L
  auth_transport <- delta.sharing:::.fake_http_transport(function(request) {
    token_calls <<- token_calls + 1L
    list(
      status = 200L,
      headers = list(),
      body = list(
        access_token = paste0("CDF-TOKEN-", token_calls),
        token_type = "Bearer",
        expires_in = 3600
      )
    )
  })
  oauth_recorder <- new.env(parent = emptyenv())
  oauth_transport <- cdf_http_transport(
    list(
      list(status = 401L, headers = cdf_headers(), chunks = list()),
      list(status = 200L, headers = cdf_headers(), chunks = list(raw()))
    ),
    oauth_recorder
  )
  response <- delta.sharing:::.perform_authenticated_cdf_http(
    client = client,
    plan = delta.sharing:::.plan_cdf_request(oauth_changes),
    stream_transport = oauth_transport,
    auth_transport = auth_transport,
    clock = function() as.POSIXct("2026-07-29", tz = "UTC")
  )
  expect_identical(token_calls, 2L)
  expect_identical(oauth_recorder$opens, 2L)
  expect_identical(oauth_recorder$closed, c(1L, 0L))
  expect_identical(
    oauth_recorder$requests[[1L]]$headers[["Authorization"]],
    "Bearer CDF-TOKEN-1"
  )
  expect_identical(
    oauth_recorder$requests[[2L]]$headers[["Authorization"]],
    "Bearer CDF-TOKEN-2"
  )
  expect_type(response$pull(), "raw")
  response$close()
  response$close()
  expect_identical(oauth_recorder$closed, c(1L, 1L))
  expect_error(response$pull(), "already been closed", fixed = TRUE)

  invalid_header_recorder <- new.env(parent = emptyenv())
  invalid_headers <- cdf_http_transport(
    list(list(status = 200L, headers = NULL, chunks = list())),
    invalid_header_recorder
  )
  expect_error(
    delta.sharing:::.perform_authenticated_cdf_http(
      client = changes@table@client,
      plan = plan,
      stream_transport = invalid_headers,
      auth_transport = delta.sharing:::.fake_http_transport(
        function(request) stop("bearer authentication must not fetch")
      )
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_identical(invalid_header_recorder$closed, 1L)
})

test_that("CDF execution rejects invalid specifications and retained ownership", {
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2
  )
  expect_error(
    delta.sharing:::.execute_snapshot_arrow_stream(
      specification = test_table(),
      batch_size = 65536L,
      concurrency = 1L,
      snapshot_transport = NULL,
      auth_transport = NULL,
      clock = Sys.time,
      sleeper = Sys.sleep,
      random = stats::runif,
      max_attempts = 1L,
      temp_parent = NULL,
      native_stream_factory = NULL
    ),
    class = "delta_sharing_validation_error"
  )

  recorder <- new.env(parent = emptyenv())
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("static bearer authentication must not perform auth HTTP")
    }),
    snapshot_transport = cdf_stream_transport(recorder),
    clock = function() as.POSIXct("2026-07-29", tz = "UTC"),
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    native_cdf_stream_factory = function(table_location, ...) {
      recorder$prepared_root <-
        delta.sharing:::.validate_snapshot_log_guard(table_location)$root
      nanoarrow::nanoarrow_allocate_array_stream()
    }
  )
  interface <- delta.sharing:::.new_execution_interface(callbacks)
  expect_error(
    delta.sharing:::.with_execution_interface(interface, {
      read_arrow_stream(changes)
    }),
    "did not accept CDF cleanup ownership",
    fixed = TRUE
  )
  expect_false(file.exists(recorder$prepared_root))
})

cdf_bytes_from_lines <- function(lines) {
  charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
}

cdf_page_lines <- function() {
  readLines(
    test_path("fixtures", "protocol", "cdf-page-1.ndjson"),
    warn = FALSE
  )
}

test_that("CDF planning validates formats, timestamps, and condition mapping", {
  expect_error(
    delta.sharing:::.validate_cdf_read(test_table()),
    class = "delta_sharing_validation_error"
  )
  parquet <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2,
    response_format = "parquet"
  )
  expect_error(
    delta.sharing:::.validate_cdf_read(parquet),
    class = "delta_sharing_unsupported_error"
  )
  expect_error(
    delta.sharing:::.cdf_query_capabilities("parquet"),
    class = "delta_sharing_unsupported_error"
  )
  expect_error(
    delta.sharing:::.plan_cdf_request(
      sharing_changes(
        test_table(),
        starting_version = 1,
        ending_version = 2
      ),
      page_token = list("invalid")
    ),
    class = "delta_sharing_protocol_error"
  )

  timestamp_plan <- delta.sharing:::.plan_cdf_request(sharing_changes(
    test_table(),
    starting_timestamp = as.POSIXct("2026-01-01", tz = "UTC"),
    ending_timestamp = as.POSIXct("2026-01-02", tz = "UTC")
  ))
  expect_named(
    timestamp_plan$query,
    c(
      "includeHistoricalMetadata",
      "includeHistoricalProtocol",
      "maxFiles",
      "startingTimestamp",
      "endingTimestamp"
    )
  )

  types <- c("validation", "unsupported", "http", "native", "protocol")
  mapped <- vapply(types, function(type) {
    condition <- delta.sharing:::.new_delta_sharing_condition(
      "safe",
      type = type,
      operation = "source_operation"
    )
    delta.sharing:::.cdf_condition_type(condition)
  }, character(1))
  expect_identical(unname(mapped), types)
  source <- delta.sharing:::.new_delta_sharing_condition(
    "safe",
    type = "http",
    operation = "source_operation",
    status = 429L,
    endpoint_host = "sharing.example.test",
    retry_count = 2L
  )
  rethrown <- expect_error(
    delta.sharing:::.cdf_rethrow(source),
    class = "delta_sharing_http_error"
  )
  expect_identical(rethrown$operation, "query_table_changes")
  expect_identical(rethrown$status, 429L)
})

test_that("CDF response headers and page structure fail closed", {
  lines <- cdf_page_lines()
  header_variants <- list(
    planned_snapshot_headers(content_type = "application/json"),
    planned_snapshot_headers(file_id_hash = "parquet"),
    planned_snapshot_headers(capabilities = "responseformat=parquet")
  )
  expected_classes <- c(
    "delta_sharing_protocol_error",
    "delta_sharing_protocol_error",
    "delta_sharing_unsupported_error"
  )
  for (index in seq_along(header_variants)) {
    expect_error(
      delta.sharing:::.consume_cdf_page(planned_pull_response(
        cdf_fixture_bytes("cdf-page-1.ndjson"),
        headers = header_variants[[index]]
      )),
      class = expected_classes[[index]]
    )
  }
  expect_error(
    delta.sharing:::.consume_cdf_page(planned_pull_response(
      cdf_fixture_bytes("cdf-page-1.ndjson"),
      headers = cdf_headers(),
      status = 503L
    )),
    class = "delta_sharing_http_error"
  )

  malformed_pages <- list(
    c(lines[2L], lines[2L], lines[4L]),
    c(lines[1L], lines[1L], lines[4L]),
    c(lines[1:2], lines[4L], lines[3L]),
    character()
  )
  for (page_lines in malformed_pages) {
    expect_error(
      delta.sharing:::.consume_cdf_page(planned_pull_response(
        cdf_bytes_from_lines(page_lines),
        headers = cdf_headers()
      )),
      class = "delta_sharing_protocol_error"
    )
  }

  no_terminal <- cdf_bytes_from_lines(lines[1:2])
  expect_error(
    delta.sharing:::.consume_cdf_page(planned_pull_response(
      no_terminal,
      headers = cdf_headers()
    )),
    class = "delta_sharing_protocol_error"
  )
  page <- delta.sharing:::.consume_cdf_page(planned_pull_response(
    no_terminal,
    headers = planned_snapshot_headers(
      version = "1",
      capabilities = "responseformat=delta"
    )
  ))
  expect_null(page$terminal$next_page_token)

  historical <- delta.sharing:::.consume_cdf_page(planned_pull_response(
    cdf_bytes_from_lines(c(lines[1:2], lines[1:2], lines[4L])),
    headers = cdf_headers()
  ))
  expect_length(historical$historical_protocols, 1L)
  expect_length(historical$historical_metadata, 1L)

  expect_error(
    delta.sharing:::.consume_cdf_page(
      planned_pull_response(
        cdf_fixture_bytes("cdf-page-1.ndjson"),
        headers = cdf_headers(),
        chunk_bytes = 1L
      ),
      max_chunks = 1L
    ),
    class = "delta_sharing_protocol_error"
  )
  broken_pull <- delta.sharing:::.new_snapshot_pull_response(
    status = 200L,
    headers = cdf_headers(),
    pull = function() stop("private pull failure"),
    close = function() invisible(NULL)
  )
  expect_error(
    delta.sharing:::.consume_cdf_page(broken_pull),
    class = "delta_sharing_protocol_error"
  )
})

test_that("CDF fetch and pagination controls preserve typed failures", {
  request <- delta.sharing:::.plan_cdf_request(sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 2
  ))
  expect_error(
    delta.sharing:::.safe_cdf_fetch("not a function", request),
    "`fetch` must be a function",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.safe_cdf_fetch(
      function(request) stop("private fetch failure"),
      request
    ),
    class = "delta_sharing_protocol_error"
  )
  source <- delta.sharing:::.new_delta_sharing_condition(
    "safe",
    type = "validation",
    operation = "other_operation"
  )
  rethrown <- expect_error(
    delta.sharing:::.safe_cdf_fetch(
      function(request) stop(source),
      request
    ),
    class = "delta_sharing_validation_error"
  )
  expect_identical(rethrown$operation, "query_table_changes")

  expect_error(
    delta.sharing:::.prepare_cdf_read(
      sharing_changes(
        test_table(),
        starting_version = 1,
        ending_version = 2
      ),
      fetch = function(request) {
        planned_pull_response(
          cdf_fixture_bytes("cdf-page-1.ndjson"),
          headers = cdf_headers()
        )
      },
      max_pages = 1L,
      checkpoint_asset = cdf_checkpoint_fixture()
    ),
    class = "delta_sharing_protocol_error"
  )

  wrong_start_headers <- cdf_headers(version = "2")
  expect_error(
    delta.sharing:::.prepare_cdf_read(
      sharing_changes(
        test_table(),
        starting_version = 1,
        ending_version = 2
      ),
      fetch = function(request) {
        planned_pull_response(
          cdf_bytes_from_lines(c(cdf_page_lines()[1:2], '{"endStreamAction":{}}')),
          headers = wrong_start_headers
        )
      },
      checkpoint_asset = cdf_checkpoint_fixture()
    ),
    class = "delta_sharing_protocol_error"
  )
})
