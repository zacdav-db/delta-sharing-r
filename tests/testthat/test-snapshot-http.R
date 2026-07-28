fake_snapshot_stream_transport <- function(responses,
                                           recorder =
                                             new.env(parent = emptyenv())) {
  recorder$opens <- 0L
  recorder$requests <- list()
  recorder$closed <- integer(length(responses))

  list(
    open = function(request) {
      recorder$opens <- recorder$opens + 1L
      recorder$requests[[recorder$opens]] <- request
      specification <- responses[[recorder$opens]]
      if (inherits(specification, "error")) {
        stop(specification)
      }
      response <- new.env(parent = emptyenv())
      response$status <- specification$status
      response$headers <- specification$headers
      response$chunks <- specification$chunks
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

snapshot_stream_specification <- function(
  status = 200L,
  bytes = planned_snapshot_bytes("snapshot-page-2.ndjson"),
  headers = planned_snapshot_headers(),
  chunk_bytes = 23L
) {
  starts <- seq.int(1L, length(bytes), by = chunk_bytes)
  chunks <- lapply(starts, function(start) {
    bytes[seq.int(start, min(length(bytes), start + chunk_bytes - 1L))]
  })
  list(status = status, headers = headers, chunks = chunks)
}

unused_snapshot_auth_transport <- function() {
  delta.sharing:::.fake_http_transport(function(request) {
    stop("The bearer flow must not call the token endpoint.")
  })
}

test_that("snapshot HTTP boundary objects validate and redact their contents", {
  response <- delta.sharing:::.new_snapshot_pull_response(
    status = 200L,
    headers = planned_snapshot_headers(),
    pull = function() charToRaw("private-response-body"),
    close = function() invisible(NULL)
  )
  expect_identical(
    delta.sharing:::.normalize_snapshot_pull_response(response),
    response
  )
  expect_false(grepl(
    "private-response-body",
    paste(capture.output(print(response)), collapse = "\n"),
    fixed = TRUE
  ))

  invalid_responses <- list(
    list(
      headers = planned_snapshot_headers(),
      pull = response$pull,
      close = response$close
    ),
    structure(
      list(
        headers = planned_snapshot_headers(),
        pull = "not-a-function",
        close = response$close
      ),
      class = "delta_sharing_snapshot_pull_response"
    ),
    structure(
      list(
        headers = planned_snapshot_headers(),
        pull = response$pull,
        close = "not-a-function"
      ),
      class = "delta_sharing_snapshot_pull_response"
    ),
    structure(
      list(headers = NULL, pull = response$pull, close = response$close),
      class = "delta_sharing_snapshot_pull_response"
    )
  )
  for (invalid_response in invalid_responses) {
    expect_error(
      delta.sharing:::.normalize_snapshot_pull_response(invalid_response),
      "invalid pull response",
      class = "delta_sharing_protocol_error"
    )
  }

  client <- test_table()@client
  plan <- delta.sharing:::.plan_snapshot_request(sharing_read(test_table()))
  request <- delta.sharing:::.new_snapshot_http_request(client, plan)
  expect_s3_class(request, "delta_sharing_snapshot_http_request")
  expect_false(grepl(
    "Authorization|private",
    paste(capture.output(print(request)), collapse = "\n"),
    fixed = TRUE
  ))

  expect_error(
    delta.sharing:::.new_snapshot_http_request(client, list()),
    class = "delta_sharing_validation_error"
  )
  invalid_method <- plan
  invalid_method$method <- "GET"
  expect_error(
    delta.sharing:::.new_snapshot_http_request(client, invalid_method),
    "snapshot request plan is invalid",
    class = "delta_sharing_validation_error"
  )
  missing_body <- plan
  missing_body$body <- NULL
  expect_error(
    delta.sharing:::.new_snapshot_http_request(client, missing_body),
    "snapshot request plan is invalid",
    class = "delta_sharing_validation_error"
  )
})

test_that("snapshot stream transports require complete callable hooks", {
  response <- list(headers = c("Retry-After" = "7"))
  transport <- fake_snapshot_stream_transport(list(
    snapshot_stream_specification()
  ))
  transport$retry_after <- NULL
  normalized <- delta.sharing:::.normalize_snapshot_stream_transport(transport)
  expect_identical(normalized$retry_after(response), "7")

  invalid_transports <- list(
    NULL,
    unname(transport),
    within(transport, open <- NULL),
    within(transport, retry_after <- 1)
  )
  for (invalid_transport in invalid_transports) {
    expect_error(
      delta.sharing:::.normalize_snapshot_stream_transport(invalid_transport),
      "must provide.*functions",
      fixed = FALSE
    )
  }
})

test_that("the httr2 raw response adapter pulls bounded chunks and closes", {
  httr_response <- httr2::new_response(
    method = "POST",
    url = "https://sharing.example.test/query",
    status_code = 206L,
    headers = planned_snapshot_headers(),
    body = charToRaw("abcdefg")
  )
  response <- delta.sharing:::.new_httr2_snapshot_response(
    httr_response,
    chunk_bytes = 3L
  )

  expect_identical(response$status, 206L)
  expect_identical(
    delta.sharing:::.httr2_snapshot_pull(response),
    charToRaw("abc")
  )
  expect_identical(
    delta.sharing:::.httr2_snapshot_pull(response),
    charToRaw("def")
  )
  expect_identical(
    delta.sharing:::.httr2_snapshot_pull(response),
    charToRaw("g")
  )
  expect_null(delta.sharing:::.httr2_snapshot_pull(response))
  expect_null(delta.sharing:::.httr2_snapshot_close(response))
  expect_error(
    delta.sharing:::.httr2_snapshot_pull(response),
    "not available"
  )
  expect_error(
    delta.sharing:::.httr2_snapshot_close(list()),
    "stream is invalid"
  )
})

test_that("the httr2 response adapter cleans up malformed streams", {
  closes <- 0L
  body <- new.env(parent = emptyenv())
  body$close <- function() {
    closes <<- closes + 1L
    stop("private-close-error")
  }
  httr_response <- httr2::new_response(
    method = "POST",
    url = "https://sharing.example.test/query",
    status_code = 200L,
    headers = planned_snapshot_headers(),
    body = raw()
  )
  httr_response$body <- body
  condition <- expect_error(
    delta.sharing:::.new_httr2_snapshot_response(
      httr_response,
      chunk_bytes = 3L
    ),
    "invalid body stream"
  )
  expect_identical(closes, 1L)
  expect_false(grepl(
    "private-close-error",
    conditionMessage(condition),
    fixed = TRUE
  ))

  make_body <- function(read, complete = function() FALSE) {
    stream <- new.env(parent = emptyenv())
    stream$read <- read
    stream$is_complete <- complete
    stream$close <- function() invisible(NULL)
    stream
  }
  make_response <- function(stream) {
    httr_response <- httr2::new_response(
      method = "POST",
      url = "https://sharing.example.test/query",
      status_code = 200L,
      headers = planned_snapshot_headers(),
      body = raw()
    )
    httr_response$body <- stream
    delta.sharing:::.new_httr2_snapshot_response(
      httr_response,
      chunk_bytes = 3L
    )
  }

  expect_error(
    delta.sharing:::.httr2_snapshot_pull(make_response(
      make_body(function(size) "not-raw")
    )),
    "invalid body chunk"
  )
  expect_error(
    delta.sharing:::.httr2_snapshot_pull(make_response(
      make_body(function(size) raw())
    )),
    "ended unexpectedly"
  )

  complete <- FALSE
  stream <- make_body(
    read = function(size) {
      complete <<- TRUE
      raw()
    },
    complete = function() complete
  )
  expect_null(delta.sharing:::.httr2_snapshot_pull(make_response(stream)))
})

test_that("snapshot transport controls validate status and preserve cleanup", {
  for (timeout in list(0, NA_real_, Inf, "120", c(1, 2))) {
    expect_error(
      delta.sharing:::.httr2_snapshot_transport(timeout_seconds = timeout),
      "positive number"
    )
  }
  expect_error(
    delta.sharing:::.httr2_snapshot_transport(chunk_bytes = 0),
    class = "delta_sharing_validation_error"
  )

  transport <- delta.sharing:::.httr2_snapshot_transport(
    timeout_seconds = 3,
    chunk_bytes = 11L
  )
  response <- new.env(parent = emptyenv())
  response$status <- 200L
  response$headers <- c("Retry-After" = "9")
  expect_identical(transport$status(response), 200L)
  expect_identical(transport$headers(response), response$headers)
  expect_identical(transport$retry_after(response), "9")

  closes <- 0L
  invalid_status_transport <- list(
    status = function(response) stop("private-status-error"),
    close = function(response) {
      closes <<- closes + 1L
      stop("private-close-error")
    }
  )
  condition <- expect_error(
    delta.sharing:::.snapshot_transport_status(
      invalid_status_transport,
      response
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_identical(closes, 1L)
  expect_false(grepl(
    "private",
    planned_condition_text(condition),
    fixed = TRUE
  ))

  for (status in list(99, 600, 200.5, NA_real_, Inf, c(200, 201))) {
    closes <- 0L
    invalid_status_transport$status <- function(response) status
    invalid_status_transport$close <- function(response) {
      closes <<- closes + 1L
    }
    expect_error(
      delta.sharing:::.snapshot_transport_status(
        invalid_status_transport,
        response
      ),
      class = "delta_sharing_protocol_error"
    )
    expect_identical(closes, 1L)
  }

  expect_null(delta.sharing:::.snapshot_retry_after(
    list(retry_after = function(response) stop("private-retry-error")),
    response
  ))
})

test_that("the authenticated snapshot seam streams without control buffering", {
  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(snapshot_stream_specification(chunk_bytes = 7L)),
    recorder
  )
  read <- sharing_read(test_table(), version = 42)
  plan <- delta.sharing:::.plan_snapshot_request(read)
  response <- delta.sharing:::.perform_authenticated_snapshot_http(
    client = read@table@client,
    plan = plan,
    stream_transport = transport,
    auth_transport = unused_snapshot_auth_transport()
  )
  page <- delta.sharing:::.consume_snapshot_page(response)

  expect_identical(recorder$opens, 1L)
  expect_identical(recorder$closed, 1L)
  expect_gt(length(
    snapshot_stream_specification(chunk_bytes = 7L)$chunks
  ), 10L)
  expect_identical(page$table_version, 42)
  request <- recorder$requests[[1L]]
  expect_identical(request$body_type, "json")
  expect_false("max_response_bytes" %in% names(request))
  expect_match(request$url, "/shares/sales/schemas/default/tables/orders/query$")
  expect_identical(
    request$headers[["Authorization"]],
    "Bearer test-only-bearer-token"
  )

  safe_print <- paste(
    capture.output(print(request)),
    capture.output(print(response)),
    collapse = "\n"
  )
  expect_false(grepl(
    "test-only-bearer-token",
    safe_print,
    fixed = TRUE
  ))
})

test_that("Query Table retries only before successful body streaming", {
  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(
      snapshot_stream_specification(status = 503L),
      snapshot_stream_specification(status = 200L)
    ),
    recorder
  )
  read <- sharing_read(test_table())
  response <- delta.sharing:::.perform_authenticated_snapshot_http(
    client = read@table@client,
    plan = delta.sharing:::.plan_snapshot_request(read),
    stream_transport = transport,
    auth_transport = unused_snapshot_auth_transport(),
    max_attempts = 2L,
    sleeper = function(seconds) NULL,
    random = function(...) 0
  )
  delta.sharing:::.consume_snapshot_page(response)
  expect_identical(recorder$opens, 2L)
  expect_identical(recorder$closed[[1L]], 1L)
  expect_identical(recorder$closed[[2L]], 1L)

  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(
      simpleError("connection-open-private-secret"),
      snapshot_stream_specification(status = 200L)
    ),
    recorder
  )
  response <- delta.sharing:::.perform_authenticated_snapshot_http(
    client = read@table@client,
    plan = delta.sharing:::.plan_snapshot_request(read),
    stream_transport = transport,
    auth_transport = unused_snapshot_auth_transport(),
    max_attempts = 2L,
    sleeper = function(seconds) NULL,
    random = function(...) 0
  )
  delta.sharing:::.consume_snapshot_page(response)
  expect_identical(recorder$opens, 2L)
  expect_identical(recorder$closed[[2L]], 1L)

  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(
      snapshot_stream_specification(status = 503L),
      snapshot_stream_specification(status = 503L)
    ),
    recorder
  )
  condition <- expect_error(
    delta.sharing:::.perform_authenticated_snapshot_http(
      client = read@table@client,
      plan = delta.sharing:::.plan_snapshot_request(read),
      stream_transport = transport,
      auth_transport = unused_snapshot_auth_transport(),
      max_attempts = 2L,
      sleeper = function(seconds) NULL,
      random = function(...) 0
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(recorder$opens, 2L)
  expect_identical(recorder$closed, c(1L, 1L))
  expect_identical(condition$status, 503L)
  expect_identical(condition$retry_count, 1L)

  recorder <- new.env(parent = emptyenv())
  cancellation <- delta.sharing:::.new_delta_sharing_condition(
    "cancelled",
    type = "cancelled",
    operation = "query_table"
  )
  transport <- fake_snapshot_stream_transport(
    list(
      cancellation,
      snapshot_stream_specification(status = 200L)
    ),
    recorder
  )
  expect_error(
    delta.sharing:::.perform_authenticated_snapshot_http(
      client = read@table@client,
      plan = delta.sharing:::.plan_snapshot_request(read),
      stream_transport = transport,
      auth_transport = unused_snapshot_auth_transport(),
      max_attempts = 2L,
      sleeper = function(seconds) NULL,
      random = function(...) 0
    ),
    class = "delta_sharing_cancelled"
  )
  expect_identical(recorder$opens, 1L)
})

test_that("snapshot retries honor server delay and redact exhausted opens", {
  read <- sharing_read(test_table())
  plan <- delta.sharing:::.plan_snapshot_request(read)

  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(
      snapshot_stream_specification(status = 429L),
      snapshot_stream_specification(status = 200L)
    ),
    recorder
  )
  transport$retry_after <- function(response) "7"
  delays <- numeric()
  response <- delta.sharing:::.perform_authenticated_snapshot_http(
    client = read@table@client,
    plan = plan,
    stream_transport = transport,
    auth_transport = unused_snapshot_auth_transport(),
    max_attempts = 2L,
    sleeper = function(seconds) delays <<- c(delays, seconds),
    random = function(...) stop("jitter must not be used")
  )
  expect_identical(delays, 7)
  expect_null(response$close())
  expect_identical(recorder$closed, c(1L, 1L))

  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(
      simpleError("private-open-secret-1"),
      simpleError("private-open-secret-2")
    ),
    recorder
  )
  condition <- expect_error(
    delta.sharing:::.perform_authenticated_snapshot_http(
      client = read@table@client,
      plan = plan,
      stream_transport = transport,
      auth_transport = unused_snapshot_auth_transport(),
      max_attempts = 2L,
      sleeper = function(seconds) NULL,
      random = function(...) 0
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(recorder$opens, 2L)
  expect_identical(condition$retry_count, 1L)
  expect_false(grepl(
    "private-open-secret",
    planned_condition_text(condition),
    fixed = TRUE
  ))
})

test_that("non-retryable snapshot status closes without replay", {
  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(snapshot_stream_specification(status = 404L)),
    recorder
  )
  read <- sharing_read(test_table())
  condition <- expect_error(
    delta.sharing:::.perform_authenticated_snapshot_http(
      client = read@table@client,
      plan = delta.sharing:::.plan_snapshot_request(read),
      stream_transport = transport,
      auth_transport = unused_snapshot_auth_transport(),
      max_attempts = 3L
    ),
    class = "delta_sharing_http_error"
  )

  expect_identical(recorder$opens, 1L)
  expect_identical(recorder$closed, 1L)
  expect_identical(condition$status, 404L)
  expect_identical(condition$retry_count, 0L)
})

test_that("a definitive OAuth 401 is closed and replayed once", {
  expect_true(delta.sharing:::.snapshot_is_oauth_auth_type(
    "oauth_client_credentials"
  ))
  expect_true(delta.sharing:::.snapshot_is_oauth_auth_type(
    "oauth_jwt_bearer_private_key_jwt"
  ))
  expect_false(delta.sharing:::.snapshot_is_oauth_auth_type("bearer_token"))

  client <- sharing_client(test_path(
    "fixtures",
    "profiles",
    "oauth-client-v2.json"
  ))
  token_calls <- 0L
  auth_transport <- delta.sharing:::.fake_http_transport(function(request) {
    token_calls <<- token_calls + 1L
    list(
      status = 200L,
      headers = list(),
      body = list(
        access_token = paste0("TOKEN-", token_calls),
        token_type = "Bearer",
        expires_in = 3600
      )
    )
  })
  recorder <- new.env(parent = emptyenv())
  stream_transport <- fake_snapshot_stream_transport(
    list(
      snapshot_stream_specification(status = 401L),
      snapshot_stream_specification(status = 200L)
    ),
    recorder
  )
  read <- sharing_read(sharing_table(
    client,
    "sales.default.orders"
  ))
  response <- delta.sharing:::.perform_authenticated_snapshot_http(
    client = client,
    plan = delta.sharing:::.plan_snapshot_request(read),
    stream_transport = stream_transport,
    auth_transport = auth_transport,
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    }
  )
  page <- delta.sharing:::.consume_snapshot_page(response)

  expect_identical(page$table_version, 42)
  expect_identical(token_calls, 2L)
  expect_identical(recorder$opens, 2L)
  expect_identical(recorder$closed, c(1L, 1L))
  expect_identical(
    recorder$requests[[1L]]$headers[["Authorization"]],
    "Bearer TOKEN-1"
  )
  expect_identical(
    recorder$requests[[2L]]$headers[["Authorization"]],
    "Bearer TOKEN-2"
  )
})

test_that("two OAuth 401 responses stop after one private replay", {
  client <- sharing_client(test_path(
    "fixtures",
    "profiles",
    "oauth-client-v2.json"
  ))
  token_calls <- 0L
  auth_transport <- delta.sharing:::.fake_http_transport(function(request) {
    token_calls <<- token_calls + 1L
    list(
      status = 200L,
      headers = list(),
      body = list(
        access_token = paste0("oauth-token-secret-", token_calls),
        token_type = "Bearer",
        expires_in = 3600
      )
    )
  })
  recorder <- new.env(parent = emptyenv())
  response_secret <- "response-header-private-secret"
  stream_transport <- fake_snapshot_stream_transport(
    list(
      snapshot_stream_specification(
        status = 401L,
        bytes = charToRaw("response-body-private-secret-1"),
        headers = c(
          planned_snapshot_headers(),
          "X-Private-Test" = response_secret
        )
      ),
      snapshot_stream_specification(
        status = 401L,
        bytes = charToRaw("response-body-private-secret-2"),
        headers = c(
          planned_snapshot_headers(),
          "X-Private-Test" = response_secret
        )
      )
    ),
    recorder
  )
  predicate_secret <- "request-body-private-secret"
  read <- sharing_read(
    sharing_table(client, "sales.default.orders"),
    predicate = list(
      op = "equal",
      column = "region",
      value = predicate_secret
    )
  )

  condition <- expect_error(
    delta.sharing:::.perform_authenticated_snapshot_http(
      client = client,
      plan = delta.sharing:::.plan_snapshot_request(read),
      stream_transport = stream_transport,
      auth_transport = auth_transport,
      clock = function() {
        as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
      }
    ),
    class = "delta_sharing_http_error"
  )

  expect_identical(token_calls, 2L)
  expect_identical(recorder$opens, 2L)
  expect_identical(recorder$closed, c(1L, 1L))
  expect_identical(
    vapply(
      recorder$requests,
      function(request) request$headers[["Authorization"]],
      character(1)
    ),
    c(
      "Bearer oauth-token-secret-1",
      "Bearer oauth-token-secret-2"
    )
  )

  condition_text <- planned_condition_text(condition)
  for (secret in c(
    "oauth-token-secret-1",
    "oauth-token-secret-2",
    response_secret,
    "response-body-private-secret-1",
    "response-body-private-secret-2",
    predicate_secret
  )) {
    expect_false(grepl(secret, condition_text, fixed = TRUE))
  }
})

test_that("bearer 401 and invalid headers close without exposing payloads", {
  read <- sharing_read(
    test_table(),
    predicate = list(
      op = "equal",
      column = "region",
      value = "private-predicate-secret"
    )
  )
  plan <- delta.sharing:::.plan_snapshot_request(read)

  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(snapshot_stream_specification(
      status = 401L,
      bytes = charToRaw("private-response-secret")
    )),
    recorder
  )
  condition <- expect_error(
    delta.sharing:::.perform_authenticated_snapshot_http(
      client = read@table@client,
      plan = plan,
      stream_transport = transport,
      auth_transport = unused_snapshot_auth_transport()
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(recorder$opens, 1L)
  expect_identical(recorder$closed, 1L)
  expect_identical(condition$status, 401L)
  for (secret in c(
    "test-only-bearer-token",
    "private-predicate-secret",
    "private-response-secret"
  )) {
    expect_false(grepl(
      secret,
      planned_condition_text(condition),
      fixed = TRUE
    ))
  }

  for (header_hook in list(
    function(response) NULL,
    function(response) stop("private-header-secret")
  )) {
    recorder <- new.env(parent = emptyenv())
    transport <- fake_snapshot_stream_transport(
      list(snapshot_stream_specification()),
      recorder
    )
    transport$headers <- header_hook
    condition <- expect_error(
      delta.sharing:::.perform_authenticated_snapshot_http(
        client = read@table@client,
        plan = plan,
        stream_transport = transport,
        auth_transport = unused_snapshot_auth_transport()
      ),
      "invalid snapshot headers",
      class = "delta_sharing_protocol_error"
    )
    expect_identical(recorder$closed, 1L)
    expect_false(grepl(
      "private-header-secret",
      planned_condition_text(condition),
      fixed = TRUE
    ))
  }
})

test_that("authenticated pull responses own one explicit close lifecycle", {
  recorder <- new.env(parent = emptyenv())
  transport <- fake_snapshot_stream_transport(
    list(snapshot_stream_specification(chunk_bytes = 5L)),
    recorder
  )
  read <- sharing_read(test_table())
  response <- delta.sharing:::.perform_authenticated_snapshot_http(
    client = read@table@client,
    plan = delta.sharing:::.plan_snapshot_request(read),
    stream_transport = transport,
    auth_transport = unused_snapshot_auth_transport()
  )

  expect_type(response$pull(), "raw")
  expect_null(response$close())
  expect_null(response$close())
  expect_identical(recorder$closed, 1L)
  expect_error(response$pull(), "already been closed")
})

test_that("authenticated snapshot controls reject invalid hooks early", {
  read <- sharing_read(test_table())
  plan <- delta.sharing:::.plan_snapshot_request(read)
  transport <- fake_snapshot_stream_transport(list(
    snapshot_stream_specification()
  ))

  for (hook in c("clock", "sleeper", "random")) {
    controls <- list(
      client = read@table@client,
      plan = plan,
      stream_transport = transport,
      auth_transport = unused_snapshot_auth_transport(),
      clock = function() Sys.time(),
      sleeper = function(seconds) NULL,
      random = function(...) 0
    )
    controls[[hook]] <- "not-a-function"
    expect_error(
      do.call(
        delta.sharing:::.perform_authenticated_snapshot_http,
        controls
      ),
      "control hooks must be functions"
    )
  }
  expect_error(
    delta.sharing:::.perform_authenticated_snapshot_http(
      client = read@table@client,
      plan = plan,
      stream_transport = transport,
      auth_transport = unused_snapshot_auth_transport(),
      max_attempts = 0
    ),
    class = "delta_sharing_validation_error"
  )
})

test_that("the httr2 connection body is pulled in bounded chunks", {
  body <- new.env(parent = emptyenv())
  body$pieces <- list(
    charToRaw("abc"),
    charToRaw("def"),
    charToRaw("ghi")
  )
  body$index <- 1L
  body$reads <- 0L
  body$closes <- 0L
  body$is_complete <- function() body$index > length(body$pieces)
  body$read <- function(size) {
    body$reads <- body$reads + 1L
    expect_identical(size, 3L)
    piece <- body$pieces[[body$index]]
    body$index <- body$index + 1L
    piece
  }
  body$close <- function() {
    body$closes <- body$closes + 1L
  }

  response <- new.env(parent = emptyenv())
  response$status <- 200L
  response$headers <- planned_snapshot_headers()
  response$body <- body
  response$chunk_bytes <- 3L
  response$offset <- 1L
  response$closed <- FALSE
  class(response) <- "delta_sharing_httr2_snapshot_response"

  expect_identical(
    delta.sharing:::.httr2_snapshot_pull(response),
    charToRaw("abc")
  )
  expect_identical(
    delta.sharing:::.httr2_snapshot_pull(response),
    charToRaw("def")
  )
  expect_identical(
    delta.sharing:::.httr2_snapshot_pull(response),
    charToRaw("ghi")
  )
  expect_null(delta.sharing:::.httr2_snapshot_pull(response))
  expect_identical(body$reads, 3L)
  expect_true(delta.sharing:::.httr2_snapshot_close(response) |> is.null())
  expect_true(delta.sharing:::.httr2_snapshot_close(response) |> is.null())
  expect_identical(body$closes, 1L)
})
