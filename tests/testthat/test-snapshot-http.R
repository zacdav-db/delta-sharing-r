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
