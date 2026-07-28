http_test_oauth_profile <- function() {
  jsonlite::fromJSON(
    test_path("fixtures", "profiles", "oauth-client-v2.json"),
    simplifyVector = FALSE
  )
}

test_that("client requests encode path and query separately", {
  client <- test_client()
  request <- delta.sharing:::.new_client_http_request(
    client = client,
    method = "GET",
    path = c("shares", "sales eu", "schemas"),
    query = list(
      pageToken = "page/token?secret",
      tag = c("one", "two")
    ),
    headers = list(
      `delta-sharing-capabilities` = "responseformat=delta"
    ),
    response_kind = "discovery",
    max_response_bytes = 1024
  )

  expect_identical(
    request$url,
    paste0(
      "https://sharing.example.test/api/",
      "shares/sales%20eu/schemas"
    )
  )
  expect_identical(request$query$pageToken, "page/token?secret")
  expect_identical(request$body_type, "none")
  expect_identical(request$max_response_bytes, 1024)

  prepared <- delta.sharing:::.httr2_prepare_request(request, 30)
  expect_identical(
    httr2::req_get_url(prepared),
    paste0(
      "https://sharing.example.test/api/",
      "shares/sales%20eu/schemas?",
      "pageToken=page%2Ftoken%3Fsecret&tag=one&tag=two"
    )
  )
  expect_identical(
    httr2::req_get_headers(
      prepared,
      redacted = "reveal"
    )[["delta-sharing-capabilities"]],
    "responseformat=delta"
  )
})

test_that("form and JSON request bodies remain transport-neutral", {
  client <- test_client()
  form <- delta.sharing:::.new_client_http_request(
    client = client,
    method = "POST",
    path = "control",
    form = list(grant_type = "client_credentials"),
    response_kind = "metadata"
  )
  json <- delta.sharing:::.new_client_http_request(
    client = client,
    method = "POST",
    path = "control",
    json = list(predicateHints = list(op = "equal", value = "secret")),
    response_kind = "metadata"
  )

  expect_identical(form$body_type, "form")
  expect_identical(form$body$grant_type, "client_credentials")
  expect_identical(json$body_type, "json")
  expect_identical(json$body$predicateHints$value, "secret")
  expect_identical(
    httr2::req_get_body_type(
      delta.sharing:::.httr2_prepare_request(form, 30)
    ),
    "form"
  )
  expect_identical(
    httr2::req_get_body_type(
      delta.sharing:::.httr2_prepare_request(json, 30)
    ),
    "json"
  )
})

test_that("unsafe request components fail without echoing values", {
  client <- test_client()
  secret <- "REQUEST-SECRET-MUST-NOT-LEAK"

  invalid_calls <- list(
    function() delta.sharing:::.new_client_http_request(
      client,
      "GET",
      c("shares", "..", secret),
      response_kind = "discovery"
    ),
    function() delta.sharing:::.new_client_http_request(
      client,
      "GET",
      "shares",
      headers = list(Authorization = paste("Bearer", secret)),
      response_kind = "discovery"
    ),
    function() delta.sharing:::.new_client_http_request(
      client,
      "GET",
      "shares",
      headers = list(`X-Test` = paste0("ok\r\n", secret)),
      response_kind = "discovery"
    ),
    function() delta.sharing:::.new_client_http_request(
      client,
      "GET",
      "shares",
      form = list(value = secret),
      response_kind = "discovery"
    )
  )

  for (invalid in invalid_calls) {
    condition <- expect_error(
      invalid(),
      class = "delta_sharing_validation_error"
    )
    expect_false(grepl(
      secret,
      conditionMessage(condition),
      fixed = TRUE
    ))
  }

  condition <- expect_error(
    delta.sharing:::.new_client_http_request(
      client,
      "GET",
      "shares",
      response_kind = "snapshot"
    ),
    class = "delta_sharing_validation_error"
  )
  expect_false(grepl("snapshot", conditionMessage(condition), fixed = TRUE))
})

test_that("fake adapter executes authenticated buffered control requests", {
  recorder <- new.env(parent = emptyenv())
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    recorder$request <- request
    list(
      status = 200L,
      headers = list(`content-type` = "application/json"),
      body = '{"shares":[]}'
    )
  })
  response <- delta.sharing:::.perform_authenticated_http(
    client = test_client(),
    method = "GET",
    path = "shares",
    query = list(pageToken = "opaque-page-token"),
    operation = "list_shares",
    response_kind = "discovery",
    replayable = TRUE,
    transport = transport
  )

  expect_identical(response$status, 200L)
  expect_identical(rawToChar(response$body), '{"shares":[]}')
  expect_identical(
    unname(recorder$request$headers[["Authorization"]]),
    "Bearer test-only-bearer-token"
  )
  expect_identical(
    recorder$request$query$pageToken,
    "opaque-page-token"
  )
})

test_that("control responses are bounded before collection", {
  secret <- "OVERSIZED-RESPONSE-SECRET"
  attempts <- 0L
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    attempts <<- attempts + 1L
    list(
      status = 200L,
      headers = list(),
      body = paste0(secret, paste(rep("x", 64), collapse = ""))
    )
  })

  condition <- expect_error(
    delta.sharing:::.perform_authenticated_http(
      client = test_client(),
      method = "GET",
      path = "shares",
      operation = "list_shares",
      response_kind = "discovery",
      replayable = FALSE,
      transport = transport,
      max_attempts = 1L,
      max_response_bytes = 16
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(attempts, 1L)
  expect_false(grepl(
    secret,
    conditionMessage(condition),
    fixed = TRUE
  ))
})

test_that("authenticated control requests use shared Retry-After policy", {
  calls <- 0L
  delays <- numeric()
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    calls <<- calls + 1L
    if (calls == 1L) {
      list(
        status = 503L,
        headers = list(`retry-after` = "2"),
        body = "discarded"
      )
    } else {
      list(status = 200L, headers = list(), body = "ok")
    }
  })

  response <- delta.sharing:::.perform_authenticated_http(
    client = test_client(),
    method = "GET",
    path = "shares",
    operation = "list_shares",
    response_kind = "discovery",
    replayable = TRUE,
    transport = transport,
    sleeper = function(delay) delays <<- c(delays, delay)
  )

  expect_identical(rawToChar(response$body), "ok")
  expect_identical(calls, 2L)
  expect_identical(delays, 2)
})

test_that("transport failures cannot expose request details", {
  secret <- "PATH-QUERY-BODY-TOKEN-SECRET"
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    stop(
      request$url,
      request$query$token,
      request$body$value,
      request$headers[["Authorization"]],
      secret
    )
  })
  condition <- expect_error(
    delta.sharing:::.perform_authenticated_http(
      client = test_client(),
      method = "POST",
      path = c("metadata", secret),
      query = list(token = secret),
      json = list(value = secret),
      operation = "table_metadata",
      response_kind = "metadata",
      replayable = FALSE,
      transport = transport,
      max_attempts = 1L
    ),
    class = "delta_sharing_http_error"
  )

  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )
  expect_false(grepl(secret, rendered, fixed = TRUE))
  expect_null(condition$path)
  expect_null(condition$query)
  expect_null(condition$body)
})

test_that("replayable OAuth requests refresh and replay exactly once on 401", {
  token_calls <- 0L
  sharing_calls <- 0L
  sharing_headers <- character()
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    if (identical(
      request$url,
      "https://identity.example.test/oauth/token"
    )) {
      token_calls <<- token_calls + 1L
      return(list(
        status = 200L,
        headers = list(),
        body = list(
          access_token = paste0("OAUTH-TOKEN-", token_calls),
          expires_in = 3600
        )
      ))
    }

    sharing_calls <<- sharing_calls + 1L
    sharing_headers <<- c(
      sharing_headers,
      unname(request$headers[["Authorization"]])
    )
    if (sharing_calls == 1L) {
      list(
        status = 401L,
        headers = list(),
        body = "401 body must be discarded"
      )
    } else {
      list(status = 200L, headers = list(), body = "ok")
    }
  })
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
  client <- sharing_client(http_test_oauth_profile())

  response <- delta.sharing:::.perform_authenticated_http(
    client = client,
    method = "GET",
    path = "shares",
    operation = "list_shares",
    response_kind = "discovery",
    replayable = TRUE,
    transport = transport,
    clock = function() now,
    max_attempts = 1L
  )

  expect_identical(rawToChar(response$body), "ok")
  expect_identical(token_calls, 2L)
  expect_identical(sharing_calls, 2L)
  expect_identical(
    sharing_headers,
    c("Bearer OAUTH-TOKEN-1", "Bearer OAUTH-TOKEN-2")
  )
  expect_identical(
    delta.sharing:::.client_context(client)$access_token_generation,
    2
  )
})

test_that("a second OAuth 401 is never replayed", {
  token_calls <- 0L
  sharing_calls <- 0L
  secret <- "SECOND-401-SECRET"
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    if (identical(
      request$url,
      "https://identity.example.test/oauth/token"
    )) {
      token_calls <<- token_calls + 1L
      return(list(
        status = 200L,
        headers = list(),
        body = list(
          access_token = paste0("OAUTH-TOKEN-", token_calls),
          expires_in = 3600
        )
      ))
    }
    sharing_calls <<- sharing_calls + 1L
    list(status = 401L, headers = list(), body = secret)
  })
  client <- sharing_client(http_test_oauth_profile())

  condition <- expect_error(
    delta.sharing:::.perform_authenticated_http(
      client = client,
      method = "GET",
      path = "shares",
      operation = "list_shares",
      response_kind = "discovery",
      replayable = TRUE,
      transport = transport,
      clock = function() as.POSIXct(
        "2026-07-29 00:00:00",
        tz = "UTC"
      ),
      max_attempts = 1L
    ),
    class = "delta_sharing_http_error"
  )

  expect_identical(condition$status, 401L)
  expect_identical(token_calls, 2L)
  expect_identical(sharing_calls, 2L)
  expect_false(grepl(secret, conditionMessage(condition), fixed = TRUE))
})

test_that("non-replayable OAuth 401 does not invalidate or refresh", {
  token_calls <- 0L
  sharing_calls <- 0L
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    if (identical(
      request$url,
      "https://identity.example.test/oauth/token"
    )) {
      token_calls <<- token_calls + 1L
      return(list(
        status = 200L,
        headers = list(),
        body = list(access_token = "OAUTH-TOKEN", expires_in = 3600)
      ))
    }
    sharing_calls <<- sharing_calls + 1L
    list(status = 401L, headers = list(), body = "ignored")
  })
  client <- sharing_client(http_test_oauth_profile())

  expect_error(
    delta.sharing:::.perform_authenticated_http(
      client = client,
      method = "GET",
      path = "shares",
      operation = "list_shares",
      response_kind = "discovery",
      replayable = FALSE,
      transport = transport,
      clock = function() as.POSIXct(
        "2026-07-29 00:00:00",
        tz = "UTC"
      ),
      max_attempts = 1L
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(token_calls, 1L)
  expect_identical(sharing_calls, 1L)
  expect_identical(
    delta.sharing:::.client_context(client)$access_token,
    "OAUTH-TOKEN"
  )
})

test_that("httr2 adapter builds redacted production requests", {
  recorder <- new.env(parent = emptyenv())
  old_options <- options(
    httr2_mock = function(request) {
      recorder$url <- httr2::req_get_url(request)
      recorder$headers <- httr2::req_get_headers(
        request,
        redacted = "reveal"
      )
      recorder$redacted_headers <- httr2::req_get_headers(
        request,
        redacted = "drop"
      )
      recorder$body_type <- httr2::req_get_body_type(request)
      httr2::response(
        status_code = 200L,
        headers = list(`content-type` = "application/json"),
        body = charToRaw('{"ok":true}')
      )
    }
  )
  on.exit(options(old_options), add = TRUE)

  response <- delta.sharing:::.perform_authenticated_http(
    client = test_client(),
    method = "POST",
    path = c("metadata", "sales eu"),
    query = list(version = 42),
    json = list(includeSchema = TRUE),
    operation = "table_metadata",
    response_kind = "metadata",
    replayable = TRUE,
    transport = delta.sharing:::.httr2_http_transport(timeout_seconds = 30)
  )

  expect_identical(response$status, 200L)
  expect_identical(rawToChar(response$body), '{"ok":true}')
  expect_identical(
    recorder$url,
    paste0(
      "https://sharing.example.test/api/",
      "metadata/sales%20eu?version=42"
    )
  )
  expect_identical(
    unname(recorder$headers[["Authorization"]]),
    "Bearer test-only-bearer-token"
  )
  expect_null(recorder$redacted_headers[["Authorization"]])
  expect_identical(recorder$body_type, "json")
})

test_that("httr2 adapter enforces the configured response bound", {
  secret <- "HTTR2-OVERSIZED-SECRET"
  old_options <- options(
    httr2_mock = function(request) {
      httr2::response(
        status_code = 200L,
        headers = list(),
        body = charToRaw(paste0(secret, "-too-large"))
      )
    }
  )
  on.exit(options(old_options), add = TRUE)

  condition <- expect_error(
    delta.sharing:::.perform_authenticated_http(
      client = test_client(),
      method = "GET",
      path = "shares",
      operation = "list_shares",
      response_kind = "discovery",
      replayable = FALSE,
      transport = delta.sharing:::.httr2_http_transport(),
      max_attempts = 1L,
      max_response_bytes = 8
    ),
    class = "delta_sharing_http_error"
  )
  expect_false(grepl(secret, conditionMessage(condition), fixed = TRUE))
})
