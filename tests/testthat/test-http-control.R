test_that("successful requests return the transport response", {
  response <- list(status = 200L, body = "ok")

  expect_identical(
    delta.sharing:::.perform_with_retry(
      request = list(method = "GET"),
      send = function(request) response,
      status_of = function(response) response$status,
      operation = "list_shares",
      replayable = TRUE
    ),
    response
  )
})

test_that("replayable requests retry transport failures", {
  attempts <- 0L
  delays <- numeric()
  response <- delta.sharing:::.perform_with_retry(
    request = list(method = "GET"),
    send = function(request) {
      attempts <<- attempts + 1L
      if (attempts < 3L) {
        stop("unsafe transport details")
      }
      list(status = 200L)
    },
    status_of = function(response) response$status,
    operation = "list_shares",
    endpoint_host = "sharing.example.test",
    replayable = TRUE,
    sleeper = function(delay) delays <<- c(delays, delay),
    random = function(n, min, max) max
  )

  expect_identical(response$status, 200L)
  expect_identical(attempts, 3L)
  expect_equal(delays, c(0.1, 0.2))
})

test_that("non-replayable requests do not retry", {
  attempts <- 0L
  condition <- expect_error(
    delta.sharing:::.perform_with_retry(
      request = list(method = "POST"),
      send = function(request) {
        attempts <<- attempts + 1L
        stop("body and URL must not escape")
      },
      status_of = function(response) response$status,
      operation = "query_table",
      endpoint_host = "sharing.example.test",
      replayable = FALSE
    ),
    class = "delta_sharing_http_error"
  )

  expect_identical(attempts, 1L)
  expect_identical(condition$retry_count, 0L)
  expect_identical(condition$endpoint_host, "sharing.example.test")
  expect_false(grepl("body and URL", condition$message, fixed = TRUE))
})

test_that("retryable statuses honor capped Retry-After", {
  attempts <- 0L
  delays <- numeric()
  response <- delta.sharing:::.perform_with_retry(
    request = list(method = "GET"),
    send = function(request) {
      attempts <<- attempts + 1L
      if (attempts == 1L) {
        list(status = 429L, retry_after = "45")
      } else {
        list(status = 204L)
      }
    },
    status_of = function(response) response$status,
    retry_after_of = function(response) response$retry_after,
    operation = "list_tables",
    replayable = TRUE,
    sleeper = function(delay) delays <<- c(delays, delay),
    delay_cap = 30
  )

  expect_identical(response$status, 204L)
  expect_identical(delays, 30)
})

test_that("retry exhaustion exposes only safe HTTP metadata", {
  attempts <- 0L
  condition <- expect_error(
    delta.sharing:::.perform_with_retry(
      request = "signed request contents",
      send = function(request) {
        attempts <<- attempts + 1L
        list(
          status = 503L,
          body = "credential=must-not-leak"
        )
      },
      status_of = function(response) response$status,
      operation = "table_metadata",
      endpoint_host = "sharing.example.test",
      replayable = TRUE,
      max_attempts = 3L,
      sleeper = function(delay) NULL,
      random = function(n, min, max) 0
    ),
    class = "delta_sharing_http_error"
  )

  expect_identical(attempts, 3L)
  expect_identical(condition$status, 503L)
  expect_identical(condition$retry_count, 2L)
  expect_null(condition$body)
  expect_false(grepl("credential", condition$message, fixed = TRUE))
})

test_that("client errors and invalid statuses fail without replay", {
  attempts <- 0L
  condition <- expect_error(
    delta.sharing:::.perform_with_retry(
      request = list(method = "GET"),
      send = function(request) {
        attempts <<- attempts + 1L
        list(status = 403L)
      },
      status_of = function(response) response$status,
      operation = "list_schemas",
      replayable = TRUE
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(attempts, 1L)
  expect_identical(condition$status, 403L)

  expect_error(
    delta.sharing:::.perform_with_retry(
      request = list(method = "GET"),
      send = function(request) list(status = "200"),
      status_of = function(response) response$status,
      operation = "list_schemas",
      replayable = TRUE
    ),
    class = "delta_sharing_protocol_error"
  )
})

test_that("HTTP controls validate injected policy", {
  expect_error(
    delta.sharing:::.perform_with_retry(
      request = NULL,
      send = NULL,
      status_of = identity,
      operation = "request"
    ),
    "HTTP control hooks must be functions",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.perform_with_retry(
      request = NULL,
      send = identity,
      status_of = identity,
      operation = "",
      max_attempts = 1L
    ),
    "`operation` must be one non-empty string",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.perform_with_retry(
      request = NULL,
      send = identity,
      status_of = identity,
      operation = "request",
      max_attempts = 0
    ),
    "`max_attempts` must be one positive whole number",
    fixed = TRUE
  )
})
