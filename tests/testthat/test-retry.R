test_that("retry status allowlist is conservative", {
  expect_true(delta.sharing:::.retryable_status(408L))
  expect_true(delta.sharing:::.retryable_status(429L))
  expect_true(delta.sharing:::.retryable_status(500L))
  expect_true(delta.sharing:::.retryable_status(599L))

  for (status in list(200L, 400L, 401L, 499L, 600L, NA_integer_, "500")) {
    expect_false(delta.sharing:::.retryable_status(status))
  }
})

test_that("Retry-After accepts seconds and HTTP dates", {
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")

  expect_identical(
    delta.sharing:::.parse_retry_after("12", now),
    12
  )
  expect_identical(
    delta.sharing:::.parse_retry_after(
      "Wed, 29 Jul 2026 00:00:08 GMT",
      now
    ),
    8
  )
  expect_identical(
    delta.sharing:::.parse_retry_after(
      "Tue, 28 Jul 2026 23:59:59 GMT",
      now
    ),
    0
  )
})

test_that("invalid Retry-After values are ignored", {
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")

  for (value in list(NULL, "", "-1", "1.5", "later", NA_character_, 2L)) {
    expect_null(delta.sharing:::.parse_retry_after(value, now))
  }
})

test_that("Retry-After takes precedence and is capped", {
  random_called <- FALSE
  delay <- delta.sharing:::.retry_delay(
    attempt = 3L,
    retry_after = "45",
    cap = 30,
    random = function(...) {
      random_called <<- TRUE
      0
    }
  )

  expect_identical(delay, 30)
  expect_false(random_called)
})

test_that("exponential retry uses injectable full jitter", {
  calls <- list()
  deterministic <- function(n, min, max) {
    calls[[length(calls) + 1L]] <<- c(n = n, min = min, max = max)
    max / 2
  }

  expect_identical(
    delta.sharing:::.retry_delay(
      attempt = 1L,
      base = 0.1,
      cap = 30,
      random = deterministic
    ),
    0.05
  )
  expect_identical(
    delta.sharing:::.retry_delay(
      attempt = 4L,
      base = 0.1,
      cap = 0.5,
      random = deterministic
    ),
    0.25
  )
  expect_equal(calls[[1L]][["max"]], 0.1)
  expect_equal(calls[[2L]][["max"]], 0.5)
})

test_that("retry policy rejects invalid control values", {
  expect_error(
    delta.sharing:::.retry_delay(0),
    "`attempt` must be one positive whole number",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.retry_delay(1, base = 0),
    "`base` and `cap` must be positive finite numbers",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.retry_delay(1, random = "not a function"),
    "`random` must be a function",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.retry_delay(
      1,
      random = function(...) 2
    ),
    "`random` returned an invalid retry delay",
    fixed = TRUE
  )
})
