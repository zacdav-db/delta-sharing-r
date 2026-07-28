oauth_client_profile <- function() {
  jsonlite::fromJSON(
    test_path("fixtures", "profiles", "oauth-client-v2.json"),
    simplifyVector = FALSE
  )
}

test_auth_transport <- function(send, body = function(response) response$body) {
  list(
    send = send,
    status = function(response) response$status,
    body = body,
    retry_after = function(response) response$retry_after
  )
}

test_that("bearer authorization checks expiry against an injected clock", {
  profile <- test_profile()
  profile$bearerToken <- "BEARER-SECRET"
  profile$expirationTime <- "2026-07-29T00:01:00Z"
  client <- sharing_client(profile)

  authorization <- delta.sharing:::.client_authorization(
    client,
    clock = function() as.POSIXct(
      "2026-07-29 00:00:59",
      tz = "UTC"
    )
  )
  expect_identical(
    unname(authorization$headers[["Authorization"]]),
    "Bearer BEARER-SECRET"
  )
  expect_null(authorization$cache_generation)

  condition <- expect_error(
    delta.sharing:::.client_authorization(
      client,
      clock = function() as.POSIXct(
        "2026-07-29 00:01:00",
        tz = "UTC"
      )
    ),
    "expired",
    class = "delta_sharing_auth_error"
  )
  expect_false(grepl(
    profile$bearerToken,
    conditionMessage(condition),
    fixed = TRUE
  ))
})

test_that("Basic authorization is constructed without exposing credentials", {
  client <- sharing_client(test_path(
    "fixtures",
    "profiles",
    "basic-v2.json"
  ))
  authorization <- delta.sharing:::.client_authorization(client)

  expect_identical(
    unname(authorization$headers[["Authorization"]]),
    "Basic Zml4dHVyZS11c2VyOmZpeHR1cmUtcGFzc3dvcmQ="
  )
  expect_identical(authorization$auth_type, "basic")

  profile <- oauth_client_profile()
  profile$type <- "basic"
  profile$username <- "unsafe:name"
  profile$password <- "BASIC-SECRET"
  condition <- expect_error(
    delta.sharing:::.client_authorization(sharing_client(profile)),
    class = "delta_sharing_auth_error"
  )
  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )
  expect_false(grepl("unsafe:name", rendered, fixed = TRUE))
  expect_false(grepl("BASIC-SECRET", rendered, fixed = TRUE))
})

test_that("OAuth client credentials construct a transport-neutral request", {
  recorder <- new.env(parent = emptyenv())
  transport <- test_auth_transport(function(request) {
    recorder$request <- request
    list(
      status = 200L,
      retry_after = NULL,
      body = list(
        access_token = "OAUTH-TOKEN-ONE",
        token_type = "Bearer",
        expires_in = 3600
      )
    )
  })
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
  client <- sharing_client(oauth_client_profile())

  authorization <- delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now
  )
  request <- recorder$request

  expect_identical(request$method, "POST")
  expect_identical(
    request$url,
    "https://identity.example.test/oauth/token"
  )
  expect_identical(
    unname(request$headers[["Authorization"]]),
    "Basic Zml4dHVyZS1jbGllbnQ6Zml4dHVyZS1jbGllbnQtc2VjcmV0"
  )
  expect_identical(
    unname(request$headers[["Content-Type"]]),
    "application/x-www-form-urlencoded"
  )
  expect_identical(request$body$grant_type, "client_credentials")
  expect_identical(request$body$scope, "delta-sharing.read")
  expect_identical(
    unname(authorization$headers[["Authorization"]]),
    "Bearer OAUTH-TOKEN-ONE"
  )
  expect_identical(authorization$cache_generation, 1)

  context <- delta.sharing:::.client_context(client)
  expect_identical(context$access_token_issued_at, now)
  expect_identical(context$access_token_expires_at, now + 3600)
  expect_identical(context$access_token_refresh_at, now + 3000)
  expect_identical(context$state, "ready")
})

test_that("OAuth tokens are cached until the bounded refresh threshold", {
  calls <- 0L
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
  transport <- test_auth_transport(function(request) {
    calls <<- calls + 1L
    list(
      status = 200L,
      retry_after = NULL,
      body = list(
        access_token = paste0("TOKEN-", calls),
        token_type = "bearer",
        expires_in = 3600
      )
    )
  })
  client <- sharing_client(oauth_client_profile())

  first <- delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now
  )
  now <- now + 2999
  cached <- delta.sharing:::.client_authorization(
    client,
    clock = function() now
  )
  expect_identical(calls, 1L)
  expect_identical(cached$headers, first$headers)
  expect_identical(cached$cache_generation, 1)

  now <- now + 1
  refreshed <- delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now
  )
  expect_identical(calls, 2L)
  expect_identical(
    unname(refreshed$headers[["Authorization"]]),
    "Bearer TOKEN-2"
  )
  expect_identical(refreshed$cache_generation, 2)
})

test_that("short OAuth lifetimes refresh at half their lifetime", {
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
  transport <- test_auth_transport(function(request) {
    list(
      status = 200L,
      retry_after = NULL,
      body = list(
        access_token = "SHORT-TOKEN",
        token_type = "Bearer",
        expires_in = 120
      )
    )
  })
  client <- sharing_client(oauth_client_profile())

  delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now
  )
  context <- delta.sharing:::.client_context(client)
  expect_identical(context$access_token_expires_at, now + 120)
  expect_identical(context$access_token_refresh_at, now + 60)
})

test_that("OAuth responses are strictly validated without leaking bodies", {
  secret <- "OAUTH-RESPONSE-SECRET"
  invalid_bodies <- list(
    list(token_type = "Bearer", expires_in = 3600),
    list(
      access_token = secret,
      token_type = "mac",
      expires_in = 3600
    ),
    list(
      access_token = secret,
      token_type = "Bearer",
      expires_in = 0
    ),
    list(
      access_token = secret,
      token_type = "Bearer",
      expires_in = "not-a-number"
    ),
    list(
      access_token = secret,
      token_type = "Bearer",
      expires_in = "999999999999999999999999"
    ),
    paste0('{"access_token":"', secret, '"')
  )

  for (body in invalid_bodies) {
    client <- sharing_client(oauth_client_profile())
    transport <- test_auth_transport(function(request) {
      list(status = 200L, retry_after = NULL, body = body)
    })
    condition <- expect_error(
      delta.sharing:::.client_authorization(
        client,
        transport = transport,
        clock = function() as.POSIXct(
          "2026-07-29 00:00:00",
          tz = "UTC"
        )
      ),
      class = "delta_sharing_auth_error"
    )
    rendered <- paste(
      conditionMessage(condition),
      capture.output(str(condition)),
      collapse = "\n"
    )
    expect_false(grepl(secret, rendered, fixed = TRUE))
  }
})

test_that("OAuth accepts absent token type and numeric-string lifetimes", {
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
  transport <- test_auth_transport(function(request) {
    list(
      status = 200L,
      retry_after = NULL,
      body = list(
        access_token = "TOKEN-WITH-IMPLICIT-TYPE",
        expires_in = "1.205e2"
      )
    )
  })
  client <- sharing_client(oauth_client_profile())

  authorization <- delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now
  )
  context <- delta.sharing:::.client_context(client)

  expect_identical(
    unname(authorization$headers[["Authorization"]]),
    "Bearer TOKEN-WITH-IMPLICIT-TYPE"
  )
  expect_identical(context$access_token_expires_at, now + 120.5)
  expect_identical(context$access_token_refresh_at, now + 60.25)
})

test_that("OAuth HTTP and transport failures remain secret-safe", {
  secret <- "TRANSPORT-SECRET"
  client <- sharing_client(oauth_client_profile())
  http_transport <- test_auth_transport(function(request) {
    list(
      status = 400L,
      retry_after = NULL,
      body = paste0('{"error_description":"', secret, '"}')
    )
  })
  condition <- expect_error(
    delta.sharing:::.client_authorization(
      client,
      transport = http_transport,
      clock = function() as.POSIXct(
        "2026-07-29 00:00:00",
        tz = "UTC"
      )
    ),
    class = "delta_sharing_http_error"
  )
  expect_identical(condition$status, 400L)
  expect_false(grepl(secret, conditionMessage(condition), fixed = TRUE))

  failing_transport <- test_auth_transport(function(request) {
    stop("failed with ", secret)
  })
  condition <- expect_error(
    delta.sharing:::.client_authorization(
      sharing_client(oauth_client_profile()),
      transport = failing_transport,
      clock = function() as.POSIXct(
        "2026-07-29 00:00:00",
        tz = "UTC"
      ),
      max_attempts = 1L
    ),
    class = "delta_sharing_http_error"
  )
  expect_false(grepl(secret, conditionMessage(condition), fixed = TRUE))
})

test_that("cache invalidation is generation-safe for future 401 replay", {
  calls <- 0L
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
  transport <- test_auth_transport(function(request) {
    calls <<- calls + 1L
    list(
      status = 200L,
      retry_after = NULL,
      body = list(
        access_token = paste0("TOKEN-", calls),
        token_type = "Bearer",
        expires_in = 3600
      )
    )
  })
  client <- sharing_client(oauth_client_profile())
  first <- delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now
  )

  expect_false(delta.sharing:::.invalidate_client_auth(
    client,
    first$cache_generation + 1
  ))
  expect_identical(
    delta.sharing:::.client_context(client)$access_token,
    "TOKEN-1"
  )
  expect_true(delta.sharing:::.invalidate_client_auth(
    client,
    first$cache_generation
  ))
  expect_null(delta.sharing:::.client_context(client)$access_token)

  second <- delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now
  )
  expect_identical(second$cache_generation, 2)
  expect_false(delta.sharing:::.invalidate_client_auth(
    client,
    first$cache_generation
  ))
  expect_identical(
    delta.sharing:::.client_context(client)$access_token,
    "TOKEN-2"
  )
})

test_that("private-key authentication remains explicitly unavailable", {
  client <- sharing_client(test_path(
    "fixtures",
    "profiles",
    "private-key-v2.json"
  ))
  condition <- expect_error(
    delta.sharing:::.client_authorization(client),
    class = "delta_sharing_unsupported_error"
  )
  expect_identical(condition$feature, "private-key JWT authentication")
  expect_false(grepl(
    "/test-only/private-key.pem",
    conditionMessage(condition),
    fixed = TRUE
  ))
})
