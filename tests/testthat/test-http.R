test_that("HTTP error bodies use provider messages when available", {
  message_response <- httr2::response(
    400,
    headers = list(`content-type` = "application/json"),
    body = charToRaw('{"errorCode":"BAD_REQUEST","message":"invalid query"}')
  )
  scalar_response <- httr2::response(
    400,
    headers = list(`content-type` = "application/json"),
    body = charToRaw('{"detail":"permission denied"}')
  )
  invalid_response <- httr2::response(
    400,
    headers = list(`content-type` = "application/json"),
    body = charToRaw("{not-json")
  )

  expect_equal(
    sharing_http_error_body(message_response),
    "BAD_REQUEST: invalid query"
  )
  expect_equal(sharing_http_error_body(scalar_response), "permission denied")
  expect_null(sharing_http_error_body(invalid_response))
})

test_that("HTTP authentication failures retain only safe diagnostics", {
  profile <- test_profile()
  request <- sharing_request(
    profile,
    sharing_auth_context(profile),
    "protected",
    operation = "authenticate"
  )
  httr2::local_mocked_responses(function(req) {
    httr2::response(
      401,
      headers = list(`content-type` = "application/json"),
      body = charToRaw('{"message":"expired"}')
    )
  })

  condition <- expect_error(
    sharing_perform(request),
    class = "delta_sharing_auth_error"
  )

  expect_identical(condition$status, 401L)
  expect_identical(condition$operation, "authenticate")
  expect_identical(condition$endpoint_host, "sharing.example.test")
})

test_that("transport and streaming conditions map to public errors", {
  profile <- test_profile()
  request <- sharing_request(
    profile,
    sharing_auth_context(profile),
    "resource",
    operation = "read"
  )
  failure <- structure(
    list(message = "socket detail"),
    class = c("httr2_failure", "error", "condition")
  )
  streaming <- structure(
    list(message = "line too large"),
    class = c("httr2_streaming_error", "error", "condition")
  )

  expect_error(
    with_sharing_errors(request, stop(failure)),
    class = "delta_sharing_http_error"
  )
  expect_error(
    with_sharing_errors(request, stop(streaming)),
    class = "delta_sharing_protocol_error"
  )
})

test_that("in-memory mocked streams enforce the line-size limit", {
  body <- paste0('{"value":"', strrep("x", 20L), '"}')
  httr2::local_mocked_responses(function(req) {
    httr2::response(200, body = charToRaw(body))
  })
  profile <- test_profile()
  request <- sharing_request(
    profile,
    sharing_auth_context(profile),
    "stream",
    operation = "read"
  )

  expect_error(
    sharing_stream_lines(
      request,
      function(lines, state) state,
      max_line_bytes = 8L
    ),
    class = "delta_sharing_protocol_error"
  )
})

test_that("invalid discovery JSON becomes a protocol error", {
  response <- httr2::response(
    200,
    headers = list(`content-type` = "application/json"),
    body = charToRaw("{not-json")
  )

  expect_error(
    discovery_body(response, "list_shares"),
    class = "delta_sharing_protocol_error"
  )
})
