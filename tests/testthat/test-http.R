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

test_that("HTTP failures retain httr2's class and provider message", {
  profile <- test_profile()
  request <- sharing_request(
    profile,
    sharing_auth_context(profile),
    "protected"
  )
  httr2::local_mocked_responses(function(req) {
    httr2::response(
      401,
      headers = list(`content-type` = "application/json"),
      body = charToRaw('{"message":"expired"}')
    )
  })

  condition <- expect_error(
    httr2::req_perform(request),
    class = "httr2_http_401"
  )

  expect_identical(condition$status, 401L)
  expect_match(conditionMessage(condition), "expired", fixed = TRUE)
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
