test_that("sharing requests carry method, query, and redacted authorization", {
  request <- delta.sharing:::req_share(
    creds = fixture_credentials(),
    method = "GET",
    endpoint = "shares",
    params = list(maxResults = 50L)
  )

  expect_identical(request$method, "GET")
  expect_identical(
    request$url,
    "https://sharing.example.test/api/shares?maxResults=50"
  )

  printed <- capture.output(print(request))
  expect_true(any(grepl("Authorization: <REDACTED>", printed, fixed = TRUE)))
  expect_false(any(grepl(
    fixture_credentials()$bearerToken,
    printed,
    fixed = TRUE
  )))
})

test_that("request bodies omit empty fields and HEAD bodies", {
  post <- delta.sharing:::req_share(
    creds = fixture_credentials(),
    method = "POST",
    endpoint = "shares/sales/query",
    body = list(predicateHints = NULL, limitHint = 10L)
  )
  head <- delta.sharing:::req_share(
    creds = fixture_credentials(),
    method = "HEAD",
    endpoint = "shares/sales/schemas/default/tables/orders",
    body = list(ignored = "value")
  )

  expect_identical(post$body$type, "json")
  expect_identical(post$body$data, list(limitHint = 10L))
  expect_null(head$body)
})

test_that("paginated requests collect every discovery page", {
  requests <- character()
  responses <- list(
    fixture_json_response("discovery", "shares-page-1.json"),
    fixture_json_response("discovery", "shares-page-2.json")
  )
  mock <- function(req) {
    requests <<- c(requests, req$url)
    responses[[length(requests)]]
  }
  request <- delta.sharing:::req_share(
    creds = fixture_credentials(),
    method = "GET",
    endpoint = "shares"
  )

  result <- httr2::with_mocked_responses(
    mock,
    delta.sharing:::make_req(request)
  )

  expect_identical(
    vapply(result$items, `[[`, character(1), "name"),
    c("sales", "operations")
  )
  expect_length(requests, 2L)
  expect_match(requests[[2]], "pageToken=page-2", fixed = TRUE)
})

test_that("HEAD requests return protocol headers", {
  request <- delta.sharing:::req_share(
    creds = fixture_credentials(),
    method = "HEAD",
    endpoint = "shares/sales/schemas/default/tables/orders"
  )
  response <- httr2::response(
    method = "HEAD",
    headers = c(
      "delta-table-version: 42",
      "date: Tue, 28 Jul 2026 00:00:00 GMT"
    )
  )

  result <- httr2::with_mocked_responses(
    list(response),
    delta.sharing:::make_req(request)
  )

  expect_identical(result[["delta-table-version"]], "42")
})

test_that("server error bodies produce actionable messages", {
  coded <- httr2::response_json(
    status_code = 403L,
    body = list(errorCode = "PERMISSION_DENIED", message = "Access denied")
  )
  scalar <- httr2::response_json(
    status_code = 500L,
    body = list("temporary failure")
  )
  fields <- httr2::response_json(
    status_code = 400L,
    body = list(error = "invalid", detail = "bad request")
  )

  expect_identical(
    delta.sharing:::req_error_body(coded),
    "PERMISSION_DENIED: Access denied"
  )
  expect_identical(
    delta.sharing:::req_error_body(scalar),
    "temporary failure"
  )
  expect_identical(
    delta.sharing:::req_error_body(fields),
    "invalid bad request"
  )
})
