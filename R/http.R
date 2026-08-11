# Authenticated HTTP against the Delta Sharing REST API, built on httr2.
# The client owns the profile (endpoint) and auth context; this module builds,
# authenticates, retries, and performs requests. HTTP failures retain httr2's
# native condition classes, with provider error messages where available.

user_agent <- function() {
  paste0(
    "r-delta-sharing/",
    utils::packageVersion("delta.sharing")
  )
}

# Build an authenticated httr2 request for a path under the profile endpoint.
# `path` is a character vector of already-unencoded segments; httr2 encodes
# them. `query` is a named list appended as query parameters.
sharing_request <- function(
  profile,
  auth,
  path,
  method = "GET",
  query = list()
) {
  req <- profile$endpoint |>
    httr2::request() |>
    httr2::req_user_agent(user_agent()) |>
    httr2::req_url_path_append(path) |>
    httr2::req_method(method) |>
    httr2::req_url_query(!!!query) |>
    # Retry transient failures; httr2 honours Retry-After.
    httr2::req_retry(
      max_tries = 5,
      is_transient = \(resp) {
        httr2::resp_status(resp) %in% c(429, 500, 502, 503, 504)
      }
    ) |>
    # Preserve httr2's native HTTP errors while displaying provider messages.
    httr2::req_error(body = sharing_http_error_body)
  auth$authenticate(req)
}

sharing_http_error_body <- function(resp) {
  body <- httr2::resp_body_string(resp)
  if (!jsonlite::validate(body)) {
    return(NULL)
  }
  parsed <- jsonlite::fromJSON(body, simplifyVector = FALSE)
  if (is.list(parsed)) {
    if (!is.null(parsed$message)) {
      return(paste(c(parsed$errorCode, parsed$message), collapse = ": "))
    }
    if (length(parsed) == 1L && is.character(parsed[[1]])) {
      return(parsed[[1]])
    }
  }
  NULL
}

# Decode server JSON and translate malformed input to a public protocol error.
parse_protocol_json <- function(json, message, operation) {
  tryCatch(
    jsonlite::fromJSON(json, simplifyVector = FALSE),
    error = function(cnd) {
      abort(message, type = "protocol", operation = operation)
    }
  )
}

# Follow `nextPageToken` pagination on a GET discovery route and return the
# concatenated `items` lists. httr2 drives the iteration: `iterate_with_cursor`
# feeds each response's `nextPageToken` into the next request's query string.
sharing_paginate <- function(
  profile,
  auth,
  path,
  operation,
  max_results = 500L
) {
  first <- sharing_request(
    profile,
    auth,
    path,
    query = list(maxResults = max_results)
  )
  next_token <- function(resp) {
    token <- discovery_body(resp, operation)$nextPageToken
    if (is_scalar_character(token)) token else NULL
  }
  resps <- httr2::req_perform_iterative(
    first,
    next_req = httr2::iterate_with_cursor("pageToken", next_token),
    max_reqs = Inf
  )
  purrr::list_flatten(purrr::map(
    resps,
    \(resp) discovery_body(resp, operation)$items
  ))
}

discovery_body <- function(resp, operation) {
  parse_protocol_json(
    httr2::resp_body_string(resp),
    "The server returned an invalid discovery page.",
    operation
  )
}
