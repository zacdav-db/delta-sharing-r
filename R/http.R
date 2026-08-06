# Authenticated HTTP against the Delta Sharing REST API, built on httr2.
# The client owns the profile (endpoint) and auth context; this module builds,
# authenticates, retries, and performs requests, and maps failures to typed
# delta_sharing conditions.

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
  query = list(),
  operation = "request"
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
    # Map HTTP error bodies to a readable message; sharing_perform then wraps
    # httr2's condition in a typed delta_sharing condition.
    httr2::req_error(body = sharing_http_error_body)
  req <- auth$authenticate(req)
  attr(req, "delta_sharing_operation") <- operation
  req
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

# Run `code` (one or more httr2 performs against `req`), translating httr2's
# HTTP and connection failures into typed delta_sharing conditions.
with_sharing_errors <- function(req, code) {
  operation <- attr(req, "delta_sharing_operation", exact = TRUE) %||% "request"
  tryCatch(
    code,
    httr2_http = function(cnd) {
      status <- if (is.null(cnd$resp)) {
        NA_integer_
      } else {
        httr2::resp_status(cnd$resp)
      }
      type <- if (!is.na(status) && status %in% c(401, 403)) "auth" else "http"
      abort(
        conditionMessage(cnd),
        type = type,
        operation = operation,
        status = status,
        endpoint_host = httr2::url_parse(req$url)$hostname
      )
    },
    httr2_failure = function(cnd) {
      abort(
        "The Delta Sharing request could not be completed.",
        type = "http",
        operation = operation,
        endpoint_host = httr2::url_parse(req$url)$hostname
      )
    },
    httr2_streaming_error = function(cnd) {
      abort(
        "The server returned an NDJSON line larger than the supported limit.",
        type = "protocol",
        operation = operation
      )
    }
  )
}

# Perform a single request, translating failures into typed conditions.
sharing_perform <- function(req) {
  with_sharing_errors(req, httr2::req_perform(req))
}

# Perform a request through httr2's pull-based response connection and pass
# bounded groups of complete NDJSON lines to `consume`. The consumer receives
# and returns its explicit state. httr2 mocks return a regular in-memory
# response, so tests use the same reducer in bounded chunks.
sharing_stream_lines <- function(
  req,
  consume,
  state = NULL,
  lines_per_chunk = 256L,
  max_line_bytes = 8 * 1024^2
) {
  with_sharing_errors(req, {
    resp <- httr2::req_perform_connection(req)
    on.exit(close(resp), add = TRUE)

    if (inherits(resp$body, "StreamingBody")) {
      repeat {
        lines <- httr2::resp_stream_lines(
          resp,
          lines = lines_per_chunk,
          max_size = max_line_bytes
        )
        if (length(lines) > 0L) {
          state <- consume(lines, state)
        }
        if (httr2::resp_stream_is_complete(resp)) {
          break
        }
      }
    } else {
      lines <- strsplit(
        httr2::resp_body_string(resp),
        "\n",
        fixed = TRUE
      )[[1L]]
      if (length(lines) > 0L) {
        if (any(nchar(lines, type = "bytes") > max_line_bytes)) {
          abort(
            "The server returned an NDJSON line larger than the supported limit.",
            type = "protocol",
            operation = attr(
              req,
              "delta_sharing_operation",
              exact = TRUE
            ) %||%
              "request"
          )
        }
        starts <- seq.int(1L, length(lines), by = lines_per_chunk)
        state <- purrr::reduce(
          starts,
          function(state, start) {
            end <- min(start + lines_per_chunk - 1L, length(lines))
            consume(lines[seq.int(start, end)], state)
          },
          .init = state
        )
      }
    }
    state
  })
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
    query = list(maxResults = max_results),
    operation = operation
  )
  next_token <- function(resp) {
    token <- discovery_body(resp, operation)$nextPageToken
    if (is_scalar_character(token) && nzchar(token)) token else NULL
  }
  resps <- with_sharing_errors(
    first,
    httr2::req_perform_iterative(
      first,
      next_req = httr2::iterate_with_cursor("pageToken", next_token),
      max_reqs = Inf
    )
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
