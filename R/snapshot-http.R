.snapshot_stream_chunk_bytes <- 65536L

.new_snapshot_pull_response <- function(status,
                                        headers,
                                        pull,
                                        close) {
  structure(
    list(
      status = status,
      headers = headers,
      pull = pull,
      close = close
    ),
    class = c("delta_sharing_snapshot_pull_response", "list")
  )
}

.normalize_snapshot_pull_response <- function(response) {
  if (!inherits(response, "delta_sharing_snapshot_pull_response") ||
      !is.list(response) ||
      !is.function(response$pull) ||
      !is.function(response$close) ||
      is.null(response$headers)) {
    .snapshot_planning_abort(
      "The snapshot fetcher returned an invalid pull response."
    )
  }
  response
}

#' @exportS3Method print delta_sharing_snapshot_pull_response
print.delta_sharing_snapshot_pull_response <- function(x, ...) {
  cat("<delta_sharing_snapshot_pull_response> body not buffered; content redacted\n")
  invisible(x)
}

.new_snapshot_http_request <- function(client, plan) {
  if (!inherits(plan, "delta_sharing_snapshot_request") ||
      !is.list(plan) ||
      !identical(plan$operation, .snapshot_query_operation)) {
    .snapshot_planning_abort(
      "`plan` must be a snapshot request plan.",
      type = "validation"
    )
  }
  context <- .client_context(client)
  method <- .normalize_http_method(plan$method)
  path <- .normalize_http_path(plan$path_segments)
  headers <- .normalize_http_headers(plan$headers)
  body <- .normalize_http_json(plan$body)
  if (!identical(method, "POST") || is.null(body)) {
    .snapshot_planning_abort(
      "The snapshot request plan is invalid.",
      type = "validation"
    )
  }
  endpoint <- sub("/+$", "", context$endpoint)
  structure(
    list(
      method = method,
      url = paste(c(endpoint, path), collapse = "/"),
      query = list(),
      headers = headers,
      body_type = "json",
      body = body
    ),
    class = c("delta_sharing_snapshot_http_request", "list")
  )
}

#' @exportS3Method print delta_sharing_snapshot_http_request
print.delta_sharing_snapshot_http_request <- function(x, ...) {
  cat("<delta_sharing_snapshot_http_request> POST; headers and body redacted\n")
  invisible(x)
}

.normalize_snapshot_stream_transport <- function(transport) {
  hooks <- c("open", "status", "headers", "pull", "close")
  values <- lapply(hooks, function(name) {
    if (is.list(transport)) {
      transport[[name, exact = TRUE]]
    } else {
      NULL
    }
  })
  names(values) <- hooks
  retry_after <- if (is.list(transport)) {
    transport[["retry_after", exact = TRUE]]
  } else {
    NULL
  }
  if (!is.list(transport) ||
      is.null(names(transport)) ||
      any(!vapply(values, is.function, logical(1))) ||
      (!is.null(retry_after) && !is.function(retry_after))) {
    stop(
      paste0(
        "`stream_transport` must provide `open`, `status`, `headers`, ",
        "`pull`, and `close` functions plus optional `retry_after`."
      ),
      call. = FALSE
    )
  }
  if (is.null(retry_after)) {
    retry_after <- function(response) {
      .http_header(values$headers(response), "retry-after")
    }
  }
  c(values, list(retry_after = retry_after))
}

.new_httr2_snapshot_response <- function(response, chunk_bytes) {
  status <- httr2::resp_status(response)
  headers <- httr2::resp_headers(response)
  body <- response$body
  valid_body <- is.raw(body) ||
    (is.environment(body) &&
      is.function(body$read) &&
      is.function(body$is_complete) &&
      is.function(body$close))
  if (!valid_body) {
    if (is.environment(body) && is.function(body$close)) {
      try(body$close(), silent = TRUE)
    }
    stop("The HTTP transport returned an invalid body stream.", call. = FALSE)
  }

  state <- new.env(parent = emptyenv())
  state$status <- status
  state$headers <- headers
  state$body <- body
  state$chunk_bytes <- chunk_bytes
  state$offset <- 1L
  state$closed <- FALSE
  class(state) <- "delta_sharing_httr2_snapshot_response"
  state
}

.httr2_open_snapshot_request <- function(request,
                                         timeout_seconds,
                                         chunk_bytes) {
  prepared <- .httr2_prepare_request(request, timeout_seconds)
  response <- httr2::req_perform_connection(prepared)
  .new_httr2_snapshot_response(response, chunk_bytes)
}

.httr2_snapshot_pull <- function(response) {
  if (!inherits(response, "delta_sharing_httr2_snapshot_response") ||
      !is.environment(response) ||
      isTRUE(response$closed)) {
    stop("The snapshot response stream is not available.", call. = FALSE)
  }
  body <- response$body
  if (is.raw(body)) {
    if (response$offset > length(body)) {
      return(NULL)
    }
    end <- min(
      length(body),
      response$offset + response$chunk_bytes - 1L
    )
    chunk <- body[seq.int(response$offset, end)]
    response$offset <- end + 1L
    return(chunk)
  }
  if (isTRUE(body$is_complete())) {
    return(NULL)
  }
  chunk <- body$read(response$chunk_bytes)
  if (!is.raw(chunk)) {
    stop("The HTTP transport returned an invalid body chunk.", call. = FALSE)
  }
  if (length(chunk) == 0L) {
    if (isTRUE(body$is_complete())) {
      return(NULL)
    }
    stop("The HTTP response body ended unexpectedly.", call. = FALSE)
  }
  chunk
}

.httr2_snapshot_close <- function(response) {
  if (!inherits(response, "delta_sharing_httr2_snapshot_response") ||
      !is.environment(response)) {
    stop("The snapshot response stream is invalid.", call. = FALSE)
  }
  if (!isTRUE(response$closed)) {
    if (is.environment(response$body)) {
      response$body$close()
    }
    response$closed <- TRUE
  }
  invisible(NULL)
}

.httr2_snapshot_transport <- function(
  timeout_seconds = 120,
  chunk_bytes = .snapshot_stream_chunk_bytes
) {
  if (!is.numeric(timeout_seconds) ||
      length(timeout_seconds) != 1L ||
      is.na(timeout_seconds) ||
      !is.finite(timeout_seconds) ||
      timeout_seconds <= 0) {
    stop("`timeout_seconds` must be one positive number.", call. = FALSE)
  }
  chunk_bytes <- .snapshot_positive_integer(chunk_bytes, "chunk_bytes")

  list(
    open = function(request) {
      .httr2_open_snapshot_request(
        request,
        timeout_seconds = timeout_seconds,
        chunk_bytes = chunk_bytes
      )
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    pull = .httr2_snapshot_pull,
    close = .httr2_snapshot_close,
    retry_after = function(response) {
      .http_header(response$headers, "retry-after")
    }
  )
}

.safe_snapshot_transport_close <- function(transport, response) {
  try(transport$close(response), silent = TRUE)
  invisible(NULL)
}

.snapshot_transport_status <- function(transport, response) {
  status <- tryCatch(
    transport$status(response),
    error = function(condition) NA_integer_
  )
  if (!is.numeric(status) ||
      length(status) != 1L ||
      is.na(status) ||
      !is.finite(status) ||
      status != floor(status) ||
      status < 100 ||
      status > 599) {
    .safe_snapshot_transport_close(transport, response)
    .snapshot_planning_abort(
      "The HTTP transport returned an invalid snapshot status."
    )
  }
  as.integer(status)
}

.snapshot_retry_after <- function(transport, response) {
  tryCatch(
    transport$retry_after(response),
    error = function(condition) NULL
  )
}

.open_snapshot_http_round <- function(request,
                                      transport,
                                      endpoint_host,
                                      max_attempts,
                                      sleeper,
                                      clock,
                                      random,
                                      return_unauthorized) {
  for (attempt in seq_len(max_attempts)) {
    outcome <- tryCatch(
      list(
        response = transport$open(request),
        error = NULL,
        typed = FALSE
      ),
      delta_sharing_error = function(condition) {
        list(response = NULL, error = condition, typed = TRUE)
      },
      error = function(condition) {
        list(response = NULL, error = condition, typed = FALSE)
      }
    )
    if (!is.null(outcome$error)) {
      if (isTRUE(outcome$typed)) {
        stop(outcome$error)
      }
      if (attempt < max_attempts) {
        sleeper(.retry_delay(
          attempt = attempt,
          now = clock(),
          random = random
        ))
        next
      }
      .snapshot_planning_abort(
        "The Delta Sharing snapshot request could not be completed.",
        type = "http",
        endpoint_host = endpoint_host,
        retry_count = attempt - 1L
      )
    }

    status <- .snapshot_transport_status(transport, outcome$response)
    if (status < 400L ||
        (return_unauthorized && identical(status, 401L))) {
      return(outcome$response)
    }
    if (.retryable_status(status) && attempt < max_attempts) {
      retry_after <- .snapshot_retry_after(transport, outcome$response)
      .safe_snapshot_transport_close(transport, outcome$response)
      sleeper(.retry_delay(
        attempt = attempt,
        retry_after = retry_after,
        now = clock(),
        random = random
      ))
      next
    }

    .safe_snapshot_transport_close(transport, outcome$response)
    .snapshot_planning_abort(
      "The Delta Sharing server rejected the snapshot request.",
      type = "http",
      status = status,
      endpoint_host = endpoint_host,
      retry_count = attempt - 1L
    )
  }
  stop("Unreachable snapshot retry state.", call. = FALSE)
}

.snapshot_is_oauth_auth_type <- function(auth_type) {
  if (exists(
    ".is_oauth_auth_type",
    mode = "function",
    inherits = TRUE
  )) {
    helper <- get(
      ".is_oauth_auth_type",
      mode = "function",
      inherits = TRUE
    )
    return(helper(auth_type))
  }
  .is_scalar_character(auth_type) &&
    auth_type %in% c(
      "oauth_client_credentials",
      "oauth_jwt_bearer_private_key_jwt"
    )
}

.perform_authenticated_snapshot_http <- function(
  client,
  plan,
  stream_transport = .httr2_snapshot_transport(),
  auth_transport = .httr2_http_transport(),
  clock = Sys.time,
  sleeper = Sys.sleep,
  random = stats::runif,
  max_attempts = 5L
) {
  stream_transport <- .normalize_snapshot_stream_transport(stream_transport)
  auth_transport <- .normalize_http_transport(auth_transport)
  max_attempts <- .snapshot_positive_integer(max_attempts, "max_attempts")
  if (!is.function(clock) ||
      !is.function(sleeper) ||
      !is.function(random)) {
    stop("Snapshot HTTP control hooks must be functions.", call. = FALSE)
  }

  request <- .new_snapshot_http_request(client, plan)
  endpoint_host <- .auth_endpoint_host(.client_context(client)$endpoint)
  authorization <- .client_authorization(
    client,
    transport = auth_transport,
    clock = clock,
    sleeper = sleeper,
    random = random,
    max_attempts = max_attempts
  )
  request <- .apply_http_authorization(request, authorization)

  response <- .open_snapshot_http_round(
    request = request,
    transport = stream_transport,
    endpoint_host = endpoint_host,
    max_attempts = max_attempts,
    sleeper = sleeper,
    clock = clock,
    random = random,
    return_unauthorized = TRUE
  )
  status <- .snapshot_transport_status(stream_transport, response)
  if (identical(status, 401L)) {
    .safe_snapshot_transport_close(stream_transport, response)
    can_refresh <- .snapshot_is_oauth_auth_type(
      authorization$auth_type
    ) &&
      !is.null(authorization$cache_generation) &&
      isTRUE(.invalidate_client_auth(
        client,
        authorization$cache_generation
      ))
    if (!can_refresh) {
      .abort_http_unauthorized(
        .snapshot_query_operation,
        endpoint_host
      )
    }
    authorization <- .client_authorization(
      client,
      transport = auth_transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts
    )
    request <- .apply_http_authorization(request, authorization)
    response <- .open_snapshot_http_round(
      request = request,
      transport = stream_transport,
      endpoint_host = endpoint_host,
      max_attempts = max_attempts,
      sleeper = sleeper,
      clock = clock,
      random = random,
      return_unauthorized = FALSE
    )
    status <- .snapshot_transport_status(stream_transport, response)
  }

  headers <- tryCatch(
    stream_transport$headers(response),
    error = function(condition) NULL
  )
  if (is.null(headers)) {
    .safe_snapshot_transport_close(stream_transport, response)
    .snapshot_planning_abort(
      "The HTTP transport returned invalid snapshot headers."
    )
  }
  closed <- FALSE
  .new_snapshot_pull_response(
    status = status,
    headers = headers,
    pull = function() {
      if (closed) {
        stop("The snapshot response has already been closed.", call. = FALSE)
      }
      stream_transport$pull(response)
    },
    close = function() {
      if (!closed) {
        stream_transport$close(response)
        closed <<- TRUE
      }
      invisible(NULL)
    }
  )
}

.new_snapshot_http_fetcher <- function(
  client,
  stream_transport = .httr2_snapshot_transport(),
  auth_transport = .httr2_http_transport(),
  clock = Sys.time,
  sleeper = Sys.sleep,
  random = stats::runif,
  max_attempts = 5L
) {
  force(client)
  force(stream_transport)
  force(auth_transport)
  force(clock)
  force(sleeper)
  force(random)
  force(max_attempts)
  function(plan) {
    .perform_authenticated_snapshot_http(
      client = client,
      plan = plan,
      stream_transport = stream_transport,
      auth_transport = auth_transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts
    )
  }
}

.prepare_snapshot_http_read <- function(
  read,
  stream_transport = .httr2_snapshot_transport(),
  auth_transport = .httr2_http_transport(),
  clock = Sys.time,
  sleeper = Sys.sleep,
  random = stats::runif,
  max_attempts = 5L,
  ...
) {
  read <- .validate_snapshot_read(read)
  fetch <- .new_snapshot_http_fetcher(
    client = read@table@client,
    stream_transport = stream_transport,
    auth_transport = auth_transport,
    clock = clock,
    sleeper = sleeper,
    random = random,
    max_attempts = max_attempts
  )
  .prepare_snapshot_read(
    read = read,
    fetch = fetch,
    clock = clock,
    ...
  )
}
