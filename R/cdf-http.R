.new_cdf_http_request <- function(client, plan) {
  if (!inherits(plan, "delta_sharing_cdf_request") ||
      !is.list(plan) ||
      !identical(plan$operation, .cdf_query_operation)) {
    .cdf_planning_abort(
      "`plan` must be a CDF request plan.",
      type = "validation"
    )
  }
  context <- .client_context(client)
  method <- .normalize_http_method(plan$method)
  path <- .normalize_http_path(plan$path_segments)
  query <- .validate_named_http_fields(
    plan$query,
    "query",
    allow_vectors = TRUE
  )
  headers <- .normalize_http_headers(plan$headers)
  if (!identical(method, "GET")) {
    .cdf_planning_abort(
      "The CDF request plan is invalid.",
      type = "validation"
    )
  }
  endpoint <- sub("/+$", "", context$endpoint)
  structure(
    list(
      method = method,
      url = paste(c(endpoint, path), collapse = "/"),
      query = query,
      headers = headers,
      body_type = "none",
      body = NULL
    ),
    class = c("delta_sharing_cdf_http_request", "list")
  )
}

#' @exportS3Method print delta_sharing_cdf_http_request
print.delta_sharing_cdf_http_request <- function(x, ...) {
  cat("<delta_sharing_cdf_http_request> GET; query and headers redacted\n")
  invisible(x)
}

.cdf_transport_status <- function(transport, response) {
  tryCatch(
    .snapshot_transport_status(transport, response),
    delta_sharing_error = function(condition) .cdf_rethrow(condition)
  )
}

.open_cdf_http_round <- function(...) {
  tryCatch(
    .open_snapshot_http_round(...),
    delta_sharing_error = function(condition) .cdf_rethrow(condition)
  )
}

.perform_authenticated_cdf_http <- function(
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
    stop("CDF HTTP control hooks must be functions.", call. = FALSE)
  }

  request <- .new_cdf_http_request(client, plan)
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

  response <- .open_cdf_http_round(
    request = request,
    transport = stream_transport,
    endpoint_host = endpoint_host,
    max_attempts = max_attempts,
    sleeper = sleeper,
    clock = clock,
    random = random,
    return_unauthorized = TRUE
  )
  status <- .cdf_transport_status(stream_transport, response)
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
        .cdf_query_operation,
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
    response <- .open_cdf_http_round(
      request = request,
      transport = stream_transport,
      endpoint_host = endpoint_host,
      max_attempts = max_attempts,
      sleeper = sleeper,
      clock = clock,
      random = random,
      return_unauthorized = FALSE
    )
    status <- .cdf_transport_status(stream_transport, response)
  }

  headers <- tryCatch(
    stream_transport$headers(response),
    error = function(condition) NULL
  )
  if (is.null(headers)) {
    .safe_snapshot_transport_close(stream_transport, response)
    .cdf_planning_abort(
      "The HTTP transport returned invalid CDF headers."
    )
  }
  closed <- FALSE
  .new_snapshot_pull_response(
    status = status,
    headers = headers,
    pull = function() {
      if (closed) {
        stop("The CDF response has already been closed.", call. = FALSE)
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

.new_cdf_http_fetcher <- function(
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
    .perform_authenticated_cdf_http(
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

.prepare_cdf_http_read <- function(
  read,
  stream_transport = .httr2_snapshot_transport(),
  auth_transport = .httr2_http_transport(),
  clock = Sys.time,
  sleeper = Sys.sleep,
  random = stats::runif,
  max_attempts = 5L,
  ...
) {
  read <- .validate_cdf_read(read, executable = TRUE)
  fetch <- .new_cdf_http_fetcher(
    client = read@table@client,
    stream_transport = stream_transport,
    auth_transport = auth_transport,
    clock = clock,
    sleeper = sleeper,
    random = random,
    max_attempts = max_attempts
  )
  .prepare_cdf_read(
    read = read,
    fetch = fetch,
    clock = clock,
    ...
  )
}
