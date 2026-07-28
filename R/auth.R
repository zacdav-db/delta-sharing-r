.oauth_response_max_bytes <- 1024L * 1024L
.oauth_refresh_cap_seconds <- 600

.auth_abort <- function(message,
                        operation = "authenticate",
                        type = "auth",
                        ...) {
  .abort_delta_sharing(
    message,
    type = type,
    operation = operation,
    ...
  )
}

.auth_now <- function(clock) {
  if (!is.function(clock)) {
    stop("`clock` must be a function.", call. = FALSE)
  }
  now <- clock()
  if (!inherits(now, "POSIXct") ||
      length(now) != 1L ||
      is.na(now) ||
      !is.finite(as.double(now))) {
    stop("`clock` must return one non-missing POSIXct value.", call. = FALSE)
  }
  structure(as.double(now), class = c("POSIXct", "POSIXt"), tzone = "UTC")
}

.bearer_authorization <- function(token, operation = "authenticate") {
  if (!.is_scalar_character(token) ||
      !grepl("^[A-Za-z0-9._~+/-]+=*$", token)) {
    .auth_abort(
      "The configured bearer credential cannot be used.",
      operation = operation
    )
  }
  paste("Bearer", token)
}

.basic_authorization <- function(username,
                                 password,
                                 operation = "authenticate") {
  if (!.is_scalar_character(username) ||
      !.is_scalar_character(password) ||
      grepl(":", username, fixed = TRUE)) {
    .auth_abort(
      "The configured Basic credential cannot be used.",
      operation = operation
    )
  }

  encoded <- tryCatch(
    jsonlite::base64_enc(charToRaw(enc2utf8(paste0(username, ":", password)))),
    error = function(error) NULL
  )
  if (!.is_scalar_character(encoded)) {
    .auth_abort(
      "The configured Basic credential cannot be used.",
      operation = operation
    )
  }
  paste("Basic", encoded)
}

.bearer_profile_authorization <- function(context, clock) {
  expiration <- context$credentials$expiration_time
  if (!is.null(expiration) && expiration <= .auth_now(clock)) {
    .auth_abort(
      paste0(
        "The configured bearer credential has expired; ",
        "obtain a new profile."
      ),
      operation = "authenticate"
    )
  }

  list(
    headers = c(
      Authorization = .bearer_authorization(
        context$credentials$bearer_token
      )
    ),
    cache_generation = NULL,
    auth_type = "bearer_token"
  )
}

.basic_profile_authorization <- function(context) {
  list(
    headers = c(
      Authorization = .basic_authorization(
        context$credentials$username,
        context$credentials$password
      )
    ),
    cache_generation = NULL,
    auth_type = "basic"
  )
}

.normalize_auth_transport <- function(transport) {
  send <- if (is.list(transport)) {
    transport[["send", exact = TRUE]]
  } else {
    NULL
  }
  status <- if (is.list(transport)) {
    transport[["status", exact = TRUE]]
  } else {
    NULL
  }
  body <- if (is.list(transport)) {
    transport[["body", exact = TRUE]]
  } else {
    NULL
  }
  retry_after <- if (is.list(transport)) {
    transport[["retry_after", exact = TRUE]]
  } else {
    NULL
  }
  if (!is.list(transport) ||
      is.null(names(transport)) ||
      !is.function(send) ||
      !is.function(status) ||
      !is.function(body) ||
      (!is.null(retry_after) && !is.function(retry_after))) {
    stop(
      paste0(
        "`transport` must provide `send`, `status`, and `body` functions, ",
        "plus an optional `retry_after` function."
      ),
      call. = FALSE
    )
  }
  if (is.null(retry_after)) {
    retry_after <- function(response) NULL
  }
  list(
    send = send,
    status = status,
    body = body,
    retry_after = retry_after
  )
}

.oauth_token_request <- function(credentials) {
  body <- list(grant_type = "client_credentials")
  if (!is.null(credentials$scope)) {
    body$scope <- credentials$scope
  }

  list(
    method = "POST",
    url = credentials$token_endpoint,
    query = list(),
    headers = c(
      Authorization = .basic_authorization(
        credentials$client_id,
        credentials$client_secret,
        operation = "oauth_client_credentials"
      ),
      Accept = "application/json",
      `Content-Type` = "application/x-www-form-urlencoded"
    ),
    body_type = "form",
    body = body,
    max_response_bytes = .oauth_response_max_bytes
  )
}

.auth_endpoint_host <- function(endpoint) {
  without_scheme <- sub("^https?://", "", endpoint, ignore.case = TRUE)
  strsplit(without_scheme, "/", fixed = TRUE)[[1L]][[1L]]
}

.oauth_response_abort <- function() {
  .auth_abort(
    "The OAuth token endpoint returned an invalid response.",
    operation = "oauth_client_credentials"
  )
}

.parse_oauth_response_body <- function(body) {
  if (is.raw(body)) {
    if (length(body) > .oauth_response_max_bytes) {
      .oauth_response_abort()
    }
    body <- tryCatch(rawToChar(body), error = function(error) NULL)
  }

  if (is.character(body)) {
    if (!.is_scalar_character(body) ||
        length(charToRaw(enc2utf8(body))) > .oauth_response_max_bytes) {
      .oauth_response_abort()
    }
    body <- tryCatch(
      jsonlite::fromJSON(
        body,
        simplifyVector = FALSE,
        simplifyDataFrame = FALSE,
        simplifyMatrix = FALSE
      ),
      error = function(error) NULL
    )
  }

  if (!is.list(body) ||
      is.null(names(body)) ||
      anyNA(names(body)) ||
      any(!nzchar(names(body))) ||
      anyDuplicated(names(body))) {
    .oauth_response_abort()
  }
  body
}

.validate_oauth_token_response <- function(body) {
  body <- .parse_oauth_response_body(body)
  access_token <- body[["access_token"]]
  token_type <- body[["token_type"]]
  expires_in <- body[["expires_in"]]

  token_type_valid <- is.null(token_type) ||
    (.is_scalar_character(token_type) &&
      identical(tolower(trimws(token_type)), "bearer"))
  if (.is_scalar_character(expires_in)) {
    expires_in <- suppressWarnings(as.numeric(trimws(expires_in)))
  }

  if (!.is_scalar_character(access_token) ||
      !grepl("^[A-Za-z0-9._~+/-]+=*$", access_token) ||
      !token_type_valid ||
      !is.numeric(expires_in) ||
      length(expires_in) != 1L ||
      is.na(expires_in) ||
      !is.finite(expires_in) ||
      expires_in <= 0 ||
      expires_in > 2^53) {
    .oauth_response_abort()
  }

  list(
    access_token = access_token,
    expires_in = as.double(expires_in)
  )
}

.oauth_cache_valid <- function(context, now) {
  .is_scalar_character(context$access_token) &&
    inherits(context$access_token_expires_at, "POSIXct") &&
    length(context$access_token_expires_at) == 1L &&
    !is.na(context$access_token_expires_at) &&
    inherits(context$access_token_refresh_at, "POSIXct") &&
    length(context$access_token_refresh_at) == 1L &&
    !is.na(context$access_token_refresh_at) &&
    now < context$access_token_refresh_at &&
    now < context$access_token_expires_at
}

.cache_oauth_token <- function(context, token, issued_at) {
  expires_at <- issued_at + token$expires_in
  refresh_threshold <- min(
    .oauth_refresh_cap_seconds,
    token$expires_in / 2
  )
  refresh_at <- expires_at - refresh_threshold
  if (!is.finite(as.double(expires_at)) ||
      !is.finite(as.double(refresh_at))) {
    .oauth_response_abort()
  }

  context$access_token <- token$access_token
  context$access_token_issued_at <- issued_at
  context$access_token_expires_at <- expires_at
  context$access_token_refresh_at <- refresh_at
  context$access_token_generation <- context$access_token_generation + 1
  context$state <- "ready"
  invisible(context)
}

.oauth_client_authorization <- function(context,
                                        transport,
                                        clock,
                                        sleeper,
                                        random,
                                        max_attempts) {
  now <- .auth_now(clock)
  if (.oauth_cache_valid(context, now)) {
    return(list(
      headers = c(
        Authorization = .bearer_authorization(
          context$access_token,
          operation = "oauth_client_credentials"
        )
      ),
      cache_generation = context$access_token_generation,
      auth_type = "oauth_client_credentials"
    ))
  }

  transport <- .normalize_auth_transport(transport)
  request <- .oauth_token_request(context$credentials)
  previous_state <- context$state
  context$state <- "refreshing"
  on.exit({
    if (identical(context$state, "refreshing")) {
      context$state <- previous_state
    }
  }, add = TRUE)

  response <- .perform_with_retry(
    request = request,
    send = transport$send,
    status_of = function(response) {
      tryCatch(
        transport$status(response),
        error = function(error) NA_integer_
      )
    },
    retry_after_of = function(response) {
      tryCatch(
        transport$retry_after(response),
        error = function(error) NULL
      )
    },
    operation = "oauth_client_credentials",
    endpoint_host = .auth_endpoint_host(context$credentials$token_endpoint),
    replayable = TRUE,
    max_attempts = max_attempts,
    sleeper = sleeper,
    clock = clock,
    random = random
  )
  body <- tryCatch(
    list(value = transport$body(response), error = FALSE),
    error = function(error) list(value = NULL, error = TRUE)
  )
  if (body$error) {
    .oauth_response_abort()
  }
  token <- .validate_oauth_token_response(body$value)
  .cache_oauth_token(context, token, .auth_now(clock))

  list(
    headers = c(
      Authorization = .bearer_authorization(
        context$access_token,
        operation = "oauth_client_credentials"
      )
    ),
    cache_generation = context$access_token_generation,
    auth_type = "oauth_client_credentials"
  )
}

.client_authorization <- function(client,
                                  transport = NULL,
                                  clock = Sys.time,
                                  sleeper = Sys.sleep,
                                  random = stats::runif,
                                  max_attempts = 5L) {
  context <- .client_context(client)
  switch(
    context$auth_type,
    bearer_token = .bearer_profile_authorization(context, clock),
    basic = .basic_profile_authorization(context),
    oauth_client_credentials = .oauth_client_authorization(
      context = context,
      transport = transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts
    ),
    oauth_jwt_bearer_private_key_jwt = .auth_abort(
      "Private-key JWT authentication is not available.",
      operation = "authenticate",
      type = "unsupported",
      feature = "private-key JWT authentication"
    ),
    stop("Unknown internal authentication type.", call. = FALSE)
  )
}

.invalidate_client_auth <- function(client, cache_generation) {
  context <- .client_context(client)
  if (!identical(context$auth_type, "oauth_client_credentials")) {
    return(invisible(FALSE))
  }
  if (!is.numeric(cache_generation) ||
      length(cache_generation) != 1L ||
      is.na(cache_generation) ||
      !is.finite(cache_generation) ||
      cache_generation < 1 ||
      cache_generation != floor(cache_generation)) {
    stop("`cache_generation` must be one positive whole number.", call. = FALSE)
  }
  if (is.null(context$access_token) ||
      !identical(
        as.double(cache_generation),
        as.double(context$access_token_generation)
      )) {
    return(invisible(FALSE))
  }

  context$access_token <- NULL
  context$access_token_issued_at <- NULL
  context$access_token_expires_at <- NULL
  context$access_token_refresh_at <- NULL
  context$state <- "configured"
  invisible(TRUE)
}
