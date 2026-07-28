.oauth_response_max_bytes <- 1024L * 1024L
.oauth_refresh_cap_seconds <- 600
.oauth_auth_types <- c(
  "oauth_client_credentials",
  "oauth_jwt_bearer_private_key_jwt"
)
.jwt_assertion_type <- paste0(
  "urn:ietf:params:oauth:client-assertion-type:",
  "jwt-bearer"
)
.jwt_assertion_lifetime_seconds <- 300L
.jwt_assertion_clock_skew_seconds <- 30L
.jwt_max_lifetime_seconds <- 600L
.jwt_max_clock_skew_seconds <- 60L
.jwt_jti_bytes <- 32L
.jwt_max_signing_input_bytes <- 16L * 1024L
.jwt_max_signature_bytes <- 1024L
.private_key_max_bytes <- 64L * 1024L

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

.is_oauth_auth_type <- function(auth_type) {
  .is_scalar_character(auth_type) && auth_type %in% .oauth_auth_types
}

.jwt_abort <- function() {
  .auth_abort(
    "The private-key JWT assertion could not be created.",
    operation = "oauth_private_key_jwt"
  )
}

.private_key_abort <- function() {
  .auth_abort(
    "The configured private key could not be used.",
    operation = "oauth_private_key_jwt"
  )
}

.base64url_encode <- function(value) {
  if (!is.raw(value)) {
    stop("`value` must be a raw vector.", call. = FALSE)
  }
  encoded <- gsub(
    "[\r\n]",
    "",
    jsonlite::base64_enc(value),
    perl = TRUE
  )
  encoded <- sub("=+$", "", chartr("+/", "-_", encoded))
  if (!.is_scalar_character(encoded) ||
      !grepl("^[A-Za-z0-9_-]*$", encoded)) {
    stop("Base64url encoding failed.", call. = FALSE)
  }
  encoded
}

.jwt_json_segment <- function(value) {
  encoded <- tryCatch(
    jsonlite::toJSON(
      value,
      auto_unbox = TRUE,
      null = "null",
      na = "null",
      digits = NA,
      pretty = FALSE
    ),
    error = function(error) NULL
  )
  if (!.is_scalar_character(encoded)) {
    .jwt_abort()
  }
  .base64url_encode(charToRaw(enc2utf8(encoded)))
}

.normalize_jwt_bound <- function(value, name, minimum, maximum) {
  if (!is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < minimum ||
      value > maximum ||
      value != floor(value)) {
    stop(
      sprintf(
        "`%s` must be one whole number from %d through %d.",
        name,
        minimum,
        maximum
      ),
      call. = FALSE
    )
  }
  as.integer(value)
}

.jwt_random_bytes <- function(n) {
  openssl::rand_bytes(n)
}

.jwt_identifier <- function(random_bytes) {
  if (!is.function(random_bytes)) {
    stop("`random_bytes` must be a function.", call. = FALSE)
  }
  value <- tryCatch(
    random_bytes(.jwt_jti_bytes),
    error = function(error) NULL
  )
  if (!is.raw(value) || length(value) != .jwt_jti_bytes) {
    .jwt_abort()
  }
  .base64url_encode(value)
}

.read_private_key_bytes <- function(path) {
  if (!.is_scalar_character(path) ||
      grepl("[[:cntrl:]]", path)) {
    .private_key_abort()
  }
  info <- suppressWarnings(file.info(path))
  if (nrow(info) != 1L ||
      is.na(info$size) ||
      isTRUE(info$isdir) ||
      info$size < 1 ||
      info$size > .private_key_max_bytes) {
    .private_key_abort()
  }

  connection <- suppressWarnings(tryCatch(
    file(path, open = "rb"),
    error = function(error) NULL
  ))
  if (is.null(connection)) {
    .private_key_abort()
  }
  on.exit(close(connection), add = TRUE)
  bytes <- tryCatch(
    readBin(
      connection,
      what = "raw",
      n = .private_key_max_bytes + 1L
    ),
    error = function(error) NULL
  )
  if (!is.raw(bytes) ||
      length(bytes) < 1L ||
      length(bytes) > .private_key_max_bytes) {
    .private_key_abort()
  }
  bytes
}

.read_openssl_rsa_key <- function(path) {
  bytes <- .read_private_key_bytes(path)
  key <- suppressWarnings(tryCatch(
    openssl::read_key(bytes, password = NULL, der = FALSE),
    error = function(error) {
      tryCatch(
        openssl::read_key(bytes, password = NULL, der = TRUE),
        error = function(error) NULL
      )
    }
  ))
  if (is.null(key) || !inherits(key, "rsa")) {
    .private_key_abort()
  }
  key
}

.openssl_rs256_sign <- function(signing_input, private_key_file) {
  if (!is.raw(signing_input) ||
      length(signing_input) < 1L ||
      length(signing_input) > .jwt_max_signing_input_bytes) {
    .jwt_abort()
  }
  key <- .read_openssl_rsa_key(private_key_file)
  signature <- tryCatch(
    openssl::signature_create(
      data = signing_input,
      hash = openssl::sha256,
      key = key,
      password = NULL
    ),
    error = function(error) NULL
  )
  if (!is.raw(signature) ||
      length(signature) < 1L ||
      length(signature) > .jwt_max_signature_bytes) {
    .private_key_abort()
  }
  signature
}

.private_key_jwt_assertion <- function(
  credentials,
  issued_at,
  random_bytes = .jwt_random_bytes,
  signer = .openssl_rs256_sign,
  lifetime_seconds = .jwt_assertion_lifetime_seconds,
  clock_skew_seconds = .jwt_assertion_clock_skew_seconds
) {
  if (!is.list(credentials) ||
      !identical(
        credentials$kind,
        "oauth_jwt_bearer_private_key_jwt"
      ) ||
      !identical(credentials$algorithm, "RS256") ||
      !is.function(signer)) {
    .jwt_abort()
  }
  lifetime_seconds <- .normalize_jwt_bound(
    lifetime_seconds,
    "lifetime_seconds",
    1L,
    .jwt_max_lifetime_seconds
  )
  clock_skew_seconds <- .normalize_jwt_bound(
    clock_skew_seconds,
    "clock_skew_seconds",
    0L,
    .jwt_max_clock_skew_seconds
  )
  now <- .auth_now(function() issued_at)
  now_seconds <- floor(as.double(now))
  if (!is.finite(now_seconds) ||
      now_seconds < clock_skew_seconds ||
      now_seconds + lifetime_seconds > 2^53) {
    .jwt_abort()
  }

  header <- list(alg = "RS256", typ = "JWT")
  if (!is.null(credentials$key_id)) {
    header$kid <- credentials$key_id
  }
  claims <- list(
    iss = credentials$issuer,
    sub = credentials$client_id,
    aud = credentials$audience,
    iat = now_seconds - clock_skew_seconds,
    exp = now_seconds + lifetime_seconds,
    jti = .jwt_identifier(random_bytes)
  )
  signing_text <- paste(
    .jwt_json_segment(header),
    .jwt_json_segment(claims),
    sep = "."
  )
  signing_input <- charToRaw(signing_text)
  if (length(signing_input) > .jwt_max_signing_input_bytes) {
    .jwt_abort()
  }
  signature <- tryCatch(
    signer(signing_input, credentials$private_key_file),
    error = function(error) {
      if (inherits(error, "delta_sharing_error")) {
        stop(error)
      }
      NULL
    }
  )
  if (is.null(signature) ||
      !is.raw(signature) ||
      length(signature) < 1L ||
      length(signature) > .jwt_max_signature_bytes) {
    .jwt_abort()
  }
  paste(signing_text, .base64url_encode(signature), sep = ".")
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

.oauth_token_request <- function(credentials, assertion = NULL) {
  body <- list(grant_type = "client_credentials")
  headers <- c(
    Accept = "application/json",
    `Content-Type` = "application/x-www-form-urlencoded"
  )
  if (identical(credentials$kind, "oauth_client_credentials")) {
    if (!is.null(assertion)) {
      stop("An assertion does not apply to client-secret OAuth.", call. = FALSE)
    }
    headers <- c(
      Authorization = .basic_authorization(
        credentials$client_id,
        credentials$client_secret,
        operation = "oauth_client_credentials"
      ),
      headers
    )
  } else if (identical(
    credentials$kind,
    "oauth_jwt_bearer_private_key_jwt"
  )) {
    if (!.is_scalar_character(assertion) ||
        !grepl(
          "^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$",
          assertion
        )) {
      .jwt_abort()
    }
    body$client_id <- credentials$client_id
    body$client_assertion_type <- .jwt_assertion_type
    body$client_assertion <- assertion
  } else {
    stop("Unknown internal OAuth credential type.", call. = FALSE)
  }
  if (!is.null(credentials$scope)) {
    body$scope <- credentials$scope
  }

  list(
    method = "POST",
    url = credentials$token_endpoint,
    query = list(),
    headers = headers,
    body_type = "form",
    body = body,
    max_response_bytes = .oauth_response_max_bytes
  )
}

.auth_endpoint_host <- function(endpoint) {
  without_scheme <- sub("^https?://", "", endpoint, ignore.case = TRUE)
  strsplit(without_scheme, "/", fixed = TRUE)[[1L]][[1L]]
}

.oauth_response_abort <- function(operation = "oauth_client_credentials") {
  .auth_abort(
    "The OAuth token endpoint returned an invalid response.",
    operation = operation
  )
}

.parse_oauth_response_body <- function(
  body,
  operation = "oauth_client_credentials"
) {
  if (is.raw(body)) {
    if (length(body) > .oauth_response_max_bytes) {
      .oauth_response_abort(operation)
    }
    body <- tryCatch(rawToChar(body), error = function(error) NULL)
  }

  if (is.character(body)) {
    if (!.is_scalar_character(body) ||
        length(charToRaw(enc2utf8(body))) > .oauth_response_max_bytes) {
      .oauth_response_abort(operation)
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
    .oauth_response_abort(operation)
  }
  body
}

.validate_oauth_token_response <- function(
  body,
  operation = "oauth_client_credentials"
) {
  body <- .parse_oauth_response_body(body, operation)
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
    .oauth_response_abort(operation)
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

.cache_oauth_token <- function(context,
                               token,
                               issued_at,
                               operation = "oauth_client_credentials") {
  expires_at <- issued_at + token$expires_in
  refresh_threshold <- min(
    .oauth_refresh_cap_seconds,
    token$expires_in / 2
  )
  refresh_at <- expires_at - refresh_threshold
  if (!is.finite(as.double(expires_at)) ||
      !is.finite(as.double(refresh_at))) {
    .oauth_response_abort(operation)
  }

  context$access_token <- token$access_token
  context$access_token_issued_at <- issued_at
  context$access_token_expires_at <- expires_at
  context$access_token_refresh_at <- refresh_at
  context$access_token_generation <- context$access_token_generation + 1
  context$state <- "ready"
  invisible(context)
}

.oauth_authorization_result <- function(context, operation, auth_type) {
  list(
    headers = c(
      Authorization = .bearer_authorization(
        context$access_token,
        operation = operation
      )
    ),
    cache_generation = context$access_token_generation,
    auth_type = auth_type
  )
}

.cached_oauth_authorization <- function(context,
                                        now,
                                        operation,
                                        auth_type) {
  if (.oauth_cache_valid(context, now)) {
    return(.oauth_authorization_result(
      context,
      operation,
      auth_type
    ))
  }
  NULL
}

.exchange_oauth_token <- function(context,
                                  request,
                                  transport,
                                  clock,
                                  sleeper,
                                  random,
                                  max_attempts,
                                  operation,
                                  auth_type) {
  transport <- .normalize_auth_transport(transport)
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
    operation = operation,
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
    .oauth_response_abort(operation)
  }
  token <- .validate_oauth_token_response(body$value, operation)
  .cache_oauth_token(
    context,
    token,
    .auth_now(clock),
    operation = operation
  )

  .oauth_authorization_result(
    context,
    operation,
    auth_type
  )
}

.oauth_client_authorization <- function(context,
                                        transport,
                                        clock,
                                        sleeper,
                                        random,
                                        max_attempts) {
  operation <- "oauth_client_credentials"
  now <- .auth_now(clock)
  cached <- .cached_oauth_authorization(
    context,
    now,
    operation,
    context$auth_type
  )
  if (!is.null(cached)) {
    return(cached)
  }

  .exchange_oauth_token(
    context = context,
    request = .oauth_token_request(context$credentials),
    transport = transport,
    clock = clock,
    sleeper = sleeper,
    random = random,
    max_attempts = max_attempts,
    operation = operation,
    auth_type = context$auth_type
  )
}

.oauth_private_key_authorization <- function(
  context,
  transport,
  clock,
  sleeper,
  random,
  max_attempts,
  assertion_random,
  assertion_signer,
  assertion_lifetime_seconds,
  assertion_clock_skew_seconds
) {
  operation <- "oauth_private_key_jwt"
  now <- .auth_now(clock)
  cached <- .cached_oauth_authorization(
    context,
    now,
    operation,
    context$auth_type
  )
  if (!is.null(cached)) {
    return(cached)
  }

  assertion <- .private_key_jwt_assertion(
    credentials = context$credentials,
    issued_at = now,
    random_bytes = assertion_random,
    signer = assertion_signer,
    lifetime_seconds = assertion_lifetime_seconds,
    clock_skew_seconds = assertion_clock_skew_seconds
  )
  request <- .oauth_token_request(
    context$credentials,
    assertion = assertion
  )
  .exchange_oauth_token(
    context = context,
    request = request,
    transport = transport,
    clock = clock,
    sleeper = sleeper,
    random = random,
    max_attempts = max_attempts,
    operation = operation,
    auth_type = context$auth_type
  )
}

.client_authorization <- function(client,
                                  transport = NULL,
                                  clock = Sys.time,
                                  sleeper = Sys.sleep,
                                  random = stats::runif,
                                  max_attempts = 5L,
                                  assertion_random = .jwt_random_bytes,
                                  assertion_signer = .openssl_rs256_sign,
                                  assertion_lifetime_seconds =
                                    .jwt_assertion_lifetime_seconds,
                                  assertion_clock_skew_seconds =
                                    .jwt_assertion_clock_skew_seconds) {
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
    oauth_jwt_bearer_private_key_jwt = .oauth_private_key_authorization(
      context = context,
      transport = transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts,
      assertion_random = assertion_random,
      assertion_signer = assertion_signer,
      assertion_lifetime_seconds = assertion_lifetime_seconds,
      assertion_clock_skew_seconds = assertion_clock_skew_seconds
    ),
    stop("Unknown internal authentication type.", call. = FALSE)
  )
}

.invalidate_client_auth <- function(client, cache_generation) {
  context <- .client_context(client)
  if (!.is_oauth_auth_type(context$auth_type)) {
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
