private_key_profile <- function(private_key_file = "/test-only/private-key.pem") {
  profile <- jsonlite::fromJSON(
    test_path("fixtures", "profiles", "private-key-v2.json"),
    simplifyVector = FALSE
  )
  profile$auth$privateKey$privateKeyFile <- private_key_file
  profile
}

decode_base64url <- function(value) {
  padding <- (4L - nchar(value) %% 4L) %% 4L
  encoded <- paste0(
    chartr("-_", "+/", value),
    strrep("=", padding)
  )
  jsonlite::base64_dec(encoded)
}

decode_jwt_object <- function(value) {
  jsonlite::fromJSON(
    rawToChar(decode_base64url(value)),
    simplifyVector = FALSE,
    simplifyDataFrame = FALSE,
    simplifyMatrix = FALSE
  )
}

fixed_assertion_random <- function(n) {
  stopifnot(identical(n, 32L))
  as.raw(0:31)
}

fixed_assertion_signer <- function(signing_input, private_key_file) {
  stopifnot(is.raw(signing_input))
  stopifnot(
    is.character(private_key_file),
    length(private_key_file) == 1L,
    !is.na(private_key_file),
    nzchar(private_key_file)
  )
  as.raw(255:224)
}

jwt_test_auth_transport <- function(send,
                                    body = function(response) response$body) {
  list(
    send = send,
    status = function(response) response$status,
    body = body,
    retry_after = function(response) response$retry_after
  )
}

test_that("private-key JWT header and claims are deterministic and bounded", {
  profile <- sharing_profile(private_key_profile())
  credentials <- delta.sharing:::.profile_credentials(profile)
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")

  assertion <- delta.sharing:::.private_key_jwt_assertion(
    credentials = credentials,
    issued_at = now,
    random_bytes = fixed_assertion_random,
    signer = fixed_assertion_signer
  )
  repeated <- delta.sharing:::.private_key_jwt_assertion(
    credentials = credentials,
    issued_at = now,
    random_bytes = fixed_assertion_random,
    signer = fixed_assertion_signer
  )
  parts <- strsplit(assertion, ".", fixed = TRUE)[[1L]]
  header <- decode_jwt_object(parts[[1L]])
  claims <- decode_jwt_object(parts[[2L]])

  expect_identical(assertion, repeated)
  expect_length(parts, 3L)
  expect_identical(
    header,
    list(alg = "RS256", typ = "JWT", kid = "fixture-key")
  )
  expect_identical(claims$iss, "https://identity.example.test/")
  expect_identical(claims$sub, "fixture-client")
  expect_identical(claims$aud, "delta-sharing")
  expect_identical(
    claims$iat,
    as.integer(floor(as.double(now)) - 30)
  )
  expect_identical(
    claims$exp,
    as.integer(floor(as.double(now)) + 300)
  )
  expect_identical(
    claims$jti,
    delta.sharing:::.base64url_encode(as.raw(0:31))
  )
  expect_identical(
    decode_base64url(parts[[3L]]),
    as.raw(255:224)
  )
})

test_that("private-key JWT supports an omitted key id", {
  source <- private_key_profile()
  source$auth$privateKey$keyId <- NULL
  credentials <- delta.sharing:::.profile_credentials(
    sharing_profile(source)
  )
  assertion <- delta.sharing:::.private_key_jwt_assertion(
    credentials,
    issued_at = as.POSIXct("2026-07-29", tz = "UTC"),
    random_bytes = fixed_assertion_random,
    signer = fixed_assertion_signer
  )
  header <- decode_jwt_object(
    strsplit(assertion, ".", fixed = TRUE)[[1L]][[1L]]
  )

  expect_identical(header, list(alg = "RS256", typ = "JWT"))
  expect_null(credentials$key_id)
})

test_that("assertion lifetime, skew, random, and signer outputs are bounded", {
  credentials <- delta.sharing:::.profile_credentials(
    sharing_profile(private_key_profile())
  )
  now <- as.POSIXct("2026-07-29", tz = "UTC")

  expect_error(
    delta.sharing:::.private_key_jwt_assertion(
      credentials,
      now,
      fixed_assertion_random,
      fixed_assertion_signer,
      lifetime_seconds = 601L
    ),
    "`lifetime_seconds` must be one whole number",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.private_key_jwt_assertion(
      credentials,
      now,
      fixed_assertion_random,
      fixed_assertion_signer,
      clock_skew_seconds = 61L
    ),
    "`clock_skew_seconds` must be one whole number",
    fixed = TRUE
  )

  secret <- "ASSERTION-INTERNAL-SECRET"
  conditions <- list(
    expect_error(
      delta.sharing:::.private_key_jwt_assertion(
        credentials,
        now,
        random_bytes = function(n) stop(secret),
        signer = fixed_assertion_signer
      ),
      class = "delta_sharing_auth_error"
    ),
    expect_error(
      delta.sharing:::.private_key_jwt_assertion(
        credentials,
        now,
        random_bytes = function(n) raw(1L),
        signer = fixed_assertion_signer
      ),
      class = "delta_sharing_auth_error"
    ),
    expect_error(
      delta.sharing:::.private_key_jwt_assertion(
        credentials,
        now,
        random_bytes = fixed_assertion_random,
        signer = function(signing_input, private_key_file) stop(secret)
      ),
      class = "delta_sharing_auth_error"
    ),
    expect_error(
      delta.sharing:::.private_key_jwt_assertion(
        credentials,
        now,
        random_bytes = fixed_assertion_random,
        signer = function(signing_input, private_key_file) "not-raw"
      ),
      class = "delta_sharing_auth_error"
    )
  )
  rendered <- paste(vapply(conditions, function(condition) {
    paste(
      conditionMessage(condition),
      capture.output(str(condition)),
      collapse = "\n"
    )
  }, character(1)), collapse = "\n")
  expect_false(grepl(secret, rendered, fixed = TRUE))
  expect_false(grepl(
    credentials$private_key_file,
    rendered,
    fixed = TRUE
  ))
})

test_that("private-key OAuth request exchanges and caches a bearer token", {
  recorder <- new.env(parent = emptyenv())
  recorder$requests <- list()
  recorder$signatures <- 0L
  transport <- jwt_test_auth_transport(function(request) {
    recorder$requests <- c(recorder$requests, list(request))
    list(
      status = 200L,
      retry_after = NULL,
      body = list(
        access_token = "JWT-OAUTH-TOKEN",
        token_type = "Bearer",
        expires_in = 3600
      )
    )
  })
  signer <- function(signing_input, private_key_file) {
    recorder$signatures <- recorder$signatures + 1L
    fixed_assertion_signer(signing_input, private_key_file)
  }
  now <- as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
  client <- sharing_client(private_key_profile())

  authorization <- delta.sharing:::.client_authorization(
    client,
    transport = transport,
    clock = function() now,
    assertion_random = fixed_assertion_random,
    assertion_signer = signer
  )
  cached <- delta.sharing:::.client_authorization(
    client,
    clock = function() now + 1,
    assertion_random = function(n) stop("cache should not create a JWT"),
    assertion_signer = function(...) stop("cache should not sign")
  )
  request <- recorder$requests[[1L]]

  expect_identical(request$method, "POST")
  expect_identical(
    request$url,
    "https://identity.example.test/oauth/token"
  )
  expect_false("Authorization" %in% names(request$headers))
  expect_identical(request$body$grant_type, "client_credentials")
  expect_identical(request$body$client_id, "fixture-client")
  expect_identical(
    request$body$client_assertion_type,
    delta.sharing:::.jwt_assertion_type
  )
  expect_match(
    request$body$client_assertion,
    "^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$"
  )
  expect_identical(request$body$scope, "delta-sharing.read")
  expect_identical(
    unname(authorization$headers[["Authorization"]]),
    "Bearer JWT-OAUTH-TOKEN"
  )
  expect_identical(
    authorization$auth_type,
    "oauth_jwt_bearer_private_key_jwt"
  )
  expect_identical(authorization$cache_generation, 1)
  expect_identical(cached$headers, authorization$headers)
  expect_identical(recorder$signatures, 1L)
  expect_length(recorder$requests, 1L)

  context <- delta.sharing:::.client_context(client)
  expect_false(any(c(
    "assertion",
    "jwt",
    "private_key"
  ) %in% names(context)))
})

test_that("real ephemeral RSA key signs a verifiable RS256 assertion", {
  key <- openssl::rsa_keygen(2048)
  key_path <- tempfile("delta-sharing-rsa-", fileext = ".pem")
  on.exit(unlink(key_path), add = TRUE)
  openssl::write_pem(key, key_path)
  recorder <- new.env(parent = emptyenv())
  recorder$request <- NULL
  client <- sharing_client(private_key_profile(key_path))
  credentials <- delta.sharing:::.profile_credentials(client@profile)
  parsed_key <- delta.sharing:::.read_openssl_rsa_key(key_path)
  authorization <- delta.sharing:::.client_authorization(
    client,
    transport = jwt_test_auth_transport(function(request) {
      recorder$request <- request
      list(
        status = 200L,
        retry_after = NULL,
        body = list(
          access_token = "REAL-RSA-TOKEN",
          token_type = "Bearer",
          expires_in = 3600
        )
      )
    }),
    clock = function() as.POSIXct("2026-07-29", tz = "UTC"),
    assertion_random = fixed_assertion_random
  )
  assertion <- recorder$request$body$client_assertion
  parts <- strsplit(assertion, ".", fixed = TRUE)[[1L]]
  signing_input <- charToRaw(paste(parts[1:2], collapse = "."))
  signature <- decode_base64url(parts[[3L]])
  public_key <- as.list(key)$pubkey

  expect_identical(
    unname(authorization$headers[["Authorization"]]),
    "Bearer REAL-RSA-TOKEN"
  )
  expect_identical(
    recorder$request$body$client_assertion_type,
    delta.sharing:::.jwt_assertion_type
  )
  expect_match(
    assertion,
    "^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$"
  )
  expect_s3_class(parsed_key, "rsa")
  expect_identical(length(signature), 256L)
  expect_true(openssl::signature_verify(
    signing_input,
    signature,
    hash = openssl::sha256,
    pubkey = public_key
  ))
  expect_error(
    openssl::signature_verify(
      charToRaw("different-input"),
      signature,
      hash = openssl::sha256,
      pubkey = public_key
    ),
    "Verification failed"
  )
})

test_that("private key parse failures do not disclose path or contents", {
  secret <- "PRIVATE-KEY-PARSE-SECRET"
  invalid_path <- tempfile(paste0(secret, "-"), fileext = ".pem")
  writeLines(paste("not a private key", secret), invalid_path)
  on.exit(unlink(invalid_path), add = TRUE)
  oversized_path <- tempfile("oversized-key-", fileext = ".pem")
  writeBin(raw(delta.sharing:::.private_key_max_bytes + 1L), oversized_path)
  on.exit(unlink(oversized_path), add = TRUE)

  conditions <- lapply(
    c(invalid_path, oversized_path, paste0(invalid_path, "-missing")),
    function(path) {
      credentials <- delta.sharing:::.profile_credentials(
        sharing_profile(private_key_profile(path))
      )
      expect_error(
        delta.sharing:::.private_key_jwt_assertion(
          credentials,
          issued_at = as.POSIXct("2026-07-29", tz = "UTC"),
          random_bytes = fixed_assertion_random
        ),
        class = "delta_sharing_auth_error"
      )
    }
  )
  rendered <- paste(vapply(conditions, function(condition) {
    paste(
      conditionMessage(condition),
      capture.output(str(condition)),
      collapse = "\n"
    )
  }, character(1)), collapse = "\n")

  expect_false(grepl(secret, rendered, fixed = TRUE))
  expect_false(grepl(invalid_path, rendered, fixed = TRUE))
  expect_false(grepl("not a private key", rendered, fixed = TRUE))
})

test_that("encrypted and non-RSA private keys fail closed without prompting", {
  rsa_key <- openssl::rsa_keygen(2048)
  encrypted_path <- tempfile("encrypted-key-", fileext = ".pem")
  on.exit(unlink(encrypted_path), add = TRUE)
  openssl::write_pem(
    rsa_key,
    encrypted_path,
    password = "ENCRYPTED-KEY-PASSWORD"
  )
  ec_path <- tempfile("ec-key-", fileext = ".pem")
  on.exit(unlink(ec_path), add = TRUE)
  openssl::write_pem(openssl::ec_keygen(), ec_path)

  started <- Sys.time()
  conditions <- lapply(c(encrypted_path, ec_path), function(path) {
    credentials <- delta.sharing:::.profile_credentials(
      sharing_profile(private_key_profile(path))
    )
    expect_error(
      delta.sharing:::.private_key_jwt_assertion(
        credentials,
        issued_at = as.POSIXct("2026-07-29", tz = "UTC"),
        random_bytes = fixed_assertion_random
      ),
      class = "delta_sharing_auth_error"
    )
  })
  expect_lt(as.double(difftime(Sys.time(), started, units = "secs")), 10)

  rendered <- paste(vapply(conditions, function(condition) {
    paste(
      conditionMessage(condition),
      capture.output(str(condition)),
      collapse = "\n"
    )
  }, character(1)), collapse = "\n")
  expect_false(grepl(
    "ENCRYPTED-KEY-PASSWORD",
    rendered,
    fixed = TRUE
  ))
  expect_false(grepl(encrypted_path, rendered, fixed = TRUE))
  expect_false(grepl(ec_path, rendered, fixed = TRUE))
})

test_that("JWT token exchange errors redact assertion, jti, key, and body", {
  path_secret <- "/private/path/JWT-ERROR-KEY.pem"
  response_secret <- "JWT-TOKEN-RESPONSE-SECRET"
  recorder <- new.env(parent = emptyenv())
  recorder$assertion <- NULL
  transport <- jwt_test_auth_transport(function(request) {
    recorder$assertion <- request$body$client_assertion
    list(
      status = 400L,
      retry_after = NULL,
      body = paste0(
        "{\"error_description\":\"",
        response_secret,
        "\"}"
      )
    )
  })
  condition <- expect_error(
    delta.sharing:::.client_authorization(
      sharing_client(private_key_profile(path_secret)),
      transport = transport,
      clock = function() as.POSIXct(
        "2026-07-29 00:00:00",
        tz = "UTC"
      ),
      assertion_random = fixed_assertion_random,
      assertion_signer = fixed_assertion_signer,
      max_attempts = 1L
    ),
    class = "delta_sharing_http_error"
  )
  claims <- decode_jwt_object(
    strsplit(recorder$assertion, ".", fixed = TRUE)[[1L]][[2L]]
  )
  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )

  expect_identical(condition$operation, "oauth_private_key_jwt")
  expect_false(grepl(recorder$assertion, rendered, fixed = TRUE))
  expect_false(grepl(claims$jti, rendered, fixed = TRUE))
  expect_false(grepl(path_secret, rendered, fixed = TRUE))
  expect_false(grepl(response_secret, rendered, fixed = TRUE))
})

test_that("private-key JWT uses one generation-matched 401 refresh replay", {
  recorder <- new.env(parent = emptyenv())
  recorder$token_requests <- 0L
  recorder$sharing_requests <- 0L
  recorder$assertions <- character()
  recorder$authorizations <- character()
  random_calls <- 0L

  transport <- delta.sharing:::.fake_http_transport(function(request) {
    if (identical(
      request$url,
      "https://identity.example.test/oauth/token"
    )) {
      recorder$token_requests <- recorder$token_requests + 1L
      recorder$assertions <- c(
        recorder$assertions,
        request$body$client_assertion
      )
      return(list(
        status = 200L,
        body = list(
          access_token = paste0("JWT-TOKEN-", recorder$token_requests),
          token_type = "Bearer",
          expires_in = 3600
        )
      ))
    }

    recorder$sharing_requests <- recorder$sharing_requests + 1L
    recorder$authorizations <- c(
      recorder$authorizations,
      request$headers[["Authorization"]]
    )
    if (recorder$sharing_requests == 1L) {
      return(list(status = 401L, body = "discarded"))
    }
    list(
      status = 200L,
      body = list(items = list())
    )
  })
  assertion_random <- function(n) {
    random_calls <<- random_calls + 1L
    as.raw(rep(random_calls, n))
  }
  assertion_signer <- function(signing_input, private_key_file) {
    openssl::sha256(signing_input)
  }
  client <- sharing_client(private_key_profile())

  response <- delta.sharing:::.perform_authenticated_http(
    client = client,
    method = "GET",
    path = "shares",
    operation = "list_shares",
    response_kind = "discovery",
    replayable = TRUE,
    transport = transport,
    clock = function() as.POSIXct(
      "2026-07-29 00:00:00",
      tz = "UTC"
    ),
    sleeper = function(delay) NULL,
    max_attempts = 1L,
    assertion_random = assertion_random,
    assertion_signer = assertion_signer
  )

  expect_identical(response$status, 200L)
  expect_identical(recorder$token_requests, 2L)
  expect_identical(recorder$sharing_requests, 2L)
  expect_identical(
    recorder$authorizations,
    c("Bearer JWT-TOKEN-1", "Bearer JWT-TOKEN-2")
  )
  expect_length(unique(recorder$assertions), 2L)
  expect_identical(
    delta.sharing:::.client_context(client)$access_token_generation,
    2
  )
})

test_that("private-key JWT never replays a sharing request more than once", {
  recorder <- new.env(parent = emptyenv())
  recorder$token_requests <- 0L
  recorder$sharing_requests <- 0L
  recorder$assertions <- character()
  recorder$tokens <- character()
  response_secret <- "SECOND-401-RESPONSE-SECRET"
  random_calls <- 0L

  transport <- delta.sharing:::.fake_http_transport(function(request) {
    if (identical(
      request$url,
      "https://identity.example.test/oauth/token"
    )) {
      recorder$token_requests <- recorder$token_requests + 1L
      recorder$assertions <- c(
        recorder$assertions,
        request$body$client_assertion
      )
      token <- paste0("JWT-REPLAY-TOKEN-", recorder$token_requests)
      recorder$tokens <- c(recorder$tokens, token)
      return(list(
        status = 200L,
        body = list(
          access_token = token,
          token_type = "Bearer",
          expires_in = 3600
        )
      ))
    }
    recorder$sharing_requests <- recorder$sharing_requests + 1L
    list(
      status = 401L,
      body = paste(response_secret, request$headers[["Authorization"]])
    )
  })
  assertion_random <- function(n) {
    random_calls <<- random_calls + 1L
    as.raw(rep(random_calls, n))
  }
  condition <- expect_error(
    delta.sharing:::.perform_authenticated_http(
      client = sharing_client(private_key_profile()),
      method = "GET",
      path = "shares",
      operation = "list_shares",
      response_kind = "discovery",
      replayable = TRUE,
      transport = transport,
      clock = function() as.POSIXct(
        "2026-07-29 00:00:00",
        tz = "UTC"
      ),
      sleeper = function(delay) NULL,
      max_attempts = 1L,
      assertion_random = assertion_random,
      assertion_signer = function(signing_input, private_key_file) {
        openssl::sha256(signing_input)
      }
    ),
    class = "delta_sharing_http_error"
  )
  jtis <- vapply(recorder$assertions, function(assertion) {
    decode_jwt_object(
      strsplit(assertion, ".", fixed = TRUE)[[1L]][[2L]]
    )$jti
  }, character(1))
  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )

  expect_identical(recorder$token_requests, 2L)
  expect_identical(recorder$sharing_requests, 2L)
  expect_false(any(vapply(
    c(recorder$assertions, jtis, recorder$tokens, response_secret),
    grepl,
    logical(1),
    x = rendered,
    fixed = TRUE
  )))
})

test_that("JWT assertions and key paths stay out of public rendering", {
  path_secret <- "/private/path/ASSERTION-KEY-SECRET.pem"
  assertion_secret <- "ASSERTION-MATERIAL-SECRET"
  profile <- sharing_profile(private_key_profile(path_secret))
  client <- sharing_client(profile)
  rendered <- paste(
    capture.output(print(profile)),
    capture.output(print(client)),
    capture.output(str(profile)),
    capture.output(str(client)),
    collapse = "\n"
  )
  condition <- expect_error(
    delta.sharing:::.client_authorization(
      client,
      assertion_random = fixed_assertion_random,
      assertion_signer = function(signing_input, private_key_file) {
        stop(assertion_secret)
      }
    ),
    class = "delta_sharing_auth_error"
  )
  rendered <- paste(
    rendered,
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )

  expect_false(grepl(path_secret, rendered, fixed = TRUE))
  expect_false(grepl(assertion_secret, rendered, fixed = TRUE))
  expect_false(grepl("fixture-key", rendered, fixed = TRUE))
})
