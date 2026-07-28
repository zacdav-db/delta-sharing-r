test_that("profile versions and supported authentication descriptors parse", {
  fixtures <- c(
    bearer_v1 = "bearer-v1.json",
    bearer_v2 = "bearer-v2.json",
    basic_v2 = "basic-v2.json",
    oauth_v2 = "oauth-client-v2.json",
    private_key_v2 = "private-key-v2.json"
  )
  expected_auth <- c(
    bearer_v1 = "bearer_token",
    bearer_v2 = "bearer_token",
    basic_v2 = "basic",
    oauth_v2 = "oauth_client_credentials",
    private_key_v2 = "oauth_jwt_bearer_private_key_jwt"
  )

  profiles <- lapply(fixtures, function(fixture) {
    sharing_profile(test_path("fixtures", "profiles", fixture))
  })

  expect_identical(
    vapply(profiles, function(profile) profile@auth_type, character(1)),
    expected_auth
  )
  expect_identical(profiles$bearer_v1@version, 1)
  expect_identical(profiles$bearer_v2@version, 2)
  expect_identical(
    profiles$private_key_v2@endpoint,
    "https://sharing.example.test/api"
  )
})

test_that("credentials and mutable client state are not S7 properties", {
  profile <- sharing_profile(test_profile())
  client <- sharing_client(profile)

  expect_setequal(
    S7::prop_names(profile),
    c(
      "source_type",
      "label",
      "version",
      "endpoint",
      "auth_type",
      "expiration_time"
    )
  )
  expect_identical(S7::prop_names(client), "profile")
  expect_false(any(
    c(
      "bearerToken",
      "clientSecret",
      "password",
      "privateKey",
      "context",
      "state"
    ) %in% c(S7::prop_names(profile), S7::prop_names(client))
  ))

  context <- delta.sharing:::.client_context(client)
  expect_true(is.environment(context))
  expect_identical(context$state, "configured")
  expect_identical(context$auth_type, "bearer_token")
  expect_identical(
    context$credentials$bearer_token,
    "test-only-bearer-token"
  )

  context$state <- "refreshing"
  expect_identical(
    delta.sharing:::.client_context(client)$state,
    "refreshing"
  )
  expect_identical(S7::prop_names(client), "profile")
})

test_that("profile structures fail early with typed safe conditions", {
  invalid_profiles <- list(
    list(endpoint = "https://sharing.example.test", bearerToken = "secret"),
    list(
      shareCredentialsVersion = 1,
      endpoint = "not-a-url",
      bearerToken = "secret"
    ),
    list(
      shareCredentialsVersion = 1,
      endpoint = "https://user:secret@sharing.example.test",
      bearerToken = "secret"
    ),
    list(
      shareCredentialsVersion = 1,
      endpoint = "https://sharing.example.test",
      bearerToken = ""
    ),
    list(
      shareCredentialsVersion = 2,
      endpoint = "https://sharing.example.test",
      type = "oauth_client_credentials",
      tokenEndpoint = "https://identity.example.test/token",
      clientId = "client",
      clientSecret = ""
    )
  )

  for (profile in invalid_profiles) {
    condition <- expect_error(
      sharing_profile(profile),
      class = "delta_sharing_validation_error"
    )
    rendered <- paste(c(
      conditionMessage(condition),
      capture.output(str(condition))
    ), collapse = "\n")
    expect_false(grepl("user:secret", rendered, fixed = TRUE))
    expect_false(grepl("bearerToken = \"secret\"", rendered, fixed = TRUE))
  }
})

test_that("newer versions and unsupported auth are actionable", {
  newer <- test_profile()
  newer$shareCredentialsVersion <- 99
  condition <- expect_error(
    sharing_profile(newer),
    "upgrade delta.sharing",
    class = "delta_sharing_unsupported_error"
  )
  expect_identical(condition$feature, "profile version")

  unknown <- test_profile()
  unknown$shareCredentialsVersion <- 2
  unknown$type <- "future_auth_with_secret-in-name"
  condition <- expect_error(
    sharing_profile(unknown),
    class = "delta_sharing_unsupported_error"
  )
  expect_identical(condition$feature, "profile authentication type")
  expect_false(grepl(
    unknown$type,
    conditionMessage(condition),
    fixed = TRUE
  ))
})

test_that("expiration and private-key descriptors are validated", {
  invalid_expiration <- test_profile()
  invalid_expiration$expirationTime <- "tomorrow"
  expect_error(
    sharing_profile(invalid_expiration),
    "RFC 3339",
    class = "delta_sharing_validation_error"
  )

  private_key <- jsonlite::fromJSON(
    test_path("fixtures", "profiles", "private-key-v2.json"),
    simplifyVector = FALSE
  )
  private_key$auth$privateKey$algorithm <- "HS256"
  expect_error(
    sharing_profile(private_key),
    "not supported",
    class = "delta_sharing_unsupported_error"
  )

  private_key$auth$privateKey$algorithm <- "RS384"
  expect_error(
    sharing_profile(private_key),
    "not supported",
    class = "delta_sharing_unsupported_error"
  )

  private_key$auth$privateKey$algorithm <- NULL
  profile <- sharing_profile(private_key)
  credentials <- delta.sharing:::.profile_credentials(profile)
  expect_identical(credentials$algorithm, "RS256")
})

test_that("connection sources use bounded binary reads", {
  profile_json <- jsonlite::toJSON(test_profile(), auto_unbox = TRUE)
  binary <- rawConnection(charToRaw(profile_json), open = "rb")
  on.exit(close(binary), add = TRUE)

  expect_identical(sharing_profile(binary)@auth_type, "bearer_token")

  text <- textConnection(profile_json, open = "r")
  on.exit(close(text), add = TRUE)
  expect_error(
    sharing_profile(text),
    "bounded binary reads",
    class = "delta_sharing_validation_error"
  )

  oversized_path <- tempfile("oversized-profile-")
  on.exit(unlink(oversized_path), add = TRUE)
  writeBin(
    charToRaw(paste(rep("x", 1024 * 1024 + 1), collapse = "")),
    oversized_path
  )
  oversized <- file(oversized_path, open = "rb")
  on.exit(close(oversized), add = TRUE)
  expect_error(
    sharing_profile(oversized),
    "1 MiB",
    class = "delta_sharing_validation_error"
  )
})

test_that("malformed, missing, and oversized sources are rejected safely", {
  secret <- "MALFORMED-SECRET"
  malformed <- paste0('{"bearerToken":"', secret, '"')
  condition <- expect_error(
    sharing_profile(malformed),
    class = "delta_sharing_validation_error"
  )
  expect_false(grepl(
    secret,
    conditionMessage(condition),
    fixed = TRUE
  ))

  expect_error(
    sharing_profile(test_path("fixtures", "profiles", "missing.json")),
    "could not be read",
    class = "delta_sharing_validation_error"
  )

  oversized <- charToRaw(paste(rep("x", 1024 * 1024 + 1), collapse = ""))
  expect_error(
    sharing_profile(oversized),
    "1 MiB",
    class = "delta_sharing_validation_error"
  )
})
