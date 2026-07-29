test_that("parses a version 2 bearer profile from a list", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "bearer_token",
    endpoint = "https://sharing.example.test/api",
    bearerToken = "tok"
  ))
  expect_equal(p$version, 2)
  expect_equal(p$endpoint, "https://sharing.example.test/api")
  expect_equal(p$auth_type, "bearer_token")
  expect_equal(p$credentials$bearer_token, "tok")
})

test_that("version 1 defaults to bearer auth", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 1,
    type = "basic",
    endpoint = "https://x.test/api",
    bearerToken = "t"
  ))
  expect_equal(p$auth_type, "bearer_token")
})

test_that("parses OAuth client-credentials profile", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "oauth_client_credentials",
    endpoint = "https://x.test/api",
    tokenEndpoint = "https://x.test/oauth/token",
    clientId = "cid",
    clientSecret = "sec",
    scope = "all"
  ))
  expect_equal(p$auth_type, "oauth_client_credentials")
  expect_equal(p$credentials$token_endpoint, "https://x.test/oauth/token")
})

test_that("profile parsing follows Python's structural validation level", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = "1",
    endpoint = "ftp://user:pw@x.test/api/",
    bearerToken = "",
    expirationTime = "not-a-timestamp"
  ))

  expect_equal(p$version, 1L)
  expect_equal(p$endpoint, "ftp://user:pw@x.test/api")
  expect_equal(p$credentials$bearer_token, "")
  expect_equal(p$expiration_time, "not-a-timestamp")
})

test_that("numeric profile versions are coerced like Python int", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 1.9,
    endpoint = "",
    bearerToken = "t"
  ))

  expect_equal(p$version, 1L)
  expect_equal(p$endpoint, "")

  expect_error(
    sharing_profile_parse(list(
      shareCredentialsVersion = "1.9",
      endpoint = "",
      bearerToken = "t"
    )),
    class = "delta_sharing_validation_error"
  )
})

test_that("private-key metadata is preserved without parser policy checks", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "oauth_jwt_bearer_private_key_jwt",
    endpoint = "https://x.test/api/",
    auth = list(
      tokenEndpoint = "https://x.test/oauth/token/",
      clientId = "cid",
      issuer = "issuer",
      audience = "audience",
      privateKey = list(
        privateKeyFile = "not-read-during-parsing.pem",
        keyId = "kid",
        algorithm = "ES256"
      )
    )
  ))

  expect_equal(p$endpoint, "https://x.test/api")
  expect_equal(p$credentials$token_endpoint, "https://x.test/oauth/token")
  expect_equal(p$credentials$algorithm, "ES256")
  expect_equal(p$credentials$private_key_file, "not-read-during-parsing.pem")
})

test_that("only structurally required profile keys are rejected", {
  expect_error(
    sharing_profile_parse(list(
      shareCredentialsVersion = 1,
      endpoint = "https://x.test/api"
    )),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_profile_parse(list(
      shareCredentialsVersion = 2,
      endpoint = "https://x.test/api",
      bearerToken = "t"
    )),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_profile_parse(list(
      shareCredentialsVersion = 1,
      endpoint = 123,
      bearerToken = "t"
    )),
    class = "delta_sharing_validation_error"
  )
})

test_that("rejects an unsupported newer profile version", {
  expect_error(
    sharing_profile_parse(list(
      shareCredentialsVersion = 3,
      endpoint = "https://x.test/api",
      bearerToken = "t"
    )),
    class = "delta_sharing_unsupported_error"
  )
})

test_that("parses inline JSON strings", {
  json <- '{"shareCredentialsVersion":1,"endpoint":"https://x.test/api","bearerToken":"t"}'
  p <- sharing_profile_parse(json)
  expect_equal(p$endpoint, "https://x.test/api")
})
