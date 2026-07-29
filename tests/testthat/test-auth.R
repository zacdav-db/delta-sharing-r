test_that("bearer auth applies an Authorization header", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "bearer_token",
    endpoint = "https://x.test/api",
    bearerToken = "tok"
  ))
  ctx <- sharing_auth_context(p)
  expect_equal(ctx$kind, "bearer_token")
  req <- ctx$authenticate(httr2::request("https://x.test/api"))
  dr <- httr2::req_dry_run(req, quiet = TRUE, redact_headers = FALSE)
  expect_equal(dr$headers$authorization, "Bearer tok")
})

test_that("bearer expiration metadata does not block requests eagerly", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 1,
    endpoint = "https://x.test/api",
    bearerToken = "t",
    expirationTime = "2000-01-01T00:00:00Z"
  ))
  ctx <- sharing_auth_context(p)
  req <- ctx$authenticate(httr2::request("https://x.test/api"))
  dr <- httr2::req_dry_run(req, quiet = TRUE, redact_headers = FALSE)

  expect_equal(dr$headers$authorization, "Bearer t")
})

test_that("basic auth applies a base64 Authorization header", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "basic",
    endpoint = "https://x.test/api",
    username = "u",
    password = "pw"
  ))
  ctx <- sharing_auth_context(p)
  req <- ctx$authenticate(httr2::request("https://x.test/api"))
  dr <- httr2::req_dry_run(req, quiet = TRUE, redact_headers = FALSE)
  expect_equal(
    dr$headers$authorization,
    paste0("Basic ", openssl::base64_encode("u:pw"))
  )
})

test_that("oauth client-credentials builds a context without network I/O", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "oauth_client_credentials",
    endpoint = "https://x.test/api",
    tokenEndpoint = "https://x.test/oauth/token",
    clientId = "cid",
    clientSecret = "sec"
  ))
  ctx <- sharing_auth_context(p)
  expect_equal(ctx$kind, "oauth_client_credentials")
  expect_type(ctx$authenticate, "closure")
})

test_that("private keys are not read while constructing an auth context", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "oauth_jwt_bearer_private_key_jwt",
    endpoint = "https://x.test/api",
    auth = list(
      tokenEndpoint = "https://x.test/oauth/token",
      clientId = "cid",
      issuer = "issuer",
      audience = "audience",
      privateKey = list(privateKeyFile = "does-not-exist.pem")
    )
  ))

  expect_no_error(ctx <- sharing_auth_context(p))
  expect_equal(ctx$kind, "oauth_jwt_bearer_private_key_jwt")
})
