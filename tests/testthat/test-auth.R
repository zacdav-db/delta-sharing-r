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

test_that("OAuth request policies are attached lazily", {
  profile <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "oauth_client_credentials",
    endpoint = "https://x.test/api",
    tokenEndpoint = "https://x.test/oauth/token",
    clientId = "cid",
    clientSecret = "secret",
    scope = "read"
  ))
  context <- sharing_auth_context(profile)

  first <- context$authenticate(httr2::request(profile$endpoint))
  second <- context$authenticate(httr2::request(profile$endpoint))

  expect_s3_class(first, "httr2_request")
  expect_s3_class(second, "httr2_request")
  expect_s3_class(oauth_client_for(profile$credentials), "httr2_oauth_client")
})

test_that("private-key OAuth uses a signed JWT bearer grant", {
  key_path <- fs::file_temp(ext = ".pem")
  withr::defer(fs::file_delete(key_path))
  openssl::write_pem(openssl::rsa_keygen(), key_path)
  profile <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "oauth_jwt_bearer_private_key_jwt",
    endpoint = "https://x.test/api",
    auth = list(
      tokenEndpoint = "https://x.test/oauth/token",
      clientId = "cid",
      issuer = "issuer",
      audience = "audience",
      scope = "read",
      privateKey = list(
        privateKeyFile = key_path,
        keyId = "key-id"
      )
    )
  ))
  context <- sharing_auth_context(profile)

  request <- context$authenticate(httr2::request(profile$endpoint))
  policy <- request$policies$auth_sign
  flow <- policy$params$flow_params

  expect_s3_class(request, "httr2_request")
  expect_s3_class(load_private_key(key_path), "key")
  expect_identical(policy$params$flow, "oauth_flow_bearer_jwt")
  expect_equal(flow$claim$iss, "cid")
  expect_equal(flow$claim$aud, "issuer")
  expect_equal(flow$claim$scope, "read")
  expect_equal(flow$claim$resource, "audience")
  expect_equal(flow$signature_params$size, 256L)
  expect_equal(flow$signature_params$header$kid, "key-id")

  state <- new.env(parent = emptyenv())
  httr2::local_mocked_responses(function(req) {
    state$token_request <- req
    httr2::response(
      200,
      headers = list(`content-type` = "application/json"),
      body = charToRaw(
        '{"access_token":"token","token_type":"bearer","expires_in":3600}'
      )
    )
  })

  token <- httr2::oauth_flow_bearer_jwt(
    client = flow$client,
    claim = flow$claim,
    signature = flow$signature,
    signature_params = flow$signature_params,
    scope = flow$scope,
    token_params = flow$token_params
  )
  token_body <- state$token_request$body$data
  assertion <- jose::jwt_split(utils::URLdecode(token_body$assertion))

  expect_s3_class(token, "httr2_token")
  expect_equal(
    utils::URLdecode(token_body$grant_type),
    "urn:ietf:params:oauth:grant-type:jwt-bearer"
  )
  expect_setequal(names(token_body), c("grant_type", "assertion"))
  expect_equal(assertion$header$alg, "RS256")
  expect_equal(assertion$header$kid, "key-id")
  expect_equal(assertion$payload$iss, "cid")
  expect_equal(assertion$payload$aud, "issuer")
  expect_equal(assertion$payload$scope, "read")
  expect_equal(assertion$payload$resource, "audience")
})

test_that("unreadable private keys become typed authentication errors", {
  expect_error(
    load_private_key(fs::file_temp(ext = ".pem")),
    class = "delta_sharing_auth_error"
  )

  profile <- sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "oauth_jwt_bearer_private_key_jwt",
    endpoint = "https://x.test/api",
    auth = list(
      tokenEndpoint = "https://x.test/oauth/token",
      clientId = "cid",
      issuer = "issuer",
      audience = "audience",
      privateKey = list(privateKeyFile = fs::file_temp(ext = ".pem"))
    )
  ))
  context <- sharing_auth_context(profile)

  expect_error(
    context$authenticate(httr2::request(profile$endpoint)),
    class = "delta_sharing_auth_error"
  )
})

test_that("unknown internal OAuth and auth kinds fail closed", {
  expect_error(
    oauth_client_for(list(kind = "unknown")),
    "Unknown internal OAuth credential type",
    fixed = TRUE
  )
  expect_error(
    sharing_auth_context(list(credentials = list(kind = "unknown"))),
    class = "delta_sharing_auth_error"
  )
})
