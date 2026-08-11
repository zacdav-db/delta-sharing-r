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

test_that("profile versions use base R integer coercion", {
  p <- sharing_profile_parse(list(
    shareCredentialsVersion = 1.9,
    endpoint = "",
    bearerToken = "t"
  ))

  expect_equal(p$version, 1L)
  expect_equal(p$endpoint, "")

  character_version <- sharing_profile_parse(list(
    shareCredentialsVersion = "1.9",
    endpoint = "",
    bearerToken = "t"
  ))
  expect_equal(character_version$version, 1L)
})

test_that("failed integer coercions warn before rejection", {
  purrr::walk(list("abc", Inf), function(version) {
    expect_warning(
      expect_error(
        sharing_profile_parse(list(
          shareCredentialsVersion = version,
          endpoint = "",
          bearerToken = "t"
        )),
        class = "delta_sharing_validation_error"
      ),
      "NAs introduced by coercion"
    )
  })
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

test_that("rejects unsupported profile versions", {
  expect_error(
    sharing_profile_parse(list(
      shareCredentialsVersion = 3,
      endpoint = "https://x.test/api",
      bearerToken = "t"
    )),
    class = "delta_sharing_validation_error"
  )
})

test_that("profile files use fs paths and preserve structural values", {
  path <- fs::file_temp(ext = ".share")
  withr::defer(fs::file_delete(path))
  jsonlite::write_json(
    list(
      shareCredentialsVersion = 2,
      type = "basic",
      endpoint = "https://x.test/api/",
      username = "user",
      password = "password"
    ),
    path,
    auto_unbox = TRUE
  )

  profile <- sharing_profile_parse(path)

  expect_equal(profile$endpoint, "https://x.test/api")
  expect_equal(profile$credentials$kind, "basic")
  expect_equal(profile$credentials$username, "user")
})

test_that("demo_profile returns the official public profile as a list", {
  httr2::local_mocked_responses(function(req) {
    expect_equal(
      req$url,
      paste0(
        "https://raw.githubusercontent.com/delta-io/delta-sharing/",
        "main/examples/open-datasets.share"
      )
    )
    httr2::response(
      200,
      headers = list(`content-type` = "text/plain"),
      body = charToRaw(paste0(
        '{"shareCredentialsVersion":1,',
        '"endpoint":"https://sharing.delta.io/delta-sharing/",',
        '"bearerToken":"public-token"}'
      ))
    )
  })

  profile <- demo_profile()

  expect_type(profile, "list")
  expect_equal(profile$shareCredentialsVersion, 1L)
  expect_equal(profile$endpoint, "https://sharing.delta.io/delta-sharing/")
})

test_that("profile sources must be readable JSON objects", {
  missing <- fs::file_temp(ext = ".share")
  invalid <- fs::file_temp(ext = ".share")
  withr::defer(fs::file_delete(invalid))
  writeLines("{not-json", invalid, useBytes = TRUE)

  expect_error(
    sharing_profile_parse(missing),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_profile_parse(invalid)
  )
  expect_error(
    sharing_profile_parse(42),
    class = "delta_sharing_validation_error"
  )
})

test_that("profile URLs reject null and non-scalar values", {
  purrr::walk(
    list(NULL, NA_character_, character(), c("https://a", "https://b"), 42),
    function(value) {
      expect_error(
        normalize_profile_url(value),
        class = "delta_sharing_validation_error"
      )
    }
  )
})

test_that("profile versions reject unsupported structural shapes", {
  base <- list(
    endpoint = "https://x.test/api",
    bearerToken = "token"
  )
  purrr::walk(
    list(NULL, character(), 0),
    function(version) {
      profile <- c(
        list(shareCredentialsVersion = version),
        base
      )
      expect_error(
        sharing_profile_parse(profile),
        class = "delta_sharing_error"
      )
    }
  )
})

test_that("private-key profiles require nested objects", {
  base <- list(
    shareCredentialsVersion = 2,
    type = "oauth_jwt_bearer_private_key_jwt",
    endpoint = "https://x.test/api"
  )

  expect_error(
    sharing_profile_parse(c(base, list(auth = "not-an-object"))),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    sharing_profile_parse(c(
      base,
      list(
        auth = list(
          tokenEndpoint = "https://x.test/token",
          clientId = "client",
          issuer = "issuer",
          audience = "audience",
          privateKey = "not-an-object"
        )
      )
    )),
    class = "delta_sharing_validation_error"
  )
})

test_that("version two rejects malformed or unknown authentication types", {
  base <- list(
    shareCredentialsVersion = 2,
    endpoint = "https://x.test/api"
  )
  purrr::walk(
    list(NULL, NA_character_, character(), 42, "unknown"),
    function(type) {
      profile <- c(base, list(type = type))
      expect_error(
        sharing_profile_parse(profile),
        class = "delta_sharing_error"
      )
    }
  )
})
