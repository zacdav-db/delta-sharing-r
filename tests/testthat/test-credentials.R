test_that("profile credentials parse expiration and redact secrets", {
  credentials <- fixture_credentials()

  expect_s3_class(credentials, "DeltaShareCredentials")
  expect_s3_class(credentials$expirationTime, "POSIXlt")
  expect_identical(attr(credentials$expirationTime, "tzone"), "UTC")

  printed <- capture.output(print(credentials))
  expect_match(printed, "sharing[.]example[.]test")
  expect_false(any(grepl(
    credentials$bearerToken,
    printed,
    fixed = TRUE
  )))
})

test_that("expired profile credentials fail before use", {
  profile <- fixture_json("profiles", "bearer-v1.json")
  profile$expirationTime <- "2000-01-01T00:00:00"

  expect_error(
    delta.sharing:::process_credentials(profile),
    "Credentials are expired as of"
  )
})

test_that("profiles without an expiration remain valid", {
  profile <- fixture_json("profiles", "bearer-v1.json")
  profile$expirationTime <- NA_character_

  credentials <- delta.sharing:::process_credentials(profile)

  expect_s3_class(credentials, "DeltaShareCredentials")
  expect_true(is.na(credentials$expirationTime))
})
