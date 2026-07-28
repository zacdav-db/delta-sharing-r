test_that("condition hierarchy is stable and metadata is safe", {
  condition <- delta.sharing:::.new_delta_sharing_condition(
    "Authentication failed.",
    type = "auth",
    operation = "profile_authenticate",
    endpoint_host = "sharing.example.test",
    bearer_token = "SECRET",
    signed_url = "https://secret.example.test"
  )

  expect_s3_class(condition, "delta_sharing_auth_error")
  expect_s3_class(condition, "delta_sharing_error")
  expect_s3_class(condition, "error")
  expect_identical(condition$operation, "profile_authenticate")
  expect_identical(condition$endpoint_host, "sharing.example.test")
  expect_null(condition$bearer_token)
  expect_null(condition$signed_url)
})

test_that("native error categories map to stable condition subclasses", {
  expected <- c(
    validation = "delta_sharing_validation_error",
    auth = "delta_sharing_auth_error",
    http = "delta_sharing_http_error",
    protocol = "delta_sharing_protocol_error",
    kernel = "delta_sharing_kernel_error",
    unsupported = "delta_sharing_unsupported_error",
    not_implemented = "delta_sharing_not_implemented_error",
    cancelled = "delta_sharing_cancelled",
    native = "delta_sharing_native_error",
    native_unavailable = "delta_sharing_native_unavailable_error"
  )

  for (type in names(expected)) {
    condition <- delta.sharing:::.new_delta_sharing_condition(
      "Safe message.",
      type = type
    )
    expect_s3_class(condition, unname(expected[[type]]))
    expect_s3_class(condition, "delta_sharing_error")
  }
})

test_that("validation errors inherit from the common error class", {
  condition <- expect_error(
    table_identifier("not-enough-parts"),
    class = "delta_sharing_validation_error"
  )

  expect_s3_class(condition, "delta_sharing_error")
  expect_s3_class(condition, "error")
  expect_identical(condition$operation, "table_identifier")
})

test_that("native unavailable errors are also unsupported errors", {
  condition <- delta.sharing:::.with_execution_interface(
    delta.sharing:::.new_execution_interface(list(
      list_shares = function(...) NULL
    )),
    expect_error(
      read_arrow_stream(sharing_read(test_table())),
      class = "delta_sharing_native_unavailable_error"
    )
  )

  expect_s3_class(condition, "delta_sharing_unsupported_error")
  expect_s3_class(condition, "delta_sharing_error")
  expect_identical(condition$operation, "read_arrow_stream")
})
