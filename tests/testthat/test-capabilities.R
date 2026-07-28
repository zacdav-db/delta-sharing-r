test_that("snapshot capabilities are deterministic and Kernel-gated", {
  expect_identical(
    delta.sharing:::.snapshot_capability_header(),
    paste0(
      "responseformat=delta,parquet;",
      "readerfeatures=columnmapping,timestampntz"
    )
  )
  expect_identical(
    delta.sharing:::.snapshot_capability_header("delta"),
    paste0(
      "responseformat=delta;",
      "readerfeatures=columnmapping,timestampntz"
    )
  )
  expect_identical(
    delta.sharing:::.snapshot_capability_header("parquet"),
    "responseformat=parquet"
  )
  expect_error(
    delta.sharing:::.snapshot_capability_header("unsupported"),
    class = "delta_sharing_validation_error"
  )
})

test_that("table versions are parsed case-insensitively", {
  expect_identical(
    delta.sharing:::.parse_table_version_header(
      c("Delta-Table-Version" = "42")
    ),
    42
  )
  expect_identical(
    delta.sharing:::.parse_table_version_header(
      list("DELTA-TABLE-VERSION" = as.character(2^53))
    ),
    2^53
  )
})

test_that("missing and invalid table versions fail closed", {
  invalid <- list(
    NULL,
    character(),
    c(other = "42"),
    c("delta-table-version" = "-1"),
    c("delta-table-version" = "1.5"),
    c("delta-table-version" = "not-a-version"),
    c("delta-table-version" = as.character(2^53 + 2)),
    c(
      "delta-table-version" = "1",
      "Delta-Table-Version" = "2"
    )
  )

  for (headers in invalid) {
    condition <- expect_error(
      delta.sharing:::.parse_table_version_header(headers),
      class = "delta_sharing_protocol_error"
    )
    expect_identical(condition$operation, "table_version")
  }
})

test_that("protocol timestamps use UTC milliseconds", {
  timestamp <- as.POSIXct(
    "2026-07-29 12:34:56.125",
    tz = "Australia/Sydney"
  )

  expect_identical(
    delta.sharing:::.format_protocol_timestamp(timestamp),
    "2026-07-29T02:34:56.125Z"
  )
  expect_identical(
    delta.sharing:::.format_protocol_timestamp(
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    ),
    "2026-07-29T00:00:00.000Z"
  )
})
