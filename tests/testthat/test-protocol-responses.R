test_that("newline-delimited protocol responses are parsed in order", {
  body <- readBin(
    fixture_path("protocol", "table-metadata.ndjson"),
    what = "raw",
    n = file.info(fixture_path(
      "protocol",
      "table-metadata.ndjson"
    ))$size
  )
  response <- httr2::response(
    url = "https://sharing.example.test/api/table/metadata",
    body = body
  )

  parsed <- delta.sharing:::clean_xndjson(response)

  expect_length(parsed, 2L)
  expect_equal(parsed[[1]]$protocol$minReaderVersion, 1L)
  expect_identical(parsed[[2]]$metaData$id, "table-id")
})

test_that("malformed protocol responses fail closed", {
  response <- httr2::response(
    url = "https://sharing.example.test/api/table/metadata",
    body = charToRaw("{\"protocol\":\n")
  )

  expect_error(delta.sharing:::clean_xndjson(response))
})
