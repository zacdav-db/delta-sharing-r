metadata_fixture_raw <- function(name) {
  readBin(
    test_path("fixtures", "protocol", name),
    what = "raw",
    n = file.info(test_path("fixtures", "protocol", name))$size
  )
}

expect_protocol_error_safe <- function(code, secrets = character()) {
  condition <- expect_error(
    code,
    class = "delta_sharing_protocol_error"
  )
  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )
  for (secret in secrets) {
    expect_false(grepl(secret, rendered, fixed = TRUE))
  }
  condition
}

test_that("metadata NDJSON is incremental at every byte boundary", {
  bytes <- metadata_fixture_raw("table-metadata-parquet.ndjson")

  for (split in 0:length(bytes)) {
    left <- if (split == 0L) raw() else bytes[seq_len(split)]
    right <- if (split == length(bytes)) {
      raw()
    } else {
      bytes[seq.int(split + 1L, length(bytes))]
    }
    result <- delta.sharing:::.parse_table_metadata_ndjson(list(left, right))

    expect_identical(result$response_format, "parquet")
    expect_identical(result$protocol$min_reader_version, 1)
    expect_identical(result$metadata$id, "table-id")
  }
})

test_that("decoder handles CRLF, blank lines, and a final line without newline", {
  document <- paste0(
    " \t\r\n",
    "{\"protocol\":{\"minReaderVersion\":1}}\r\n",
    "\r\n",
    paste0(
      "{\"metaData\":{\"id\":\"table-id\",\"format\":",
      "{\"provider\":\"parquet\"},\"schemaString\":\"{}\",",
      "\"partitionColumns\":[]}}"
    )
  )
  chunks <- strsplit(document, "", fixed = TRUE)[[1]]
  result <- delta.sharing:::.parse_table_metadata_ndjson(chunks)

  expect_identical(result$response_format, "parquet")
  expect_identical(result$metadata$partition_columns, character())
  expect_identical(result$metadata$format$options, setNames(character(), character()))
})

test_that("decoder enforces the line limit before growing its buffer", {
  decoder <- delta.sharing:::.new_ndjson_decoder(
    "test bounded decoding",
    max_line_bytes = 8
  )
  expect_length(
    delta.sharing:::.ndjson_decoder_push(decoder, charToRaw("12345678")),
    0L
  )
  expect_length(decoder$buffer, 8L)

  condition <- expect_error(
    delta.sharing:::.ndjson_decoder_push(decoder, charToRaw("9")),
    class = "delta_sharing_protocol_error"
  )
  expect_match(conditionMessage(condition), "line 1")
  expect_length(decoder$buffer, 8L)
})

test_that("decoder accepts only JSON objects and unique action fields", {
  for (document in c("[]\n", "\"value\"\n", "null\n", "true\n")) {
    expect_error(
      delta.sharing:::.ndjson_decoder_push(
        delta.sharing:::.new_ndjson_decoder("test object"),
        document
      ),
      class = "delta_sharing_protocol_error"
    )
  }

  duplicate <- paste0(
    "{\"protocol\":{\"minReaderVersion\":1},",
    "\"protocol\":{\"minReaderVersion\":2}}\n"
  )
  expect_error(
    delta.sharing:::.ndjson_decoder_push(
      delta.sharing:::.new_ndjson_decoder("test duplicate"),
      duplicate
    ),
    "duplicate object fields",
    class = "delta_sharing_protocol_error"
  )
})

test_that("one object cannot contain multiple recognized actions", {
  document <- paste0(
    "{\"protocol\":{\"minReaderVersion\":1},",
    "\"metaData\":{\"id\":\"table-id\"}}\n"
  )
  expect_error(
    delta.sharing:::.ndjson_decoder_push(
      delta.sharing:::.new_ndjson_decoder("test actions"),
      document
    ),
    "multiple recognized actions",
    class = "delta_sharing_protocol_error"
  )
})

test_that("metadata responses reject duplicate and missing required actions", {
  protocol <- "{\"protocol\":{\"minReaderVersion\":1}}\n"
  metadata <- paste0(
    "{\"metaData\":{\"id\":\"table-id\",\"format\":",
    "{\"provider\":\"parquet\"},\"schemaString\":\"{}\",",
    "\"partitionColumns\":[]}}\n"
  )

  expect_error(
    delta.sharing:::.parse_table_metadata_ndjson(protocol),
    "missing `metaData`",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.parse_table_metadata_ndjson(metadata),
    "missing `protocol`",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.parse_table_metadata_ndjson(
      c(protocol, protocol, metadata)
    ),
    "unexpected or duplicate",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.parse_table_metadata_ndjson(
      c(protocol, metadata, metadata)
    ),
    "unexpected or duplicate",
    class = "delta_sharing_protocol_error"
  )
})

test_that("metadata responses reject opaque file and unknown actions safely", {
  protocol <- "{\"protocol\":{\"minReaderVersion\":1}}\n"
  metadata <- paste0(
    "{\"metaData\":{\"id\":\"table-id\",\"format\":",
    "{\"provider\":\"parquet\"},\"schemaString\":\"{}\",",
    "\"partitionColumns\":[]}}\n"
  )
  secret <- "must-not-leak"
  file <- paste0(
    "{\"file\":{\"url\":\"https://bucket/path?sig=",
    secret,
    "\"}}\n"
  )
  unknown <- paste0("{\"futureAction\":{\"credential\":\"", secret, "\"}}\n")

  expect_protocol_error_safe(
    delta.sharing:::.parse_table_metadata_ndjson(
      c(protocol, file, metadata)
    ),
    secret
  )
  expect_protocol_error_safe(
    delta.sharing:::.parse_table_metadata_ndjson(
      c(protocol, unknown, metadata)
    ),
    secret
  )
})

test_that("streaming error messages are replaced by a generic safe failure", {
  secret <- "Bearer must-not-leak https://bucket/path?sig=must-not-leak"
  document <- paste0("{\"errorMessage\":\"", secret, "\"}\n")

  condition <- expect_protocol_error_safe(
    delta.sharing:::.ndjson_decoder_push(
      delta.sharing:::.new_ndjson_decoder("query table"),
      document
    ),
    c(secret, "bucket/path", "Bearer")
  )
  expect_match(conditionMessage(condition), "streaming error")
  expect_identical(condition$operation, "query table")
})

test_that("protocol and metadata actions normalize Delta envelopes", {
  result <- delta.sharing:::.parse_table_metadata_ndjson(
    metadata_fixture_raw("table-metadata-delta.ndjson")
  )

  expect_identical(result$response_format, "delta")
  expect_identical(result$protocol$min_reader_version, 3)
  expect_identical(result$protocol$min_writer_version, 7)
  expect_identical(
    result$protocol$reader_features,
    c("deletionVectors", "columnMapping")
  )
  expect_identical(result$metadata$version, 42)
  expect_identical(result$metadata$size, 2048)
  expect_identical(result$metadata$num_files, 3)
  expect_identical(result$metadata$created_time, 1700000000000)
})

test_that("malformed protocol and metadata actions are typed failures", {
  malformed_protocol <- "{\"protocol\":{\"readerFeatures\":[]}}\n"
  malformed_metadata <- paste0(
    "{\"metaData\":{\"id\":\"table-id\",\"format\":",
    "{\"provider\":\"parquet\"},\"partitionColumns\":[]}}\n"
  )

  expect_error(
    delta.sharing:::.parse_table_metadata_ndjson(malformed_protocol),
    "minReaderVersion",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.parse_table_metadata_ndjson(malformed_metadata),
    "schemaString",
    class = "delta_sharing_protocol_error"
  )
})
