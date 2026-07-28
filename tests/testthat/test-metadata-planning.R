private_metadata_fixture_raw <- function() {
  path <- test_path(
    "fixtures",
    "protocol",
    "table-metadata-private.ndjson"
  )
  readBin(path, what = "raw", n = file.info(path)$size)
}

chunk_reader <- function(bytes, sizes = c(1L, 3L, 7L, 11L)) {
  offset <- 1L
  index <- 0L
  function() {
    if (offset > length(bytes)) {
      return(NULL)
    }
    index <<- index + 1L
    size <- sizes[[(index - 1L) %% length(sizes) + 1L]]
    end <- min(length(bytes), offset + size - 1L)
    chunk <- bytes[seq.int(offset, end)]
    offset <<- end + 1L
    chunk
  }
}

test_that("table routes encode every identifier component independently", {
  identifier <- table_identifier(
    "Share Name/100%",
    schema = "café?#",
    table = "Orders%2FArchive"
  )

  expect_identical(
    delta.sharing:::.table_route(identifier, "table_metadata"),
    paste0(
      "/shares/Share%20Name%2F100%25/",
      "schemas/caf%C3%A9%3F%23/",
      "tables/Orders%252FArchive"
    )
  )
})

test_that("version planning uses the current safe GET descriptor", {
  request <- delta.sharing:::.plan_table_version_request(
    table_identifier("sales.default.orders")
  )

  expect_identical(request$method, "GET")
  expect_identical(
    request$path_segments,
    c("shares", "sales", "schemas", "default", "tables", "orders", "version")
  )
  expect_identical(request$query, list())
  expect_identical(request$headers, list())
  expect_identical(request$operation, "table_version")
  expect_false("authorization" %in% tolower(names(request$headers)))
})

test_that("metadata request planning is deterministic for all time modes", {
  identifier <- table_identifier("sales.default.orders")
  latest <- delta.sharing:::.plan_table_metadata_request(identifier)
  version <- delta.sharing:::.plan_table_metadata_request(
    identifier,
    version = 42
  )
  timestamp <- delta.sharing:::.plan_table_metadata_request(
    identifier,
    timestamp = as.POSIXct(
      "2026-07-29 12:34:56.125",
      tz = "Australia/Sydney"
    ),
    response_format = "delta"
  )

  expect_identical(latest$method, "GET")
  expect_identical(
    latest$path_segments,
    c("shares", "sales", "schemas", "default", "tables", "orders", "metadata")
  )
  expect_identical(latest$query, list())
  expect_identical(version$query, list(version = "42"))
  expect_identical(
    timestamp$query,
    list(timestamp = "2026-07-29T02:34:56.125Z")
  )
  expect_identical(
    latest$headers,
    list(
      "delta-sharing-capabilities" = paste0(
        "responseformat=delta,parquet;",
        "readerfeatures=columnmapping,deletionvectors,timestampntz"
      )
    )
  )
  expect_identical(
    timestamp$headers,
    list(
      "delta-sharing-capabilities" = paste0(
        "responseformat=delta;",
        "readerfeatures=columnmapping,deletionvectors,timestampntz"
      )
    )
  )
})

test_that("metadata planning rejects mixed time travel before fetching", {
  identifier <- table_identifier("sales.default.orders")
  calls <- 0L

  expect_error(
    delta.sharing:::.fetch_table_metadata(
      identifier,
      fetch = function(request) {
        calls <<- calls + 1L
      },
      version = 1,
      timestamp = as.POSIXct("2026-01-01", tz = "UTC")
    ),
    class = "delta_sharing_validation_error"
  )
  expect_identical(calls, 0L)
})

test_that("table version fetch parses GET response headers", {
  seen <- NULL
  version <- delta.sharing:::.fetch_table_version(
    table_identifier("sales.default.orders"),
    fetch = function(request) {
      seen <<- request
      list(headers = c("DELTA-TABLE-VERSION" = "125"))
    }
  )

  expect_identical(version, 125)
  expect_identical(seen$method, "GET")
  expect_identical(seen$operation, "table_version")
})

test_that("metadata fetch incrementally consumes bounded NDJSON chunks", {
  bytes <- private_metadata_fixture_raw()
  seen <- NULL
  response <- delta.sharing:::.fetch_table_metadata(
    table_identifier("sales.default.orders"),
    fetch = function(request) {
      seen <<- request
      list(
        headers = c("Delta-Table-Version" = "125"),
        chunks = chunk_reader(bytes)
      )
    }
  )

  expect_identical(response$table_version, 125)
  expect_identical(response$response_format, "parquet")
  expect_identical(response$metadata$id, "table-private")
  expect_identical(seen$method, "GET")
  expect_identical(
    seen$headers[["delta-sharing-capabilities"]],
    delta.sharing:::.snapshot_capability_header()
  )
})

test_that("protocol and public metadata projections are stable and safe", {
  bytes <- private_metadata_fixture_raw()
  response <- delta.sharing:::.fetch_table_metadata(
    table_identifier("sales.default.orders"),
    fetch = function(request) {
      list(
        headers = c("delta-table-version" = "125"),
        chunks = bytes
      )
    }
  )
  protocol <- delta.sharing:::.project_table_protocol(response)
  metadata <- delta.sharing:::.project_table_metadata(response)
  rendered <- paste(capture.output(str(metadata)), collapse = "\n")

  expect_identical(
    names(protocol),
    c(
      "response_format",
      "min_reader_version",
      "min_writer_version",
      "reader_features",
      "writer_features"
    )
  )
  expect_identical(protocol$response_format, "parquet")
  expect_identical(
    names(metadata),
    c(
      "table_version",
      "response_format",
      "id",
      "name",
      "description",
      "format",
      "schema_string",
      "configuration",
      "partition_columns",
      "version",
      "size",
      "num_files",
      "created_time",
      "access_modes"
    )
  )
  expect_identical(metadata$table_version, 125)
  expect_identical(metadata$partition_columns, "region")
  expect_false("location" %in% names(metadata))
  expect_false("auxiliary_locations" %in% names(metadata))
  expect_false(grepl("private-bucket", rendered, fixed = TRUE))
  expect_false(grepl("must-not-leak", rendered, fixed = TRUE))
  expect_null(attr(metadata, "private_storage", exact = TRUE))
})

test_that("signed storage locations remain available only internally", {
  bytes <- private_metadata_fixture_raw()
  response <- delta.sharing:::.fetch_table_metadata(
    table_identifier("sales.default.orders"),
    fetch = function(request) {
      list(
        headers = c("delta-table-version" = "125"),
        chunks = bytes
      )
    }
  )
  storage <- delta.sharing:::.private_table_storage(response)

  expect_identical(
    storage$location,
    "s3://private-bucket/orders?sig=storage-secret-must-not-leak"
  )
  expect_identical(
    storage$auxiliary_locations,
    "s3://private-bucket/dv?sig=aux-secret-must-not-leak"
  )
})

test_that("schema projection parses a logical Delta struct without Arrow", {
  bytes <- private_metadata_fixture_raw()
  response <- delta.sharing:::.fetch_table_metadata(
    table_identifier("sales.default.orders"),
    fetch = function(request) {
      list(
        headers = c("delta-table-version" = "125"),
        chunks = bytes
      )
    }
  )
  schema <- delta.sharing:::.project_table_schema(response)

  expect_s3_class(schema, "delta_sharing_schema")
  expect_identical(schema$type, "struct")
  expect_identical(
    vapply(schema$fields, `[[`, character(1), "name"),
    c("order_id", "details")
  )
  expect_identical(schema$fields[[2L]]$type$type, "struct")
})

test_that("schema failures never expose schema contents", {
  secret <- "schema-secret-must-not-leak"
  condition <- expect_error(
    delta.sharing:::.parse_table_schema_json(
      paste0("{\"type\":\"", secret, "\",\"fields\":[]}")
    ),
    class = "delta_sharing_protocol_error"
  )
  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )

  expect_identical(condition$operation, "table_schema")
  expect_false(grepl(secret, rendered, fixed = TRUE))
})

test_that("metadata line and chunk ceilings fail with typed safe errors", {
  bytes <- private_metadata_fixture_raw()
  identifier <- table_identifier("sales.default.orders")
  response <- function(request) {
    list(
      headers = c("delta-table-version" = "125"),
      chunks = chunk_reader(bytes)
    )
  }
  single_chunk <- function(request) {
    emitted <- FALSE
    list(
      headers = c("delta-table-version" = "125"),
      chunks = function() {
        if (emitted) {
          return(NULL)
        }
        emitted <<- TRUE
        bytes
      }
    )
  }

  expect_error(
    delta.sharing:::.fetch_table_metadata(
      identifier,
      response,
      max_line_bytes = 32
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.fetch_table_metadata(
      identifier,
      response,
      max_chunks = 1
    ),
    "chunk limit",
    class = "delta_sharing_protocol_error"
  )
  expect_identical(
    delta.sharing:::.fetch_table_metadata(
      identifier,
      single_chunk,
      max_chunks = 1
    )$table_version,
    125
  )
})

test_that("untyped fetch and chunk errors are wrapped without secrets", {
  secret <- "transport-secret-must-not-leak"
  identifier <- table_identifier("sales.default.orders")

  version_condition <- expect_error(
    delta.sharing:::.fetch_table_version(
      identifier,
      function(request) stop(secret)
    ),
    class = "delta_sharing_protocol_error"
  )
  chunk_condition <- expect_error(
    delta.sharing:::.fetch_table_metadata(
      identifier,
      function(request) {
        list(
          headers = c("delta-table-version" = "1"),
          chunks = function() stop(secret)
        )
      }
    ),
    class = "delta_sharing_protocol_error"
  )

  expect_false(grepl(secret, conditionMessage(version_condition), fixed = TRUE))
  expect_false(grepl(secret, conditionMessage(chunk_condition), fixed = TRUE))

  expect_error(
    delta.sharing:::.fetch_table_metadata(
      identifier,
      function(request) {
        list(
          headers = c("delta-table-version" = "1"),
          chunks = function() list(secret = secret)
        )
      }
    ),
    class = "delta_sharing_protocol_error"
  )
})
