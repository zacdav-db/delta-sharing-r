parquet_response_actions <- function() {
  lines <- readLines(
    test_path(
      "fixtures",
      "protocol",
      "snapshot-parquet-kernel-proof.ndjson"
    ),
    warn = FALSE,
    encoding = "UTF-8"
  )
  lapply(lines, jsonlite::fromJSON, simplifyVector = FALSE)
}

parquet_response_bytes <- function(actions) {
  lines <- vapply(
    actions,
    function(action) {
      unclass(jsonlite::toJSON(
        action,
        auto_unbox = TRUE,
        null = "null",
        digits = NA
      ))
    },
    character(1)
  )
  charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
}

parquet_response_headers <- function(version = "42", format = "parquet") {
  planned_snapshot_headers(
    version = version,
    capabilities = paste0("responseformat=", format)
  )
}

prepare_parquet_response <- function(
  actions = parquet_response_actions(),
  response_format = "auto",
  headers = parquet_response_headers(),
  clock = function() {
    as.POSIXct(
      "2026-07-29 00:00:00",
      tz = "UTC"
    )
  }
) {
  requests <- list()
  prepared <- delta.sharing:::.prepare_snapshot_read(
    sharing_read(
      test_table(),
      columns = c("region", "value"),
      limit = 2,
      response_format = response_format
    ),
    fetch = function(request) {
      requests[[length(requests) + 1L]] <<- request
      planned_pull_response(
        parquet_response_bytes(actions),
        headers = headers
      )
    },
    clock = clock
  )
  attr(prepared, "test_requests") <- requests
  prepared
}

parquet_prepared_actions <- function(prepared) {
  guard <- delta.sharing:::.prepared_snapshot_state(prepared)$guard
  commit <- file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log",
    "00000000000000000000.json"
  )
  lines <- readLines(
    commit,
    warn = FALSE,
    encoding = "UTF-8"
  )
  lapply(lines, jsonlite::fromJSON, simplifyVector = FALSE)
}

parquet_response_condition_text <- function(condition) {
  paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )
}

parquet_empty_object <- function() {
  structure(list(), names = character())
}

test_that("explicit and auto Parquet selections use the production mapping", {
  for (requested in c("parquet", "auto")) {
    prepared <- prepare_parquet_response(response_format = requested)
    on.exit(delta.sharing:::.release_prepared_snapshot(prepared), add = TRUE)
    request <- attr(prepared, "test_requests", exact = TRUE)[[1L]]
    diagnostics <- delta.sharing:::.prepared_snapshot_diagnostics(prepared)
    invocation <- delta.sharing:::.prepared_snapshot_invocation(prepared)
    actions <- parquet_prepared_actions(prepared)

    expected_capability <- if (identical(requested, "auto")) {
      "responseformat=delta,parquet;"
    } else {
      "responseformat=parquet;"
    }
    expect_match(
      request$headers[["delta-sharing-capabilities"]],
      expected_capability,
      fixed = TRUE
    )
    expect_identical(diagnostics$response_format, "parquet")
    expect_identical(diagnostics$table_version, 42)
    expect_identical(diagnostics$file_count, 1L)
    expect_identical(invocation$projection, c("region", "value"))
    expect_identical(invocation$exact_limit, 2)
    expect_identical(
      actions[[1L]]$protocol,
      list(minReaderVersion = 1L, minWriterVersion = 2L)
    )
    expect_identical(actions[[2L]]$metaData$format$provider, "parquet")
    expect_length(actions[[2L]]$metaData$format$options, 0L)
    expect_length(actions[[2L]]$metaData$configuration, 0L)
    expect_identical(actions[[3L]]$add$modificationTime, 0L)
    expect_true(actions[[3L]]$add$dataChange)
    expect_false(any(
      c(
        "version",
        "timestamp",
        "expirationTimestamp",
        "id"
      ) %in%
        names(actions[[3L]]$add)
    ))
    expect_true(delta.sharing:::.release_prepared_snapshot(prepared))
  }
})

test_that("empty Parquet responses retain schema and validate totals", {
  actions <- parquet_response_actions()[1:2]
  actions[[2L]]$metaData$numFiles <- 0
  actions[[2L]]$metaData$size <- 0
  prepared <- prepare_parquet_response(actions)
  on.exit(delta.sharing:::.release_prepared_snapshot(prepared), add = TRUE)

  expect_length(parquet_prepared_actions(prepared), 2L)
  expect_identical(
    delta.sharing:::.prepared_snapshot_diagnostics(prepared)$file_count,
    0L
  )

  invalid_count <- actions
  invalid_count[[2L]]$metaData$numFiles <- 1
  expect_error(
    prepare_parquet_response(invalid_count),
    "file count",
    class = "delta_sharing_protocol_error"
  )
  invalid_size <- actions
  invalid_size[[2L]]$metaData$size <- 1
  expect_error(
    prepare_parquet_response(invalid_size),
    "total size",
    class = "delta_sharing_protocol_error"
  )
})

test_that("Parquet response selection and versions cannot drift", {
  actions <- parquet_response_actions()
  expect_error(
    prepare_parquet_response(
      actions,
      headers = parquet_response_headers(format = "delta")
    ),
    "format",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    prepare_parquet_response(
      actions,
      response_format = "delta"
    ),
    "different",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    prepare_parquet_response(
      actions,
      headers = parquet_response_headers(version = "43")
    ),
    "versions",
    class = "delta_sharing_protocol_error"
  )

  mismatched_actions <- actions
  delta_fixture <- readLines(
    test_path("fixtures", "protocol", "snapshot-page-2.ndjson"),
    warn = FALSE
  )
  mismatched_actions[[2L]] <- jsonlite::fromJSON(
    delta_fixture[[2L]],
    simplifyVector = FALSE
  )
  expect_error(
    prepare_parquet_response(mismatched_actions),
    "inconsistent response formats",
    class = "delta_sharing_protocol_error"
  )
})

test_that("Parquet file identities and URLs are unique", {
  actions <- parquet_response_actions()
  duplicate <- actions[[3L]]
  actions[[2L]]$metaData$numFiles <- 2
  actions[[2L]]$metaData$size <- 3878
  actions[[4L]] <- duplicate
  actions[[4L]]$file$url <-
    "https://objects.example.test/other.parquet?proof=redacted"
  expect_error(
    prepare_parquet_response(actions),
    "duplicate",
    class = "delta_sharing_protocol_error"
  )

  actions[[4L]]$file$id <- "parquet-file-b"
  actions[[4L]]$file$url <- actions[[3L]]$file$url
  expect_error(
    prepare_parquet_response(actions),
    "duplicate",
    class = "delta_sharing_protocol_error"
  )
})

test_that("Parquet schemas reject collisions and reader-sensitive metadata", {
  actions <- parquet_response_actions()
  schema <- jsonlite::fromJSON(
    actions[[2L]]$metaData$schemaString,
    simplifyVector = FALSE
  )
  collision <- schema$fields[[1L]]
  collision$name <- "VALUE"
  schema$fields[[length(schema$fields) + 1L]] <- collision
  actions[[2L]]$metaData$schemaString <- unclass(jsonlite::toJSON(
    schema,
    auto_unbox = TRUE,
    null = "null"
  ))
  expect_error(
    prepare_parquet_response(actions),
    "case-insensitive",
    class = "delta_sharing_protocol_error"
  )

  actions <- parquet_response_actions()
  schema <- jsonlite::fromJSON(
    actions[[2L]]$metaData$schemaString,
    simplifyVector = FALSE
  )
  schema$fields[[1L]]$metadata[["delta.columnMapping.id"]] <- 1
  actions[[2L]]$metaData$schemaString <- unclass(jsonlite::toJSON(
    schema,
    auto_unbox = TRUE,
    null = "null"
  ))
  expect_error(
    prepare_parquet_response(actions),
    "column mapping",
    class = "delta_sharing_unsupported_error"
  )

  actions <- parquet_response_actions()
  actions[[2L]]$metaData$configuration[["delta.columnMapping.mode"]] <- "name"
  expect_error(
    prepare_parquet_response(actions),
    "reader features",
    class = "delta_sharing_unsupported_error"
  )
})

test_that("partition order, primitive bounds, and null encoding are enforced", {
  actions <- parquet_response_actions()
  schema <- list(
    type = "struct",
    fields = list(
      list(
        name = "payload",
        type = "string",
        nullable = TRUE,
        metadata = structure(list(), names = character())
      ),
      list(
        name = "part_long",
        type = "long",
        nullable = TRUE,
        metadata = structure(list(), names = character())
      ),
      list(
        name = "part_text",
        type = "string",
        nullable = TRUE,
        metadata = structure(list(), names = character())
      ),
      list(
        name = "part_byte",
        type = "byte",
        nullable = TRUE,
        metadata = structure(list(), names = character())
      ),
      list(
        name = "part_day",
        type = "date",
        nullable = TRUE,
        metadata = structure(list(), names = character())
      )
    )
  )
  actions[[2L]]$metaData$schemaString <- unclass(jsonlite::toJSON(
    schema,
    auto_unbox = TRUE,
    null = "null"
  ))
  actions[[2L]]$metaData$partitionColumns <- list(
    "part_long",
    "part_text",
    "part_byte",
    "part_day"
  )
  actions[[3L]]$file$partitionValues <- list(
    part_day = "2026-07-29",
    part_byte = "-128",
    part_text = "",
    part_long = "9223372036854775807"
  )
  prepared <- prepare_parquet_response(actions)
  on.exit(delta.sharing:::.release_prepared_snapshot(prepared), add = TRUE)
  mapped <- parquet_prepared_actions(prepared)
  expect_identical(
    unlist(mapped[[2L]]$metaData$partitionColumns, use.names = FALSE),
    c("part_long", "part_text", "part_byte", "part_day")
  )
  expect_identical(mapped[[3L]]$add$partitionValues$part_text, "")
  expect_identical(mapped[[3L]]$add$partitionValues$part_byte, "-128")

  invalid_byte <- actions
  invalid_byte[[3L]]$file$partitionValues$part_byte <- "128"
  expect_error(
    prepare_parquet_response(invalid_byte),
    "partition value",
    class = "delta_sharing_protocol_error"
  )
  invalid_long <- actions
  invalid_long[[3L]]$file$partitionValues$part_long <-
    "9223372036854775808"
  expect_error(
    prepare_parquet_response(invalid_long),
    "partition value",
    class = "delta_sharing_protocol_error"
  )
  missing <- actions
  missing[[3L]]$file$partitionValues$part_day <- NULL
  expect_error(
    prepare_parquet_response(missing),
    "partition values",
    class = "delta_sharing_protocol_error"
  )
  complex <- actions
  complex_schema <- jsonlite::fromJSON(
    complex[[2L]]$metaData$schemaString,
    simplifyVector = FALSE
  )
  complex_schema$fields[[2L]]$type <- list(
    type = "array",
    elementType = "long",
    containsNull = FALSE
  )
  complex[[2L]]$metaData$schemaString <- unclass(jsonlite::toJSON(
    complex_schema,
    auto_unbox = TRUE,
    null = "null"
  ))
  expect_error(
    prepare_parquet_response(complex),
    "primitive",
    class = "delta_sharing_unsupported_error"
  )
})

test_that("Parquet validation conditions redact URLs and response content", {
  secret <- "parquet-validation-private-secret"
  cases <- list(
    function(actions) {
      actions[[3L]]$file$url <- paste0(
        "http://objects.example.test/data?sig=",
        secret
      )
      actions
    },
    function(actions) {
      actions[[3L]]$file$stats <- paste0("{\"", secret, "\":")
      actions
    },
    function(actions) {
      actions[[3L]]$file$partitionValues$region <- paste0("\n", secret)
      actions
    }
  )
  for (mutate in cases) {
    condition <- expect_error(
      prepare_parquet_response(mutate(parquet_response_actions())),
      class = "delta_sharing_error"
    )
    expect_false(grepl(
      secret,
      parquet_response_condition_text(condition),
      fixed = TRUE
    ))
  }
})

test_that("expired Parquet URLs fail before publication", {
  actions <- parquet_response_actions()
  actions[[3L]]$file$expirationTimestamp <- 1
  parent <- tempfile("parquet-expiry-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  condition <- expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table(), response_format = "parquet"),
      fetch = function(request) {
        planned_pull_response(
          parquet_response_bytes(actions),
          headers = parquet_response_headers()
        )
      },
      temp_parent = parent,
      clock = function() {
        as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
      }
    ),
    class = "delta_sharing_http_error"
  )
  expect_match(conditionMessage(condition), "expired")
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
  expect_false(grepl(
    "proof=redacted",
    parquet_response_condition_text(condition),
    fixed = TRUE
  ))
})

test_that("recognized Parquet wrappers contain exactly one action", {
  actions <- parquet_response_actions()
  names <- c("protocol", "metaData", "file")
  for (index in seq_along(actions)) {
    invalid <- actions[[index]]
    invalid$futureWrapperField <- "private-wrapper-value"
    condition <- expect_error(
      delta.sharing:::.parse_ndjson_action(
        charToRaw(unclass(jsonlite::toJSON(
          invalid,
          auto_unbox = TRUE,
          null = "null"
        ))),
        line_number = index,
        operation = "query_table"
      ),
      paste0(
        "invalid ",
        if (identical(names[[index]], "metaData")) {
          "metadata"
        } else {
          names[[index]]
        },
        " wrapper"
      ),
      class = "delta_sharing_protocol_error"
    )
    expect_false(grepl(
      "private-wrapper-value",
      parquet_response_condition_text(condition),
      fixed = TRUE
    ))
  }
})

test_that("Parquet schema helpers cover nested and primitive type contracts", {
  primitives <- c(
    "string",
    "boolean",
    "byte",
    "short",
    "integer",
    "long",
    "float",
    "double",
    "date",
    "timestamp",
    "timestamp_ntz",
    "binary"
  )
  for (type in primitives) {
    expect_identical(
      delta.sharing:::.parquet_schema_type(type),
      list(kind = "primitive", primitive = type)
    )
  }
  expect_identical(
    delta.sharing:::.parquet_schema_type("decimal(10,2)"),
    list(kind = "primitive", primitive = "decimal(10,2)")
  )
  expect_null(delta.sharing:::.parquet_schema_decimal("decimal(39,2)"))
  expect_null(delta.sharing:::.parquet_schema_decimal("not-decimal"))

  nested <- list(
    type = "struct",
    fields = list(list(
      name = "items",
      type = list(
        type = "array",
        elementType = list(
          type = "map",
          keyType = "string",
          valueType = "long",
          valueContainsNull = TRUE
        ),
        containsNull = FALSE
      ),
      nullable = TRUE,
      metadata = parquet_empty_object()
    ))
  )
  parsed <- delta.sharing:::.parquet_schema_type(nested)
  expect_identical(parsed$kind, "struct")
  expect_identical(parsed$fields$items$type$kind, "array")
  expect_identical(parsed$fields$items$type$element$kind, "map")

  invalid <- list(
    "unsupported",
    list(noType = "string"),
    list(type = "struct", fields = parquet_empty_object()),
    list(type = "array", elementType = "long", containsNull = NA),
    list(
      type = "map",
      keyType = "string",
      valueType = "long",
      valueContainsNull = "yes"
    )
  )
  for (type in invalid) {
    expect_error(
      delta.sharing:::.parquet_schema_type(type),
      class = "delta_sharing_protocol_error"
    )
  }
  expect_error(
    delta.sharing:::.parquet_schema_fields(list(list(
      name = "",
      type = "string",
      nullable = TRUE,
      metadata = parquet_empty_object()
    ))),
    "field name must be one safe string",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.parquet_schema_metadata(list()),
    "metadata is invalid",
    class = "delta_sharing_protocol_error"
  )
})

test_that("Parquet partition serialization covers every supported family", {
  valid <- list(
    string = "anything",
    boolean = "false",
    byte = "127",
    short = "-32768",
    integer = "2147483647",
    long = "-9223372036854775808",
    float = "-1.5e+03",
    double = ".25",
    `decimal(10,2)` = "-12.50",
    date = "2026-07-29",
    timestamp = "2026-07-29 12:34:56.123Z",
    timestamp_ntz = "2026-07-29T12:34:56"
  )
  for (type in names(valid)) {
    expect_true(delta.sharing:::.validate_parquet_partition_value(
      valid[[type]],
      type
    ))
    expect_true(delta.sharing:::.validate_parquet_partition_value("", type))
  }
  invalid <- list(
    boolean = "TRUE",
    byte = "01",
    short = "-32769",
    integer = "2147483648",
    float = "NaN",
    `decimal(10,2)` = "1e2",
    date = "not-a-date",
    timestamp = "2026-07-29",
    timestamp_ntz = "2026-07-29T12:34:56Z",
    binary = "bytes"
  )
  for (type in names(invalid)) {
    expect_false(delta.sharing:::.validate_parquet_partition_value(
      invalid[[type]],
      type
    ))
  }
  expect_false(delta.sharing:::.parquet_integer_in_range(
    "not-an-integer",
    "-128",
    "127"
  ))
  expect_true(delta.sharing:::.parquet_integer_in_range(
    "-0",
    "-128",
    "127"
  ))
})

test_that("Parquet protocol, metadata, and file state fail closed", {
  invalid_protocols <- list(
    structure(
      list(
        min_reader_version = 2,
        min_writer_version = NULL,
        reader_features = character(),
        writer_features = character()
      ),
      class = c("delta_sharing_protocol", "list")
    ),
    structure(
      list(
        min_reader_version = 1,
        min_writer_version = 2,
        reader_features = character(),
        writer_features = character()
      ),
      class = c("delta_sharing_protocol", "list")
    )
  )
  expect_error(
    delta.sharing:::.parquet_snapshot_protocol_action(invalid_protocols[[1L]]),
    class = "delta_sharing_unsupported_error"
  )
  expect_error(
    delta.sharing:::.parquet_snapshot_protocol_action(invalid_protocols[[2L]]),
    "protocol is invalid",
    class = "delta_sharing_protocol_error"
  )

  actions <- parquet_response_actions()
  actions[[2L]]$metaData$format$options <- list(compression = "secret")
  expect_error(
    prepare_parquet_response(actions),
    "without format options",
    class = "delta_sharing_unsupported_error"
  )

  actions <- parquet_response_actions()
  actions[[2L]]$metaData$configuration <- list("bad\nkey" = "value")
  expect_error(
    prepare_parquet_response(actions),
    "configuration is invalid",
    class = "delta_sharing_protocol_error"
  )

  schema <- delta.sharing:::.validate_parquet_schema(
    parquet_response_actions()[[2L]]$metaData$schemaString,
    "region"
  )
  incompatible <- delta.sharing:::.new_private_snapshot_file(
    id = "delta-file",
    action_type = "add",
    delta_action = list(
      add = list(
        path = "https://objects.example.test/data.parquet",
        partitionValues = list(region = "apac"),
        size = 1,
        modificationTime = 0,
        dataChange = TRUE
      )
    ),
    response_format = "delta"
  )
  expect_error(
    delta.sharing:::.validate_parquet_snapshot_files(
      list(incompatible),
      schema
    ),
    "incompatible",
    class = "delta_sharing_protocol_error"
  )

  expect_error(
    delta.sharing:::.normalize_parquet_file_action(
      list(),
      "query_table"
    ),
    "JSON object",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.normalize_parquet_file_action(
      list(
        url = "https://objects.example.test/data.parquet",
        id = "file-a",
        partitionValues = parquet_empty_object(),
        size = 1,
        unknown = "value"
      ),
      "query_table"
    ),
    "unsupported fields",
    class = "delta_sharing_protocol_error"
  )
})

test_that("Parquet CDF is rejected before authentication or snapshot I/O", {
  reads <- 0L
  auth <- 0L
  snapshot_transport <- list(
    open = function(request) {
      reads <<- reads + 1L
      stop("must not open")
    },
    status = function(response) 500L,
    headers = function(response) character(),
    pull = function(response) NULL,
    close = function(response) invisible(NULL),
    retry_after = function(response) NULL
  )
  condition <- expect_error(
    delta.sharing:::.execute_snapshot_arrow_stream(
      specification = sharing_changes(
        test_table(),
        starting_version = 1,
        ending_version = 2,
        response_format = "parquet"
      ),
      batch_size = NULL,
      concurrency = NULL,
      snapshot_transport = snapshot_transport,
      auth_transport = delta.sharing:::.fake_http_transport(
        function(request) {
          auth <<- auth + 1L
          stop("must not authenticate")
        }
      ),
      clock = Sys.time,
      sleeper = Sys.sleep,
      random = stats::runif,
      max_attempts = 1L,
      temp_parent = tempdir(),
      native_stream_factory = function(...) stop("must not scan")
    ),
    class = "delta_sharing_unsupported_error"
  )
  expect_identical(condition$response_format, "parquet")
  expect_identical(reads, 0L)
  expect_identical(auth, 0L)
})
