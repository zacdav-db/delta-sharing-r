with_fake_control_execution <- function(handler,
                                        code,
                                        max_attempts = 1L,
                                        metadata_chunk_bytes = 65536L) {
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(handler),
    sleeper = function(delay) NULL,
    random = function(n, min, max) 0,
    max_attempts = max_attempts,
    metadata_chunk_bytes = metadata_chunk_bytes
  )
  interface <- delta.sharing:::.new_execution_interface(callbacks)
  delta.sharing:::.with_execution_interface(interface, force(code))
}

execution_metadata_fixture <- function() {
  path <- test_path(
    "fixtures",
    "protocol",
    "table-metadata-private.ndjson"
  )
  readBin(path, what = "raw", n = file.info(path)$size)
}

encoded_execution_path <- function(...) {
  segments <- c(...)
  paste0(
    "https://sharing.example.test/api/",
    paste(
      vapply(
        segments,
        utils::URLencode,
        character(1),
        reserved = TRUE,
        repeated = TRUE,
        USE.NAMES = FALSE
      ),
      collapse = "/"
    )
  )
}

test_that("production callbacks preserve R control-plane ownership and unload clears them", {
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      list(status = 200L, body = list(items = list()))
    })
  )

  expect_setequal(
    names(callbacks),
    c(
      "list_shares",
      "list_schemas",
      "list_tables",
      "table_version",
      "table_protocol",
      "table_metadata",
      "table_schema",
      "read_arrow_stream",
      "data_frame_from_stream",
      if (requireNamespace("arrow", quietly = TRUE)) {
        "arrow_from_stream"
      }
    )
  )
  expect_false(any(c(
    "read_schema",
    "read_diagnostics"
  ) %in% names(callbacks)))

  old <- delta.sharing:::.set_execution_callbacks(callbacks)
  on.exit(delta.sharing:::.set_execution_interface(old), add = TRUE)
  expect_s3_class(
    delta.sharing:::.execution_state$interface,
    "delta_sharing_execution_interface"
  )

  delta.sharing:::.onUnload()
  expect_null(delta.sharing:::.execution_state$interface)
})

test_that("Arrow production callback follows the optional dependency seam", {
  transport <- delta.sharing:::.fake_http_transport(function(request) {
    list(status = 200L, body = list(items = list()))
  })

  absent <- delta.sharing:::.new_control_execution_callbacks(
    transport = transport,
    arrow_available = function() FALSE
  )
  present <- delta.sharing:::.new_control_execution_callbacks(
    transport = transport,
    arrow_available = function() TRUE,
    arrow_reader_factory = function(stream) NULL
  )

  expect_false("arrow_from_stream" %in% names(absent))
  expect_true("arrow_from_stream" %in% names(present))
  expect_true("data_frame_from_stream" %in% names(absent))
  expect_true("data_frame_from_stream" %in% names(present))
})

test_that("HTTP execution paginates and fans out raw provider identifiers", {
  recorder <- new.env(parent = emptyenv())
  recorder$requests <- list()
  raw_share <- "café/100%"
  second_share <- "operations"
  first_token <- "page/token%二"

  handler <- function(request) {
    recorder$requests <- c(recorder$requests, list(request))
    authorization <- request$headers[["Authorization"]]
    if (!identical(authorization, "Bearer test-only-bearer-token")) {
      stop("missing test authorization")
    }

    if (identical(
      request$url,
      encoded_execution_path("shares")
    )) {
      if (length(request$query) == 0L) {
        return(list(
          status = 200L,
          body = list(
            items = list(list(name = raw_share)),
            nextPageToken = first_token
          )
        ))
      }
      if (identical(request$query, list(pageToken = first_token))) {
        return(list(
          status = 200L,
          body = list(items = list(list(name = second_share)))
        ))
      }
    }

    if (identical(
      request$url,
      encoded_execution_path("shares", raw_share, "schemas")
    )) {
      return(list(
        status = 200L,
        body = list(items = list(list(
          share = raw_share,
          name = "schema/β%"
        )))
      ))
    }
    if (identical(
      request$url,
      encoded_execution_path("shares", second_share, "schemas")
    )) {
      return(list(
        status = 200L,
        body = list(items = list(list(
          share = second_share,
          name = "default"
        )))
      ))
    }
    if (identical(
      request$url,
      encoded_execution_path("shares", raw_share, "all-tables")
    )) {
      return(list(
        status = 200L,
        body = list(items = list(list(
          share = raw_share,
          schema = "schema/β%",
          name = "orders/2026%",
          accessModes = list("url")
        )))
      ))
    }
    if (identical(
      request$url,
      encoded_execution_path("shares", second_share, "all-tables")
    )) {
      return(list(
        status = 200L,
        body = list(items = list(list(
          share = second_share,
          schema = "default",
          name = "events"
        )))
      ))
    }
    stop("unexpected fake route")
  }

  with_fake_control_execution(handler, {
    client <- test_client()
    shares <- list_shares(client)
    schemas <- list_schemas(client)
    tables <- list_tables(client)

    expect_identical(shares$name, c(raw_share, second_share))
    expect_identical(schemas$share, c(raw_share, second_share))
    expect_identical(schemas$name, c("schema/β%", "default"))
    expect_identical(tables$share, c(raw_share, second_share))
    expect_identical(tables$name, c("orders/2026%", "events"))
  })

  urls <- vapply(recorder$requests, `[[`, character(1), "url")
  expect_true(encoded_execution_path(
    "shares",
    raw_share,
    "schemas"
  ) %in% urls)
  expect_true(encoded_execution_path(
    "shares",
    raw_share,
    "all-tables"
  ) %in% urls)
  expect_false(any(grepl(raw_share, urls, fixed = TRUE)))
  expect_true(any(vapply(recorder$requests, function(request) {
    identical(request$query, list(pageToken = first_token))
  }, logical(1))))
})

test_that("table callbacks use GET version and chunked metadata projections", {
  recorder <- new.env(parent = emptyenv())
  recorder$requests <- list()
  bytes <- execution_metadata_fixture()
  share <- "café/100%"
  schema <- "schema/β%"
  table_name <- "orders/2026%"
  base_path <- c(
    "shares",
    share,
    "schemas",
    schema,
    "tables",
    table_name
  )

  handler <- function(request) {
    recorder$requests <- c(recorder$requests, list(request))
    expect_identical(
      request$headers[["Authorization"]],
      "Bearer test-only-bearer-token"
    )
    if (identical(
      request$url,
      do.call(encoded_execution_path, as.list(c(base_path, "version")))
    )) {
      return(list(
        status = 200L,
        headers = list("Delta-Table-Version" = "125"),
        body = raw()
      ))
    }
    if (identical(
      request$url,
      do.call(encoded_execution_path, as.list(c(base_path, "metadata")))
    )) {
      return(list(
        status = 200L,
        headers = list("Delta-Table-Version" = "125"),
        body = bytes
      ))
    }
    stop("unexpected table route")
  }

  with_fake_control_execution(
    handler,
    {
      table <- sharing_table(
        test_client(),
        share = share,
        schema = schema,
        table = table_name
      )

      expect_identical(table_version(table), 125)
      protocol <- table_protocol(table)
      metadata <- table_metadata(table)
      schema_result <- table_schema(table)

      expect_identical(protocol$min_reader_version, 1)
      expect_identical(metadata$id, "table-private")
      expect_false("location" %in% names(metadata))
      expect_s3_class(schema_result, "delta_sharing_schema")
      expect_identical(schema_result$type, "struct")
    },
    metadata_chunk_bytes = 7L
  )

  requests <- recorder$requests
  expect_true(all(vapply(requests, function(request) {
    identical(request$method, "GET")
  }, logical(1))))
  metadata_requests <- Filter(function(request) {
    endsWith(request$url, "/metadata")
  }, requests)
  expect_length(metadata_requests, 3L)
  expect_true(all(vapply(metadata_requests, function(request) {
    identical(
      request$headers[["delta-sharing-capabilities"]],
      delta.sharing:::.snapshot_capability_header()
    )
  }, logical(1))))
  expect_false(any(grepl(
    "private-bucket",
    paste(capture.output(str(list(
      protocol = protocol,
      metadata = metadata,
      schema = schema_result
    ))), collapse = "\n"),
    fixed = TRUE
  )))
})

test_that("execution failures are typed and redact payloads and request data", {
  client <- test_client()
  secret <- "EXECUTION-SECRET-MUST-NOT-LEAK"

  invalid_json <- with_fake_control_execution(
    function(request) {
      list(
        status = 200L,
        body = paste0("{\"items\":", secret)
      )
    },
    expect_error(
      list_shares(client),
      class = "delta_sharing_protocol_error"
    )
  )
  expect_identical(invalid_json$operation, "list_shares")

  page <- 0L
  transport_failure <- with_fake_control_execution(
    function(request) {
      page <<- page + 1L
      if (page == 1L) {
        return(list(
          status = 200L,
          body = list(
            items = list(),
            nextPageToken = paste0("token/", secret)
          )
        ))
      }
      stop(
        request$url,
        request$query$pageToken,
        request$headers[["Authorization"]],
        secret
      )
    },
    expect_error(
      list_shares(client),
      class = "delta_sharing_http_error"
    )
  )
  expect_identical(transport_failure$operation, "list_shares")
  expect_identical(transport_failure$endpoint_host, "sharing.example.test")

  rejected <- with_fake_control_execution(
    function(request) {
      list(
        status = 403L,
        body = paste0("server body ", secret)
      )
    },
    expect_error(
      list_schemas(client, share = paste0("share/", secret)),
      class = "delta_sharing_http_error"
    )
  )
  expect_identical(rejected$status, 403L)
  expect_identical(rejected$operation, "list_schemas")

  invalid_metadata <- with_fake_control_execution(
    function(request) {
      list(
        status = 200L,
        headers = list("Delta-Table-Version" = "1"),
        body = paste0("{\"protocol\":\"", secret, "\"}\n")
      )
    },
    expect_error(
      table_protocol(test_table()),
      class = "delta_sharing_protocol_error"
    ),
    metadata_chunk_bytes = 3L
  )
  expect_identical(invalid_metadata$operation, "table_protocol")

  rendered <- paste(
    vapply(
      list(
        invalid_json,
        transport_failure,
        rejected,
        invalid_metadata
      ),
      function(condition) {
        paste(
          conditionMessage(condition),
          capture.output(str(condition)),
          collapse = "\n"
        )
      },
      character(1)
    ),
    collapse = "\n"
  )
  expect_false(grepl(secret, rendered, fixed = TRUE))
  expect_false(grepl("test-only-bearer-token", rendered, fixed = TRUE))
})
