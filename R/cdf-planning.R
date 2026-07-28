.cdf_query_operation <- "query_table_changes"

.cdf_planning_abort <- function(message, type = "protocol", ...) {
  .abort_delta_sharing(
    message,
    type = type,
    operation = .cdf_query_operation,
    ...
  )
}

.validate_cdf_read <- function(read, executable = FALSE) {
  if (!.object_is(read, SharingChanges)) {
    .cdf_planning_abort(
      "`read` must be a SharingChanges.",
      type = "validation"
    )
  }
  if (identical(read@response_format, "parquet")) {
    .cdf_planning_abort(
      "Parquet CDF responses are not supported.",
      type = "unsupported",
      response_format = "parquet",
      feature = "parquet_response"
    )
  }
  if (isTRUE(executable) && !is.null(read@starting_timestamp)) {
    .cdf_planning_abort(
      "Timestamp CDF execution is not enabled until both provider bounds can be resolved exactly.",
      type = "unsupported",
      response_format = "delta",
      feature = "cdf_timestamp_bounds"
    )
  }
  if (isTRUE(executable) && is.null(read@ending_version)) {
    .cdf_planning_abort(
      "Open-ended CDF execution is not enabled until the provider end version can be proven exactly.",
      type = "unsupported",
      response_format = "delta",
      feature = "cdf_open_end"
    )
  }
  read
}

.cdf_query_capabilities <- function(response_format) {
  response_format <- .normalize_response_format(response_format)
  if (identical(response_format, "parquet")) {
    .cdf_planning_abort(
      "Parquet CDF responses are not supported.",
      type = "unsupported",
      response_format = "parquet",
      feature = "parquet_response"
    )
  }
  paste0(
    .snapshot_capability_header("delta"),
    ";includeendstreamaction=true"
  )
}

.new_cdf_request_plan <- function(path_segments,
                                  query,
                                  headers,
                                  page_number) {
  structure(
    list(
      method = "GET",
      path_segments = path_segments,
      query = query,
      headers = headers,
      page_number = as.integer(page_number),
      operation = .cdf_query_operation
    ),
    class = c("delta_sharing_cdf_request", "list")
  )
}

.plan_cdf_request <- function(
  read,
  page_token = NULL,
  page_number = 1L,
  max_files_per_page = .snapshot_default_max_files_per_page
) {
  read <- .validate_cdf_read(read)
  page_number <- .snapshot_positive_integer(page_number, "page_number")
  max_files_per_page <- .snapshot_positive_integer(
    max_files_per_page,
    "max_files_per_page"
  )
  page_token <- tryCatch(
    .snapshot_page_token(page_token),
    delta_sharing_error = function(condition) {
      .cdf_planning_abort("The CDF page token is invalid.")
    }
  )

  query <- list(
    includeHistoricalMetadata = "true",
    includeHistoricalProtocol = "true",
    maxFiles = max_files_per_page
  )
  if (!is.null(read@starting_version)) {
    query$startingVersion <- .format_protocol_version(
      read@starting_version
    )
    if (!is.null(read@ending_version)) {
      query$endingVersion <- .format_protocol_version(read@ending_version)
    }
  } else {
    query$startingTimestamp <- .format_protocol_timestamp(
      read@starting_timestamp
    )
    if (!is.null(read@ending_timestamp)) {
      query$endingTimestamp <- .format_protocol_timestamp(
        read@ending_timestamp
      )
    }
  }
  if (!is.null(page_token)) {
    query$pageToken <- page_token
  }

  .new_cdf_request_plan(
    path_segments = c(
      .table_route_segments(read@table@identifier, .cdf_query_operation),
      "changes"
    ),
    query = query,
    headers = list(
      Accept = "application/x-ndjson",
      "delta-sharing-capabilities" =
        .cdf_query_capabilities(read@response_format),
      fileidhash = "delta"
    ),
    page_number = page_number
  )
}

#' @exportS3Method print delta_sharing_cdf_request
print.delta_sharing_cdf_request <- function(x, ...) {
  cat(sprintf(
    "<delta_sharing_cdf_request> page %d; GET; query redacted\n",
    x$page_number
  ))
  invisible(x)
}

.cdf_condition_type <- function(condition) {
  if (inherits(condition, "delta_sharing_validation_error")) {
    "validation"
  } else if (inherits(condition, "delta_sharing_unsupported_error")) {
    "unsupported"
  } else if (inherits(condition, "delta_sharing_http_error")) {
    "http"
  } else if (inherits(condition, "delta_sharing_native_error")) {
    "native"
  } else {
    "protocol"
  }
}

.cdf_rethrow <- function(condition) {
  .abort_delta_sharing(
    conditionMessage(condition),
    type = .cdf_condition_type(condition),
    operation = .cdf_query_operation,
    status = condition$status,
    feature = condition$feature,
    response_format = condition$response_format,
    endpoint_host = condition$endpoint_host,
    retry_count = condition$retry_count
  )
}

.validate_cdf_response_headers <- function(headers) {
  tryCatch(
    {
      content_type <- .snapshot_header(
        headers,
        "content-type",
        required = TRUE
      )
      media_type <- tolower(trimws(strsplit(
        content_type,
        ";",
        fixed = TRUE
      )[[1L]][[1L]]))
      if (!identical(media_type, "application/x-ndjson")) {
        .cdf_planning_abort("The CDF response is not NDJSON.")
      }
      file_id_hash <- .snapshot_header(
        headers,
        "fileidhash",
        required = TRUE
      )
      if (!identical(tolower(file_id_hash), "delta")) {
        .cdf_planning_abort(
          "The CDF response used an inconsistent file ID scheme."
        )
      }
      capabilities <- .parse_snapshot_capabilities(headers)
      if (!is.null(capabilities$responseformat) &&
          !identical(capabilities$responseformat, "delta")) {
        .cdf_planning_abort(
          "The server selected a Parquet CDF response.",
          type = "unsupported",
          response_format = "parquet",
          feature = "parquet_response"
        )
      }
      list(
        start_version = .parse_table_version_header(
          headers,
          operation = .cdf_query_operation
        ),
        capabilities = capabilities
      )
    },
    delta_sharing_error = function(condition) {
      if (identical(condition$operation, .cdf_query_operation)) {
        stop(condition)
      }
      .cdf_rethrow(condition)
    }
  )
}

.next_cdf_chunk <- function(response) {
  tryCatch(
    response$pull(),
    delta_sharing_error = function(condition) .cdf_rethrow(condition),
    error = function(condition) {
      .cdf_planning_abort("The streamed CDF response could not be read.")
    }
  )
}

.consume_cdf_page <- function(
  response,
  max_line_bytes = .ndjson_default_max_line_bytes,
  max_chunks = .snapshot_max_chunks_per_page
) {
  response <- tryCatch(
    .normalize_snapshot_pull_response(response),
    delta_sharing_error = function(condition) .cdf_rethrow(condition)
  )
  max_chunks <- .snapshot_positive_integer(max_chunks, "max_chunks")
  closed <- FALSE
  close_response <- function() {
    if (!closed) {
      closed <<- TRUE
      try(response$close(), silent = TRUE)
    }
    invisible(NULL)
  }
  on.exit(close_response(), add = TRUE)

  status <- response$status
  if (!is.numeric(status) ||
      length(status) != 1L ||
      is.na(status) ||
      !is.finite(status) ||
      status != floor(status) ||
      status < 100 ||
      status > 599) {
    .cdf_planning_abort("The CDF response has an invalid HTTP status.")
  }
  if (status < 200 || status >= 300) {
    .cdf_planning_abort(
      "The Delta Sharing server rejected the CDF request.",
      type = "http",
      status = as.integer(status)
    )
  }
  header_state <- .validate_cdf_response_headers(response$headers)
  decoder <- .new_ndjson_decoder(
    operation = .cdf_query_operation,
    max_line_bytes = max_line_bytes
  )
  protocol <- NULL
  metadata <- NULL
  historical_protocols <- list()
  historical_metadata <- list()
  files <- list()
  terminal <- NULL
  action_count <- 0L

  handle_actions <- function(actions) {
    for (action in actions) {
      action_count <<- action_count + 1L
      if (!inherits(action, "delta_sharing_ndjson_action")) {
        .cdf_planning_abort("The CDF response contains an invalid action.")
      }
      if (!is.null(terminal)) {
        .cdf_planning_abort(
          "The CDF terminal action must be the final NDJSON action."
        )
      }
      if (action_count == 1L) {
        if (!identical(action$type, "protocol")) {
          .cdf_planning_abort(
            "The first CDF action must be `protocol`."
          )
        }
        protocol <<- action$value
      } else if (action_count == 2L) {
        if (!identical(action$type, "metadata")) {
          .cdf_planning_abort(
            "The second CDF action must be `metaData`."
          )
        }
        metadata <<- action$value
      } else if (identical(action$type, "protocol")) {
        historical_protocols[[length(historical_protocols) + 1L]] <<-
          action$value
      } else if (identical(action$type, "metadata")) {
        historical_metadata[[length(historical_metadata) + 1L]] <<-
          action$value
      } else if (identical(action$type, "file")) {
        if (length(files) >= .snapshot_log_max_files) {
          .cdf_planning_abort(
            "The CDF response contains too many file actions."
          )
        }
        files[[length(files) + 1L]] <<- action$value
      } else if (identical(action$type, "end_stream")) {
        terminal <<- action$value
      } else {
        .cdf_planning_abort(
          "The CDF response contains an unexpected action."
        )
      }
    }
    invisible(NULL)
  }

  chunk_count <- 0L
  repeat {
    chunk <- .next_cdf_chunk(response)
    if (is.null(chunk)) {
      break
    }
    chunk_count <- chunk_count + 1L
    if (chunk_count > max_chunks) {
      .cdf_planning_abort(
        "The CDF response exceeded the internal chunk limit."
      )
    }
    handle_actions(.ndjson_decoder_push(decoder, chunk))
  }
  handle_actions(.ndjson_decoder_finish(decoder))

  if (is.null(protocol) || is.null(metadata)) {
    .cdf_planning_abort(
      "The CDF response is missing head protocol or metadata."
    )
  }
  if (!identical(protocol$response_format, "delta") ||
      !identical(metadata$response_format, "delta")) {
    .cdf_planning_abort(
      "The server selected a Parquet CDF response.",
      type = "unsupported",
      response_format = "parquet",
      feature = "parquet_response"
    )
  }
  terminal_required <- identical(
    header_state$capabilities$includeendstreamaction,
    "true"
  )
  if (is.null(terminal) && terminal_required) {
    .cdf_planning_abort(
      "The negotiated CDF stream is missing its terminal action."
    )
  }
  if (is.null(terminal)) {
    terminal <- .new_private_end_stream(
      next_page_token = NULL,
      refresh_token = NULL,
      min_url_expiration_timestamp = NULL
    )
  }

  list(
    protocol = protocol,
    metadata = metadata,
    historical_protocols = historical_protocols,
    historical_metadata = historical_metadata,
    files = files,
    terminal = .end_stream_state(
      terminal,
      operation = .cdf_query_operation
    ),
    start_version = header_state$start_version,
    capabilities = header_state$capabilities,
    chunk_count = chunk_count
  )
}

.safe_cdf_fetch <- function(fetch, request) {
  if (!is.function(fetch)) {
    stop("`fetch` must be a function.", call. = FALSE)
  }
  result <- tryCatch(
    list(value = fetch(request)),
    error = function(condition) {
      list(condition = condition)
    }
  )
  if (!is.null(result$condition)) {
    condition <- result$condition
    if (inherits(condition, "delta_sharing_error")) {
      if (identical(condition$operation, .cdf_query_operation)) {
        stop(condition)
      }
      .cdf_rethrow(condition)
    }
    .cdf_planning_abort("The CDF request could not be completed.")
  }
  result$value
}

.cdf_file_expiration <- function(file) {
  .snapshot_expiration_time(.snapshot_file_expiration_timestamp(file))
}

.cdf_min_expiration <- function(current, candidate) {
  if (is.null(candidate)) {
    return(current)
  }
  if (is.null(current) || candidate < current) {
    candidate
  } else {
    current
  }
}

.prepare_cdf_read <- function(
  read,
  fetch,
  temp_parent = tempdir(),
  max_pages = .pagination_page_limit,
  max_files_per_page = .snapshot_default_max_files_per_page,
  max_line_bytes = .ndjson_default_max_line_bytes,
  max_chunks_per_page = .snapshot_max_chunks_per_page,
  clock = Sys.time,
  checkpoint_asset = .cdf_checkpoint_asset(),
  write_commit = .write_snapshot_commit
) {
  read <- .validate_cdf_read(read, executable = TRUE)
  max_pages <- .snapshot_positive_integer(max_pages, "max_pages")
  max_files_per_page <- .snapshot_positive_integer(
    max_files_per_page,
    "max_files_per_page"
  )
  max_chunks_per_page <- .snapshot_positive_integer(
    max_chunks_per_page,
    "max_chunks_per_page"
  )
  .snapshot_now(clock)

  seen_tokens <- new.env(parent = emptyenv(), hash = TRUE)
  page_token <- NULL
  page_count <- 0L
  file_count <- 0L
  protocol <- NULL
  metadata <- NULL
  historical_protocols <- list()
  historical_metadata <- list()
  files <- list()
  start_version <- NULL
  min_expiration <- NULL

  repeat {
    if (page_count >= max_pages) {
      .cdf_planning_abort(
        "CDF pagination exceeded the internal page limit."
      )
    }
    page_number <- page_count + 1L
    request <- .plan_cdf_request(
      read,
      page_token = page_token,
      page_number = page_number,
      max_files_per_page = max_files_per_page
    )
    page <- .consume_cdf_page(
      .safe_cdf_fetch(fetch, request),
      max_line_bytes = max_line_bytes,
      max_chunks = max_chunks_per_page
    )
    page_count <- page_number

    if (page_count == 1L) {
      protocol <- page$protocol
      metadata <- page$metadata
      start_version <- page$start_version
      if (!identical(read@starting_version, start_version)) {
        .cdf_planning_abort(
          "The server resolved a different CDF start version than requested."
        )
      }
    } else if (!identical(protocol, page$protocol) ||
        !identical(metadata, page$metadata) ||
        !identical(start_version, page$start_version)) {
      .cdf_planning_abort("The CDF response changed across pages.")
    }

    if (file_count + length(page$files) > .snapshot_log_max_files) {
      .cdf_planning_abort(
        "The CDF response contains too many file actions."
      )
    }
    file_count <- file_count + length(page$files)
    historical_protocols <- c(
      historical_protocols,
      page$historical_protocols
    )
    historical_metadata <- c(
      historical_metadata,
      page$historical_metadata
    )
    files <- c(files, page$files)
    for (file in page$files) {
      min_expiration <- .cdf_min_expiration(
        min_expiration,
        .cdf_file_expiration(file)
      )
    }
    min_expiration <- .cdf_min_expiration(
      min_expiration,
      .snapshot_expiration_time(
        page$terminal$min_url_expiration_timestamp
      )
    )

    page_token <- page$terminal$next_page_token
    if (is.null(page_token)) {
      break
    }
    if (exists(page_token, envir = seen_tokens, inherits = FALSE)) {
      .cdf_planning_abort("The server repeated a CDF page token.")
    }
    assign(page_token, TRUE, envir = seen_tokens)
    .assert_snapshot_urls_live(min_expiration, clock)
  }

  .assert_snapshot_urls_live(min_expiration, clock)
  end_version <- read@ending_version
  guard <- .prepare_cdf_log(
    protocol = protocol,
    metadata = metadata,
    historical_protocols = historical_protocols,
    historical_metadata = historical_metadata,
    files = files,
    start_version = start_version,
    end_version = end_version,
    temp_parent = temp_parent,
    checkpoint_asset = checkpoint_asset,
    write_commit = write_commit
  )
  keep_guard <- FALSE
  on.exit({
    if (!keep_guard) {
      try(.release_snapshot_log(guard), silent = TRUE)
    }
  }, add = TRUE)
  .assert_snapshot_urls_live(min_expiration, clock)

  current_time <- .snapshot_now(clock)
  invocation <- list(
    table_uri = .snapshot_log_uri(guard),
    read_kind = "cdf",
    start_version = start_version,
    end_version = end_version,
    projection = read@columns,
    exact_limit = NULL
  )
  diagnostics <- list(
    response_format = "delta",
    start_version = start_version,
    end_version = end_version,
    page_count = as.integer(page_count),
    file_count = as.integer(file_count),
    min_url_expiration = min_expiration,
    url_expires_in_seconds = if (is.null(min_expiration)) {
      NULL
    } else {
      max(0, as.numeric(difftime(
        min_expiration,
        current_time,
        units = "secs"
      )))
    }
  )
  prepared <- .new_prepared_snapshot(
    guard = guard,
    invocation = invocation,
    diagnostics = diagnostics,
    refresh_token = NULL
  )
  keep_guard <- TRUE
  prepared
}
