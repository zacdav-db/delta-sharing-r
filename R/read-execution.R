# Read orchestration: turn a snapshot/changes spec into a Delta Kernel Arrow
# stream. R performs the Query Table request, parses the NDJSON response into
# protocol/metadata/file actions, writes a synthetic `_delta_log`, and hands
# the local path to the native kernel scan. Cleanup of the temp log is
# transferred to the native stream once it is constructed.

# Fetch Query Table pages and append their file actions to one staging file.
# Each response page is buffered by httr2, matching its ordinary request path.
write_snapshot_query_pages <- function(
  profile,
  auth,
  identifier,
  spec,
  format,
  output
) {
  protocol <- NULL
  metadata <- NULL
  page_token <- NULL
  page_count <- 0L
  file_count <- 0L

  repeat {
    page_count <- page_count + 1L
    req <- sharing_request(
      profile,
      auth,
      c(table_path(identifier), "query"),
      method = "POST"
    )
    req <- httr2::req_headers(
      req,
      `delta-sharing-capabilities` = capability_header(format)
    )
    req <- httr2::req_body_json(req, query_body(spec, page_token))
    resp <- httr2::req_perform(req)
    actions <- parse_ndjson_lines(httr2::resp_body_string(resp), "read")
    next_page_token <- find_next_page_token(actions, "read")
    page <- purrr::reduce(
      actions,
      function(state, action) {
        if (!is.null(action$protocol)) {
          state$protocol <- action$protocol
        } else if (!is.null(action$metaData)) {
          state$metadata <- action$metaData
        }
        state
      },
      .init = list(
        protocol = protocol,
        metadata = metadata
      )
    )

    files <- purrr::map(
      purrr::keep(actions, function(action) !is.null(action$file)),
      "file"
    )
    file_lines <- purrr::map_chr(files, function(file) {
      log_json_line(synthetic_file_action(file, format, "read"))
    })
    writeLines(file_lines, output, useBytes = TRUE)

    protocol <- page$protocol
    metadata <- page$metadata
    page_token <- next_page_token
    file_count <- file_count + length(files)
    if (is.null(page_token)) {
      break
    }
  }

  if (is.null(protocol) || is.null(metadata)) {
    abort(
      "The query response did not include protocol and metadata.",
      type = "protocol",
      operation = "read"
    )
  }

  list(
    protocol = protocol,
    metadata = metadata,
    page_count = page_count,
    file_count = file_count
  )
}

# Prepare the private snapshot log. The staging file is inside the private root
# and is removed before the handle transfers to the native cleanup guard.
prepare_snapshot_query_log <- function(
  profile,
  auth,
  identifier,
  spec,
  format
) {
  prepare_log(function(log_dir) {
    staged_actions <- fs::path(log_dir, ".snapshot-actions")
    query_result <- local({
      output <- file(staged_actions, open = "wb")
      on.exit(close(output), add = TRUE)
      write_snapshot_query_pages(
        profile,
        auth,
        identifier,
        spec,
        format,
        output
      )
    })
    header <- synthetic_log_header(
      format,
      query_result$protocol,
      query_result$metadata,
      "read"
    )
    write_staged_snapshot_commit(log_dir, header, staged_actions)
    list(
      response_format = format,
      page_count = query_result$page_count,
      file_count = query_result$file_count
    )
  })
}

changes_query <- function(spec, page_token) {
  query <- list(includeHistoricalMetadata = "true")
  if (!is.null(spec$starting_version)) {
    query$startingVersion <- spec$starting_version
  }
  if (!is.null(spec$ending_version)) {
    query$endingVersion <- spec$ending_version
  }
  if (!is.null(spec$starting_timestamp)) {
    query$startingTimestamp <- format_timestamp(spec$starting_timestamp)
  }
  if (!is.null(spec$ending_timestamp)) {
    query$endingTimestamp <- format_timestamp(spec$ending_timestamp)
  }
  if (!is.null(page_token)) {
    query$pageToken <- page_token
  }
  query
}

# Fetch a change data feed and bucket its file actions by commit version, so
# the synthetic log can be written as one commit per version. The returned
# effective bounds come from the versions represented in the response, as in
# the Python Kernel reader.
sharing_query_changes <- function(profile, auth, identifier, spec) {
  pages <- list()
  page_token <- NULL
  repeat {
    req <- sharing_request(
      profile,
      auth,
      c(table_path(identifier), "changes"),
      method = "GET",
      query = changes_query(spec, page_token)
    )
    req <- httr2::req_headers(
      req,
      `delta-sharing-capabilities` = capability_header(
        "delta",
        for_cdf = TRUE
      )
    )
    resp <- httr2::req_perform(req)
    page_actions <- parse_ndjson_lines(httr2::resp_body_string(resp), "changes")
    pages[[length(pages) + 1L]] <- page_actions
    page_token <- find_next_page_token(page_actions, "changes")
    if (is.null(page_token)) {
      break
    }
  }
  bucket_cdf_actions(
    purrr::list_c(pages),
    spec$starting_version,
    spec$ending_version
  )
}

# EndStreamAction is optional, but servers use it for pagination and failures.
find_next_page_token <- function(actions, operation) {
  action <- purrr::detect(actions, function(action) {
    !is.null(action$endStreamAction)
  })
  terminal <- action$endStreamAction
  if (!is.null(terminal$errorMessage)) {
    abort(
      "The server ended the query with an error: {terminal$errorMessage}",
      type = "protocol",
      operation = operation,
      status_code = terminal$httpStatusErrorCode
    )
  }
  token <- terminal$nextPageToken
  if (is_scalar_character(token)) token else NULL
}

bucket_cdf_actions <- function(actions, start_version, end_version) {
  protocol_action <- purrr::detect(
    rev(actions),
    function(action) !is.null(action$protocol)
  )
  protocol <- protocol_action$protocol$deltaProtocol %||%
    protocol_action$protocol

  # Normalize and group once rather than repeatedly growing each version list.
  entries <- purrr::map(actions, function(action) {
    if (!is.null(action$metaData)) {
      # Metadata applies at the start of the range when the server omits a
      # version (as Databricks does).
      v <- action$metaData$version
      if (is.null(v)) {
        if (is.null(start_version)) {
          abort(
            "Timestamp-bounded change metadata did not include its version.",
            type = "protocol",
            operation = "changes"
          )
        }
        v <- start_version
      }
      return(list(
        version = v,
        action = list(
          metaData = action$metaData$deltaMetadata %||% action$metaData
        )
      ))
    }
    if (!is.null(action$file)) {
      f <- action$file
      return(list(
        version = f$version,
        timestamp_ms = f$timestamp,
        action = f$deltaSingleAction
      ))
    }
    NULL
  })
  entries <- purrr::compact(entries)
  by_version <- split(
    entries,
    purrr::map_chr(entries, function(entry) as.character(entry$version))
  )
  by_version <- purrr::map(by_version, function(entries) {
    last_file <- purrr::detect(
      rev(entries),
      function(entry) !is.null(entry$timestamp_ms)
    )
    list(
      version = entries[[1L]]$version,
      timestamp_ms = last_file$timestamp_ms %||% NA_real_,
      actions = purrr::map(entries, "action")
    )
  })
  if (is.null(protocol)) {
    abort(
      "The change data feed response did not include a protocol.",
      type = "protocol",
      operation = "changes"
    )
  }
  if (length(by_version) == 0L) {
    abort(
      "The change data feed response did not include metadata or file actions.",
      type = "protocol",
      operation = "changes"
    )
  }
  versions <- purrr::map_dbl(by_version, "version")
  below_start <- !is.null(start_version) && any(versions < start_version)
  above_end <- !is.null(end_version) && any(versions > end_version)
  if (below_start || above_end) {
    abort(
      "The change data feed response contained actions outside the requested range.",
      type = "protocol",
      operation = "changes"
    )
  }
  list(
    protocol = protocol,
    by_version = by_version,
    start_version = start_version %||% min(versions),
    end_version = max(versions)
  )
}

encode_predicate_hint <- function(predicate) {
  jsonlite::toJSON(
    predicate,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
}

query_body <- function(spec, page_token) {
  body <- list()
  if (!is.null(spec$predicate)) {
    body$jsonPredicateHints <- encode_predicate_hint(spec$predicate)
  }
  if (!is.null(spec$limit)) body$limitHint <- spec$limit
  if (!is.null(spec$version)) {
    body$version <- spec$version
  } else if (!is.null(spec$timestamp)) {
    body$timestamp <- format_timestamp(spec$timestamp)
  }
  if (!is.null(page_token)) body$pageToken <- page_token
  # An empty body must serialize as a JSON object `{}`, not an array `[]`; the
  # server rejects the latter. This is the common case: a plain latest-version
  # snapshot with no options.
  if (length(body) == 0L) {
    return(rlang::set_names(list(), character()))
  }
  body
}

# Build a synthetic log from a parsed query and open a native snapshot stream.
sharing_snapshot_stream <- function(
  profile,
  auth,
  identifier,
  spec,
  batch_size = DEFAULT_BATCH_SIZE
) {
  fmt <- resolve_query_format(
    profile,
    auth,
    identifier,
    spec$response_format
  )
  log <- prepare_snapshot_query_log(
    profile,
    auth,
    identifier,
    spec,
    format = fmt
  )

  # If native construction fails, clean up here; on success the native stream
  # owns the temp log root and deletes it on release.
  ownership_transferred <- FALSE
  on.exit(
    {
      if (!ownership_transferred) {
        log$cleanup()
      }
    },
    add = TRUE
  )

  stream <- native_snapshot_stream(
    table_location = log$path,
    columns = spec$columns,
    limit = spec$limit,
    batch_size = batch_size,
    cleanup_root = log$root
  )
  ownership_transferred <- TRUE
  stream
}

sharing_changes_stream <- function(
  profile,
  auth,
  identifier,
  spec,
  batch_size = DEFAULT_BATCH_SIZE
) {
  # Change data feed is read through the kernel, which requires delta format;
  # the parquet CDF path is not supported. An explicit parquet request is a
  # user error, so reject it rather than silently upgrading.
  if (identical(spec$response_format, "parquet")) {
    abort(
      "Parquet-format change data feed is not supported.",
      type = "unsupported",
      operation = "changes",
      feature = "parquet_cdf"
    )
  }
  parsed <- sharing_query_changes(profile, auth, identifier, spec)
  log <- prepare_cdf_log(
    parsed$protocol,
    parsed$by_version,
    parsed$start_version,
    parsed$end_version
  )

  ownership_transferred <- FALSE
  on.exit(
    {
      if (!ownership_transferred) {
        log$cleanup()
      }
    },
    add = TRUE
  )

  stream <- native_cdf_stream(
    table_location = log$path,
    start_version = log$start_version,
    end_version = log$end_version,
    columns = spec$columns,
    batch_size = batch_size,
    cleanup_root = log$root
  )
  ownership_transferred <- TRUE
  stream
}

# ---- Materializers ---------------------------------------------------------

require_arrow <- function(operation) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    abort(
      "The optional package {.pkg arrow} is required for {.fn {operation}}.",
      type = "unsupported",
      operation = operation,
      feature = "arrow_package"
    )
  }
}

sharing_stream_to_arrow_reader <- function(
  stream,
  operation = "to_arrow_reader"
) {
  require_arrow(operation)
  arrow::RecordBatchReader$import_from_c(stream)
}

sharing_stream_to_arrow <- function(stream) {
  require_arrow("to_arrow")
  on.exit(release_materializer_stream(stream), add = TRUE)
  reader <- sharing_stream_to_arrow_reader(stream, operation = "to_arrow")
  with_native_stream_conditions(
    reader$read_table(),
    operation = "read_arrow_stream",
    stream = stream
  )
}

sharing_stream_to_data_frame <- function(stream) {
  on.exit(release_materializer_stream(stream), add = TRUE)
  with_native_stream_conditions(
    as.data.frame(nanoarrow::convert_array_stream(stream)),
    operation = "read_arrow_stream",
    stream = stream
  )
}
