# Read orchestration: turn a snapshot/changes spec into a Delta Kernel Arrow
# stream. R performs the Query Table request, parses the NDJSON response into
# protocol/metadata/file actions, streams a synthetic `_delta_log`, and hands
# the local path to the native kernel scan. Cleanup of the temp log is
# transferred to the native stream once it is constructed.

# The query endpoints require a single response format, unlike /metadata.
query_capabilities <- function(format, for_cdf = FALSE) {
  paste0(
    capability_header(format, for_cdf = for_cdf),
    ";includeendstreamaction=true"
  )
}

# Query a table's files and return parsed { protocol, metadata, files }.
# `format` is already resolved to "delta" or "parquet". Files are collected
# across pages because the kernel needs the full manifest to scan.
sharing_query_table <- function(
  profile,
  auth,
  identifier,
  spec,
  format
) {
  actions <- list()
  page_token <- NULL
  repeat {
    req <- sharing_request(
      profile,
      auth,
      c(table_path(identifier), "query"),
      method = "POST",
      operation = "read"
    )
    req <- httr2::req_headers(
      req,
      `delta-sharing-capabilities` = query_capabilities(format)
    )
    req <- httr2::req_body_json(req, query_body(spec, page_token))
    resp <- sharing_perform(req)
    page_actions <- parse_ndjson_lines(httr2::resp_body_string(resp), "read")
    actions <- c(actions, page_actions)
    page_token <- find_next_page_token(page_actions)
    if (is.null(page_token)) {
      break
    }
  }
  split_query_actions(actions, "read")
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
  actions <- list()
  page_token <- NULL
  repeat {
    req <- sharing_request(
      profile,
      auth,
      c(table_path(identifier), "changes"),
      method = "GET",
      query = changes_query(spec, page_token),
      operation = "changes"
    )
    req <- httr2::req_headers(
      req,
      `delta-sharing-capabilities` = query_capabilities(
        "delta",
        for_cdf = TRUE
      )
    )
    resp <- sharing_perform(req)
    page_actions <- parse_ndjson_lines(httr2::resp_body_string(resp), "changes")
    actions <- c(actions, page_actions)
    page_token <- find_next_page_token(page_actions)
    if (is.null(page_token)) {
      break
    }
  }
  bucket_cdf_actions(
    actions,
    spec$starting_version,
    spec$ending_version
  )
}

bucket_cdf_actions <- function(actions, start_version, end_version) {
  protocol <- NULL
  by_version <- list() # keyed by as.character(version)
  ensure <- function(v) {
    key <- as.character(v)
    if (is.null(by_version[[key]])) {
      by_version[[key]] <<- list(
        version = v,
        timestamp_ms = NA_real_,
        actions = list()
      )
    }
    key
  }
  for (action in actions) {
    if (!is.null(action$protocol)) {
      protocol <- action$protocol$deltaProtocol %||% action$protocol
    } else if (!is.null(action$metaData)) {
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
      key <- ensure(v)
      by_version[[key]]$actions <- c(
        by_version[[key]]$actions,
        list(list(
          metaData = action$metaData$deltaMetadata %||% action$metaData
        ))
      )
    } else if (!is.null(action$file)) {
      f <- action$file
      key <- ensure(f$version)
      by_version[[key]]$timestamp_ms <- as.numeric(f$timestamp)
      by_version[[key]]$actions <- c(
        by_version[[key]]$actions,
        list(f$deltaSingleAction)
      )
    }
  }
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
  as.character(jsonlite::toJSON(
    predicate,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  ))
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

find_next_page_token <- function(actions) {
  action <- purrr::detect(actions, function(action) {
    token <- action$nextPageToken
    is_scalar_character(token) && nzchar(token)
  })
  action$nextPageToken %||% NULL
}

# Separate protocol/metadata/file actions and detect the response format.
split_query_actions <- function(actions, operation) {
  protocol <- NULL
  metadata <- NULL
  files <- list()
  response_format <- "parquet"
  for (action in actions) {
    if (!is.null(action$protocol)) {
      protocol <- action$protocol
      if (!is.null(protocol$deltaProtocol)) response_format <- "delta"
    } else if (!is.null(action$metaData)) {
      metadata <- action$metaData
      if (!is.null(metadata$deltaMetadata)) response_format <- "delta"
    } else if (!is.null(action$file)) {
      files[[length(files) + 1L]] <- action$file
    }
  }
  if (is.null(protocol) || is.null(metadata)) {
    abort(
      "The query response did not include protocol and metadata.",
      type = "protocol",
      operation = operation
    )
  }
  list(
    response_format = response_format,
    protocol = protocol,
    metadata = metadata,
    files = files
  )
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
    spec$response_format,
    "read"
  )
  parsed <- sharing_query_table(
    profile,
    auth,
    identifier,
    spec,
    format = fmt
  )

  lines <- synthetic_log_lines(
    fmt,
    parsed$protocol,
    parsed$metadata,
    parsed$files,
    "read"
  )
  log <- prepare_synthetic_log(lines)

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

# Run an eager materializer with an indeterminate cli progress bar. The native
# scan cannot know the final row count when predicates, deletion vectors, or
# CDF actions are involved, so progress reports rows actually emitted.
with_read_progress <- function(progress, materialize) {
  if (!isTRUE(progress)) {
    return(materialize(NULL))
  }

  progress_id <- cli::cli_progress_bar("Reading rows", total = NA)
  completed <- FALSE
  on.exit(
    cli::cli_progress_done(
      progress_id,
      result = if (completed) "done" else "failed"
    ),
    add = TRUE
  )

  rows_read <- 0
  update <- function(rows) {
    rows_read <<- rows_read + rows
    cli::cli_progress_update(id = progress_id, set = rows_read)
  }

  result <- materialize(update)
  completed <- TRUE
  result
}

# Pull a native stream in R so an eager materializer can report each emitted
# batch, then replay those Arrow arrays into the requested output adapter.
collect_stream_batches <- function(stream, update) {
  schema <- stream$get_schema()
  batches <- list()

  repeat {
    batch <- stream$get_next(schema)
    if (is.null(batch)) {
      break
    }
    batches[[length(batches) + 1L]] <- batch
    update(batch$length)
  }

  nanoarrow::basic_array_stream(batches, schema = schema)
}

sharing_stream_to_arrow <- function(stream, progress = FALSE) {
  require_arrow("to_arrow")
  with_read_progress(progress, function(update) {
    if (is.null(update)) {
      reader <- sharing_stream_to_arrow_reader(stream, operation = "to_arrow")
    } else {
      batches <- collect_stream_batches(stream, update)
      reader <- sharing_stream_to_arrow_reader(batches, operation = "to_arrow")
    }
    reader$read_table()
  })
}

sharing_stream_to_data_frame <- function(stream, progress = FALSE) {
  with_read_progress(progress, function(update) {
    if (!is.null(update)) {
      stream <- collect_stream_batches(stream, update)
    }
    as.data.frame(nanoarrow::convert_array_stream(stream))
  })
}
