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

# Stream Query Table pages into one bounded action staging connection. Only the
# latest protocol/metadata wrappers, progress counters, and page token remain
# in memory; file actions are normalized and written as each chunk arrives.
stream_snapshot_query <- function(
  profile,
  auth,
  identifier,
  spec,
  format,
  output
) {
  protocol <- NULL
  metadata <- NULL
  total_rows <- 0
  total_rows_known <- TRUE
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
    next_page_token <- NULL

    sharing_stream_lines(req, function(lines) {
      actions <- parse_ndjson_lines(lines, "read")
      purrr::walk(actions, function(action) {
        if (!is.null(action$protocol)) {
          protocol <<- action$protocol
        } else if (!is.null(action$metaData)) {
          metadata <<- action$metaData
        }
        token <- action$nextPageToken
        if (is_scalar_character(token) && nzchar(token)) {
          next_page_token <<- token
        }
      })

      files <- purrr::map(
        purrr::keep(actions, function(action) !is.null(action$file)),
        "file"
      )
      if (length(files) == 0L) {
        return(invisible(NULL))
      }

      file_lines <- purrr::map_chr(files, function(file) {
        log_json_line(synthetic_file_action(file, format, "read"))
      })
      writeLines(file_lines, output, useBytes = TRUE)

      rows <- purrr::map_dbl(files, snapshot_file_rows, format = format)
      if (anyNA(rows)) {
        total_rows_known <<- FALSE
      } else if (total_rows_known) {
        total_rows <<- total_rows + sum(rows)
        if (!is.finite(total_rows) || total_rows > 2^53) {
          total_rows_known <<- FALSE
        }
      }
      invisible(NULL)
    })

    page_token <- next_page_token
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

  if (!is.null(spec$limit) && spec$limit == 0) {
    total_rows <- 0
    total_rows_known <- TRUE
  } else if (total_rows_known && !is.null(spec$limit)) {
    total_rows <- min(total_rows, spec$limit)
  }

  list(
    protocol = protocol,
    metadata = metadata,
    total_rows = if (total_rows_known) total_rows else NULL
  )
}

# Prepare the private snapshot log while the HTTP response is being consumed.
# The staging file is inside the private root and is removed before the handle
# transfers to the native cleanup guard.
prepare_snapshot_query_log <- function(
  profile,
  auth,
  identifier,
  spec,
  format
) {
  query_result <- NULL
  log <- prepare_log(function(log_dir) {
    staged_actions <- fs::path(log_dir, ".snapshot-actions")
    query_result <<- local({
      output <- file(staged_actions, open = "wb")
      on.exit(close(output), add = TRUE)
      stream_snapshot_query(
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
  })
  log$total_rows <- query_result$total_rows
  log
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

find_next_page_token <- function(actions) {
  action <- purrr::detect(actions, function(action) {
    token <- action$nextPageToken
    is_scalar_character(token) && nzchar(token)
  })
  action$nextPageToken %||% NULL
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

# Return the logical row count for one snapshot file action. Delta statistics
# are JSON strings; parquet responses expose the same field on the file wrapper.
snapshot_file_rows <- function(file, format) {
  add <- if (identical(format, "delta")) {
    (file$deltaSingleAction %||% file)$add
  } else {
    file
  }
  if (is.null(add)) {
    return(NA_real_)
  }

  stats <- add$stats
  if (is_scalar_character(stats)) {
    stats <- tryCatch(
      jsonlite::fromJSON(stats, simplifyVector = FALSE),
      error = function(...) NULL
    )
  }
  rows <- stats$numRecords
  if (!rlang::is_scalar_integerish(rows, finite = TRUE) || rows < 0) {
    return(NA_real_)
  }

  deleted <- add$deletionVector$cardinality %||% 0
  if (
    !rlang::is_scalar_integerish(deleted, finite = TRUE) ||
      deleted < 0 ||
      deleted > rows
  ) {
    return(NA_real_)
  }
  as.numeric(rows - deleted)
}

# A snapshot percentage is exact only when every returned file supplies usable
# row statistics. A provider limit is exact once enough logical rows exist.
snapshot_total_rows <- function(files, format, limit = NULL) {
  rows <- purrr::map_dbl(files, snapshot_file_rows, format = format)
  if (anyNA(rows)) {
    return(NULL)
  }

  total <- sum(rows)
  if (!is.finite(total) || total > 2^53) {
    return(NULL)
  }
  if (is.null(limit)) total else min(total, limit)
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
  log <- prepare_snapshot_query_log(
    profile,
    auth,
    identifier,
    spec,
    format = fmt
  )
  total_rows <- log$total_rows

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
  attr(stream, "delta_sharing_progress") <- list(total_rows = total_rows)
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
  attr(stream, "delta_sharing_progress") <- list(
    total_rows = NULL,
    versions = c(log$start_version, log$end_version)
  )
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

# Drain an eager read on a native worker while R polls lightweight counters.
# Repainting is forced even when no batch has arrived, so the spinner remains
# live while the worker is blocked on object-store I/O.
collect_stream_with_progress <- function(stream, progress_info = NULL) {
  total_rows <- progress_info$total_rows
  determinate <- !is.null(total_rows)
  format <- if (determinate) {
    paste(
      "{cli::pb_spin} {cli::pb_name} {cli::pb_bar} {cli::pb_percent}",
      "| {cli::pb_current}/{cli::pb_total} rows | ETA {cli::pb_eta}"
    )
  } else {
    "{cli::pb_spin} {cli::pb_name} {cli::pb_current} rows"
  }
  name <- if (is.null(progress_info$versions)) {
    "Reading rows"
  } else {
    sprintf(
      "Reading rows (versions %.0f-%.0f)",
      progress_info$versions[[1]],
      progress_info$versions[[2]]
    )
  }
  progress_id <- cli::cli_progress_bar(
    name,
    total = total_rows %||% NA,
    format = format
  )
  completed <- FALSE
  on.exit(
    cli::cli_progress_done(
      progress_id,
      result = if (completed) "done" else "failed"
    ),
    add = TRUE
  )

  job <- native_collect_start(stream)
  job_active <- TRUE
  force_update <- !identical(getOption("cli.progress_show_after"), Inf)
  on.exit(
    {
      if (job_active) {
        try(native_collect_cancel(job), silent = TRUE)
      }
    },
    add = TRUE
  )

  with_native_stream_conditions(
    {
      repeat {
        status <- native_collect_status(job)
        cli::cli_progress_update(
          id = progress_id,
          set = status$rows,
          force = force_update
        )
        if (isTRUE(status$done)) {
          break
        }
        Sys.sleep(0.05)
      }
    },
    operation = "read_arrow_stream",
    stream = stream
  )

  result <- native_collect_finish(job)
  job_active <- FALSE
  completed <- TRUE
  result
}

sharing_stream_to_arrow <- function(stream, progress = FALSE) {
  require_arrow("to_arrow")
  on.exit(release_materializer_stream(stream), add = TRUE)
  if (isTRUE(progress)) {
    stream <- collect_stream_with_progress(
      stream,
      attr(stream, "delta_sharing_progress")
    )
  }
  reader <- sharing_stream_to_arrow_reader(stream, operation = "to_arrow")
  reader$read_table()
}

sharing_stream_to_data_frame <- function(stream, progress = FALSE) {
  on.exit(release_materializer_stream(stream), add = TRUE)
  if (isTRUE(progress)) {
    stream <- collect_stream_with_progress(
      stream,
      attr(stream, "delta_sharing_progress")
    )
  }
  as.data.frame(nanoarrow::convert_array_stream(stream))
}
