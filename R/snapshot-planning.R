.snapshot_query_operation <- "query_table"
.snapshot_default_max_files_per_page <- 100000L
.snapshot_max_chunks_per_page <- 1000000L
.prepared_snapshot_registry <- new.env(hash = TRUE, parent = emptyenv())

.snapshot_planning_abort <- function(message,
                                     type = "protocol",
                                     ...) {
  .abort_delta_sharing(
    message,
    type = type,
    operation = .snapshot_query_operation,
    ...
  )
}

.validate_snapshot_read <- function(read) {
  if (!.object_is(read, SharingRead)) {
    .snapshot_planning_abort(
      "`read` must be a SharingRead.",
      type = "validation"
    )
  }
  if (!is.null(read@version) && !is.null(read@timestamp)) {
    .snapshot_planning_abort(
      "`version` and `timestamp` are mutually exclusive.",
      type = "validation"
    )
  }
  read
}

.snapshot_positive_integer <- function(value, name, maximum = .Machine$integer.max) {
  if (!is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 1 ||
      value != floor(value) ||
      value > maximum) {
    .snapshot_planning_abort(
      sprintf("`%s` must be one supported positive whole number.", name),
      type = "validation"
    )
  }
  as.integer(value)
}

.canonical_snapshot_json <- function(value) {
  if (is.list(value)) {
    if (is.object(value)) {
      .snapshot_planning_abort(
        "The structured predicate contains an unsupported value.",
        type = "validation"
      )
    }
    if (!is.null(names(value))) {
      if (anyNA(names(value)) ||
          any(!nzchar(names(value))) ||
          anyDuplicated(names(value))) {
        .snapshot_planning_abort(
          "The structured predicate contains invalid object fields.",
          type = "validation"
        )
      }
      value <- value[order(names(value), method = "radix")]
    }
    return(lapply(value, .canonical_snapshot_json))
  }
  if (is.null(value)) {
    return(NULL)
  }
  valid <- !is.object(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    (is.character(value) ||
      is.logical(value) ||
      (is.numeric(value) && is.finite(value)))
  if (!valid) {
    .snapshot_planning_abort(
      "The structured predicate contains an unsupported value.",
      type = "validation"
    )
  }
  value
}

.encode_snapshot_predicate <- function(predicate) {
  if (is.null(predicate)) {
    return(NULL)
  }
  if (!.snapshot_has_valid_names(predicate)) {
    .snapshot_planning_abort(
      "The structured predicate must be one JSON object with unique fields.",
      type = "validation"
    )
  }
  canonical <- .canonical_snapshot_json(predicate)
  encoded <- tryCatch(
    jsonlite::toJSON(
      canonical,
      auto_unbox = TRUE,
      null = "null",
      digits = NA
    ),
    error = function(condition) NULL
  )
  if (!.is_scalar_character(encoded) ||
      length(charToRaw(enc2utf8(encoded))) > .ndjson_default_max_line_bytes) {
    .snapshot_planning_abort(
      "The structured predicate could not be encoded safely.",
      type = "validation"
    )
  }
  unclass(encoded)
}

.snapshot_page_token <- function(token) {
  if (is.null(token)) {
    return(NULL)
  }
  if (!.is_scalar_character(token) ||
      grepl("[\r\n]", token) ||
      nchar(token, type = "bytes") > .ndjson_default_max_line_bytes) {
    .snapshot_planning_abort(
      "The snapshot page token is invalid."
    )
  }
  token
}

.snapshot_server_limit_hint <- function(limit) {
  if (is.null(limit) || limit > .Machine$integer.max) {
    return(NULL)
  }
  limit
}

.snapshot_query_capabilities <- function(response_format) {
  response_format <- .normalize_response_format(response_format)
  paste0(
    .snapshot_capability_header(response_format),
    ";includeendstreamaction=true"
  )
}

.new_snapshot_request_plan <- function(path_segments,
                                       headers,
                                       body,
                                       page_number) {
  structure(
    list(
      method = "POST",
      path_segments = path_segments,
      headers = headers,
      body = body,
      page_number = as.integer(page_number),
      operation = .snapshot_query_operation
    ),
    class = c("delta_sharing_snapshot_request", "list")
  )
}

.plan_snapshot_request <- function(
  read,
  page_token = NULL,
  page_number = 1L,
  max_files_per_page = .snapshot_default_max_files_per_page
) {
  read <- .validate_snapshot_read(read)
  page_number <- .snapshot_positive_integer(
    page_number,
    "page_number"
  )
  max_files_per_page <- .snapshot_positive_integer(
    max_files_per_page,
    "max_files_per_page"
  )
  page_token <- .snapshot_page_token(page_token)

  body <- list()
  predicate <- .encode_snapshot_predicate(read@predicate)
  if (!is.null(predicate)) {
    body$jsonPredicateHints <- predicate
  }
  limit_hint <- .snapshot_server_limit_hint(read@limit)
  if (!is.null(limit_hint)) {
    body$limitHint <- limit_hint
  }
  if (!is.null(read@version)) {
    body$version <- read@version
  } else if (!is.null(read@timestamp)) {
    body$timestamp <- .format_protocol_timestamp(read@timestamp)
  }
  body$maxFiles <- max_files_per_page
  if (is.null(page_token) &&
      is.null(read@version) &&
      is.null(read@timestamp)) {
    body$includeRefreshToken <- TRUE
  }
  if (!is.null(page_token)) {
    body$pageToken <- page_token
  }

  identifier <- read@table@identifier
  .new_snapshot_request_plan(
    path_segments = c(
      .table_route_segments(identifier, .snapshot_query_operation),
      "query"
    ),
    headers = list(
      Accept = "application/x-ndjson",
      "delta-sharing-capabilities" =
        .snapshot_query_capabilities(read@response_format),
      fileidhash = "delta"
    ),
    body = body,
    page_number = page_number
  )
}

#' @exportS3Method print delta_sharing_snapshot_request
print.delta_sharing_snapshot_request <- function(x, ...) {
  cat(sprintf(
    "<delta_sharing_snapshot_request> page %d; POST; body redacted\n",
    x$page_number
  ))
  invisible(x)
}

.snapshot_header <- function(headers,
                             name,
                             required = FALSE) {
  if (is.null(headers) || is.null(names(headers))) {
    if (required) {
      .snapshot_planning_abort(
        sprintf("The snapshot response is missing `%s`.", name)
      )
    }
    return(NULL)
  }
  index <- which(tolower(names(headers)) == tolower(name))
  if (length(index) == 0L && !required) {
    return(NULL)
  }
  if (length(index) != 1L) {
    .snapshot_planning_abort(
      sprintf("The snapshot response has an invalid `%s` header.", name)
    )
  }
  value <- headers[[index]]
  if (!.is_scalar_character(value) || grepl("[\r\n]", value)) {
    .snapshot_planning_abort(
      sprintf("The snapshot response has an invalid `%s` header.", name)
    )
  }
  value
}

.parse_snapshot_capabilities <- function(headers) {
  header <- .snapshot_header(
    headers,
    "delta-sharing-capabilities",
    required = FALSE
  )
  if (is.null(header)) {
    return(list())
  }
  entries <- strsplit(tolower(header), ";", fixed = TRUE)[[1L]]
  entries <- trimws(entries)
  if (length(entries) == 0L || any(!nzchar(entries))) {
    .snapshot_planning_abort(
      "The snapshot response has an invalid capabilities header."
    )
  }
  capabilities <- list()
  for (entry in entries) {
    split <- strsplit(entry, "=", fixed = TRUE)[[1L]]
    if (length(split) != 2L ||
        !nzchar(trimws(split[[1L]])) ||
        !nzchar(trimws(split[[2L]]))) {
      .snapshot_planning_abort(
        "The snapshot response has an invalid capabilities header."
      )
    }
    key <- trimws(split[[1L]])
    value <- trimws(split[[2L]])
    if (key %in% names(capabilities)) {
      .snapshot_planning_abort(
        "The snapshot response repeats a capability."
      )
    }
    if (key %in% c(
      "responseformat",
      "readerfeatures",
      "includeendstreamaction"
    )) {
      capabilities[[key]] <- value
    }
  }

  format <- capabilities$responseformat
  if (!is.null(format)) {
    formats <- trimws(strsplit(format, ",", fixed = TRUE)[[1L]])
    if (length(formats) != 1L ||
        !formats %in% .snapshot_response_formats) {
      .snapshot_planning_abort(
        "The snapshot response selected an invalid response format."
      )
    }
    capabilities$responseformat <- formats
  }

  features <- capabilities$readerfeatures
  if (!is.null(features)) {
    features <- trimws(strsplit(features, ",", fixed = TRUE)[[1L]])
    if (any(!nzchar(features)) ||
        anyDuplicated(features) ||
        length(setdiff(features, .snapshot_reader_features)) > 0L) {
      .snapshot_planning_abort(
        "The snapshot response selected unsupported reader features.",
        type = "unsupported",
        response_format = "delta",
        feature = "reader_feature"
      )
    }
    capabilities$readerfeatures <- features
  }

  terminal <- capabilities$includeendstreamaction
  if (!is.null(terminal) && !terminal %in% c("true", "false")) {
    .snapshot_planning_abort(
      "The snapshot response has an invalid terminal-action capability."
    )
  }
  capabilities
}

.validate_snapshot_response_headers <- function(headers) {
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
    .snapshot_planning_abort(
      "The snapshot response is not NDJSON."
    )
  }

  file_id_hash <- .snapshot_header(
    headers,
    "fileidhash",
    required = TRUE
  )
  if (!identical(tolower(file_id_hash), "delta")) {
    .snapshot_planning_abort(
      "The snapshot response used an inconsistent file ID scheme."
    )
  }

  list(
    table_version = .parse_table_version_header(
      headers,
      operation = .snapshot_query_operation
    ),
    capabilities = .parse_snapshot_capabilities(headers)
  )
}

.next_snapshot_chunk <- function(response) {
  outcome <- tryCatch(
    list(value = response$pull(), condition = NULL),
    delta_sharing_error = function(condition) {
      list(value = NULL, condition = condition)
    },
    error = function(condition) {
      list(
        value = NULL,
        condition = .new_delta_sharing_condition(
          "The streamed snapshot response could not be read.",
          type = "protocol",
          operation = .snapshot_query_operation
        )
      )
    }
  )
  if (!is.null(outcome$condition)) {
    stop(outcome$condition)
  }
  outcome$value
}

.consume_snapshot_page <- function(
  response,
  max_line_bytes = .ndjson_default_max_line_bytes,
  max_chunks = .snapshot_max_chunks_per_page,
  file_handler = NULL
) {
  response <- .normalize_snapshot_pull_response(response)
  max_chunks <- .snapshot_positive_integer(max_chunks, "max_chunks")
  if (!is.null(file_handler) && !is.function(file_handler)) {
    .snapshot_planning_abort(
      "`file_handler` must be a function.",
      type = "validation"
    )
  }
  close_guard <- .new_snapshot_pull_close_guard(response)
  on.exit(.close_snapshot_pull_guard(close_guard), add = TRUE)

  status <- response$status
  if (!is.numeric(status) ||
      length(status) != 1L ||
      is.na(status) ||
      !is.finite(status) ||
      status != floor(status) ||
      status < 100 ||
      status > 599) {
    .snapshot_planning_abort(
      "The snapshot response has an invalid HTTP status."
    )
  }
  if (status < 200 || status >= 300) {
    .snapshot_planning_abort(
      "The Delta Sharing server rejected the snapshot request.",
      type = "http",
      status = as.integer(status)
    )
  }
  header_state <- .validate_snapshot_response_headers(response$headers)

  decoder <- .new_ndjson_decoder(
    operation = .snapshot_query_operation,
    max_line_bytes = max_line_bytes
  )
  protocol <- NULL
  metadata <- NULL
  files <- list()
  file_count <- 0L
  file_size <- 0
  terminal <- NULL
  action_count <- 0L

  handle_actions <- function(actions) {
    for (action in actions) {
      action_count <<- action_count + 1L
      if (!inherits(action, "delta_sharing_ndjson_action")) {
        .snapshot_planning_abort(
          "The snapshot response contains an invalid action."
        )
      }
      if (!is.null(terminal)) {
        .snapshot_planning_abort(
          "The terminal action must be the final NDJSON action."
        )
      }
      if (action_count == 1L) {
        if (!identical(action$type, "protocol")) {
          .snapshot_planning_abort(
            "The first snapshot action must be `protocol`."
          )
        }
        protocol <<- action$value
      } else if (action_count == 2L) {
        if (!identical(action$type, "metadata")) {
          .snapshot_planning_abort(
            "The second snapshot action must be `metaData`."
          )
        }
        metadata <<- action$value
      } else if (identical(action$type, "file")) {
        if (file_count >= .snapshot_log_max_files) {
          .snapshot_planning_abort(
            "The snapshot response contains too many file actions."
          )
        }
        file_state <- .snapshot_file_state(action$value)
        if (!identical(
          file_state$response_format,
          protocol$response_format
        )) {
          .snapshot_planning_abort(
            "The snapshot response mixes file response formats."
          )
        }
        if (identical(protocol$response_format, "parquet")) {
          version <- .snapshot_file_version(action$value)
          if (!is.null(version) &&
              !identical(version, header_state$table_version)) {
            .snapshot_planning_abort(
              "The Parquet snapshot response has inconsistent table versions."
            )
          }
          file_size <<- file_size +
            file_state$delta_action$add$size
        }
        file_count <<- file_count + 1L
        if (is.null(file_handler)) {
          files[[length(files) + 1L]] <<- action$value
        } else {
          file_handler(
            action$value,
            protocol,
            metadata,
            header_state$table_version
          )
        }
      } else if (identical(action$type, "end_stream")) {
        terminal <<- action$value
      } else {
        .snapshot_planning_abort(
          "The snapshot response contains an unexpected action."
        )
      }
    }
    invisible(NULL)
  }

  chunk_count <- 0L
  repeat {
    chunk <- .next_snapshot_chunk(response)
    if (is.null(chunk)) {
      break
    }
    chunk_count <- chunk_count + 1L
    if (chunk_count > max_chunks) {
      .snapshot_planning_abort(
        "The snapshot response exceeded the internal chunk limit."
      )
    }
    handle_actions(.ndjson_decoder_push(decoder, chunk))
  }
  handle_actions(.ndjson_decoder_finish(decoder))

  if (is.null(protocol) || is.null(metadata)) {
    .snapshot_planning_abort(
      "The snapshot response is missing protocol or metadata."
    )
  }
  if (!identical(protocol$response_format, metadata$response_format)) {
    .snapshot_planning_abort(
      "The snapshot response uses inconsistent response formats."
    )
  }
  response_format <- protocol$response_format
  selected_format <- header_state$capabilities$responseformat
  if (!is.null(selected_format) &&
      !identical(selected_format, response_format)) {
    .snapshot_planning_abort(
      "The snapshot response format does not match the selected capability."
    )
  }
  if (identical(response_format, "parquet")) {
    .validate_parquet_response_versions(
      protocol,
      metadata,
      files,
      header_state$table_version
    )
  }
  terminal_required <- identical(
    header_state$capabilities$includeendstreamaction,
    "true"
  )
  if (is.null(terminal) && terminal_required) {
    .snapshot_planning_abort(
      "The negotiated snapshot stream is missing its terminal action."
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
    files = files,
    terminal = .end_stream_state(
      terminal,
      operation = .snapshot_query_operation
    ),
    response_format = response_format,
    table_version = header_state$table_version,
    capabilities = header_state$capabilities,
    chunk_count = chunk_count,
    file_count = as.integer(file_count),
    file_size = file_size
  )
}

.snapshot_now <- function(clock) {
  if (!is.function(clock)) {
    stop("`clock` must be a function.", call. = FALSE)
  }
  now <- clock()
  if (!inherits(now, "POSIXct") ||
      length(now) != 1L ||
      is.na(now) ||
      !is.finite(as.double(now))) {
    stop("`clock` must return one non-missing POSIXct value.", call. = FALSE)
  }
  structure(as.double(now), class = c("POSIXct", "POSIXt"), tzone = "UTC")
}

.snapshot_expiration_time <- function(milliseconds) {
  if (is.null(milliseconds)) {
    return(NULL)
  }
  structure(
    milliseconds / 1000,
    class = c("POSIXct", "POSIXt"),
    tzone = "UTC"
  )
}

.assert_snapshot_urls_live <- function(expiration, clock) {
  if (!is.null(expiration) && expiration <= .snapshot_now(clock)) {
    .snapshot_planning_abort(
      "Snapshot URLs expired before preparation completed.",
      type = "http"
    )
  }
  invisible(expiration)
}

.safe_snapshot_fetch <- function(fetch, request) {
  if (!is.function(fetch)) {
    stop("`fetch` must be a function.", call. = FALSE)
  }
  outcome <- tryCatch(
    list(value = fetch(request), condition = NULL),
    delta_sharing_error = function(condition) {
      list(value = NULL, condition = condition)
    },
    error = function(condition) {
      list(
        value = NULL,
        condition = .new_delta_sharing_condition(
          "The snapshot request could not be completed.",
          type = "http",
          operation = .snapshot_query_operation
        )
      )
    }
  )
  if (!is.null(outcome$condition)) {
    stop(outcome$condition)
  }
  outcome$value
}

.same_snapshot_page_value <- function(left, right) {
  isTRUE(identical(left, right))
}

.new_prepared_snapshot <- function(guard,
                                   invocation,
                                   diagnostics,
                                   refresh_token) {
  state <- new.env(parent = emptyenv())
  state$guard <- guard
  state$invocation <- invocation
  state$diagnostics <- diagnostics
  state$refresh_token <- refresh_token
  state$released <- FALSE

  prepared <- new.env(parent = emptyenv())
  attr(prepared, ".state_handle") <- .new_private_handle(
    .prepared_snapshot_registry,
    state,
    "prepared-snapshot"
  )
  class(prepared) <- "delta_sharing_prepared_snapshot"
  lockEnvironment(prepared, bindings = TRUE)
  reg.finalizer(
    prepared,
    function(value) {
      handle <- attr(value, ".state_handle", exact = TRUE)
      if (is.environment(handle) &&
          .is_scalar_character(handle$id) &&
          exists(
            handle$id,
            envir = .prepared_snapshot_registry,
            inherits = FALSE
          )) {
        state <- get(
          handle$id,
          envir = .prepared_snapshot_registry,
          inherits = FALSE
        )
        if (!isTRUE(state$released)) {
          try(.release_snapshot_log(state$guard), silent = TRUE)
          state$released <- TRUE
        }
      }
      invisible(NULL)
    },
    onexit = TRUE
  )
  prepared
}

.prepared_snapshot_state <- function(prepared) {
  if (!inherits(prepared, "delta_sharing_prepared_snapshot") ||
      !is.environment(prepared)) {
    .snapshot_planning_abort(
      "`prepared` must be a prepared snapshot.",
      type = "validation"
    )
  }
  tryCatch(
    .private_handle_value(
      prepared,
      ".state_handle",
      .prepared_snapshot_registry,
      "prepared snapshot state"
    ),
    delta_sharing_error = function(condition) {
      .snapshot_planning_abort(
        "The prepared snapshot state is no longer available.",
        type = "validation"
      )
    }
  )
}

.prepared_snapshot_invocation <- function(prepared) {
  state <- .prepared_snapshot_state(prepared)
  if (isTRUE(state$released)) {
    .snapshot_planning_abort(
      "The prepared snapshot has already been released.",
      type = "validation"
    )
  }
  state$invocation
}

.prepared_snapshot_diagnostics <- function(prepared) {
  state <- .prepared_snapshot_state(prepared)
  state$diagnostics
}

.prepared_snapshot_refresh_token <- function(prepared) {
  state <- .prepared_snapshot_state(prepared)
  if (isTRUE(state$released)) {
    return(NULL)
  }
  state$refresh_token
}

.release_prepared_snapshot <- function(prepared) {
  state <- .prepared_snapshot_state(prepared)
  if (!isTRUE(state$released)) {
    .release_snapshot_log(state$guard)
    state$released <- TRUE
  }
  invisible(TRUE)
}

#' @exportS3Method print delta_sharing_prepared_snapshot
print.delta_sharing_prepared_snapshot <- function(x, ...) {
  state <- .prepared_snapshot_state(x)
  diagnostics <- state$diagnostics
  cat(sprintf(
    "<delta_sharing_prepared_snapshot> %s; version %.0f; %d file action%s; %d page%s\n",
    if (isTRUE(state$released)) "released" else "active",
    diagnostics$table_version,
    diagnostics$file_count,
    if (identical(diagnostics$file_count, 1L)) "" else "s",
    diagnostics$page_count,
    if (identical(diagnostics$page_count, 1L)) "" else "s"
  ))
  invisible(x)
}

.prepare_snapshot_read <- function(
  read,
  fetch,
  temp_parent = tempdir(),
  max_pages = .pagination_page_limit,
  max_files_per_page = .snapshot_default_max_files_per_page,
  max_line_bytes = .ndjson_default_max_line_bytes,
  max_chunks_per_page = .snapshot_max_chunks_per_page,
  clock = Sys.time,
  write_commit = .write_snapshot_commit,
  stage_run_files = .snapshot_stage_run_files
) {
  read <- .validate_snapshot_read(read)
  max_pages <- .snapshot_positive_integer(max_pages, "max_pages")
  max_files_per_page <- .snapshot_positive_integer(
    max_files_per_page,
    "max_files_per_page"
  )
  max_chunks_per_page <- .snapshot_positive_integer(
    max_chunks_per_page,
    "max_chunks_per_page"
  )
  now <- .snapshot_now(clock)
  if (!is.function(write_commit)) {
    stop("`write_commit` must be a function.", call. = FALSE)
  }
  stage_run_files <- .snapshot_stage_positive_integer(
    stage_run_files,
    "stage_run_files"
  )

  seen_tokens <- new.env(parent = emptyenv(), hash = TRUE)
  page_token <- NULL
  page_count <- 0L
  file_count <- 0L
  protocol <- NULL
  metadata <- NULL
  table_version <- NULL
  refresh_token <- NULL
  min_expiration <- NULL
  stage <- .new_snapshot_stage(
    temp_parent = temp_parent,
    run_files = stage_run_files
  )
  keep_stage <- FALSE
  on.exit({
    if (!keep_stage) {
      try(.release_snapshot_stage(stage), silent = TRUE)
    }
  }, add = TRUE)
  stage_file <- function(file,
                         page_protocol,
                         page_metadata,
                         page_table_version) {
    stage_state <- .snapshot_stage_state(stage)
    if (!isTRUE(stage_state$initialized)) {
      if (!identical(
        page_protocol$response_format,
        page_metadata$response_format
      )) {
        .snapshot_planning_abort(
          "The snapshot response uses inconsistent response formats."
        )
      }
      .initialize_snapshot_stage(stage, page_protocol, page_metadata)
    }
    .snapshot_stage_add_file(stage, file)
    file_expiration <- .snapshot_expiration_time(
      .snapshot_file_expiration_timestamp(file)
    )
    if (!is.null(file_expiration) &&
        (is.null(min_expiration) ||
          file_expiration < min_expiration)) {
      min_expiration <<- file_expiration
    }
    invisible(page_table_version)
  }

  repeat {
    if (page_count >= max_pages) {
      .snapshot_planning_abort(
        "Snapshot pagination exceeded the internal page limit."
      )
    }
    page_number <- page_count + 1L
    request <- .plan_snapshot_request(
      read,
      page_token = page_token,
      page_number = page_number,
      max_files_per_page = max_files_per_page
    )
    page <- .consume_snapshot_page(
      .safe_snapshot_fetch(fetch, request),
      max_line_bytes = max_line_bytes,
      max_chunks = max_chunks_per_page,
      file_handler = stage_file
    )
    page_count <- page_number
    if (!isTRUE(.snapshot_stage_state(stage)$initialized)) {
      .initialize_snapshot_stage(stage, page$protocol, page$metadata)
    }

    if (page_count == 1L) {
      protocol <- page$protocol
      metadata <- page$metadata
      table_version <- page$table_version
      # A server that does not recognize the capabilities header may return
      # the protocol-default Parquet format after Delta was requested. The
      # reverse mismatch is unsafe because a Parquet-only client did not
      # advertise Delta reader-feature support.
      format_mismatch <- !identical(read@response_format, "auto") &&
        !identical(read@response_format, page$response_format) &&
        !(identical(read@response_format, "delta") &&
          identical(page$response_format, "parquet"))
      if (format_mismatch) {
        .snapshot_planning_abort(
          "The server selected a different snapshot response format than requested."
        )
      }
      if (!is.null(read@version) &&
          !identical(read@version, table_version)) {
        .snapshot_planning_abort(
          "The server returned a different table version than requested."
        )
      }
    } else if (!.same_snapshot_page_value(protocol, page$protocol) ||
        !.same_snapshot_page_value(metadata, page$metadata) ||
        !identical(table_version, page$table_version)) {
      .snapshot_planning_abort(
        "The snapshot response changed across pages."
      )
    }

    if (file_count + page$file_count > .snapshot_log_max_files) {
      .snapshot_planning_abort(
        "The snapshot response contains too many file actions."
      )
    }
    file_count <- file_count + page$file_count
    page_refresh_token <- page$terminal$refresh_token
    if (!is.null(page_refresh_token)) {
      if (!is.null(refresh_token) &&
          !identical(refresh_token, page_refresh_token)) {
        .snapshot_planning_abort(
          "The snapshot refresh token changed across pages."
        )
      }
      refresh_token <- page_refresh_token
    }
    page_expiration <- .snapshot_expiration_time(
      page$terminal$min_url_expiration_timestamp
    )
    if (!is.null(page_expiration) &&
        (is.null(min_expiration) ||
          page_expiration < min_expiration)) {
      min_expiration <- page_expiration
    }

    page_token <- page$terminal$next_page_token
    if (is.null(page_token)) {
      break
    }
    if (exists(page_token, envir = seen_tokens, inherits = FALSE)) {
      .snapshot_planning_abort(
        "The server repeated a snapshot page token."
      )
    }
    assign(page_token, TRUE, envir = seen_tokens)
    .assert_snapshot_urls_live(min_expiration, clock)
  }

  .assert_snapshot_urls_live(min_expiration, clock)
  if (identical(protocol$response_format, "parquet")) {
    stage_state <- .snapshot_stage_state(stage)
    .validate_parquet_response_total_values(
      metadata,
      file_count,
      stage_state$total_size
    )
  }
  guard <- .publish_snapshot_stage(
    stage,
    write_commit = write_commit
  )
  keep_stage <- TRUE
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
    read_kind = "snapshot",
    version = 0,
    projection = read@columns,
    exact_limit = read@limit
  )
  diagnostics <- list(
    response_format = protocol$response_format,
    table_version = table_version,
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
    },
    predicate_hint_sent = !is.null(read@predicate),
    server_limit_hint = .snapshot_server_limit_hint(read@limit)
  )
  prepared <- .new_prepared_snapshot(
    guard = guard,
    invocation = invocation,
    diagnostics = diagnostics,
    refresh_token = refresh_token
  )
  keep_guard <- TRUE
  prepared
}
