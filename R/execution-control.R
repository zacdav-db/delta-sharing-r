.parse_discovery_http_body <- function(body, operation) {
  parsed <- tryCatch(
    {
      if (!is.raw(body)) {
        stop("invalid body")
      }
      jsonlite::fromJSON(
        rawToChar(body),
        simplifyVector = FALSE,
        simplifyDataFrame = FALSE,
        simplifyMatrix = FALSE
      )
    },
    error = function(error) NULL
  )
  if (!.json_is_object(parsed) ||
      anyNA(names(parsed)) ||
      any(!nzchar(names(parsed))) ||
      anyDuplicated(names(parsed))) {
    .protocol_abort(
      "The server returned an invalid discovery page.",
      operation
    )
  }
  parsed
}

.new_discovery_http_fetcher <- function(client,
                                        operation,
                                        transport,
                                        clock,
                                        sleeper,
                                        random,
                                        max_attempts) {
  force(client)
  force(operation)
  force(transport)
  force(clock)
  force(sleeper)
  force(random)
  force(max_attempts)

  function(path_segments, page_token) {
    query <- if (is.null(page_token)) {
      list()
    } else {
      list(pageToken = page_token)
    }
    response <- .perform_authenticated_http(
      client = client,
      method = "GET",
      path = path_segments,
      query = query,
      operation = operation,
      response_kind = "discovery",
      replayable = TRUE,
      transport = transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts
    )
    .parse_discovery_http_body(response$body, operation)
  }
}

.new_raw_chunk_source <- function(bytes,
                                  chunk_bytes = .http_read_chunk_bytes) {
  if (!is.raw(bytes)) {
    stop("`bytes` must be a raw vector.", call. = FALSE)
  }
  if (!is.numeric(chunk_bytes) ||
      length(chunk_bytes) != 1L ||
      is.na(chunk_bytes) ||
      !is.finite(chunk_bytes) ||
      chunk_bytes < 1 ||
      chunk_bytes != floor(chunk_bytes) ||
      chunk_bytes > .Machine$integer.max) {
    stop("`chunk_bytes` must be one positive whole number.", call. = FALSE)
  }

  offset <- 1L
  chunk_bytes <- as.integer(chunk_bytes)
  function() {
    if (offset > length(bytes)) {
      return(NULL)
    }
    end <- min(length(bytes), offset + chunk_bytes - 1L)
    chunk <- bytes[seq.int(offset, end)]
    offset <<- end + 1L
    chunk
  }
}

.execute_table_http_request <- function(client,
                                        request,
                                        transport,
                                        clock,
                                        sleeper,
                                        random,
                                        max_attempts,
                                        metadata_chunk_bytes) {
  response <- .perform_authenticated_http(
    client = client,
    method = request$method,
    path = request$path_segments,
    query = request$query,
    headers = request$headers,
    operation = request$operation,
    response_kind = "metadata",
    replayable = TRUE,
    transport = transport,
    clock = clock,
    sleeper = sleeper,
    random = random,
    max_attempts = max_attempts
  )
  list(
    headers = response$headers,
    chunks = .new_raw_chunk_source(
      response$body,
      chunk_bytes = metadata_chunk_bytes
    )
  )
}

.normalize_read_batch_size <- function(batch_size) {
  if (is.null(batch_size)) {
    return(65536L)
  }
  if (!is.numeric(batch_size) ||
      length(batch_size) != 1L ||
      is.na(batch_size) ||
      !is.finite(batch_size) ||
      batch_size < 1 ||
      batch_size > 1000000 ||
      batch_size != floor(batch_size)) {
    .abort_delta_sharing(
      "`batch_size` must be one whole number between 1 and 1000000.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  as.integer(batch_size)
}

.validate_read_concurrency <- function(concurrency) {
  if (is.null(concurrency)) {
    return(invisible(NULL))
  }
  if (!is.numeric(concurrency) ||
      length(concurrency) != 1L ||
      is.na(concurrency) ||
      !is.finite(concurrency) ||
      concurrency < 1 ||
      concurrency > .Machine$integer.max ||
      concurrency != floor(concurrency)) {
    .abort_delta_sharing(
      "`concurrency` must be NULL or one positive whole number.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  .abort_delta_sharing(
    "Explicit read concurrency is not supported by this native boundary.",
    type = "unsupported",
    operation = "read_arrow_stream",
    feature = "concurrency"
  )
}

.execute_snapshot_arrow_stream <- function(
  specification,
  batch_size,
  concurrency,
  snapshot_transport,
  auth_transport,
  clock,
  sleeper,
  random,
  max_attempts,
  temp_parent,
  native_stream_factory,
  native_cdf_stream_factory = .native_cdf_stream
) {
  is_cdf <- .object_is(specification, SharingChanges)
  if (!is_cdf && !.object_is(specification, SharingRead)) {
    .abort_delta_sharing(
      "`read` must be a SharingRead or SharingChanges.",
      type = "validation",
      operation = "read_arrow_stream"
    )
  }
  batch_size <- .normalize_read_batch_size(batch_size)
  .validate_read_concurrency(concurrency)

  prepared <- if (is_cdf) {
    .prepare_cdf_http_read(
      read = specification,
      stream_transport = snapshot_transport,
      auth_transport = auth_transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts,
      temp_parent = temp_parent
    )
  } else {
    .prepare_snapshot_http_read(
      read = specification,
      stream_transport = snapshot_transport,
      auth_transport = auth_transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts,
      temp_parent = temp_parent
    )
  }
  transferred <- FALSE
  on.exit({
    if (!transferred) {
      try(.release_prepared_snapshot(prepared), silent = TRUE)
    }
  }, add = TRUE)

  state <- .prepared_snapshot_state(prepared)
  invocation <- .prepared_snapshot_invocation(prepared)
  planning <- .prepared_snapshot_diagnostics(prepared)
  diagnostics <- if (is_cdf) {
    .new_cdf_read_diagnostics(
      specification = specification,
      planning = planning,
      batch_size = batch_size,
      concurrency = concurrency
    )
  } else {
    .new_snapshot_read_diagnostics(
      specification = specification,
      planning = planning,
      batch_size = batch_size,
      concurrency = concurrency
    )
  }
  stream <- if (is_cdf) {
    native_cdf_stream_factory(
      table_location = state$guard,
      start_version = invocation$start_version,
      end_version = invocation$end_version,
      columns = invocation$projection,
      batch_size = batch_size
    )
  } else {
    native_stream_factory(
      table_location = state$guard,
      columns = invocation$projection,
      limit = invocation$exact_limit,
      batch_size = batch_size
    )
  }
  if (!isTRUE(.validate_snapshot_log_guard(state$guard)$released)) {
    .abort_delta_sharing(
      if (is_cdf) {
        "The native stream did not accept CDF cleanup ownership."
      } else {
        "The native stream did not accept snapshot cleanup ownership."
      },
      type = "native",
      operation = "read_arrow_stream"
    )
  }
  state$released <- TRUE
  stream <- .attach_read_diagnostics(stream, diagnostics)
  transferred <- TRUE
  stream
}

.new_control_execution_callbacks <- function(
  transport = .httr2_http_transport(),
  snapshot_transport = .httr2_snapshot_transport(),
  clock = Sys.time,
  sleeper = Sys.sleep,
  random = stats::runif,
  max_attempts = 5L,
  metadata_chunk_bytes = .http_read_chunk_bytes,
  snapshot_temp_parent = tempdir(),
  native_stream_factory = .native_snapshot_stream,
  native_cdf_stream_factory = .native_cdf_stream,
  arrow_available = .arrow_package_available,
  arrow_reader_factory = .arrow_reader_from_stream,
  data_frame_converter = .nanoarrow_data_frame_from_stream
) {
  transport <- .normalize_http_transport(transport)
  snapshot_transport <- .normalize_snapshot_stream_transport(
    snapshot_transport
  )
  if (!is.function(clock) ||
      !is.function(sleeper) ||
      !is.function(random) ||
      !is.function(native_stream_factory) ||
      !is.function(native_cdf_stream_factory) ||
      !is.function(arrow_available) ||
      !is.function(arrow_reader_factory) ||
      !is.function(data_frame_converter)) {
    stop("Execution control hooks must be functions.", call. = FALSE)
  }
  snapshot_temp_parent <- .validate_snapshot_temp_parent(
    snapshot_temp_parent
  )
  if (!is.numeric(max_attempts) ||
      length(max_attempts) != 1L ||
      is.na(max_attempts) ||
      !is.finite(max_attempts) ||
      max_attempts < 1 ||
      max_attempts != floor(max_attempts) ||
      max_attempts > .Machine$integer.max) {
    stop("`max_attempts` must be one positive whole number.", call. = FALSE)
  }
  if (!is.numeric(metadata_chunk_bytes) ||
      length(metadata_chunk_bytes) != 1L ||
      is.na(metadata_chunk_bytes) ||
      !is.finite(metadata_chunk_bytes) ||
      metadata_chunk_bytes < 1 ||
      metadata_chunk_bytes != floor(metadata_chunk_bytes) ||
      metadata_chunk_bytes > .Machine$integer.max) {
    stop(
      "`metadata_chunk_bytes` must be one positive whole number.",
      call. = FALSE
    )
  }
  max_attempts <- as.integer(max_attempts)
  metadata_chunk_bytes <- as.integer(metadata_chunk_bytes)
  arrow_is_available <- isTRUE(arrow_available())

  discovery_fetcher <- function(client, operation) {
    .new_discovery_http_fetcher(
      client = client,
      operation = operation,
      transport = transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts
    )
  }
  table_fetcher <- function(client) {
    force(client)
    function(request) {
      .execute_table_http_request(
        client = client,
        request = request,
        transport = transport,
        clock = clock,
        sleeper = sleeper,
        random = random,
        max_attempts = max_attempts,
        metadata_chunk_bytes = metadata_chunk_bytes
      )
    }
  }

  callbacks <- list(
    list_shares = function(client) {
      .collect_share_records(
        discovery_fetcher(client, "list_shares")
      )
    },
    list_schemas = function(client, share) {
      .collect_schema_records(
        discovery_fetcher(client, "list_schemas"),
        share = share
      )
    },
    list_tables = function(client, share, schema) {
      .collect_table_records(
        discovery_fetcher(client, "list_tables"),
        share = share,
        schema = schema
      )
    },
    table_version = function(client, identifier) {
      .fetch_table_version(identifier, table_fetcher(client))
    },
    table_protocol = function(client, identifier) {
      response <- .fetch_table_metadata(
        identifier,
        table_fetcher(client),
        operation = "table_protocol"
      )
      .project_table_protocol(response)
    },
    table_metadata = function(client, identifier) {
      response <- .fetch_table_metadata(
        identifier,
        table_fetcher(client),
        operation = "table_metadata"
      )
      .project_table_metadata(response)
    },
    table_schema = function(client, identifier) {
      response <- .fetch_table_metadata(
        identifier,
        table_fetcher(client),
        operation = "table_schema"
      )
      .project_table_schema(response)
    },
    read_arrow_stream = function(
      specification,
      batch_size = NULL,
      concurrency = NULL
    ) {
      .execute_snapshot_arrow_stream(
        specification = specification,
        batch_size = batch_size,
        concurrency = concurrency,
        snapshot_transport = snapshot_transport,
        auth_transport = transport,
        clock = clock,
        sleeper = sleeper,
        random = random,
        max_attempts = max_attempts,
        temp_parent = snapshot_temp_parent,
        native_stream_factory = native_stream_factory,
        native_cdf_stream_factory = native_cdf_stream_factory
      )
    },
    data_frame_from_stream = function(stream) {
      .materialize_data_frame_stream(
        stream,
        converter = data_frame_converter
      )
    }
  )
  if (arrow_is_available) {
    callbacks$arrow_from_stream <- function(stream) {
      .materialize_arrow_stream(
        stream,
        arrow_available = function() TRUE,
        reader_factory = arrow_reader_factory
      )
    }
  }
  callbacks
}
