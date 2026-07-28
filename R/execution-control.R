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

.new_control_execution_callbacks <- function(
  transport = .httr2_http_transport(),
  clock = Sys.time,
  sleeper = Sys.sleep,
  random = stats::runif,
  max_attempts = 5L,
  metadata_chunk_bytes = .http_read_chunk_bytes
) {
  transport <- .normalize_http_transport(transport)
  if (!is.function(clock) ||
      !is.function(sleeper) ||
      !is.function(random)) {
    stop("Execution control hooks must be functions.", call. = FALSE)
  }
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

  list(
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
    }
  )
}
