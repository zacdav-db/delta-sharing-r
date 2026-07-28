.metadata_chunk_limit <- 1000000L

.validate_table_identifier <- function(identifier, operation) {
  if (!.object_is(identifier, SharingTableIdentifier)) {
    .abort_delta_sharing(
      "`identifier` must be a SharingTableIdentifier.",
      type = "validation",
      operation = operation
    )
  }
  identifier
}

.table_route <- function(identifier, operation) {
  identifier <- .validate_table_identifier(identifier, operation)
  share <- .encode_discovery_segment(
    identifier@share,
    "share",
    operation
  )
  schema <- .encode_discovery_segment(
    identifier@schema,
    "schema",
    operation
  )
  table <- .encode_discovery_segment(
    identifier@table,
    "table",
    operation
  )
  paste0(
    "/shares/",
    share,
    "/schemas/",
    schema,
    "/tables/",
    table
  )
}

.new_table_request <- function(method,
                               path,
                               operation,
                               query = stats::setNames(
                                 character(),
                                 character()
                               ),
                               headers = stats::setNames(
                                 character(),
                                 character()
                               )) {
  structure(
    list(
      method = method,
      path = path,
      query = query,
      headers = headers,
      operation = operation
    ),
    class = c("delta_sharing_request_plan", "list")
  )
}

.plan_table_version_request <- function(identifier) {
  .new_table_request(
    method = "GET",
    path = paste0(
      .table_route(identifier, "table_version"),
      "/version"
    ),
    operation = "table_version"
  )
}

.format_protocol_version <- function(version) {
  sprintf("%.0f", version)
}

.plan_table_metadata_request <- function(identifier,
                                         version = NULL,
                                         timestamp = NULL,
                                         response_format = "auto") {
  identifier <- .validate_table_identifier(identifier, "table_metadata")
  version <- .normalize_version(version, "version")
  timestamp <- .normalize_timestamp(timestamp, "timestamp")
  response_format <- .normalize_response_format(response_format)

  if (!is.null(version) && !is.null(timestamp)) {
    .abort_delta_sharing(
      "`version` and `timestamp` are mutually exclusive.",
      type = "validation",
      operation = "table_metadata"
    )
  }

  query <- stats::setNames(character(), character())
  if (!is.null(version)) {
    query <- c(version = .format_protocol_version(version))
  } else if (!is.null(timestamp)) {
    query <- c(timestamp = .format_protocol_timestamp(timestamp))
  }

  .new_table_request(
    method = "GET",
    path = paste0(
      .table_route(identifier, "table_metadata"),
      "/metadata"
    ),
    query = query,
    headers = c(
      "delta-sharing-capabilities" =
        .snapshot_capability_header(response_format)
    ),
    operation = "table_metadata"
  )
}

.safe_table_fetch <- function(fetch, request) {
  if (!is.function(fetch)) {
    stop("`fetch` must be a function.", call. = FALSE)
  }
  tryCatch(
    fetch(request),
    delta_sharing_error = function(condition) stop(condition),
    error = function(condition) {
      .abort_delta_sharing(
        "The table metadata request could not be completed.",
        type = "protocol",
        operation = request$operation
      )
    }
  )
}

.validate_table_response <- function(response, operation) {
  if (!is.list(response) ||
      is.null(names(response)) ||
      anyNA(names(response)) ||
      any(!nzchar(names(response))) ||
      anyDuplicated(names(response))) {
    .abort_delta_sharing(
      "The server returned an invalid table metadata response.",
      type = "protocol",
      operation = operation
    )
  }
  response
}

.fetch_table_version <- function(identifier, fetch) {
  request <- .plan_table_version_request(identifier)
  response <- .validate_table_response(
    .safe_table_fetch(fetch, request),
    request$operation
  )
  .parse_table_version_header(response$headers)
}

.next_metadata_chunk <- function(read_chunk, operation) {
  chunk <- tryCatch(
    read_chunk(),
    delta_sharing_error = function(condition) stop(condition),
    error = function(condition) {
      .abort_delta_sharing(
        "The table metadata response could not be read.",
        type = "protocol",
        operation = operation
      )
    }
  )
  if (!is.null(chunk) &&
      !is.raw(chunk) &&
      !(is.character(chunk) &&
        length(chunk) == 1L &&
        !is.na(chunk))) {
    .protocol_abort(
      "The table metadata response returned an invalid chunk.",
      operation
    )
  }
  chunk
}

.consume_table_metadata_chunks <- function(
  chunks,
  max_line_bytes = .ndjson_default_max_line_bytes,
  max_chunks = .metadata_chunk_limit
) {
  if (!is.function(chunks)) {
    return(.parse_table_metadata_ndjson(
      chunks,
      max_line_bytes = max_line_bytes,
      operation = "table_metadata"
    ))
  }
  if (!is.numeric(max_chunks) ||
      length(max_chunks) != 1L ||
      is.na(max_chunks) ||
      !is.finite(max_chunks) ||
      max_chunks < 1 ||
      max_chunks != floor(max_chunks) ||
      max_chunks > .Machine$integer.max) {
    stop("`max_chunks` must be one positive whole number.", call. = FALSE)
  }

  decoder <- .new_ndjson_decoder(
    operation = "table_metadata",
    max_line_bytes = max_line_bytes
  )
  actions <- list()
  chunk_count <- 0L
  repeat {
    chunk <- .next_metadata_chunk(chunks, "table_metadata")
    if (is.null(chunk)) {
      break
    }
    chunk_count <- chunk_count + 1L
    if (chunk_count > max_chunks) {
      .protocol_abort(
        "Table metadata response exceeded the internal chunk limit.",
        "table_metadata"
      )
    }
    actions <- c(actions, .ndjson_decoder_push(decoder, chunk))
  }
  actions <- c(actions, .ndjson_decoder_finish(decoder))
  .metadata_from_actions(actions, operation = "table_metadata")
}

.new_private_metadata_storage <- function(metadata) {
  storage <- new.env(parent = emptyenv())
  storage$location <- metadata$location
  storage$auxiliary_locations <- metadata$auxiliary_locations
  class(storage) <- "delta_sharing_private_metadata_storage"
  lockEnvironment(storage, bindings = TRUE)
  storage
}

.safe_metadata_fields <- function(metadata) {
  structure(
    list(
      response_format = metadata$response_format,
      id = metadata$id,
      name = metadata$name,
      description = metadata$description,
      format = metadata$format,
      schema_string = metadata$schema_string,
      configuration = metadata$configuration,
      partition_columns = metadata$partition_columns,
      version = metadata$version,
      size = metadata$size,
      num_files = metadata$num_files,
      created_time = metadata$created_time,
      access_modes = metadata$access_modes
    ),
    class = c("delta_sharing_metadata", "list")
  )
}

.new_table_metadata_response <- function(table_version, parsed) {
  safe_metadata <- .safe_metadata_fields(parsed$metadata)
  storage <- .new_private_metadata_storage(parsed$metadata)
  structure(
    list(
      table_version = table_version,
      response_format = parsed$response_format,
      protocol = parsed$protocol,
      metadata = safe_metadata
    ),
    class = c("delta_sharing_table_metadata_response", "list"),
    private_storage = storage
  )
}

.fetch_table_metadata <- function(identifier,
                                  fetch,
                                  version = NULL,
                                  timestamp = NULL,
                                  response_format = "auto",
                                  max_line_bytes =
                                    .ndjson_default_max_line_bytes,
                                  max_chunks = .metadata_chunk_limit) {
  request <- .plan_table_metadata_request(
    identifier = identifier,
    version = version,
    timestamp = timestamp,
    response_format = response_format
  )
  response <- .validate_table_response(
    .safe_table_fetch(fetch, request),
    request$operation
  )
  if (!"chunks" %in% names(response)) {
    .protocol_abort(
      "The table metadata response is missing its body.",
      "table_metadata"
    )
  }

  table_version <- .parse_table_version_header(response$headers)
  parsed <- .consume_table_metadata_chunks(
    chunks = response$chunks,
    max_line_bytes = max_line_bytes,
    max_chunks = max_chunks
  )
  .new_table_metadata_response(table_version, parsed)
}

.validate_table_metadata_response <- function(response) {
  if (!inherits(response, "delta_sharing_table_metadata_response") ||
      !is.list(response)) {
    .abort_delta_sharing(
      "`response` must be parsed table metadata.",
      type = "validation",
      operation = "table_metadata"
    )
  }
  response
}

.project_table_protocol <- function(response) {
  response <- .validate_table_metadata_response(response)
  protocol <- response$protocol
  structure(
    list(
      response_format = protocol$response_format,
      min_reader_version = protocol$min_reader_version,
      min_writer_version = protocol$min_writer_version,
      reader_features = protocol$reader_features,
      writer_features = protocol$writer_features
    ),
    class = c("delta_sharing_protocol", "list")
  )
}

.project_table_metadata <- function(response) {
  response <- .validate_table_metadata_response(response)
  metadata <- response$metadata
  structure(
    c(
      list(
        table_version = response$table_version,
        response_format = response$response_format
      ),
      unclass(metadata)[setdiff(
        names(metadata),
        "response_format"
      )]
    ),
    class = c("delta_sharing_public_metadata", "list")
  )
}

.private_table_storage <- function(response) {
  response <- .validate_table_metadata_response(response)
  storage <- attr(response, "private_storage", exact = TRUE)
  list(
    location = storage$location,
    auxiliary_locations = storage$auxiliary_locations
  )
}

.parse_table_schema_json <- function(schema_string) {
  if (!.is_scalar_character(schema_string)) {
    .protocol_abort(
      "The table schema is not valid JSON.",
      "table_schema"
    )
  }
  schema <- tryCatch(
    jsonlite::fromJSON(
      schema_string,
      simplifyVector = FALSE,
      simplifyDataFrame = FALSE,
      simplifyMatrix = FALSE
    ),
    error = function(condition) NULL
  )
  valid <- .json_is_object(schema) &&
    !anyDuplicated(names(schema)) &&
    identical(schema$type, "struct") &&
    is.list(schema$fields) &&
    is.null(names(schema$fields))
  if (!valid) {
    .protocol_abort(
      "The table schema is not a valid struct schema.",
      "table_schema"
    )
  }
  for (field in schema$fields) {
    field_type <- if (.json_is_object(field)) field$type else NULL
    type_valid <- .is_scalar_character(field_type) ||
      (.json_is_object(field_type) &&
        !anyDuplicated(names(field_type)) &&
        .is_scalar_character(field_type$type))
    field_valid <- .json_is_object(field) &&
      !anyDuplicated(names(field)) &&
      .is_scalar_character(field$name) &&
      type_valid &&
      is.logical(field$nullable) &&
      length(field$nullable) == 1L &&
      !is.na(field$nullable)
    if (!field_valid) {
      .protocol_abort(
        "The table schema contains an invalid field.",
        "table_schema"
      )
    }
  }
  structure(
    schema,
    class = c("delta_sharing_schema", "list")
  )
}

.project_table_schema <- function(response) {
  response <- .validate_table_metadata_response(response)
  .parse_table_schema_json(response$metadata$schema_string)
}
