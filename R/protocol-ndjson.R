.ndjson_default_max_line_bytes <- 8L * 1024L * 1024L

.protocol_abort <- function(message, operation, ...) {
  .abort_delta_sharing(
    message,
    type = "protocol",
    operation = operation,
    ...
  )
}

.new_ndjson_decoder <- function(operation,
                                max_line_bytes = .ndjson_default_max_line_bytes) {
  if (!.is_scalar_character(operation)) {
    .abort_delta_sharing(
      "`operation` must be one non-empty string.",
      type = "validation",
      operation = "new_ndjson_decoder"
    )
  }
  if (!is.numeric(max_line_bytes) ||
      length(max_line_bytes) != 1L ||
      is.na(max_line_bytes) ||
      !is.finite(max_line_bytes) ||
      max_line_bytes < 1 ||
      max_line_bytes != floor(max_line_bytes) ||
      max_line_bytes > .Machine$integer.max) {
    .abort_delta_sharing(
      "`max_line_bytes` must be one positive whole number.",
      type = "validation",
      operation = operation
    )
  }

  decoder <- new.env(parent = emptyenv())
  decoder$operation <- operation
  decoder$max_line_bytes <- as.integer(max_line_bytes)
  decoder$buffer <- raw()
  decoder$line_number <- 0L
  decoder$finished <- FALSE
  class(decoder) <- "delta_sharing_ndjson_decoder"
  decoder
}

.validate_ndjson_decoder <- function(decoder) {
  if (!inherits(decoder, "delta_sharing_ndjson_decoder") ||
      !is.environment(decoder)) {
    .abort_delta_sharing(
      "`decoder` must be an incremental NDJSON decoder.",
      type = "validation",
      operation = "decode_ndjson"
    )
  }
  invisible(decoder)
}

.ndjson_chunk_raw <- function(chunk, operation) {
  if (is.raw(chunk)) {
    return(chunk)
  }
  if (is.character(chunk) &&
      length(chunk) == 1L &&
      !is.na(chunk)) {
    return(charToRaw(enc2utf8(chunk)))
  }
  .abort_delta_sharing(
    "Each NDJSON chunk must be a raw vector or one character string.",
    type = "validation",
    operation = operation
  )
}

.ndjson_append_range <- function(decoder, bytes, start, end) {
  segment_size <- if (end < start) 0L else end - start + 1L
  new_size <- length(decoder$buffer) + segment_size
  if (new_size > decoder$max_line_bytes) {
    .protocol_abort(
      sprintf(
        "NDJSON line %d exceeds the configured size limit.",
        decoder$line_number + 1L
      ),
      decoder$operation
    )
  }
  if (segment_size > 0L) {
    decoder$buffer <- c(
      decoder$buffer,
      bytes[seq.int(start, end)]
    )
  }
  invisible(decoder)
}

.raw_line_is_blank <- function(line) {
  length(line) == 0L ||
    all(as.integer(line) %in% c(9L, 13L, 32L))
}

.new_opaque_json <- function(value) {
  opaque <- new.env(parent = emptyenv())
  opaque$value <- value
  class(opaque) <- "delta_sharing_opaque_json"
  lockEnvironment(opaque, bindings = TRUE)
  opaque
}

.new_private_end_stream <- function(next_page_token,
                                    refresh_token,
                                    min_url_expiration_timestamp) {
  state <- new.env(parent = emptyenv())
  state$next_page_token <- next_page_token
  state$refresh_token <- refresh_token
  state$min_url_expiration_timestamp <- min_url_expiration_timestamp
  lockEnvironment(state, bindings = TRUE)

  action <- new.env(parent = emptyenv())
  action$state <- state
  class(action) <- "delta_sharing_private_end_stream"
  lockEnvironment(action, bindings = TRUE)
  action
}

.end_stream_state <- function(action, operation = "query_table") {
  if (!inherits(action, "delta_sharing_private_end_stream") ||
      !is.environment(action) ||
      !is.environment(action$state)) {
    .protocol_abort(
      "The response contains an invalid terminal action.",
      operation
    )
  }
  action$state
}

.normalize_end_stream_action <- function(value, operation) {
  value <- .require_json_object(value, "End stream action", operation)
  allowed <- c(
    "refreshToken",
    "nextPageToken",
    "minUrlExpirationTimestamp",
    "errorMessage",
    "httpStatusErrorCode"
  )
  if (length(setdiff(names(value), allowed)) > 0L) {
    .protocol_abort(
      "End stream action contains unsupported fields.",
      operation
    )
  }

  token <- function(name) {
    if (!.json_has(value, name) || is.null(value[[name]]) ||
        identical(value[[name]], "")) {
      return(NULL)
    }
    candidate <- value[[name]]
    if (!.is_scalar_character(candidate) ||
        grepl("[\r\n]", candidate) ||
        nchar(candidate, type = "bytes") > .ndjson_default_max_line_bytes) {
      .protocol_abort(
        sprintf("End stream field `%s` is invalid.", name),
        operation
      )
    }
    candidate
  }

  status <- .wire_integer(
    value,
    "httpStatusErrorCode",
    operation,
    required = FALSE,
    nonnegative = TRUE,
    maximum = 599
  )
  if (!is.null(status) && status < 100) {
    .protocol_abort(
      "End stream field `httpStatusErrorCode` is invalid.",
      operation
    )
  }
  if (.json_has(value, "errorMessage") &&
      !is.null(value$errorMessage)) {
    if (!is.character(value$errorMessage) ||
        length(value$errorMessage) != 1L ||
        is.na(value$errorMessage)) {
      .protocol_abort(
        "End stream field `errorMessage` is invalid.",
        operation
      )
    }
    .abort_delta_sharing(
      "The Delta Sharing server reported a streaming error.",
      type = "protocol",
      operation = operation,
      status = status
    )
  }

  expiration <- .wire_integer(
    value,
    "minUrlExpirationTimestamp",
    operation,
    required = FALSE,
    nonnegative = TRUE,
    maximum = 2^53
  )
  .new_private_end_stream(
    next_page_token = token("nextPageToken"),
    refresh_token = token("refreshToken"),
    min_url_expiration_timestamp = expiration
  )
}

.new_ndjson_action <- function(type, value, line_number) {
  structure(
    list(
      type = type,
      value = value,
      line_number = as.integer(line_number)
    ),
    class = c("delta_sharing_ndjson_action", "list")
  )
}

.json_is_object <- function(value) {
  is.list(value) && !is.null(names(value))
}

.require_json_object <- function(value, label, operation) {
  if (!.json_is_object(value) || anyDuplicated(names(value))) {
    .protocol_abort(
      sprintf("%s must be a JSON object with unique fields.", label),
      operation
    )
  }
  value
}

.json_has <- function(object, name) {
  name %in% names(object)
}

.wire_character <- function(object,
                            name,
                            operation,
                            required = FALSE,
                            nonempty = FALSE) {
  if (!.json_has(object, name) || is.null(object[[name]])) {
    if (required) {
      .protocol_abort(
        sprintf("Metadata field `%s` is required.", name),
        operation
      )
    }
    return(NULL)
  }
  value <- object[[name]]
  valid <- is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    (!nonempty || nzchar(value))
  if (!valid) {
    .protocol_abort(
      sprintf("Metadata field `%s` must be one string.", name),
      operation
    )
  }
  value
}

.wire_integer <- function(object,
                          name,
                          operation,
                          required = FALSE,
                          nonnegative = TRUE,
                          maximum = 2^53) {
  if (!.json_has(object, name) || is.null(object[[name]])) {
    if (required) {
      .protocol_abort(
        sprintf("Protocol field `%s` is required.", name),
        operation
      )
    }
    return(NULL)
  }
  value <- object[[name]]
  valid <- is.numeric(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    is.finite(value) &&
    value == floor(value) &&
    value <= maximum &&
    (!nonnegative || value >= 0)
  if (!valid) {
    .protocol_abort(
      sprintf("Field `%s` must be a supported whole number.", name),
      operation
    )
  }
  as.double(value)
}

.wire_character_array <- function(object,
                                  name,
                                  operation,
                                  required = FALSE) {
  if (!.json_has(object, name) || is.null(object[[name]])) {
    if (required) {
      .protocol_abort(
        sprintf("Metadata field `%s` is required.", name),
        operation
      )
    }
    return(character())
  }
  value <- object[[name]]
  valid <- is.list(value) &&
    is.null(names(value)) &&
    all(vapply(
      value,
      function(element) {
        is.character(element) &&
          length(element) == 1L &&
          !is.na(element)
      },
      logical(1)
    ))
  if (!valid) {
    .protocol_abort(
      sprintf("Field `%s` must be an array of strings.", name),
      operation
    )
  }
  if (length(value) == 0L) character() else unlist(value, use.names = FALSE)
}

.wire_character_map <- function(object,
                                name,
                                operation,
                                required = FALSE) {
  if (!.json_has(object, name) || is.null(object[[name]])) {
    if (required) {
      .protocol_abort(
        sprintf("Metadata field `%s` is required.", name),
        operation
      )
    }
    return(stats::setNames(character(), character()))
  }
  value <- object[[name]]
  valid <- .json_is_object(value) &&
    !anyDuplicated(names(value)) &&
    all(vapply(
      value,
      function(element) {
        is.character(element) &&
          length(element) == 1L &&
          !is.na(element)
      },
      logical(1)
    ))
  if (!valid) {
    .protocol_abort(
      sprintf("Field `%s` must be an object of string values.", name),
      operation
    )
  }
  if (length(value) == 0L) {
    stats::setNames(character(), character())
  } else {
    stats::setNames(unlist(value, use.names = FALSE), names(value))
  }
}

.normalize_protocol_action <- function(value, operation) {
  value <- .require_json_object(value, "Protocol action", operation)
  if (.json_has(value, "deltaProtocol")) {
    protocol <- .require_json_object(
      value$deltaProtocol,
      "Delta protocol",
      operation
    )
    response_format <- "delta"
  } else {
    protocol <- value
    response_format <- "parquet"
  }

  normalized <- list(
      response_format = response_format,
      min_reader_version = .wire_integer(
        protocol,
        "minReaderVersion",
        operation,
        required = TRUE,
        maximum = 2^32 - 1
      ),
      min_writer_version = .wire_integer(
        protocol,
        "minWriterVersion",
        operation,
        required = identical(response_format, "delta"),
        maximum = 2^32 - 1
      ),
      reader_features = .wire_character_array(
        protocol,
        "readerFeatures",
        operation
      ),
      writer_features = .wire_character_array(
        protocol,
        "writerFeatures",
        operation
      )
    )
  if (.json_has(value, "version")) {
    normalized$version <- .wire_integer(value, "version", operation)
  }
  structure(
    normalized,
    class = c("delta_sharing_protocol", "list")
  )
}

.normalize_format <- function(value, operation) {
  value <- .require_json_object(value, "Metadata format", operation)
  provider <- .wire_character(
    value,
    "provider",
    operation,
    nonempty = TRUE
  )
  if (is.null(provider)) {
    provider <- "parquet"
  }
  list(
    provider = provider,
    options = .wire_character_map(value, "options", operation)
  )
}

.normalize_metadata_action <- function(value, operation) {
  value <- .require_json_object(value, "Metadata action", operation)
  if (.json_has(value, "deltaMetadata")) {
    metadata <- .require_json_object(
      value$deltaMetadata,
      "Delta metadata",
      operation
    )
    envelope <- value
    response_format <- "delta"
  } else {
    metadata <- value
    envelope <- value
    response_format <- "parquet"
  }

  if (!.json_has(metadata, "format") || is.null(metadata$format)) {
    .protocol_abort("Metadata field `format` is required.", operation)
  }

  structure(
    list(
      response_format = response_format,
      id = .wire_character(
        metadata,
        "id",
        operation,
        required = TRUE,
        nonempty = TRUE
      ),
      name = .wire_character(metadata, "name", operation),
      description = .wire_character(metadata, "description", operation),
      format = .normalize_format(metadata$format, operation),
      schema_string = .wire_character(
        metadata,
        "schemaString",
        operation,
        required = TRUE
      ),
      configuration = .wire_character_map(
        metadata,
        "configuration",
        operation
      ),
      partition_columns = .wire_character_array(
        metadata,
        "partitionColumns",
        operation,
        required = TRUE
      ),
      version = .wire_integer(envelope, "version", operation),
      size = .wire_integer(envelope, "size", operation),
      num_files = .wire_integer(envelope, "numFiles", operation),
      created_time = .wire_integer(
        metadata,
        "createdTime",
        operation,
        nonnegative = FALSE
      ),
      location = .wire_character(envelope, "location", operation),
      auxiliary_locations = .wire_character_array(
        envelope,
        "auxiliaryLocations",
        operation
      ),
      access_modes = .wire_character_array(
        envelope,
        "accessModes",
        operation
      )
    ),
    class = c("delta_sharing_metadata", "list")
  )
}

.parse_ndjson_action <- function(line, line_number, operation) {
  text <- tryCatch(
    rawToChar(line),
    error = function(cnd) NULL
  )
  value <- if (is.null(text)) {
    NULL
  } else {
    tryCatch(
      jsonlite::fromJSON(text, simplifyVector = FALSE),
      error = function(cnd) NULL
    )
  }
  if (is.null(value)) {
    .protocol_abort(
      sprintf("NDJSON line %d is not valid JSON.", line_number),
      operation
    )
  }
  if (!.json_is_object(value)) {
    .protocol_abort(
      sprintf("NDJSON line %d must be a JSON object.", line_number),
      operation
    )
  }
  if (anyDuplicated(names(value))) {
    .protocol_abort(
      sprintf("NDJSON line %d has duplicate object fields.", line_number),
      operation
    )
  }

  known <- intersect(
    c("protocol", "metaData", "file", "endStreamAction"),
    names(value)
  )
  if (length(known) > 1L) {
    .protocol_abort(
      sprintf("NDJSON line %d contains multiple recognized actions.", line_number),
      operation
    )
  }
  if (identical(known, "protocol")) {
    if (length(names(value)) != 1L) {
      .protocol_abort(
        sprintf("NDJSON line %d contains an invalid protocol wrapper.", line_number),
        operation
      )
    }
    return(.new_ndjson_action(
      "protocol",
      .normalize_protocol_action(value$protocol, operation),
      line_number
    ))
  }
  if (identical(known, "metaData")) {
    if (length(names(value)) != 1L) {
      .protocol_abort(
        sprintf("NDJSON line %d contains an invalid metadata wrapper.", line_number),
        operation
      )
    }
    return(.new_ndjson_action(
      "metadata",
      .normalize_metadata_action(value$metaData, operation),
      line_number
    ))
  }
  if (identical(known, "file")) {
    if (length(names(value)) != 1L) {
      .protocol_abort(
        sprintf("NDJSON line %d contains an invalid file wrapper.", line_number),
        operation
      )
    }
    return(.new_ndjson_action(
      "file",
      .normalize_snapshot_file_action(value$file, operation),
      line_number
    ))
  }
  if (identical(known, "endStreamAction")) {
    if (length(names(value)) != 1L) {
      .protocol_abort(
        sprintf(
          "NDJSON line %d contains an invalid terminal action.",
          line_number
        ),
        operation
      )
    }
    return(.new_ndjson_action(
      "end_stream",
      .normalize_end_stream_action(value$endStreamAction, operation),
      line_number
    ))
  }

  end_stream_fields <- c(
    "refreshToken",
    "nextPageToken",
    "minUrlExpirationTimestamp",
    "errorMessage",
    "httpStatusErrorCode"
  )
  if (any(end_stream_fields %in% names(value))) {
    return(.new_ndjson_action(
      "end_stream",
      .normalize_end_stream_action(value, operation),
      line_number
    ))
  }

  .new_ndjson_action(
    "unknown",
    .new_opaque_json(value),
    line_number
  )
}

.ndjson_decoder_push <- function(decoder, chunk) {
  .validate_ndjson_decoder(decoder)
  if (isTRUE(decoder$finished)) {
    .protocol_abort(
      "Cannot add data to a finished NDJSON decoder.",
      decoder$operation
    )
  }

  bytes <- .ndjson_chunk_raw(chunk, decoder$operation)
  if (length(bytes) == 0L) {
    return(list())
  }

  newline_positions <- which(bytes == as.raw(0x0a))
  actions <- list()
  start <- 1L
  for (newline in newline_positions) {
    .ndjson_append_range(
      decoder,
      bytes,
      start,
      newline - 1L
    )
    decoder$line_number <- decoder$line_number + 1L
    line <- decoder$buffer
    decoder$buffer <- raw()
    if (length(line) > 0L &&
        identical(line[[length(line)]], as.raw(0x0d))) {
      line <- line[-length(line)]
    }
    if (!.raw_line_is_blank(line)) {
      actions[[length(actions) + 1L]] <- .parse_ndjson_action(
        line,
        decoder$line_number,
        decoder$operation
      )
    }
    start <- newline + 1L
  }

  .ndjson_append_range(
    decoder,
    bytes,
    start,
    length(bytes)
  )
  actions
}

.ndjson_decoder_finish <- function(decoder) {
  .validate_ndjson_decoder(decoder)
  if (isTRUE(decoder$finished)) {
    .protocol_abort(
      "NDJSON decoder has already been finished.",
      decoder$operation
    )
  }
  decoder$finished <- TRUE
  if (length(decoder$buffer) == 0L) {
    return(list())
  }

  decoder$line_number <- decoder$line_number + 1L
  line <- decoder$buffer
  decoder$buffer <- raw()
  if (length(line) > 0L &&
      identical(line[[length(line)]], as.raw(0x0d))) {
    line <- line[-length(line)]
  }
  if (.raw_line_is_blank(line)) {
    return(list())
  }
  list(.parse_ndjson_action(
    line,
    decoder$line_number,
    decoder$operation
  ))
}

.metadata_from_actions <- function(actions,
                                   operation = "parse_table_metadata") {
  protocol <- NULL
  metadata <- NULL
  for (action in actions) {
    if (!inherits(action, "delta_sharing_ndjson_action")) {
      .protocol_abort(
        "Metadata response contains an invalid action.",
        operation
      )
    }
    if (identical(action$type, "protocol") && is.null(protocol)) {
      protocol <- action$value
    } else if (identical(action$type, "metadata") && is.null(metadata)) {
      metadata <- action$value
    } else if (!identical(action$type, "end_stream")) {
      .protocol_abort(
        "Metadata response contains an unexpected or duplicate action.",
        operation
      )
    }
  }
  if (is.null(protocol)) {
    .protocol_abort("Metadata response is missing `protocol`.", operation)
  }
  if (is.null(metadata)) {
    .protocol_abort("Metadata response is missing `metaData`.", operation)
  }
  if (!identical(protocol$response_format, metadata$response_format)) {
    .protocol_abort(
      "Metadata response uses inconsistent response formats.",
      operation
    )
  }
  structure(
    list(
      response_format = protocol$response_format,
      protocol = protocol,
      metadata = metadata
    ),
    class = c("delta_sharing_table_metadata", "list")
  )
}

.parse_table_metadata_ndjson <- function(chunks,
                                         max_line_bytes =
                                           .ndjson_default_max_line_bytes,
                                         operation =
                                           "parse_table_metadata") {
  if (is.raw(chunks)) {
    chunks <- list(chunks)
  } else if (is.character(chunks)) {
    chunks <- as.list(chunks)
  }
  if (!is.list(chunks)) {
    .abort_delta_sharing(
      "`chunks` must be raw data, character data, or a list of chunks.",
      type = "validation",
      operation = operation
    )
  }

  decoder <- .new_ndjson_decoder(
    operation = operation,
    max_line_bytes = max_line_bytes
  )
  actions <- list()
  for (chunk in chunks) {
    actions <- c(actions, .ndjson_decoder_push(decoder, chunk))
  }
  actions <- c(actions, .ndjson_decoder_finish(decoder))
  .metadata_from_actions(actions, operation = operation)
}
