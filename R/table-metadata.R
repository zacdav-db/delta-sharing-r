# Table control-plane queries: version, protocol, metadata, and schema. These
# do not scan rows. Version comes from the `delta-table-version` response
# header; protocol and metadata come from the NDJSON body of the metadata
# endpoint. Returned values are safe projections; storage locations are omitted.

# The `delta-sharing-capabilities` request header. `/metadata` accepts both
# formats and resolves one; `queryTable`/`queryTableChanges` require exactly
# one, so the read path resolves the format first (see resolve_query_format).
#
# Reader features are a delta-format concept: the server rejects them alongside
# a parquet response format, so they are only advertised when delta is possible
# (i.e. not for a resolved parquet query).
capability_header <- function(response_format = "auto", for_cdf = FALSE) {
  formats <- switch(
    response_format,
    delta = "responseformat=delta",
    parquet = "responseformat=parquet",
    "responseformat=delta,parquet"
  )
  if (identical(response_format, "parquet")) {
    return(formats)
  }
  features <- if (for_cdf) {
    "readerfeatures=deletionvectors,columnmapping"
  } else {
    "readerfeatures=deletionvectors,columnmapping,timestampntz"
  }
  paste(
    formats,
    features,
    sep = ";"
  )
}

# Resolve "auto" to a single concrete format by asking the metadata endpoint
# (which accepts both) and reading the format the server chose from its
# response capabilities header. delta/parquet are returned unchanged.
resolve_query_format <- function(
  profile,
  auth,
  identifier,
  requested,
  operation = "read"
) {
  if (!identical(requested, "auto")) {
    return(requested)
  }
  req <- metadata_request(profile, auth, identifier, operation, "auto")
  resp <- sharing_perform(req)
  caps <- httr2::resp_header(resp, "delta-sharing-capabilities") %||% ""
  if (grepl("responseformat=delta", caps, fixed = TRUE)) "delta" else "parquet"
}

table_path <- function(identifier) {
  c(
    "shares",
    identifier$share,
    "schemas",
    identifier$schema,
    "tables",
    identifier$table
  )
}

metadata_request <- function(
  profile,
  auth,
  identifier,
  operation,
  response_format = "auto"
) {
  req <- sharing_request(
    profile,
    auth,
    c(table_path(identifier), "metadata"),
    method = "GET",
    operation = operation
  )
  httr2::req_headers(
    req,
    `delta-sharing-capabilities` = capability_header(response_format)
  )
}

parse_version_header <- function(resp, operation) {
  value <- suppressWarnings(
    as.numeric(httr2::resp_header(resp, "delta-table-version"))
  )
  if (!rlang::is_scalar_integerish(value, finite = TRUE) || value < 0) {
    abort(
      "The server did not return a valid table version.",
      type = "protocol",
      operation = operation
    )
  }
  as.double(value)
}

# Split an NDJSON body into parsed JSON objects (one per non-empty line).
parse_ndjson_lines <- function(text, operation) {
  lines <- strsplit(text, "\n", fixed = TRUE)[[1]]
  lines <- lines[nzchar(trimws(lines))]
  purrr::map(lines, function(line) {
    parsed <- tryCatch(
      jsonlite::fromJSON(line, simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (!is.list(parsed)) {
      abort(
        "The server returned an invalid metadata line.",
        type = "protocol",
        operation = operation
      )
    }
    parsed
  })
}

# Coerce an optional scalar wire field to character, or `NA` when absent.
wire_character <- function(x) {
  if (is.null(x) || length(x) == 0L) {
    return(NA_character_)
  }
  as.character(x)[[1L]]
}

# Coerce an optional scalar wire field to integer, or `NA` when absent.
wire_integer <- function(x) {
  if (is.null(x) || length(x) == 0L) {
    return(NA_integer_)
  }
  as.integer(x)[[1L]]
}

# Coerce a finite scalar wire field to double, or `NA` when unusable.
wire_number <- function(x) {
  if (is.null(x) || length(x) == 0L) {
    return(NA_real_)
  }
  value <- suppressWarnings(as.numeric(x)[[1L]])
  if (!is.finite(value)) {
    return(NA_real_)
  }
  value
}

# Coerce an optional wire array to a character vector.
wire_character_vector <- function(x) {
  if (is.null(x)) {
    return(character())
  }
  purrr::map_chr(x, as.character)
}

# Extract protocol and metadata actions from the metadata NDJSON body.
parse_table_actions <- function(resp, operation) {
  text <- httr2::resp_body_string(resp)
  actions <- parse_ndjson_lines(text, operation)

  protocol <- NULL
  metadata <- NULL
  response_format <- "parquet"
  for (action in actions) {
    if (!is.null(action$protocol)) {
      p <- action$protocol
      if (!is.null(p$deltaProtocol)) {
        response_format <- "delta"
        dp <- p$deltaProtocol
        protocol <- list(
          response_format = "delta",
          min_reader_version = wire_integer(dp$minReaderVersion),
          min_writer_version = wire_integer(dp$minWriterVersion),
          reader_features = wire_character_vector(dp$readerFeatures),
          writer_features = wire_character_vector(dp$writerFeatures)
        )
      } else {
        protocol <- list(
          response_format = "parquet",
          min_reader_version = wire_integer(p$minReaderVersion),
          min_writer_version = wire_integer(p$minWriterVersion),
          reader_features = character(0),
          writer_features = character(0)
        )
      }
    } else if (!is.null(action$metaData) || !is.null(action$metadata)) {
      m <- if (!is.null(action$metaData)) action$metaData else action$metadata
      envelope <- if (!is.null(m$deltaMetadata)) m$deltaMetadata else m
      if (!is.null(m$deltaMetadata)) response_format <- "delta"
      metadata <- list(
        id = wire_character(envelope$id),
        name = wire_character(envelope$name),
        description = wire_character(envelope$description),
        format = envelope$format,
        schema_string = wire_character(envelope$schemaString),
        partition_columns = wire_character_vector(
          envelope$partitionColumns
        ),
        configuration = envelope$configuration,
        num_files = wire_number(m$numFiles),
        size = wire_number(m$size),
        created_time = wire_number(envelope$createdTime)
      )
    }
  }
  list(
    protocol = protocol,
    metadata = metadata,
    response_format = response_format
  )
}

sharing_table_version <- function(profile, auth, identifier) {
  # A HEAD on the table path returns the version in the `delta-table-version`
  # header with no body. The protocol also defines a newer `GET .../version`
  # endpoint, but HEAD is the widely-supported form (the reference server does
  # not implement `/version`), and both avoid downloading the metadata body.
  req <- sharing_request(
    profile,
    auth,
    table_path(identifier),
    method = "HEAD",
    operation = "table_version"
  )
  resp <- sharing_perform(req)
  parse_version_header(resp, "table_version")
}

sharing_table_protocol <- function(profile, auth, identifier) {
  req <- metadata_request(profile, auth, identifier, "table_protocol")
  resp <- sharing_perform(req)
  parsed <- parse_table_actions(resp, "table_protocol")
  structure(parsed$protocol, class = c("delta_sharing_protocol", "list"))
}

sharing_table_metadata <- function(profile, auth, identifier) {
  req <- metadata_request(profile, auth, identifier, "table_metadata")
  resp <- sharing_perform(req)
  version <- parse_version_header(resp, "table_metadata")
  parsed <- parse_table_actions(resp, "table_metadata")
  structure(
    c(
      list(table_version = version, response_format = parsed$response_format),
      parsed$metadata
    ),
    class = c("delta_sharing_metadata", "list")
  )
}

sharing_table_schema <- function(profile, auth, identifier) {
  req <- metadata_request(profile, auth, identifier, "table_schema")
  resp <- sharing_perform(req)
  parsed <- parse_table_actions(resp, "table_schema")
  schema_string <- parsed$metadata$schema_string
  if (!is_scalar_character(schema_string)) {
    abort(
      "The table schema is not available.",
      type = "protocol",
      operation = "table_schema"
    )
  }
  schema <- tryCatch(
    jsonlite::fromJSON(schema_string, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (!is.list(schema) || !identical(schema$type, "struct")) {
    abort(
      "The table schema is not a valid struct schema.",
      type = "protocol",
      operation = "table_schema"
    )
  }
  structure(schema, class = c("delta_sharing_schema", "list"))
}
