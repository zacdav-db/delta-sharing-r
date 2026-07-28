.parquet_response_reader_version <- 1
.parquet_response_delta_reader_version <- 1
.parquet_response_delta_writer_version <- 2

.parquet_response_abort <- function(message, type = "protocol") {
  .abort_delta_sharing(
    message,
    type = type,
    operation = .snapshot_log_operation,
    response_format = "parquet",
    feature = "parquet_response"
  )
}

.parquet_safe_text <- function(value, label) {
  if (
    !.is_scalar_character(value) ||
      is.na(Encoding(value)) ||
      grepl("[\\x00-\\x1f\\x7f]", value, perl = TRUE)
  ) {
    .parquet_response_abort(sprintf("%s must be one safe string.", label))
  }
  enc2utf8(value)
}

.parquet_schema_metadata <- function(value) {
  if (
    !.snapshot_has_valid_names(value) ||
      !.snapshot_json_value_is_valid(value)
  ) {
    .parquet_response_abort("Parquet response schema metadata is invalid.")
  }
  keys <- tolower(names(value))
  if (
    any(grepl(
      "^(delta\\.columnmapping\\.|parquet\\.field\\.id$)",
      keys,
      perl = TRUE
    ))
  ) {
    .parquet_response_abort(
      "Parquet response schema uses unsupported column mapping metadata.",
      type = "unsupported"
    )
  }
  invisible(value)
}

.parquet_schema_decimal <- function(value) {
  match <- regexec(
    "^decimal\\(([1-9][0-9]*),([0-9]+)\\)$",
    value,
    perl = TRUE
  )
  parts <- regmatches(value, match)[[1L]]
  if (length(parts) != 3L) {
    return(NULL)
  }
  precision <- suppressWarnings(as.integer(parts[[2L]]))
  scale <- suppressWarnings(as.integer(parts[[3L]]))
  if (
    is.na(precision) ||
      is.na(scale) ||
      precision < 1L ||
      precision > 38L ||
      scale < 0L ||
      scale > precision
  ) {
    return(NULL)
  }
  list(kind = "primitive", primitive = value)
}

.parquet_schema_type <- function(value) {
  if (.is_scalar_character(value)) {
    type <- tolower(.parquet_safe_text(value, "Parquet response schema type"))
    primitive <- c(
      "string",
      "boolean",
      "byte",
      "short",
      "integer",
      "long",
      "float",
      "double",
      "date",
      "timestamp",
      "timestamp_ntz",
      "binary"
    )
    if (type %in% primitive) {
      return(list(kind = "primitive", primitive = type))
    }
    decimal <- .parquet_schema_decimal(type)
    if (!is.null(decimal)) {
      return(decimal)
    }
    .parquet_response_abort(
      "Parquet response schema contains an unsupported type."
    )
  }

  if (
    !.snapshot_has_valid_names(value) ||
      !"type" %in% names(value) ||
      !.is_scalar_character(value$type)
  ) {
    .parquet_response_abort("Parquet response schema contains an invalid type.")
  }
  kind <- tolower(value$type)
  if (identical(kind, "struct")) {
    if (
      !identical(sort(names(value)), sort(c("type", "fields"))) ||
        !is.list(value$fields) ||
        !is.null(names(value$fields))
    ) {
      .parquet_response_abort("Parquet response struct schema is invalid.")
    }
    fields <- .parquet_schema_fields(value$fields)
    return(list(kind = "struct", fields = fields))
  }
  if (identical(kind, "array")) {
    if (
      !identical(
        sort(names(value)),
        sort(c("type", "elementType", "containsNull"))
      ) ||
        !is.logical(value$containsNull) ||
        length(value$containsNull) != 1L ||
        is.na(value$containsNull)
    ) {
      .parquet_response_abort("Parquet response array schema is invalid.")
    }
    return(list(
      kind = "array",
      element = .parquet_schema_type(value$elementType)
    ))
  }
  if (identical(kind, "map")) {
    if (
      !identical(
        sort(names(value)),
        sort(c("type", "keyType", "valueType", "valueContainsNull"))
      ) ||
        !is.logical(value$valueContainsNull) ||
        length(value$valueContainsNull) != 1L ||
        is.na(value$valueContainsNull)
    ) {
      .parquet_response_abort("Parquet response map schema is invalid.")
    }
    return(list(
      kind = "map",
      key = .parquet_schema_type(value$keyType),
      value = .parquet_schema_type(value$valueType)
    ))
  }
  .parquet_response_abort(
    "Parquet response schema contains an unsupported type."
  )
}

.parquet_schema_fields <- function(fields) {
  parsed <- vector("list", length(fields))
  field_names <- character(length(fields))
  for (index in seq_along(fields)) {
    field <- fields[[index]]
    if (
      !.snapshot_has_valid_names(field) ||
        !identical(
          sort(names(field)),
          sort(c("name", "type", "nullable", "metadata"))
        ) ||
        !is.logical(field$nullable) ||
        length(field$nullable) != 1L ||
        is.na(field$nullable)
    ) {
      .parquet_response_abort(
        "Parquet response schema contains an invalid field."
      )
    }
    name <- .parquet_safe_text(
      field$name,
      "Parquet response schema field name"
    )
    .parquet_schema_metadata(field$metadata)
    field_names[[index]] <- name
    parsed[[index]] <- list(
      name = name,
      type = .parquet_schema_type(field$type)
    )
  }
  if (anyDuplicated(tolower(field_names))) {
    .parquet_response_abort(
      "Parquet response schema has case-insensitive field-name collisions."
    )
  }
  stats::setNames(parsed, field_names)
}

.validate_parquet_schema <- function(schema_string, partition_columns) {
  schema_string <- .parquet_safe_text(
    schema_string,
    "Parquet response schema"
  )
  if (length(charToRaw(schema_string)) > .snapshot_log_max_action_bytes) {
    .parquet_response_abort("Parquet response schema exceeds the size limit.")
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
  if (
    !.snapshot_has_valid_names(schema) ||
      !identical(sort(names(schema)), sort(c("type", "fields"))) ||
      !identical(schema$type, "struct") ||
      !is.list(schema$fields) ||
      !is.null(names(schema$fields))
  ) {
    .parquet_response_abort("Parquet response schema is not a valid struct.")
  }
  fields <- .parquet_schema_fields(schema$fields)
  if (
    anyDuplicated(partition_columns) ||
      any(!partition_columns %in% names(fields))
  ) {
    .parquet_response_abort(
      "Parquet response partition columns do not match the schema."
    )
  }
  for (name in partition_columns) {
    type <- fields[[name]]$type
    if (
      !identical(type$kind, "primitive") ||
        identical(type$primitive, "binary")
    ) {
      .parquet_response_abort(
        "Parquet response partition columns must use supported primitive types.",
        type = "unsupported"
      )
    }
  }
  list(fields = fields, partition_columns = partition_columns)
}

.parquet_configuration_is_sensitive <- function(configuration) {
  keys <- tolower(names(configuration))
  any(
    grepl("^delta\\.(columnmapping\\.|feature\\.)", keys, perl = TRUE) |
      keys %in%
        c(
          "delta.minreaderversion",
          "delta.minwriterversion",
          "delta.enabledeletionvectors",
          "delta.enablerowtracking"
        )
  )
}

.parquet_integer_in_range <- function(value, minimum, maximum) {
  if (!grepl("^-?(0|[1-9][0-9]*)$", value, perl = TRUE)) {
    return(FALSE)
  }
  negative <- startsWith(value, "-")
  digits <- if (negative) substring(value, 2L) else value
  if (identical(digits, "0")) {
    negative <- FALSE
  }
  bound <- if (negative) substring(minimum, 2L) else maximum
  length(digits) < length(bound) ||
    (length(digits) == length(bound) && digits <= bound)
}

.validate_parquet_partition_value <- function(value, type) {
  if (identical(value, "") || identical(type, "string")) {
    return(TRUE)
  }
  if (identical(type, "boolean")) {
    return(value %in% c("true", "false"))
  }
  bounds <- list(
    byte = c("-128", "127"),
    short = c("-32768", "32767"),
    integer = c("-2147483648", "2147483647"),
    long = c("-9223372036854775808", "9223372036854775807")
  )
  if (type %in% names(bounds)) {
    return(.parquet_integer_in_range(
      value,
      bounds[[type]][[1L]],
      bounds[[type]][[2L]]
    ))
  }
  if (type %in% c("float", "double")) {
    return(grepl(
      "^-?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?$",
      value,
      perl = TRUE
    ))
  }
  if (startsWith(type, "decimal(")) {
    return(grepl("^-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?$", value, perl = TRUE))
  }
  if (identical(type, "date")) {
    return(
      grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", value, perl = TRUE) &&
        !is.na(suppressWarnings(as.Date(value)))
    )
  }
  if (identical(type, "timestamp")) {
    return(grepl(
      "^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})?$",
      value,
      perl = TRUE
    ))
  }
  if (identical(type, "timestamp_ntz")) {
    return(grepl(
      "^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?$",
      value,
      perl = TRUE
    ))
  }
  FALSE
}

.validate_parquet_partition_values <- function(action, schema) {
  values <- action$partitionValues
  expected <- schema$partition_columns
  if (
    !setequal(names(values), expected) ||
      length(values) != length(expected)
  ) {
    .parquet_response_abort(
      "Parquet response partition values do not match partition columns."
    )
  }
  for (name in expected) {
    type <- schema$fields[[name]]$type$primitive
    if (!.validate_parquet_partition_value(values[[name]], type)) {
      .parquet_response_abort(
        "Parquet response contains an invalid serialized partition value."
      )
    }
  }
  invisible(action)
}

.normalize_parquet_file_action <- function(value, operation) {
  if (identical(operation, .cdf_query_operation)) {
    .abort_delta_sharing(
      "Parquet CDF responses are not supported.",
      type = "unsupported",
      operation = operation,
      response_format = "parquet",
      feature = "parquet_response"
    )
  }
  if (!.snapshot_has_valid_names(value)) {
    .protocol_abort("Parquet file wrapper must be a JSON object.", operation)
  }
  tryCatch(
    {
      .snapshot_reject_unknown_fields(
        value,
        c(
          "url",
          "id",
          "partitionValues",
          "size",
          "stats",
          "version",
          "timestamp",
          "expirationTimestamp"
        ),
        "Parquet file wrapper"
      )
      id <- .snapshot_required_string(value, "id", "Parquet file wrapper")
      url <- .snapshot_https_url(
        .snapshot_required_string(value, "url", "Parquet file wrapper"),
        "Parquet file URL"
      )
      action <- list(
        path = url,
        partitionValues = .snapshot_string_map(
          value,
          "partitionValues",
          "Parquet file wrapper"
        ),
        size = .snapshot_whole_number(value, "size", "Parquet file wrapper"),
        modificationTime = 0,
        dataChange = TRUE
      )
      stats <- .snapshot_stats(value, "Parquet file wrapper")
      if (!is.null(stats)) {
        action$stats <- stats
      }
      version <- .snapshot_whole_number(
        value,
        "version",
        "Parquet file wrapper",
        required = FALSE
      )
      timestamp <- .snapshot_whole_number(
        value,
        "timestamp",
        "Parquet file wrapper",
        required = FALSE,
        nonnegative = FALSE
      )
      expiration <- .snapshot_whole_number(
        value,
        "expirationTimestamp",
        "Parquet file wrapper",
        required = FALSE
      )
      .new_private_snapshot_file(
        id = id,
        action_type = "add",
        delta_action = list(add = action),
        expiration_timestamp = expiration,
        version = version,
        timestamp = timestamp,
        response_format = "parquet"
      )
    },
    delta_sharing_error = function(condition) {
      if (identical(condition$operation, operation)) {
        stop(condition)
      }
      .abort_delta_sharing(
        conditionMessage(condition),
        type = if (inherits(condition, "delta_sharing_unsupported_error")) {
          "unsupported"
        } else {
          "protocol"
        },
        operation = operation,
        response_format = "parquet",
        feature = condition$feature
      )
    }
  )
}

.parquet_snapshot_protocol_action <- function(protocol) {
  if (
    !identical(protocol$min_reader_version, .parquet_response_reader_version)
  ) {
    .parquet_response_abort(
      "Parquet response requires an unsupported Sharing reader version.",
      type = "unsupported"
    )
  }
  if (
    !is.null(protocol$min_writer_version) ||
      length(protocol$reader_features) > 0L ||
      length(protocol$writer_features) > 0L
  ) {
    .parquet_response_abort("Parquet response protocol is invalid.")
  }
  list(
    minReaderVersion = .parquet_response_delta_reader_version,
    minWriterVersion = .parquet_response_delta_writer_version
  )
}

.parquet_snapshot_metadata_action <- function(metadata) {
  id <- .snapshot_required_string(metadata, "id", "Parquet response metadata")
  if (
    !identical(metadata$format$provider, "parquet") ||
      length(metadata$format$options) > 0L
  ) {
    .parquet_response_abort(
      "Parquet response metadata must use Parquet without format options.",
      type = "unsupported"
    )
  }
  configuration <- metadata$configuration
  if (
    !is.character(configuration) ||
      is.null(names(configuration)) ||
      any(!nzchar(names(configuration))) ||
      anyDuplicated(names(configuration)) ||
      anyNA(configuration) ||
      any(grepl(
        "[\\x00-\\x1f\\x7f]",
        c(names(configuration), configuration),
        perl = TRUE
      ))
  ) {
    .parquet_response_abort("Parquet response configuration is invalid.")
  }
  if (.parquet_configuration_is_sensitive(configuration)) {
    .parquet_response_abort(
      "Parquet response metadata requires unsupported reader features.",
      type = "unsupported"
    )
  }
  schema <- .validate_parquet_schema(
    metadata$schema_string,
    metadata$partition_columns
  )

  action <- list(id = id)
  for (field in c("name", "description")) {
    value <- .snapshot_optional_string(
      metadata,
      field,
      "Parquet response metadata"
    )
    if (!is.null(value)) {
      action[[field]] <- value
    }
  }
  action$format <- list(
    provider = "parquet",
    options = .snapshot_json_object(character())
  )
  action$schemaString <- metadata$schema_string
  action$partitionColumns <- .snapshot_json_array(metadata$partition_columns)
  action$configuration <- .snapshot_json_object(character())
  list(action = action, schema = schema)
}

.validate_parquet_snapshot_files <- function(files, schema) {
  if (length(files) == 0L) {
    return(list())
  }
  states <- lapply(files, .snapshot_file_state)
  if (
    any(vapply(
      states,
      function(state)
        !identical(state$response_format, "parquet") ||
          !identical(state$action_type, "add"),
      logical(1)
    ))
  ) {
    .parquet_response_abort(
      "Parquet response contains an incompatible file action."
    )
  }
  ids <- vapply(states, `[[`, character(1), "id")
  paths <- vapply(
    states,
    function(state) state$delta_action$add$path,
    character(1)
  )
  if (anyDuplicated(ids) || anyDuplicated(paths)) {
    .parquet_response_abort("Parquet response contains duplicate files.")
  }
  for (state in states) {
    .validate_parquet_partition_values(state$delta_action$add, schema)
  }
  order_key <- order(ids, method = "radix")
  lapply(states[order_key], `[[`, "delta_action")
}

.validate_parquet_response_versions <- function(
  protocol,
  metadata,
  files,
  table_version
) {
  versions <- c(
    protocol$version,
    metadata$version,
    unlist(lapply(files, .snapshot_file_version), use.names = FALSE)
  )
  if (length(versions) > 0L && any(versions != table_version)) {
    .snapshot_planning_abort(
      "The Parquet snapshot response has inconsistent table versions."
    )
  }
  invisible(table_version)
}

.validate_parquet_response_totals <- function(metadata, files) {
  if (
    !is.null(metadata$num_files) &&
      !identical(metadata$num_files, as.double(length(files)))
  ) {
    .snapshot_planning_abort(
      "The Parquet snapshot response has an inconsistent file count."
    )
  }
  if (!is.null(metadata$size)) {
    total <- sum(vapply(
      files,
      function(file) .snapshot_file_state(file)$delta_action$add$size,
      numeric(1)
    ))
    if (!identical(metadata$size, total)) {
      .snapshot_planning_abort(
        "The Parquet snapshot response has an inconsistent total size."
      )
    }
  }
  invisible(metadata)
}
