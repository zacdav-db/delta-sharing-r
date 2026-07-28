.snapshot_log_operation <- "prepare_snapshot_log"
.snapshot_log_max_files <- 1000000L
.snapshot_log_max_action_bytes <- 8L * 1024L * 1024L
.snapshot_log_marker_name <- ".delta-sharing-r-prepared-log"
.snapshot_log_marker_value <- "delta-sharing-r:vnext"

.snapshot_log_abort <- function(message, type = "protocol") {
  .abort_delta_sharing(
    message,
    type = type,
    operation = .snapshot_log_operation
  )
}

.snapshot_json_object <- function(values) {
  if (length(values) == 0L) {
    return(structure(list(), names = character()))
  }
  as.list(values)
}

.snapshot_json_array <- function(values) {
  I(unname(values))
}

.snapshot_has_valid_names <- function(value) {
  .json_is_object(value) &&
    !anyDuplicated(names(value)) &&
    all(nzchar(names(value)))
}

.snapshot_json_value_is_valid <- function(value) {
  if (is.null(value)) {
    return(TRUE)
  }
  if (is.list(value)) {
    object <- !is.null(names(value))
    if (
      object &&
        (anyDuplicated(names(value)) || any(!nzchar(names(value))))
    ) {
      return(FALSE)
    }
    return(all(vapply(value, .snapshot_json_value_is_valid, logical(1))))
  }
  (is.character(value) ||
    is.logical(value) ||
    (is.numeric(value) && all(is.finite(value)))) &&
    length(value) == 1L &&
    !is.na(value)
}

.snapshot_reject_unknown_fields <- function(value, allowed, label) {
  unknown <- setdiff(names(value), allowed)
  if (length(unknown) > 0L) {
    .snapshot_log_abort(sprintf("%s contains unsupported fields.", label))
  }
  invisible(value)
}

.snapshot_required_string <- function(object, name, label) {
  if (
    !name %in% names(object) ||
      !is.character(object[[name]]) ||
      length(object[[name]]) != 1L ||
      is.na(object[[name]]) ||
      !nzchar(object[[name]]) ||
      is.na(Encoding(object[[name]]))
  ) {
    .snapshot_log_abort(sprintf(
      "%s field `%s` must be one string.",
      label,
      name
    ))
  }
  value <- enc2utf8(object[[name]])
  if (grepl("[\\x00-\\x1f\\x7f]", value, perl = TRUE)) {
    .snapshot_log_abort(sprintf(
      "%s field `%s` contains invalid characters.",
      label,
      name
    ))
  }
  value
}

.snapshot_optional_string <- function(object, name, label) {
  if (!name %in% names(object) || is.null(object[[name]])) {
    return(NULL)
  }
  .snapshot_required_string(object, name, label)
}

.snapshot_whole_number <- function(
  object,
  name,
  label,
  required = TRUE,
  nonnegative = TRUE
) {
  if (!name %in% names(object) || is.null(object[[name]])) {
    if (required) {
      .snapshot_log_abort(sprintf("%s field `%s` is required.", label, name))
    }
    return(NULL)
  }
  value <- object[[name]]
  valid <- is.numeric(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    is.finite(value) &&
    value == floor(value) &&
    abs(value) <= 2^53 &&
    (!nonnegative || value >= 0)
  if (!valid) {
    .snapshot_log_abort(
      sprintf("%s field `%s` must be a supported whole number.", label, name)
    )
  }
  as.double(value)
}

.snapshot_logical <- function(object, name, label, required = TRUE) {
  if (!name %in% names(object) || is.null(object[[name]])) {
    if (required) {
      .snapshot_log_abort(sprintf("%s field `%s` is required.", label, name))
    }
    return(NULL)
  }
  value <- object[[name]]
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .snapshot_log_abort(sprintf(
      "%s field `%s` must be true or false.",
      label,
      name
    ))
  }
  value
}

.snapshot_string_map <- function(
  object,
  name,
  label,
  required = TRUE,
  nullable = FALSE
) {
  if (!name %in% names(object) || is.null(object[[name]])) {
    if (required && !(nullable && name %in% names(object))) {
      .snapshot_log_abort(sprintf("%s field `%s` is required.", label, name))
    }
    return(NULL)
  }
  value <- object[[name]]
  if (
    !.snapshot_has_valid_names(value) ||
      !all(vapply(
        value,
        function(element) {
          is.character(element) &&
            length(element) == 1L &&
            !is.na(element)
        },
        logical(1)
      ))
  ) {
    .snapshot_log_abort(
      sprintf("%s field `%s` must be an object of strings.", label, name)
    )
  }
  normalized <- vapply(value, enc2utf8, character(1), USE.NAMES = TRUE)
  if (any(grepl("[\\x00-\\x1f\\x7f]", normalized, perl = TRUE))) {
    .snapshot_log_abort(
      sprintf("%s field `%s` contains invalid characters.", label, name)
    )
  }
  .snapshot_json_object(normalized)
}

.snapshot_https_url <- function(value, label) {
  valid <- is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    nzchar(value)
  if (valid) {
    value <- enc2utf8(value)
    valid <- grepl(
      "^https://[^/?#[:space:]]+(?:[/?][^#]*)?$",
      value,
      perl = TRUE
    ) &&
      !grepl("[\\\\\\x00-\\x20\\x7f]", value, perl = TRUE) &&
      !grepl("%(?![0-9A-Fa-f]{2})", value, perl = TRUE) &&
      !grepl("^https://[^/@]+@", value, perl = TRUE)
  }
  if (!valid) {
    .snapshot_log_abort(sprintf("%s must be an absolute HTTPS URL.", label))
  }
  value
}

.snapshot_stats <- function(object, label) {
  if (!"stats" %in% names(object) || is.null(object$stats)) {
    return(NULL)
  }
  stats <- object$stats
  if (
    !is.character(stats) ||
      length(stats) != 1L ||
      is.na(stats) ||
      !nzchar(stats)
  ) {
    .snapshot_log_abort(sprintf("%s field `stats` must be JSON text.", label))
  }
  parsed <- tryCatch(
    jsonlite::fromJSON(stats, simplifyVector = FALSE),
    error = function(cnd) NULL
  )
  if (
    !.snapshot_has_valid_names(parsed) ||
      !.snapshot_json_value_is_valid(parsed)
  ) {
    .snapshot_log_abort(sprintf(
      "%s field `stats` must be a JSON object.",
      label
    ))
  }
  enc2utf8(stats)
}

.normalize_snapshot_deletion_vector <- function(value, label) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!.snapshot_has_valid_names(value)) {
    .snapshot_log_abort(sprintf(
      "%s deletion vector must be a JSON object.",
      label
    ))
  }
  .snapshot_reject_unknown_fields(
    value,
    c("storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality"),
    sprintf("%s deletion vector", label)
  )
  storage_type <- .snapshot_required_string(
    value,
    "storageType",
    sprintf("%s deletion vector", label)
  )
  if (!storage_type %in% c("i", "p")) {
    .snapshot_log_abort(
      sprintf("%s deletion vector uses an unsupported storage type.", label)
    )
  }
  payload <- .snapshot_required_string(
    value,
    "pathOrInlineDv",
    sprintf("%s deletion vector", label)
  )
  if (identical(storage_type, "p")) {
    payload <- .snapshot_https_url(
      payload,
      sprintf("%s deletion vector path", label)
    )
  }

  descriptor <- list(
    storageType = storage_type,
    pathOrInlineDv = payload
  )
  offset <- .snapshot_whole_number(
    value,
    "offset",
    sprintf("%s deletion vector", label),
    required = FALSE
  )
  if (!is.null(offset)) {
    descriptor$offset <- offset
  }
  descriptor$sizeInBytes <- .snapshot_whole_number(
    value,
    "sizeInBytes",
    sprintf("%s deletion vector", label)
  )
  descriptor$cardinality <- .snapshot_whole_number(
    value,
    "cardinality",
    sprintf("%s deletion vector", label)
  )
  descriptor
}

.normalize_snapshot_add <- function(value) {
  label <- "Snapshot add action"
  if (!.snapshot_has_valid_names(value)) {
    .snapshot_log_abort("Snapshot add action must be a JSON object.")
  }
  .snapshot_reject_unknown_fields(
    value,
    c(
      "path",
      "partitionValues",
      "size",
      "modificationTime",
      "dataChange",
      "stats",
      "tags",
      "deletionVector"
    ),
    label
  )

  action <- list(
    path = .snapshot_https_url(
      .snapshot_required_string(value, "path", label),
      "Snapshot add path"
    ),
    partitionValues = .snapshot_string_map(
      value,
      "partitionValues",
      label
    ),
    size = .snapshot_whole_number(value, "size", label),
    modificationTime = .snapshot_whole_number(
      value,
      "modificationTime",
      label,
      nonnegative = FALSE
    ),
    dataChange = .snapshot_logical(value, "dataChange", label)
  )
  stats <- .snapshot_stats(value, label)
  if (!is.null(stats)) {
    action$stats <- stats
  }
  if ("tags" %in% names(value)) {
    action["tags"] <- list(
      .snapshot_string_map(
        value,
        "tags",
        label,
        required = FALSE,
        nullable = TRUE
      )
    )
  }
  if ("deletionVector" %in% names(value)) {
    deletion_vector <- .normalize_snapshot_deletion_vector(
      value$deletionVector,
      label
    )
    action["deletionVector"] <- list(deletion_vector)
    if (!is.null(deletion_vector)) {
      if (is.null(stats)) {
        .snapshot_log_abort(
          "Snapshot add actions with deletion vectors require file statistics."
        )
      }
      parsed_stats <- jsonlite::fromJSON(stats, simplifyVector = FALSE)
      .snapshot_whole_number(
        parsed_stats,
        "numRecords",
        "Snapshot add statistics"
      )
    }
  }
  action
}

.normalize_snapshot_remove <- function(value) {
  label <- "Snapshot remove action"
  if (!.snapshot_has_valid_names(value)) {
    .snapshot_log_abort("Snapshot remove action must be a JSON object.")
  }
  .snapshot_reject_unknown_fields(
    value,
    c(
      "path",
      "deletionTimestamp",
      "dataChange",
      "extendedFileMetadata",
      "partitionValues",
      "size",
      "deletionVector"
    ),
    label
  )

  action <- list(
    path = .snapshot_https_url(
      .snapshot_required_string(value, "path", label),
      "Snapshot remove path"
    )
  )
  if ("deletionTimestamp" %in% names(value)) {
    action["deletionTimestamp"] <- list(.snapshot_whole_number(
      value,
      "deletionTimestamp",
      label,
      required = FALSE,
      nonnegative = FALSE
    ))
  }
  action$dataChange <- .snapshot_logical(value, "dataChange", label)
  if ("extendedFileMetadata" %in% names(value)) {
    action["extendedFileMetadata"] <- list(
      .snapshot_logical(
        value,
        "extendedFileMetadata",
        label,
        required = FALSE
      )
    )
  }
  if ("partitionValues" %in% names(value)) {
    action["partitionValues"] <- list(
      .snapshot_string_map(
        value,
        "partitionValues",
        label,
        required = FALSE,
        nullable = TRUE
      )
    )
  }
  if ("size" %in% names(value)) {
    action["size"] <- list(
      .snapshot_whole_number(
        value,
        "size",
        label,
        required = FALSE
      )
    )
  }
  if ("deletionVector" %in% names(value)) {
    action["deletionVector"] <- list(
      .normalize_snapshot_deletion_vector(
        value$deletionVector,
        label
      )
    )
  }
  action
}

.normalize_cdf_file <- function(value) {
  label <- "CDF file action"
  if (!.snapshot_has_valid_names(value)) {
    .snapshot_log_abort("CDF file action must be a JSON object.")
  }
  .snapshot_reject_unknown_fields(
    value,
    c("path", "partitionValues", "size", "dataChange", "tags"),
    label
  )

  data_change <- if ("dataChange" %in% names(value)) {
    .snapshot_logical(value, "dataChange", label)
  } else {
    FALSE
  }
  if (isTRUE(data_change)) {
    .snapshot_log_abort("CDF file actions must not set `dataChange` to true.")
  }

  action <- list(
    path = .snapshot_https_url(
      .snapshot_required_string(value, "path", label),
      "CDF file path"
    ),
    partitionValues = .snapshot_string_map(
      value,
      "partitionValues",
      label
    ),
    size = .snapshot_whole_number(value, "size", label),
    dataChange = FALSE
  )
  if ("tags" %in% names(value)) {
    action["tags"] <- list(
      .snapshot_string_map(
        value,
        "tags",
        label,
        required = FALSE,
        nullable = TRUE
      )
    )
  }
  action
}

.new_private_snapshot_file <- function(id,
                                       action_type,
                                       delta_action,
                                       expiration_timestamp = NULL,
                                       version = NULL,
                                       timestamp = NULL) {
  state <- new.env(parent = emptyenv())
  state$id <- id
  state$action_type <- action_type
  state$delta_action <- delta_action
  state$expiration_timestamp <- expiration_timestamp
  state$version <- version
  state$timestamp <- timestamp
  lockEnvironment(state, bindings = TRUE)

  file <- new.env(parent = emptyenv())
  file$state <- state
  class(file) <- "delta_sharing_private_snapshot_file"
  lockEnvironment(file, bindings = TRUE)
  file
}

.normalize_snapshot_file_action <- function(
  value,
  operation = .snapshot_log_operation
) {
  if (!.snapshot_has_valid_names(value)) {
    .protocol_abort("Snapshot file wrapper must be a JSON object.", operation)
  }
  tryCatch(
    {
      .snapshot_reject_unknown_fields(
        value,
        c(
          "id",
          "deletionVectorFileId",
          "version",
          "timestamp",
          "expirationTimestamp",
          "size",
          "deltaSingleAction"
        ),
        "Snapshot file wrapper"
      )
      id <- .snapshot_required_string(value, "id", "Snapshot file wrapper")
      if (
        "deletionVectorFileId" %in%
          names(value) &&
          !is.null(value$deletionVectorFileId)
      ) {
        .snapshot_required_string(
          value,
          "deletionVectorFileId",
          "Snapshot file wrapper"
        )
      }
      wire_numbers <- lapply(c("version", "timestamp", "size"), function(field) {
        .snapshot_whole_number(
          value,
          field,
          "Snapshot file wrapper",
          required = FALSE,
          nonnegative = !identical(field, "timestamp")
        )
      })
      names(wire_numbers) <- c("version", "timestamp", "size")
      expiration_timestamp <- .snapshot_whole_number(
        value,
        "expirationTimestamp",
        "Snapshot file wrapper",
        required = FALSE
      )
      single <- if ("deltaSingleAction" %in% names(value)) {
        value$deltaSingleAction
      } else {
        NULL
      }
      if (!.snapshot_has_valid_names(single)) {
        .snapshot_log_abort("Snapshot file action must be a JSON object.")
      }
      action_type <- intersect(c("add", "remove", "cdc"), names(single))
      if (length(action_type) != 1L || length(names(single)) != 1L) {
        .snapshot_log_abort(
          "Snapshot file action must contain exactly one supported action."
        )
      }
      if (identical(action_type, "cdc")) {
        if (!identical(operation, "query_table_changes")) {
          .abort_delta_sharing(
            "Snapshot preparation does not accept change-data actions.",
            type = "unsupported",
            operation = .snapshot_log_operation,
            feature = "cdf"
          )
        }
      }
      action <- if (identical(action_type, "add")) {
        .normalize_snapshot_add(single$add)
      } else if (identical(action_type, "remove")) {
        .normalize_snapshot_remove(single$remove)
      } else {
        .normalize_cdf_file(single$cdc)
      }
      .new_private_snapshot_file(
        id,
        action_type,
        stats::setNames(list(action), action_type),
        expiration_timestamp = expiration_timestamp,
        version = wire_numbers$version,
        timestamp = wire_numbers$timestamp
      )
    },
    delta_sharing_error = function(cnd) {
      if (identical(cnd$operation, operation)) {
        stop(cnd)
      }
      .abort_delta_sharing(
        conditionMessage(cnd),
        type = if (inherits(cnd, "delta_sharing_unsupported_error")) {
          "unsupported"
        } else {
          "protocol"
        },
        operation = operation,
        feature = cnd$feature
      )
    }
  )
}

.snapshot_file_state <- function(file) {
  if (
    !inherits(file, "delta_sharing_private_snapshot_file") ||
      !is.environment(file) ||
      !is.environment(file$state)
  ) {
    .snapshot_log_abort(
      "Snapshot files must be validated private file actions."
    )
  }
  file$state
}

.snapshot_file_expiration_timestamp <- function(file) {
  .snapshot_file_state(file)$expiration_timestamp
}

.snapshot_file_version <- function(file) {
  .snapshot_file_state(file)$version
}

.snapshot_file_timestamp <- function(file) {
  .snapshot_file_state(file)$timestamp
}

.validate_snapshot_protocol <- function(protocol) {
  if (
    !inherits(protocol, "delta_sharing_protocol") ||
      !is.list(protocol) ||
      !identical(protocol$response_format, "delta")
  ) {
    .snapshot_log_abort("Snapshot protocol must use Delta response format.")
  }
  min_reader <- .snapshot_whole_number(
    protocol,
    "min_reader_version",
    "Snapshot protocol"
  )
  min_writer <- .snapshot_whole_number(
    protocol,
    "min_writer_version",
    "Snapshot protocol",
    required = TRUE
  )
  feature_array <- function(name) {
    value <- protocol[[name]]
    if (is.null(value)) {
      return(character())
    }
    if (
      !is.character(value) ||
        anyNA(value) ||
        any(!nzchar(value)) ||
        any(grepl("[\\x00-\\x1f\\x7f]", value, perl = TRUE)) ||
        anyDuplicated(value)
    ) {
      .snapshot_log_abort("Snapshot protocol contains invalid feature names.")
    }
    unname(value)
  }

  action <- list(minReaderVersion = min_reader)
  if (!is.null(min_writer)) {
    action$minWriterVersion <- min_writer
  }
  reader_features <- feature_array("reader_features")
  writer_features <- feature_array("writer_features")
  if (length(reader_features) > 0L) {
    action$readerFeatures <- .snapshot_json_array(reader_features)
  }
  if (length(writer_features) > 0L) {
    action$writerFeatures <- .snapshot_json_array(writer_features)
  }
  action
}

.validate_snapshot_metadata <- function(metadata) {
  if (
    !inherits(metadata, "delta_sharing_metadata") ||
      !is.list(metadata) ||
      !identical(metadata$response_format, "delta")
  ) {
    .snapshot_log_abort("Snapshot metadata must use Delta response format.")
  }
  id <- .snapshot_required_string(metadata, "id", "Snapshot metadata")
  format <- metadata$format
  if (
    !is.list(format) ||
      !.is_scalar_character(format$provider) ||
      !identical(format$provider, "parquet")
  ) {
    .snapshot_log_abort("Snapshot metadata must describe Parquet data files.")
  }
  options <- format$options
  if (
    !is.character(options) ||
      is.null(names(options)) ||
      any(!nzchar(names(options))) ||
      anyDuplicated(names(options)) ||
      anyNA(options) ||
      any(grepl("[\\x00-\\x1f\\x7f]", options, perl = TRUE))
  ) {
    .snapshot_log_abort("Snapshot metadata format options are invalid.")
  }
  schema_string <- .snapshot_required_string(
    metadata,
    "schema_string",
    "Snapshot metadata"
  )
  tryCatch(
    .parse_table_schema_json(schema_string),
    delta_sharing_error = function(cnd) {
      .snapshot_log_abort("Snapshot metadata schema is invalid.")
    }
  )
  partition_columns <- metadata$partition_columns
  if (
    !is.character(partition_columns) ||
      anyNA(partition_columns) ||
      any(!nzchar(partition_columns)) ||
      any(grepl("[\\x00-\\x1f\\x7f]", partition_columns, perl = TRUE)) ||
      anyDuplicated(partition_columns)
  ) {
    .snapshot_log_abort("Snapshot metadata partition columns are invalid.")
  }
  configuration <- metadata$configuration
  if (
    !is.character(configuration) ||
      is.null(names(configuration)) ||
      any(!nzchar(names(configuration))) ||
      anyDuplicated(names(configuration)) ||
      anyNA(configuration) ||
      any(grepl("[\\x00-\\x1f\\x7f]", configuration, perl = TRUE))
  ) {
    .snapshot_log_abort("Snapshot metadata configuration is invalid.")
  }

  action <- list(id = id)
  for (field in c("name", "description")) {
    value <- .snapshot_optional_string(metadata, field, "Snapshot metadata")
    if (!is.null(value)) {
      action[[field]] <- value
    }
  }
  action$format <- list(
    provider = "parquet",
    options = .snapshot_json_object(options)
  )
  action$schemaString <- schema_string
  action$partitionColumns <- .snapshot_json_array(partition_columns)
  action$configuration <- .snapshot_json_object(configuration)
  created_time <- .snapshot_whole_number(
    metadata,
    "created_time",
    "Snapshot metadata",
    required = FALSE,
    nonnegative = FALSE
  )
  if (!is.null(created_time)) {
    action$createdTime <- created_time
  }
  action
}

.validate_snapshot_files <- function(files, protocol_action) {
  if (!is.list(files)) {
    .snapshot_log_abort("`files` must be a list of validated file actions.")
  }
  if (length(files) > .snapshot_log_max_files) {
    .snapshot_log_abort("Snapshot response contains too many file actions.")
  }
  if (length(files) == 0L) {
    return(list())
  }
  states <- lapply(files, .snapshot_file_state)
  ids <- vapply(states, `[[`, character(1), "id")
  types <- vapply(states, `[[`, character(1), "action_type")
  paths <- vapply(
    states,
    function(state) state$delta_action[[state$action_type]]$path,
    character(1)
  )
  if (anyDuplicated(ids) || anyDuplicated(paths)) {
    .snapshot_log_abort("Snapshot response contains duplicate file actions.")
  }

  has_dv <- vapply(
    states,
    function(state) {
      !is.null(state$delta_action[[state$action_type]]$deletionVector)
    },
    logical(1)
  )
  if (any(has_dv)) {
    reader_features <- unclass(protocol_action$readerFeatures)
    writer_features <- unclass(protocol_action$writerFeatures)
    dv_protocol <- protocol_action$minReaderVersion >= 3 &&
      !is.null(protocol_action$minWriterVersion) &&
      protocol_action$minWriterVersion >= 7 &&
      "deletionVectors" %in% reader_features &&
      "deletionVectors" %in% writer_features
    if (!dv_protocol) {
      .snapshot_log_abort(
        "Snapshot deletion vectors are inconsistent with the table protocol."
      )
    }
  }

  order_key <- order(types, ids, method = "radix")
  lapply(states[order_key], `[[`, "delta_action")
}

.snapshot_json_line <- function(action) {
  line <- tryCatch(
    jsonlite::toJSON(
      action,
      auto_unbox = TRUE,
      null = "null",
      digits = NA,
      pretty = FALSE
    ),
    error = function(cnd) NULL
  )
  if (is.null(line) || length(line) != 1L) {
    .snapshot_log_abort("Snapshot action could not be encoded as JSON.")
  }
  bytes <- charToRaw(enc2utf8(line))
  if (length(bytes) > .snapshot_log_max_action_bytes) {
    .snapshot_log_abort("Snapshot action exceeds the internal size limit.")
  }
  line
}

.snapshot_commit_lines <- function(protocol, metadata, files) {
  protocol_action <- .validate_snapshot_protocol(protocol)
  metadata_action <- .validate_snapshot_metadata(metadata)
  file_actions <- .validate_snapshot_files(files, protocol_action)
  actions <- c(
    list(
      list(protocol = protocol_action),
      list(metaData = metadata_action)
    ),
    file_actions
  )
  lapply(actions, .snapshot_json_line)
}

.write_snapshot_commit <- function(path, lines) {
  connection <- file(path, open = "wb")
  on.exit(close(connection), add = TRUE)
  for (line in lines) {
    writeBin(charToRaw(paste0(line, "\n")), connection)
  }
  flush(connection)
  invisible(path)
}

.validate_snapshot_temp_parent <- function(temp_parent) {
  if (
    !.is_scalar_character(temp_parent) ||
      !dir.exists(temp_parent) ||
      nzchar(Sys.readlink(temp_parent))
  ) {
    .snapshot_log_abort(
      "`temp_parent` must be an existing non-symlink directory.",
      type = "validation"
    )
  }
  normalizePath(temp_parent, winslash = "/", mustWork = TRUE)
}

.snapshot_temp_root_is_safe <- function(root) {
  if (!.is_scalar_character(root)) {
    return(FALSE)
  }
  base <- basename(root)
  parent <- dirname(root)
  startsWith(base, ".delta-sharing-snapshot-") &&
    dir.exists(parent) &&
    identical(
      normalizePath(parent, winslash = "/", mustWork = TRUE),
      parent
    )
}

.cleanup_snapshot_root <- function(root) {
  if (.snapshot_temp_root_is_safe(root) && file.exists(root)) {
    unlink(root, recursive = TRUE, force = TRUE)
  }
  invisible(!file.exists(root))
}

.new_snapshot_log_guard <- function(root, table_path, file_count) {
  state <- new.env(parent = emptyenv())
  state$root <- root
  state$table_path <- table_path
  state$file_count <- as.integer(file_count)
  state$released <- FALSE

  guard <- new.env(parent = emptyenv())
  guard$state <- state
  class(guard) <- "delta_sharing_snapshot_log"
  lockEnvironment(guard, bindings = TRUE)
  reg.finalizer(
    guard,
    function(value) {
      state <- value$state
      if (!isTRUE(state$released)) {
        removed <- .cleanup_snapshot_root(state$root)
        if (isTRUE(removed)) {
          state$released <- TRUE
        }
      }
      invisible(NULL)
    },
    onexit = TRUE
  )
  guard
}

.validate_snapshot_log_guard <- function(guard) {
  if (
    !inherits(guard, "delta_sharing_snapshot_log") ||
      !is.environment(guard) ||
      !is.environment(guard$state)
  ) {
    .snapshot_log_abort("`guard` must be a prepared snapshot log.")
  }
  guard$state
}

.snapshot_log_path <- function(guard) {
  state <- .validate_snapshot_log_guard(guard)
  if (isTRUE(state$released) || !dir.exists(state$table_path)) {
    .snapshot_log_abort("Prepared snapshot log has already been released.")
  }
  state$table_path
}

.snapshot_log_uri <- function(guard) {
  path <- .snapshot_log_path(guard)
  parts <- strsplit(path, "/", fixed = TRUE)[[1L]]
  if (
    .Platform$OS.type == "windows" &&
      length(parts) > 0L &&
      grepl("^[A-Za-z]:$", parts[[1L]])
  ) {
    drive <- parts[[1L]]
    rest <- vapply(
      parts[-1L],
      utils::URLencode,
      character(1),
      reserved = TRUE,
      USE.NAMES = FALSE
    )
    encoded <- paste0("/", drive, "/", paste(rest, collapse = "/"))
  } else {
    encoded <- paste(
      vapply(
        parts,
        utils::URLencode,
        character(1),
        reserved = TRUE,
        USE.NAMES = FALSE
      ),
      collapse = "/"
    )
  }
  paste0("file://", encoded)
}

.release_snapshot_log <- function(
  guard,
  cleanup = .cleanup_snapshot_root
) {
  state <- .validate_snapshot_log_guard(guard)
  if (!is.function(cleanup)) {
    stop("`cleanup` must be a function.", call. = FALSE)
  }
  if (!isTRUE(state$released)) {
    removed <- cleanup(state$root)
    if (!isTRUE(removed)) {
      .snapshot_log_abort("Prepared snapshot log could not be released.")
    }
    state$released <- TRUE
  }
  invisible(TRUE)
}

#' @exportS3Method print delta_sharing_snapshot_log
print.delta_sharing_snapshot_log <- function(x, ...) {
  state <- .validate_snapshot_log_guard(x)
  status <- if (isTRUE(state$released)) "released" else "active"
  cat(
    sprintf(
      "<delta_sharing_snapshot_log> %s; %d file action%s\n",
      status,
      state$file_count,
      if (identical(state$file_count, 1L)) "" else "s"
    )
  )
  invisible(x)
}

.prepare_snapshot_log <- function(
  protocol,
  metadata,
  files,
  temp_parent = tempdir(),
  write_commit = .write_snapshot_commit
) {
  if (!is.function(write_commit)) {
    .snapshot_log_abort(
      "`write_commit` must be a function.",
      type = "validation"
    )
  }
  lines <- .snapshot_commit_lines(protocol, metadata, files)
  parent <- .validate_snapshot_temp_parent(temp_parent)
  root <- tempfile(".delta-sharing-snapshot-", tmpdir = parent)
  if (!dir.create(root, mode = "0700", showWarnings = FALSE)) {
    .snapshot_log_abort("Snapshot temporary root could not be created.")
  }
  published <- FALSE
  on.exit(
    {
      if (!published) {
        .cleanup_snapshot_root(root)
      }
    },
    add = TRUE
  )

  tryCatch(
    {
      marker <- file.path(root, .snapshot_log_marker_name)
      .write_snapshot_commit(marker, .snapshot_log_marker_value)
      if (!file.exists(marker) || isTRUE(file.info(marker)$isdir)) {
        .snapshot_log_abort("Snapshot ownership marker was not written.")
      }
      marker_permissions <- suppressWarnings(Sys.chmod(marker, mode = "0600"))
      if (.Platform$OS.type != "windows" && !isTRUE(marker_permissions)) {
        .snapshot_log_abort("Snapshot ownership marker could not be secured.")
      }

      staging <- file.path(root, ".staging")
      log_dir <- file.path(staging, "_delta_log")
      if (!dir.create(log_dir, recursive = TRUE, mode = "0700")) {
        .snapshot_log_abort("Snapshot staging directory could not be created.")
      }
      commit <- file.path(log_dir, "00000000000000000000.json")
      write_commit(commit, lines)
      if (!file.exists(commit) || isTRUE(file.info(commit)$isdir)) {
        .snapshot_log_abort("Snapshot commit was not written.")
      }
      permissions_set <- suppressWarnings(Sys.chmod(commit, mode = "0600"))
      if (.Platform$OS.type != "windows" && !isTRUE(permissions_set)) {
        .snapshot_log_abort("Snapshot commit permissions could not be secured.")
      }
      table_path <- file.path(root, "table")
      if (!file.rename(staging, table_path)) {
        .snapshot_log_abort("Snapshot log could not be published atomically.")
      }
      published <- TRUE
      .new_snapshot_log_guard(root, table_path, length(files))
    },
    error = function(cnd) {
      if (inherits(cnd, "delta_sharing_error")) {
        stop(cnd)
      }
      .snapshot_log_abort("Snapshot log preparation failed.")
    }
  )
}
