# Table identifier parsing and change data feed bound validation. These define
# the public table/read contract and are independent of HTTP transport.

sharing_table_identifier <- function(name, schema = NULL, share = NULL) {
  if (is.null(schema) && is.null(share)) {
    if (!is_scalar_character(name)) {
      abort(
        "{.arg name} must be a {.val share.schema.name} string when \\
         {.arg share} and {.arg schema} are omitted.",
        type = "validation",
        operation = "table"
      )
    }
    parts <- strsplit(name, ".", fixed = TRUE)[[1]]
    if (length(parts) != 3L || any(!nzchar(parts))) {
      abort(
        "A compact table name must have exactly three non-empty \\
         dot-separated parts.",
        type = "validation",
        operation = "table"
      )
    }
    share <- parts[[1]]
    schema <- parts[[2]]
    name <- parts[[3]]
  }
  list(
    share = normalize_identifier_part(share, "share"),
    schema = normalize_identifier_part(schema, "schema"),
    table = normalize_identifier_part(name, "name")
  )
}

sharing_changes_validate <- function(
  starting_version,
  ending_version,
  starting_timestamp,
  ending_timestamp,
  columns,
  response_format
) {
  starting_version <- normalize_version(starting_version, "starting_version")
  ending_version <- normalize_version(ending_version, "ending_version")
  starting_timestamp <- normalize_timestamp(
    starting_timestamp,
    "starting_timestamp"
  )
  ending_timestamp <- normalize_timestamp(ending_timestamp, "ending_timestamp")

  has_version <- !is.null(starting_version) || !is.null(ending_version)
  has_timestamp <- !is.null(starting_timestamp) || !is.null(ending_timestamp)

  if (has_version && has_timestamp) {
    abort(
      "Version and timestamp bounds cannot be mixed.",
      type = "validation",
      operation = "changes"
    )
  }
  if (!has_version && !has_timestamp) {
    abort(
      "One of `starting_version` or `starting_timestamp` is required.",
      type = "validation",
      operation = "changes"
    )
  }
  if (has_version && is.null(starting_version)) {
    abort(
      "`starting_version` is required for a version range.",
      type = "validation",
      operation = "changes"
    )
  }
  if (has_timestamp && is.null(starting_timestamp)) {
    abort(
      "`starting_timestamp` is required for a timestamp range.",
      type = "validation",
      operation = "changes"
    )
  }
  if (!is.null(ending_version) && ending_version < starting_version) {
    abort(
      "`ending_version` must be greater than or equal to `starting_version`.",
      type = "validation",
      operation = "changes"
    )
  }
  comparable_timestamps <- inherits(starting_timestamp, "POSIXct") &&
    inherits(ending_timestamp, "POSIXct")
  if (
    !is.null(ending_timestamp) &&
      comparable_timestamps &&
      ending_timestamp < starting_timestamp
  ) {
    abort(
      "`ending_timestamp` must be greater than or equal to `starting_timestamp`.",
      type = "validation",
      operation = "changes"
    )
  }

  list(
    starting_version = starting_version,
    ending_version = ending_version,
    starting_timestamp = starting_timestamp,
    ending_timestamp = ending_timestamp,
    columns = normalize_columns(columns),
    response_format = normalize_response_format(response_format)
  )
}
