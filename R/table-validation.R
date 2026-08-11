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

  list(
    starting_version = starting_version,
    ending_version = ending_version,
    starting_timestamp = starting_timestamp,
    ending_timestamp = ending_timestamp,
    columns = normalize_columns(columns),
    response_format = normalize_response_format(response_format)
  )
}
