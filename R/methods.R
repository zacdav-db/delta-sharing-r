#' Create a Delta Sharing profile descriptor
#'
#' @inheritParams SharingProfile
#' @return A [SharingProfile].
#' @examples
#' profile <- sharing_profile(list(
#'   shareCredentialsVersion = 2,
#'   type = "bearer_token",
#'   endpoint = "https://sharing.example.test/api",
#'   bearerToken = "example-only-not-a-secret"
#' ))
#' profile@auth_type
#' @export
sharing_profile <- function(source, source_type = NULL) {
  SharingProfile(source, source_type = source_type)
}

#' Create a Delta Sharing client
#'
#' `sharing_client()` creates a small immutable descriptor from a standard
#' Delta Sharing profile source. The profile is parsed and validated
#' immediately. Credential material and future refresh state remain in a
#' hidden R-owned context; construction performs no network request or OAuth
#' exchange.
#'
#' @param profile A profile file path, inline JSON string or raw vector,
#'   connection, list, or existing [SharingProfile].
#' @return A [SharingClient].
#' @examples
#' client <- sharing_client(list(
#'   shareCredentialsVersion = 2,
#'   type = "bearer_token",
#'   endpoint = "https://sharing.example.test/api",
#'   bearerToken = "example-only-not-a-secret"
#' ))
#' client@profile@endpoint
#' @export
sharing_client <- function(profile) {
  SharingClient(.as_sharing_profile(profile))
}

S7::method(table_identifier, S7::class_character) <- function(
  x,
  schema = NULL,
  table = NULL
) {
  if (is.null(schema) && is.null(table)) {
    if (!.is_scalar_character(x)) {
      .abort_delta_sharing(
        "`x` must be one three-part table name.",
        type = "validation",
        operation = "table_identifier"
      )
    }
    parts <- strsplit(x, ".", fixed = TRUE)[[1L]]
    if (length(parts) != 3L || any(!nzchar(parts))) {
      .abort_delta_sharing(
        paste0(
          "A compact table name must contain exactly three non-empty parts. ",
          "Supply `share`, `schema`, and `table` when a part contains a dot."
        ),
        type = "validation",
        operation = "table_identifier"
      )
    }
    return(SharingTableIdentifier(parts[[1L]], parts[[2L]], parts[[3L]]))
  }

  if (is.null(schema) || is.null(table)) {
    .abort_delta_sharing(
      "`schema` and `table` must either both be supplied or both be omitted.",
      type = "validation",
      operation = "table_identifier"
    )
  }
  SharingTableIdentifier(x, schema, table)
}

S7::method(table_identifier, SharingTableIdentifier) <- function(
  x,
  schema = NULL,
  table = NULL
) {
  if (!is.null(schema) || !is.null(table)) {
    .abort_delta_sharing(
      "`schema` and `table` must be omitted when `x` is already structured.",
      type = "validation",
      operation = "table_identifier"
    )
  }
  x
}

S7::method(table_identifier, SharingTable) <- function(
  x,
  schema = NULL,
  table = NULL
) {
  if (!is.null(schema) || !is.null(table)) {
    .abort_delta_sharing(
      "`schema` and `table` must be omitted when `x` is a SharingTable.",
      type = "validation",
      operation = "table_identifier"
    )
  }
  x@identifier
}

S7::method(sharing_table, SharingClient) <- function(
  client,
  name = NULL,
  share = NULL,
  schema = NULL,
  table = NULL
) {
  uses_name <- !is.null(name)
  uses_parts <- !is.null(share) || !is.null(schema) || !is.null(table)

  if (uses_name == uses_parts) {
    .abort_delta_sharing(
      "Supply exactly one of `name` or the `share`, `schema`, and `table` components.",
      type = "validation",
      operation = "sharing_table"
    )
  }

  if (uses_name) {
    identifier <- table_identifier(name)
  } else {
    if (is.null(share) || is.null(schema) || is.null(table)) {
      .abort_delta_sharing(
        "`share`, `schema`, and `table` must all be supplied.",
        type = "validation",
        operation = "sharing_table"
      )
    }
    identifier <- SharingTableIdentifier(share, schema, table)
  }

  SharingTable(client, identifier)
}

S7::method(sharing_read, SharingTable) <- function(
  table,
  version = NULL,
  timestamp = NULL,
  columns = NULL,
  limit = NULL,
  predicate = NULL,
  response_format = "auto"
) {
  SharingRead(
    table = table,
    version = version,
    timestamp = timestamp,
    columns = columns,
    limit = limit,
    predicate = predicate,
    response_format = response_format
  )
}

S7::method(sharing_changes, SharingTable) <- function(
  table,
  starting_version = NULL,
  ending_version = NULL,
  starting_timestamp = NULL,
  ending_timestamp = NULL,
  columns = NULL,
  response_format = "auto"
) {
  SharingChanges(
    table = table,
    starting_version = starting_version,
    ending_version = ending_version,
    starting_timestamp = starting_timestamp,
    ending_timestamp = ending_timestamp,
    columns = columns,
    response_format = response_format
  )
}

S7::method(list_shares, SharingClient) <- function(client) {
  .invoke_execution("list_shares", client = client)
}

S7::method(list_schemas, SharingClient) <- function(client, share = NULL) {
  if (!is.null(share)) {
    share <- .normalize_identifier_part(share, "share")
  }
  .invoke_execution("list_schemas", client = client, share = share)
}

S7::method(list_tables, SharingClient) <- function(
  client,
  share = NULL,
  schema = NULL
) {
  if (!is.null(share)) {
    share <- .normalize_identifier_part(share, "share")
  }
  if (!is.null(schema)) {
    if (is.null(share)) {
      .abort_delta_sharing(
        "`schema` cannot be supplied without `share`.",
        type = "validation",
        operation = "list_tables"
      )
    }
    schema <- .normalize_identifier_part(schema, "schema")
  }
  .invoke_execution(
    "list_tables",
    client = client,
    share = share,
    schema = schema
  )
}

.table_execution <- function(operation, table) {
  .invoke_execution(
    operation,
    client = table@client,
    identifier = table@identifier
  )
}

S7::method(table_version, SharingTable) <- function(table) {
  .table_execution("table_version", table)
}

S7::method(table_protocol, SharingTable) <- function(table) {
  .table_execution("table_protocol", table)
}

S7::method(table_metadata, SharingTable) <- function(table) {
  .table_execution("table_metadata", table)
}

S7::method(table_schema, SharingTable) <- function(table) {
  .table_execution("table_schema", table)
}

.read_specification <- S7::new_union(SharingRead, SharingChanges)

S7::method(read_schema, .read_specification) <- function(read) {
  .invoke_execution("read_schema", specification = read)
}

S7::method(read_arrow_stream, .read_specification) <- function(read, ...) {
  .invoke_execution("read_arrow_stream", specification = read, ...)
}

S7::method(read_arrow, .read_specification) <- function(read, ...) {
  .execution_callback("arrow_from_stream")
  stream <- read_arrow_stream(read, ...)
  .invoke_execution("arrow_from_stream", stream = stream)
}

S7::method(read_data_frame, .read_specification) <- function(read, ...) {
  .execution_callback("data_frame_from_stream")
  stream <- read_arrow_stream(read, ...)
  .invoke_execution("data_frame_from_stream", stream = stream)
}

S7::method(as.data.frame, SharingRead) <- function(
  x,
  row.names = NULL,
  optional = FALSE,
  ...
) {
  read_data_frame(x, ...)
}

S7::method(as.data.frame, SharingChanges) <- function(
  x,
  row.names = NULL,
  optional = FALSE,
  ...
) {
  read_data_frame(x, ...)
}

S7::method(read_diagnostics, S7::class_any) <- function(stream) {
  .stream_read_diagnostics(stream)
}
