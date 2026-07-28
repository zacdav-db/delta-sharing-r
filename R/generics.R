#' Create or retrieve a structured table identifier
#'
#' A three-part string is convenient when none of its components contains a
#' dot. Supplying the three components separately is unambiguous and preserves
#' dots within names. Applied to a [SharingTable], this generic returns the
#' table's identifier.
#'
#' @param x A three-part `"share.schema.table"` string, a
#'   [SharingTableIdentifier], or a [SharingTable].
#' @param schema,table Schema and table components when `x` is the share name.
#' @return A [SharingTableIdentifier].
#' @export
table_identifier <- S7::new_generic(
  "table_identifier",
  "x",
  function(x, schema = NULL, table = NULL) {
    S7::S7_dispatch()
  }
)

#' Create a table descriptor
#'
#' @param client A [SharingClient].
#' @param name Optional three-part string or [SharingTableIdentifier].
#' @param share,schema,table Explicit identifier components. Use these instead
#'   of `name` when a component contains a dot.
#' @return A [SharingTable].
#' @export
sharing_table <- S7::new_generic(
  "sharing_table",
  "client",
  function(client,
           name = NULL,
           share = NULL,
           schema = NULL,
           table = NULL) {
    S7::S7_dispatch()
  }
)

#' Specify a snapshot read
#'
#' Calling `sharing_read(table)` represents the latest snapshot. Query options
#' create a new immutable descriptor and never mutate the table.
#'
#' @inheritParams SharingRead
#' @return A [SharingRead].
#' @export
sharing_read <- S7::new_generic(
  "sharing_read",
  "table",
  function(table,
           version = NULL,
           timestamp = NULL,
           columns = NULL,
           limit = NULL,
           predicate = NULL,
           response_format = "auto") {
    S7::S7_dispatch()
  }
)

#' Specify a change data feed read
#'
#' @inheritParams SharingChanges
#' @return A [SharingChanges].
#' @export
sharing_changes <- S7::new_generic(
  "sharing_changes",
  "table",
  function(table,
           starting_version = NULL,
           ending_version = NULL,
           starting_timestamp = NULL,
           ending_timestamp = NULL,
           columns = NULL,
           response_format = "auto") {
    S7::S7_dispatch()
  }
)

#' List shares
#'
#' Discovery results use a compact data frame containing all pages.
#'
#' @param client A [SharingClient].
#' @return A base data frame with stable `name`, `id`, `display_name`, and
#'   `comment` character columns. Missing optional values are `NA`.
#' @export
list_shares <- S7::new_generic(
  "list_shares",
  "client",
  function(client) S7::S7_dispatch()
)

#' List schemas
#'
#' @param client A [SharingClient].
#' @param share Optional share name. When omitted, schemas in all accessible
#'   shares are listed.
#' @return A base data frame with stable `share` and `name` character columns.
#' @export
list_schemas <- S7::new_generic(
  "list_schemas",
  "client",
  function(client, share = NULL) {
    S7::S7_dispatch()
  }
)

#' List tables
#'
#' @param client A [SharingClient].
#' @param share Optional share name.
#' @param schema Optional schema name. `schema` requires `share`. When both are
#'   omitted, all accessible tables are listed.
#' @return A base data frame with stable `share`, `schema`, `name`, `share_id`,
#'   and `id` character columns plus an `access_modes` list-column. Storage
#'   locations and auxiliary locations are deliberately excluded.
#' @export
list_tables <- S7::new_generic(
  "list_tables",
  "client",
  function(client, share = NULL, schema = NULL) {
    S7::S7_dispatch()
  }
)

#' Retrieve a table version
#'
#' @param table A [SharingTable].
#' @return A non-negative whole-number table version.
#' @export
table_version <- S7::new_generic(
  "table_version",
  "table",
  function(table) S7::S7_dispatch()
)

#' Retrieve table protocol capabilities
#'
#' @inheritParams table_version
#' @return A safe list containing `response_format`, `min_reader_version`,
#'   `min_writer_version`, `reader_features`, and `writer_features`.
#' @export
table_protocol <- S7::new_generic(
  "table_protocol",
  "table",
  function(table) S7::S7_dispatch()
)

#' Retrieve table metadata
#'
#' This operation does not scan table rows.
#'
#' @inheritParams table_version
#' @return A safe structured list containing table version, response format,
#'   identifiers, format, schema JSON, configuration, partition columns,
#'   optional size statistics, creation time, and access modes. Storage
#'   locations and auxiliary locations are excluded.
#' @export
table_metadata <- S7::new_generic(
  "table_metadata",
  "table",
  function(table) S7::S7_dispatch()
)

#' Retrieve a table schema
#'
#' @inheritParams table_version
#' @return The table's parsed logical struct schema as a JSON-style list.
#' @export
table_schema <- S7::new_generic(
  "table_schema",
  "table",
  function(table) S7::S7_dispatch()
)

#' Retrieve a projected read schema
#'
#' @param read A [SharingRead] or [SharingChanges].
#' @return The logical schema after applying the read projection.
#' @export
read_schema <- S7::new_generic(
  "read_schema",
  "read",
  function(read) S7::S7_dispatch()
)

#' Read an Arrow C stream
#'
#' This is the primary materialization interface. The future Kernel bridge
#' returns a lazy, bounded `nanoarrow_array_stream`.
#'
#' @param read A [SharingRead] or [SharingChanges].
#' @param ... Kernel scan options such as batch size or concurrency.
#' @return A native Arrow C stream.
#' @export
read_arrow_stream <- S7::new_generic("read_arrow_stream", "read")

#' Read an Arrow table
#'
#' The optional eager adapter consumes the exact stream returned by
#' [read_arrow_stream()]; it cannot implement a second reader.
#'
#' @inheritParams read_arrow_stream
#' @return An eager Arrow table.
#' @export
read_arrow <- S7::new_generic("read_arrow", "read")

#' Read a data frame
#'
#' This eager adapter consumes the native Arrow stream and therefore requires
#' the result to fit in memory.
#'
#' @inheritParams read_arrow_stream
#' @return A base data frame.
#' @export
read_data_frame <- S7::new_generic("read_data_frame", "read")

#' Retrieve read diagnostics
#'
#' @param stream A stream returned by [read_arrow_stream()].
#' @return Safe diagnostics from the active or completed stream.
#' @export
read_diagnostics <- S7::new_generic(
  "read_diagnostics",
  "stream",
  function(stream) S7::S7_dispatch()
)
