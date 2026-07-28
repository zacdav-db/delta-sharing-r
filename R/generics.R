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
#' @examples
#' client <- sharing_client(list(
#'   shareCredentialsVersion = 2,
#'   type = "bearer_token",
#'   endpoint = "https://sharing.example.test/api",
#'   bearerToken = "example-token"
#' ))
#' table <- sharing_table(client, "sales.default.orders")
#' sharing_read(table, columns = c("order_id", "amount"), limit = 100)
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
#' This creates and validates an immutable CDF specification. CDF execution is
#' not yet implemented; passing the result to a materializer raises a typed
#' `cdf` unsupported condition before I/O.
#'
#' @inheritParams SharingChanges
#' @return A [SharingChanges].
#' @examples
#' client <- sharing_client(list(
#'   shareCredentialsVersion = 2,
#'   type = "bearer_token",
#'   endpoint = "https://sharing.example.test/api",
#'   bearerToken = "example-token"
#' ))
#' table <- sharing_table(client, "sales.default.orders")
#' sharing_changes(table, starting_version = 120, ending_version = 125)
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
#' This exported operation is reserved for projected schema execution. It is
#' not yet implemented by the default execution interface and currently raises
#' a typed unsupported condition.
#'
#' @param read A [SharingRead] or [SharingChanges].
#' @return When implemented, the logical schema after applying the read
#'   projection.
#' @export
read_schema <- S7::new_generic(
  "read_schema",
  "read",
  function(read) S7::S7_dispatch()
)

#' Read an Arrow C stream
#'
#' This is the primary materialization interface. R prepares the Delta Sharing
#' Query Table response and a private synthetic Delta log, then Delta Kernel
#' returns a lazy, bounded `nanoarrow_array_stream`.
#'
#' The stream is single-consumer. Exhaustion, explicit `stream$release()`, and
#' finalization release the native scan and private temporary state. Explicit
#' release is recommended when consumption stops early.
#'
#' `SharingRead` supports Delta- and Parquet-format snapshot responses through
#' the same Kernel stream. `SharingChanges` supports Delta-format explicit
#' version ranges. Parquet-format changes and any non-`NULL` `concurrency`
#' value fail with typed unsupported conditions before materialization.
#'
#' @param read A [SharingRead]. A [SharingChanges] descriptor is accepted by
#'   dispatch but CDF execution is not yet supported.
#' @param ... Scan options. `batch_size` must be a whole number from 1 through
#'   1,000,000 and defaults to 65,536. `concurrency` must currently be `NULL`.
#' @return A live `nanoarrow_array_stream`.
#' @export
read_arrow_stream <- S7::new_generic("read_arrow_stream", "read")

#' Read an Arrow table
#'
#' The optional eager adapter consumes the exact stream returned by
#' [read_arrow_stream()]; it cannot implement a second reader. The adapter
#' requires the optional `{arrow}` package and imports the Arrow C Stream
#' directly without an IPC or R-vector round trip. Stream ownership is released
#' after complete materialization or an adapter error.
#'
#' @inheritParams read_arrow_stream
#' @return An eager Arrow table.
#' @export
read_arrow <- S7::new_generic("read_arrow", "read")

#' Read a data frame
#'
#' This eager adapter consumes the exact stream returned by
#' [read_arrow_stream()] through `{nanoarrow}`. It allocates all result columns
#' and rows in R memory, so use the lazy stream interface when the full result
#' may not fit comfortably in memory. Stream ownership is released after
#' complete materialization or an adapter error.
#'
#' @inheritParams read_arrow_stream
#' @return A base data frame.
#' @export
read_data_frame <- S7::new_generic("read_data_frame", "read")

#' Retrieve read diagnostics
#'
#' Returns immutable, redacted planning and selection facts attached to the
#' stream by [read_arrow_stream()]. Diagnostics remain available before
#' consumption, after exhaustion, and after explicit `stream$release()`.
#'
#' The result never contains credentials, URLs, paths, query strings, tokens,
#' predicate values, protocol actions, or private temporary locations. It also
#' does not report active/released state because nanoarrow does not expose that
#' per-stream state reliably.
#'
#' @param stream A stream returned by [read_arrow_stream()].
#' @return A [SharingReadDiagnostics].
#' @export
read_diagnostics <- S7::new_generic(
  "read_diagnostics",
  "stream",
  function(stream) S7::S7_dispatch()
)
