# Delta Sharing R vNext API mock
#
# This file is a non-executable design artifact. Names, argument defaults, and
# printed output remain proposals until their implementation lands.

library(delta.sharing)

# Connect from a standard Delta Sharing profile. Secrets are never printed.
client <- sharing_client("recipient.share")
client
#> <SharingClient>
#> endpoint: https://sharing.example.com/api/2.0/delta-sharing/metastores/...
#> auth: bearer_token
#> state: ready

# Discovery returns ordinary compact data frames.
list_shares(client)
list_schemas(client, share = "sales")
list_tables(client, share = "sales", schema = "default")
list_tables(client, share = "sales")

# A reusable table handle can be created from structured identifier components.
orders <- sharing_table(
  client,
  share = "sales",
  schema = "default",
  table = "orders"
)
orders
#> <SharingTable sales.default.orders>
#> endpoint: sharing.example.com

# A structured identifier avoids ambiguity if an identifier contains a dot.
events <- sharing_table(
  client,
  share = "product",
  schema = "default",
  table = "events.v2"
)

# The concise latest-snapshot form is still an explicit read descriptor.
latest <- sharing_read(orders)
latest_stream <- read_arrow_stream(latest)

# Query configuration belongs to an immutable read specification.
orders_q2 <- sharing_read(
  orders,
  version = 125L,
  columns = c("order_id", "ordered_at", "amount"),
  limit = 1e6,
  response_format = "auto"
)

# timestamp and version are mutually exclusive.
orders_at_time <- sharing_read(
  orders,
  timestamp = as.POSIXct("2026-07-01 00:00:00", tz = "UTC")
)

# Primary materializer: a lazy, bounded Arrow C Stream owned by nanoarrow.
stream <- read_arrow_stream(
  orders_q2,
  batch_size = 65536L,
  concurrency = NULL
)

stream$get_schema()
first_batch <- stream$get_next()

# Stop early and release network/kernel/temp resources deterministically.
stream$release()

# Optional eager Arrow adapter. The arrow package is suggested, not imported by
# the package core.
arrow_table <- read_arrow(orders_q2)

# Eager R adapter. This consumes the stream and should be reserved for data that
# fits comfortably in memory.
orders_df <- read_data_frame(orders_q2)
orders_df_2 <- as.data.frame(orders_q2)

# Direct Arrow composition: no intermediate R data frame and no IPC file.
if (requireNamespace("duckdb", quietly = TRUE) &&
    requireNamespace("DBI", quietly = TRUE)) {
  con <- DBI::dbConnect(duckdb::duckdb())
  duckdb::duckdb_register_arrow(
    con,
    "shared_orders",
    read_arrow_stream(orders_q2)
  )
  DBI::dbGetQuery(
    con,
    "select date_trunc('month', ordered_at), sum(amount)
       from shared_orders
      group by all"
  )
}

# Change Data Feed uses the same materializers but a separate read
# specification and kernel planner.
order_changes <- sharing_changes(
  orders,
  starting_version = 120L,
  ending_version = 125L,
  columns = c("order_id", "status", "amount"),
  response_format = "delta"
)

cdf_stream <- read_arrow_stream(order_changes)
cdf_arrow <- read_arrow(order_changes)
cdf_df <- read_data_frame(order_changes)

# Metadata does not trigger a row scan.
table_version(orders)
table_protocol(orders)
table_metadata(orders)
table_schema(orders)
read_schema(orders_q2)

# Safe diagnostics are available before/after stream consumption.
read_diagnostics(stream)
#> <SharingReadDiagnostics>
#> response_format: delta
#> table_version: 125
#> files_considered: 412
#> files_read: 37
#> batches_emitted: 18
#> retries: 0
#> credentials/signed_urls: redacted

# vNext exports only the canonical S7 interface shown above. It has no aliases,
# transition wrappers, deprecation layer, or mutable R6 query state.
