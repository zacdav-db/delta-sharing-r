# Delta Sharing for R

This development version provides a small, immutable S7 interface for Delta
Sharing. Profiles, authentication, HTTP, protocol handling, and planning stay
in R. Snapshot rows are read by Delta Kernel and returned through an Arrow C
Stream; the native layer is limited to the Kernel scan and the Arrow lifecycle
tied to that stream.

## Create a snapshot read

``` r
library(delta.sharing)

client <- sharing_client("~/Desktop/config.share")
orders <- sharing_table(client, "sales.default.orders")

latest_orders <- sharing_read(
  orders,
  columns = c("order_id", "ordered_at", "amount"),
  limit = 1000
)
```

The client, table, and read are reusable descriptors. Creating a new read does
not mutate the table. A compact name is convenient when its three components
do not contain dots; use explicit components otherwise:

``` r
events <- sharing_table(
  client,
  share = "product",
  schema = "default",
  table = "events.v2"
)
```

`limit` is enforced exactly by the Kernel scan. A structured `predicate` is
only a best-effort server hint and is not an exact row filter.

## Read rows

Use the lazy stream when the full result may not fit in memory:

``` r
stream <- read_arrow_stream(latest_orders, batch_size = 65536L)

# Safe planning facts remain available for this stream after consumption.
diagnostics <- read_diagnostics(stream)

# Consume the nanoarrow_array_stream here.
# Release it explicitly when stopping before exhaustion.
stream$release()

# Diagnostics do not own the stream and remain available after release.
read_diagnostics(stream)
```

Exhaustion, explicit release, and finalization all release the private
synthetic log and native scan state. A stream is single-consumer.
`read_diagnostics()` returns immutable, redacted R-owned facts such as the
selected format and version, page/file counts, projection, limit, batch size,
and URL-expiry summary. It never contains credentials, URLs, paths, tokens,
predicate values, protocol actions, or mutable execution state.

The eager adapters consume that same stream path:

``` r
orders_df <- read_data_frame(latest_orders)
orders_arrow <- read_arrow(latest_orders) # requires the optional arrow package
```

`read_data_frame()` allocates the complete result in R memory.
`read_arrow()` imports the C Stream directly, without an IPC round trip, and
fails before HTTP or Kernel work if `{arrow}` is not installed.

## Discover data and inspect a table

Discovery and table control-plane calls execute entirely in R through the
authenticated client:

``` r
list_shares(client)
list_schemas(client, share = "sales")
list_tables(client, share = "sales", schema = "default")

table_version(orders)
table_protocol(orders)
table_metadata(orders)
table_schema(orders)
```

Discovery follows every page. Metadata results are safe projections and omit
storage locations, credentials, response bodies, and other private fields.

## Profiles and credentials

Version 2 profiles support bearer, Basic, OAuth client-secret, and RS256
private-key JWT client authentication. Private keys and OAuth tokens remain
behind the client's opaque R context and are never descriptor properties.

## Current scope

Snapshot materialization currently supports `SharingRead` with Delta-format
Query Table responses, including latest, version, and timestamp reads,
projection, exact limits, bounded Arrow batches, and best-effort predicate
hints. `read_arrow_stream()`, `read_arrow()`, and `read_data_frame()` all
consume the same Delta Kernel Arrow stream.

CDF execution supports explicit inclusive version ranges through the separate
immutable `SharingChanges` descriptor. Timestamp-bound and open-ended CDF
descriptors fail with typed unsupported conditions before HTTP until both
provider versions can be resolved exactly.

The following work remains before release readiness:

- Protocol Parquet responses are not materialized. An explicit
  `response_format = "parquet"` or a Parquet server selection raises a typed
  unsupported condition.
- `batch_size` is supported; any non-`NULL` `concurrency` value is explicitly
  unsupported.
- Projected `read_schema()` is not implemented.
- Cross-platform build proof, representative performance measurements, and
  final lifecycle/release evidence are still pending.

The R package does not fall back to a second downloader, Parquet reader, offset
version map, or compatibility layer.
