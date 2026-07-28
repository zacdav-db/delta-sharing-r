# Delta Sharing for R

This development version provides a small, immutable S7 interface for Delta
Sharing. Profiles, authentication, HTTP, protocol handling, and planning stay
in R. Snapshot rows are read by Delta Kernel and returned through an Arrow C
Stream; the native layer is limited to the Kernel scan and the Arrow lifecycle
tied to that stream.

For a complete walkthrough, see the
[vNext vignette](vignettes/delta-sharing-vnext.Rmd).

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

Serialized descriptors contain no credential or token state and are
deliberately inert after deserialization. Reconstruct the client from its
protected profile source in the receiving process before making requests.

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
An R interrupt during a pull or eager materialization cancels and releases the
native stream before raising a typed cancellation condition.
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

Snapshot materialization supports `SharingRead` with Delta- and
Parquet-format Query Table responses, including latest, version, and timestamp
reads, projection, exact limits, bounded Arrow batches, and best-effort
predicate hints. R normalizes protocol Parquet actions into the same private
Kernel-readable log used by Delta responses; there is no second downloader or
Parquet reader. `read_arrow_stream()`, `read_arrow()`, and
`read_data_frame()` all consume the same Delta Kernel Arrow stream.

Reader capabilities are response-specific. Delta snapshot requests advertise
`columnmapping` and `timestampntz`; Parquet-response normalization does not
advertise Delta reader features. `deletionvectors` is intentionally
unadvertised until exact absolute-path HTTPS deletion-vector resolution has
end-to-end proof.

CDF execution supports explicit inclusive version ranges through the separate
immutable `SharingChanges` descriptor. Timestamp-bound and open-ended CDF
descriptors fail with typed unsupported conditions before HTTP until both
provider versions can be resolved exactly.

The following work remains before release readiness:

- `batch_size` is supported; any non-`NULL` `concurrency` value is explicitly
  unsupported.
- Projected `read_schema()` is not implemented.
- Large-manifest planning uses permission-restricted, disk-backed R staging
  runs instead of retaining whole-manifest action lists. On the recorded
  100,000-file Darwin arm64 workload this reduced peak memory above baseline
  by 66.0%, from 450.1 MiB to 153.0 MiB, while increasing preparation time by
  69.7%, from 45.2 to 76.8 seconds. The release RSS/time envelope remains open.
- Comparable local performance and lifecycle evidence exists and supports
  keeping the native boundary narrow. Release and target-platform performance
  gates remain unresolved, alongside hosted cross-platform build and check
  evidence.

The R package does not fall back to a second downloader, Parquet reader, offset
version map, or R-side row synthesis.
