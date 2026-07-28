# Delta Sharing for R

The vNext API uses small immutable S7 descriptors. Query configuration creates
new snapshot or change data feed specifications. Profiles, authentication,
HTTP, protocol handling, and planning stay in R; Rust is limited to Delta
Kernel scans and the Arrow/native lifecycle tied to an active stream.

``` r
library(delta.sharing)

client <- sharing_client("~/Desktop/config.share")
orders <- sharing_table(client, "sales.default.orders")

latest <- sharing_read(
  orders,
  columns = c("order_id", "ordered_at", "amount"),
  limit = 1000
)

changes <- sharing_changes(
  orders,
  starting_version = 120,
  ending_version = 125
)
```

Use a structured identifier when a component contains a dot:

``` r
events <- sharing_table(
  client,
  share = "product",
  schema = "default",
  table = "events.v2"
)
```

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

Version 2 profiles support bearer, Basic, OAuth client-secret, and RS256
private-key JWT client authentication. Private keys and OAuth tokens remain
behind the client's opaque R context and are never descriptor properties.

`read_arrow_stream()`, `read_arrow()`, and `read_data_frame()` define the
execution interface. Until the Rust layer is linked, they fail with a typed
`delta_sharing_native_unavailable_error`; the R package does not fall back to a
second downloader or reader.
