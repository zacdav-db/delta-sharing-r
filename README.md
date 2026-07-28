# Delta Sharing for R

The vNext API uses small immutable S7 descriptors. Query configuration creates
new snapshot or change data feed specifications; mutable execution state stays
behind the future Rust/Delta Kernel boundary.

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

`read_arrow_stream()`, `read_arrow()`, and `read_data_frame()` define the
execution interface. Until the Rust layer is linked, they fail with a typed
`delta_sharing_native_unavailable_error`; the R package does not fall back to a
second downloader or reader.
