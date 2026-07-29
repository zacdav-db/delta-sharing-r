# delta.sharing <img src="https://user-images.githubusercontent.com/1446829/144671151-b095e1b9-2d24-4d3b-b3c6-a7041e491077.png" align="right" width="180" alt="Delta Sharing logo" />

An R client for [Delta Sharing](https://delta.io/sharing/), backed by
[Delta Kernel](https://delta.io/delta-kernel/) and Arrow.

## Installation

```r
# install.packages("pak")
pak::pak("zacdav-db/delta-sharing-r")
```

Building from source requires Cargo, `rustc >= 1.88`, and CMake.

## Quick start

```r
library(delta.sharing)

client <- sharing_client("~/config.share")

# Discover everything available through the profile
client$list_shares()
client$list_schemas()
client$list_tables()

# Create a reusable table handle
orders <- client$table("sales.default.orders")

# Inspect metadata without scanning rows
orders$version()
orders$metadata()
orders$schema()

# Read a snapshot
snapshot <- orders$snapshot(columns = c("order_id", "amount"), limit = 1000)
orders_df <- snapshot$to_data_frame()
orders_arrow <- snapshot$to_arrow()
orders_stream <- snapshot$to_arrow_stream()
```

`to_data_frame()` and `to_arrow()` show rows read in interactive sessions.
Pass `progress = FALSE` to disable the indicator, or `progress = TRUE` to show
it from a script.

## Snapshots and changes

Read a table at a specific version or timestamp:

```r
orders$snapshot(version = 42)$to_data_frame()
orders$snapshot(timestamp = "2026-01-01T00:00:00Z")$to_data_frame()
```

Read an inclusive change data feed range:

```r
orders$changes(
  starting_version = 120,
  ending_version = 125
)$to_data_frame()
```

See `vignette("delta-sharing")` for a full walkthrough.

## Query with DuckDB

DuckDB accepts both Arrow materializers:

- `to_arrow_reader()` is lazy and suited to one pass over a large result.
- `to_arrow()` materializes an Arrow table in memory and is useful when DuckDB
  should scan the same result more than once.

Both avoid an intermediate R data frame. This requires the optional `arrow`,
`DBI`, and `duckdb` packages.

```r
snapshot <- orders$snapshot(
  columns = c("status", "amount")
)

reader <- snapshot$to_arrow_reader()

con <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
duckdb::duckdb_register_arrow(con, "shared_orders", reader)

revenue <- DBI::dbGetQuery(con, "
  SELECT status, count(*) AS orders, sum(amount) AS revenue
  FROM shared_orders
  GROUP BY status
  ORDER BY revenue DESC
")

duckdb::duckdb_unregister_arrow(con, "shared_orders")
reader$Close()
DBI::dbDisconnect(con)
```

For the eager path, replace the reader construction and registration lines
above with:

```r
arrow_table <- snapshot$to_arrow()
duckdb::duckdb_register_arrow(con, "shared_orders", arrow_table)
```

An Arrow reader is single-consumer. Use an Arrow table, or create a temporary
DuckDB table during the first query, when the result needs to be scanned
several times.
