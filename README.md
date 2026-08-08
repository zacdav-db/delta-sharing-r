# delta.sharing <img src="https://user-images.githubusercontent.com/1446829/144671151-b095e1b9-2d24-4d3b-b3c6-a7041e491077.png" align="right" width="140" alt="Delta Sharing logo" />

[![R CMD check](https://github.com/zacdav-db/delta-sharing-r/actions/workflows/package-check.yaml/badge.svg)](https://github.com/zacdav-db/delta-sharing-r/actions/workflows/package-check.yaml)
[![Codecov](https://codecov.io/gh/zacdav-db/delta-sharing-r/branch/main/graph/badge.svg)](https://app.codecov.io/gh/zacdav-db/delta-sharing-r)

An R client for [Delta Sharing](https://delta.io/sharing/), backed by
[Delta Kernel](https://docs.delta.io/kernel/rust/introduction.html) and Arrow.

See the [package website](https://zacdav-db.github.io/delta-sharing-r/) for the
complete reference and introductory guide.

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

Eager reads use the same direct Arrow stream as the lazy materializers, without
an intermediate collection or replay step.

The default `response_format = "auto"` negotiation is reused for subsequent
reads of the same table through one client. Metadata and schema inspection
remain fresh requests.

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
DBI::dbExecute(con, "SET threads = 1")
duckdb::duckdb_register_arrow(con, "shared_orders", reader)

revenue <- DBI::dbGetQuery(con, "
  SELECT status, count(*) AS orders, sum(amount) AS revenue
  FROM shared_orders
  GROUP BY status
  ORDER BY revenue DESC
")

duckdb::duckdb_unregister_arrow(con, "shared_orders")
DBI::dbDisconnect(con, shutdown = TRUE)
reader$Close()
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

## Performance

Directional end-to-end snapshot results, reported as medians of three reads
after one warm-up.

*Apple M2 Pro MacBook Pro, 32 GB RAM, R 4.5.1; VPN connection: 93 Mbps
down, 115 ms base round-trip latency.*

| Rows | Materialized R size | Elapsed, median (range) | Median rows/s |
|---:|---:|---:|---:|
| 10,000 | 0.38 MiB | 5.83 s (5.82–7.77) | 1,700 |
| 1,000,000 | 38.15 MiB | 11.91 s (11.33–25.66) | 84,000 |
| 10,000,000 | 381.47 MiB | 71.49 s (66.36–71.79) | 140,000 |
