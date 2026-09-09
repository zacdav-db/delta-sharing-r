# delta.sharing <img src="https://user-images.githubusercontent.com/1446829/144671151-b095e1b9-2d24-4d3b-b3c6-a7041e491077.png" align="right" width="140" alt="Delta Sharing logo" />

[![R CMD check](https://github.com/zacdav-db/delta-sharing-r/actions/workflows/package-check.yaml/badge.svg)](https://github.com/zacdav-db/delta-sharing-r/actions/workflows/package-check.yaml)
[![R coverage](https://codecov.io/gh/zacdav-db/delta-sharing-r/branch/main/graph/badge.svg)](https://app.codecov.io/gh/zacdav-db/delta-sharing-r)

`delta.sharing` reads [Delta Sharing](https://delta.io/sharing/) tables from R.
Discover shares, schemas, and tables, then read snapshots or change data feeds
as tibbles, data frames, or Arrow objects.

Reads are powered by [Delta Kernel](https://docs.delta.io/kernel/rust/introduction.html),
including support for deletion vectors and column mapping.

See the [package website](https://zacdav-db.github.io/delta-sharing-r/) for the
guides and the complete reference.

## Installation

Install the development version from GitHub:

```r
# install.packages("pak")
pak::pak("zacdav-db/delta-sharing-r")
```

Installation from source requires Cargo and `rustc >= 1.88`.

## Quick start

The public example server needs no registration or private credential:

```r
library(delta.sharing)

client <- sharing_client(demo_profile())
housing <- client$table("delta_sharing.default.boston-housing")

housing$snapshot(
  columns = c("chas", "medv"),
  limit = 5
)$to_tibble()
```

`demo_profile()` retrieves the public profile maintained by the Delta Sharing
project.

For your own share, pass the path to its profile, discover the available
tables, and create a reusable table handle:

```r
client <- sharing_client("~/config.share")
client$list_tables("sales", "default")

orders <- client$table("sales.default.orders")
```

The [Getting started guide](https://zacdav-db.github.io/delta-sharing-r/articles/delta-sharing.html)
walks through profiles, discovery, table metadata, and reads.

## Read snapshots and changes

Read the latest snapshot, optionally selecting columns and limiting rows:

```r
orders_tbl <- orders$snapshot(
  columns = c("order_id", "status", "amount"),
  limit = 1000
)$to_tibble()
```

Snapshots can also target a specific version or timestamp:

```r
orders$snapshot(version = 42)$to_tibble()
orders$snapshot(timestamp = "2026-01-01T00:00:00Z")$to_tibble()
```

Read an inclusive change data feed range:

```r
changes_tbl <- orders$changes(
  starting_version = 120,
  ending_version = 125
)$to_tibble()
```

`to_tibble()` is the usual choice for R analysis. Use `to_data_frame()` when a
base data frame is required. Both automatically return BIGINT columns as
`bit64::integer64`, including small values, empty results, and nested columns.
The value -9223372036854775808 raises a conversion error because bit64 reserves
it for missing values; use an Arrow materializer to retain it.

For Arrow workflows, `to_arrow()` returns an in-memory table and
`to_arrow_reader()` returns a lazy reader. Arrow is a required dependency.
`to_arrow_stream()` exposes the lower-level Arrow C Stream directly.

Selected files are downloaded concurrently and cached for the R session. See
the [Performance and caching guide](https://zacdav-db.github.io/delta-sharing-r/articles/performance-caching.html)
for cold and repeated reads, cache lifetime, concurrency, batching, and tuning.

## Query with DuckDB

DuckDB can query a lazy Arrow reader without first creating an R data frame.
This requires the optional `DBI` and `duckdb` packages.

```r
snapshot <- housing$snapshot(
  columns = c("chas", "medv")
)
reader <- snapshot$to_arrow_reader()

con <- DBI::dbConnect(duckdb::duckdb())
duckdb::duckdb_register_arrow(con, "housing", reader)

summary <- DBI::dbGetQuery(con, "
  SELECT chas, count(*) AS homes, avg(medv) AS mean_value
  FROM housing
  GROUP BY chas
  ORDER BY chas
")
summary
#>   chas homes mean_value
#> 1    0   471   22.29553
#> 2    1    35   30.17500

duckdb::duckdb_unregister_arrow(con, "housing")
reader$Close()
DBI::dbDisconnect(con)
```

Use `snapshot$to_arrow()` instead when the same result will be queried more
than once. This materializes the result in Arrow memory and does not require
`Close()`.

## Performance

These results are medians of three end-to-end `to_tibble()` snapshot reads
using four concurrent downloads. The cached read repeats the same query after
its selected files have been staged locally.

*Apple M2 Pro (12 cores), 32 GB RAM, R 4.5.1; VPN connection: 92 Mbps down,
111 ms base round-trip latency.*

| Rows | R result size | Empty cache | Cached |
|---:|---:|---:|---:|
| 10,000 | 0.38 MiB | 5.15 s | 0.82 s |
| 1,000,000 | 38.1 MiB | 8.16 s | 1.69 s |
| 10,000,000 | 381 MiB | 28.3 s | 6.45 s |

Each measurement includes the Sharing request, local log construction, Delta
Kernel scan, and tibble materialization—not just network transfer. Reproduce
the benchmark with
[`bench/snapshot.R`](https://github.com/zacdav-db/delta-sharing-r/blob/main/bench/snapshot.R).
