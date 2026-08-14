# delta.sharing <img src="https://user-images.githubusercontent.com/1446829/144671151-b095e1b9-2d24-4d3b-b3c6-a7041e491077.png" align="right" width="140" alt="Delta Sharing logo" />

[![R CMD check](https://github.com/zacdav-db/delta-sharing-r/actions/workflows/package-check.yaml/badge.svg)](https://github.com/zacdav-db/delta-sharing-r/actions/workflows/package-check.yaml)
[![R coverage](https://codecov.io/gh/zacdav-db/delta-sharing-r/branch/main/graph/badge.svg)](https://app.codecov.io/gh/zacdav-db/delta-sharing-r)

An R client for [Delta Sharing](https://delta.io/sharing/), backed by
[Delta Kernel](https://docs.delta.io/kernel/rust/introduction.html) and Arrow.

See the [package website](https://zacdav-db.github.io/delta-sharing-r/) for the
complete reference and introductory guide.

## Installation

```r
# install.packages("pak")
pak::pak("zacdav-db/delta-sharing-r")
```

Building from source requires Cargo and `rustc >= 1.88`.

## Quick start

```r
library(delta.sharing)

client <- sharing_client(demo_profile())

# Discover the public example tables
client$list_tables("delta_sharing")

housing <- client$table("delta_sharing.default.boston-housing")
housing_tbl <- housing$snapshot(limit = 1000)$to_tibble()
```

`to_tibble()` is the usual eager R materializer. The same reader can instead
return a base data frame with `to_data_frame()`, an Arrow table with
`to_arrow()`, or a lazy Arrow reader with `to_arrow_reader()`. The Arrow
materializers require the optional `arrow` package.

Each materializer call performs its own read through the same Arrow C stream
path. For snapshots, the default response format negotiation is reused by later
reads of the same table through one client. Metadata and schema inspection
remain fresh requests.

For your own share, pass a profile file and select its table:

```r
client <- sharing_client("~/config.share")
orders <- client$table("sales.default.orders")
```

## Snapshots and changes

Read a table at a specific version or timestamp:

```r
orders$snapshot(version = 42)$to_tibble()
orders$snapshot(timestamp = "2026-01-01T00:00:00Z")$to_tibble()
```

Read an inclusive change data feed range:

```r
orders$changes(
  starting_version = 120,
  ending_version = 125
)$to_tibble()
```

Each table downloads up to four selected files concurrently by default. Its
downloads are cached for the R session and reused by other handles for the same
endpoint and table:

```r
orders_tbl <- orders$snapshot()$to_tibble()

# A later read can reuse unchanged Delta data files.
refreshed_tbl <- orders$snapshot()$to_tibble()

# The cache is an ordinary directory under R's session temp directory.
orders$cache_path
```

Set `concurrency` when creating the table handle to tune downloads. The cache is
stored in R's session temporary directory and is normally removed when R exits.
Advanced users can delete `orders$cache_path` manually; do not do that while a
lazy reader is active. Interactive missing-file downloads report completed
files and total bytes when sizes are available from the sharing server.

See `vignette("delta-sharing")` for a full walkthrough.

## Query with DuckDB

DuckDB accepts both Arrow materializers after the selected files have been
staged in the session cache:

- `to_arrow_reader()` streams rows lazily and is suited to one pass over a large
  result.
- `to_arrow()` materializes an Arrow table in memory and is useful when DuckDB
  should scan the same result more than once.

Both avoid an intermediate R data frame. This requires the optional `arrow`,
`DBI`, and `duckdb` packages.

Register a lazy Arrow reader directly:

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

For repeated queries, materialize and register an in-memory Arrow table
instead:

```r
arrow_table <- snapshot$to_arrow()
duckdb::duckdb_register_arrow(con, "housing", arrow_table)
```

Arrow tables keep the result in memory and do not need `Close()`.

## Performance

Directional results from one consumer setup. Each value is the median of three
end-to-end `to_tibble()` reads at the default concurrency of four. A cached read
repeats the query after its selected files have been downloaded into the session
cache. The benchmark can be rerun with
[`bench/snapshot.R`](bench/snapshot.R).

*Apple M2 Pro (12 cores), 32 GB RAM, R 4.5.1; VPN connection: 92 Mbps down,
111 ms base round-trip latency.*

| Rows | Materialized R size | Empty file cache | Cached files |
|---:|---:|---:|---:|
| 10,000 | 0.38 MiB | 5.15 s | 0.82 s |
| 1,000,000 | 38.1 MiB | 8.16 s | 1.69 s |
| 10,000,000 | 381 MiB | 28.3 s | 6.45 s |
