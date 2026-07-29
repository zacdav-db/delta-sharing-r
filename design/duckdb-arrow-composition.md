# DuckDB Arrow-stream composition

Status: implemented and locally proven against `{duckdb}` 1.5.5,
`{arrow}` 22.0.0, and `{nanoarrow}` 0.8.0.1.

## Boundary decision

DuckDB's R function
[`duckdb_register_arrow()`](https://r.duckdb.org/reference/duckdb_register_arrow.html)
expects an Arrow-scannable object. It does not accept the
`nanoarrow_array_stream` returned by `to_arrow_stream()` directly. The package
therefore exposes the supported adapter object directly:

```r
reader <- table$snapshot()$to_arrow_reader()
```

`to_arrow_reader()` imports the Arrow C Stream without an IPC or R-vector round
trip. Import transfers ownership to the returned `arrow::RecordBatchReader`,
which is the object registered with DuckDB:

```r
result <- local({
  reader <- table$snapshot()$to_arrow_reader()
  on.exit(try(reader$Close(), silent = TRUE), add = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(shared_home = FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  duckdb::duckdb_register_arrow(con, "shared_rows", reader)
  on.exit(
    try(duckdb::duckdb_unregister_arrow(con, "shared_rows"), silent = TRUE),
    add = TRUE
  )

  DBI::dbGetQuery(
    con,
    "select \"group\", count(*) as n
       from shared_rows
      group by \"group\"
      order by \"group\" nulls last"
  )
})
```

The data returned by `dbGetQuery()` is only the SQL result. The shared source
rows travel through the single Delta Kernel to Arrow C Stream path and are not
first materialized as an R data frame or an Arrow table.

## Lifecycle contract

- A read stream is single-consumer. Register one `RecordBatchReader`, execute
  one SQL statement that scans it, then unregister it.
- DuckDB retains the registered reader until `duckdb_unregister_arrow()` or
  connection shutdown. Always unregister explicitly and close the reader.
- Normal exhaustion, `Close()`, and finalization release the exported native
  Arrow stream.
- If more than one SQL statement must scan the rows, use the first statement to
  create a DuckDB temporary table. Do not attempt to rescan the one-shot reader.
- The proof uses one DuckDB worker thread. Parallel DuckDB execution is not a
  promise of parallel Delta Sharing download or Kernel scan execution.

## Optional dependencies and limitations

`{arrow}` is an optional `Suggests` dependency because `to_arrow_reader()` is a
public materializer. `{duckdb}` and `{DBI}` are downstream consumers shown in
the README; they are not package dependencies.

This is downstream composition, not a second delta.sharing materializer and not
a public DuckDB-specific API. DuckDB projection and filter planning cannot
recover columns or rows omitted by the immutable sharing read descriptor. The
sharing descriptor therefore remains the authority for remote projection,
predicate, and exact-limit semantics.

The package has no DuckDB-specific Rust code. DuckDB receives only the standard
Arrow reader imported from the existing C Stream boundary.
