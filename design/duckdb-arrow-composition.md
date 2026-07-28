# DuckDB Arrow-stream composition

Status: implemented and locally proven against `{duckdb}` 1.5.5,
`{arrow}` 22.0.0, and `{nanoarrow}` 0.8.0.1.

## Boundary decision

DuckDB's R function
[`duckdb_register_arrow()`](https://r.duckdb.org/reference/duckdb_register_arrow.html)
expects an Arrow-scannable object. It does not accept the
`nanoarrow_array_stream` returned by `read_arrow_stream()` directly. The
supported adapter object is an `arrow::RecordBatchReader` created with
[`as_record_batch_reader()`](https://arrow.apache.org/docs/r/reference/as_record_batch_reader.html):

```r
stream <- read_arrow_stream(read)
reader <- arrow::as_record_batch_reader(stream)
```

`as_record_batch_reader()` imports the Arrow C Stream without an IPC or
R-vector round trip. Import transfers ownership: after the call, the original
`nanoarrow_array_stream` pointer is invalid and must not be consumed or
released independently. The reader is the object registered with DuckDB:

```r
con <- DBI::dbConnect(
  duckdb::duckdb(shared_home = FALSE),
  dbdir = ":memory:"
)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
on.exit(try(reader$Close(), silent = TRUE), add = TRUE)

duckdb::duckdb_register_arrow(con, "shared_rows", reader)
on.exit(
  try(duckdb::duckdb_unregister_arrow(con, "shared_rows"), silent = TRUE),
  add = TRUE
)

result <- DBI::dbGetQuery(
  con,
  "select \"group\", count(*) as n
     from shared_rows
    group by \"group\"
    order by \"group\" nulls last"
)
```

The data returned by `dbGetQuery()` is only the SQL result. The shared source
rows travel through the single Delta Kernel to Arrow C Stream path and are not
first materialized as an R data frame or an Arrow table.

## Lifecycle contract

- A read stream is single-consumer. Register one `RecordBatchReader`, execute
  one SQL statement that scans it, then unregister it.
- DuckDB retains the registered reader until `duckdb_unregister_arrow()` or
  connection shutdown. Always unregister explicitly and close the reader.
- Normal exhaustion and DuckDB query errors both release the exported native
  Arrow stream. The integration test asserts that native active-stream counts
  return to their baseline in both cases.
- If more than one SQL statement must scan the rows, use the first statement to
  create a DuckDB temporary table. Do not attempt to rescan the one-shot reader.
- The proof uses one DuckDB worker thread. Parallel DuckDB execution is not a
  promise of parallel Delta Sharing download or Kernel scan execution.

## Optional dependencies and limitations

`{duckdb}`, `{DBI}`, and `{arrow}` are optional `Suggests`; none is imported by
the package core. The proof is skipped when any of them is absent. Its dedicated
dependency set is declared in `Config/Needs/duckdb`.

This is downstream composition, not a second delta.sharing materializer and not
a public DuckDB-specific API. DuckDB projection and filter planning cannot
recover columns or rows omitted by the immutable sharing read descriptor. The
sharing descriptor therefore remains the authority for remote projection,
predicate, and exact-limit semantics.

The package has no DuckDB-specific Rust code. DuckDB receives only the standard
Arrow reader imported from the existing C Stream boundary.
