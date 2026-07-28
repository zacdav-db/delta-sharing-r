# Query a Delta Sharing read through DuckDB without materializing source rows
# as an R data frame or Arrow table.
duckdb_group_summary <- function(read) {
  stopifnot(
    requireNamespace("arrow", quietly = TRUE),
    requireNamespace("DBI", quietly = TRUE),
    requireNamespace("duckdb", quietly = TRUE)
  )

  stream <- read_arrow_stream(read, batch_size = 65536L)
  reader <- NULL
  con <- NULL
  registered <- FALSE

  on.exit({
    if (registered) {
      try(
        duckdb::duckdb_unregister_arrow(con, "shared_rows"),
        silent = TRUE
      )
    }
    if (!is.null(reader)) {
      try(reader$Close(), silent = TRUE)
    }
    if (!is.null(con)) {
      try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
    }
    if (nanoarrow::nanoarrow_pointer_is_valid(stream)) {
      try(stream$release(), silent = TRUE)
    }
  }, add = TRUE)

  reader <- arrow::as_record_batch_reader(stream)
  con <- DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE),
    dbdir = ":memory:"
  )
  duckdb::duckdb_register_arrow(con, "shared_rows", reader)
  registered <- TRUE

  DBI::dbGetQuery(
    con,
    "select \"group\", count(*) as n, sum(id) as id_sum
       from shared_rows
      group by \"group\"
      order by \"group\" nulls last"
  )
}
