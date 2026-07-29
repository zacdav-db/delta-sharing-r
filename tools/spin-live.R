# Live spin against the credentialed Databricks share on this machine. The
# selected source has 250 million nested event rows, deletion vectors, 257
# files, and about 1.8 GB of Parquet data.
pkgload::load_all(".", quiet = TRUE)


client <- sharing_client("~/Desktop/config.share")

cat("== Client ==\n")
print(client)

cat("\n== Shares ==\n")
print(client$list_shares())

share <- "delta_sharing_r_vnext_share"
schema <- "delta_sharing_r_vnext"

cat("\n== Tables in ", share, ".", schema, " ==\n", sep = "")
tables <- client$list_tables(share = share, schema = schema)
print(tables)

cat("\n== Nuanced 250M-row source ==\n")
tbl <- client$table(
  "delta_sharing_r_vnext_share.delta_sharing_r_vnext.dv_nested_events_250m"
)
cat("version:", tbl$version(), "\n")
print(tbl$protocol())

metadata <- tbl$metadata()
cat("description:", metadata$description, "\n")
cat(
  "source files:",
  format(metadata$num_files, big.mark = ","),
  " bytes:",
  format(metadata$size, big.mark = ","),
  "\n"
)

table_schema <- tbl$schema()
top_level_fields <- purrr::map_chr(table_schema$fields, "name")
payload <- purrr::detect(
  table_schema$fields,
  function(field) identical(field$name, "payload")
)
payload_fields <- purrr::map_chr(payload$type$fields, "name")
cat("top-level fields:", paste(top_level_fields, collapse = ", "), "\n")
cat("payload fields:", paste(payload_fields, collapse = ", "), "\n")

# This is large enough to display sustained progress without attempting to
# materialize all 250 million nested rows into R memory.
row_limit <- 1000000
cat(
  "\n== Kernel snapshot read (",
  format(row_limit, big.mark = ","),
  " rows) ==\n",
  sep = ""
)
started <- Sys.time()
events <- tbl$snapshot(
  columns = c("event_id", "tenant_id", "event_date", "event_ts", "payload"),
  limit = row_limit
)$to_data_frame(
  batch_size = 16384L,
  progress = TRUE
)
elapsed <- difftime(Sys.time(), started, units = "secs")

cat(
  "materialized:",
  format(nrow(events), big.mark = ","),
  "rows and",
  ncol(events),
  "columns in",
  round(as.numeric(elapsed), 1),
  "seconds\n"
)
utils::str(events, max.level = 2, vec.len = 3)

cat("\nLive spin complete.\n")
