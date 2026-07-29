# Take the package for a spin: real kernel reads against local Delta fixtures,
# plus mocked discovery/metadata so the full R6 surface is exercised end to end.
pkgload::load_all(".", quiet = TRUE)

fixture <- function(name) {
  normalizePath(file.path("tests/testthat/fixtures/delta", name), winslash = "/")
}

cat("\n== 1. Client from an inline profile ==\n")
client <- sharing_client(list(
  shareCredentialsVersion = 2,
  type = "bearer_token",
  endpoint = "https://sharing.example.test/api",
  bearerToken = "example-only-not-a-secret"
))
print(client)

cat("\n== 2. Discovery (mocked server) ==\n")
mock <- function(req) {
  path <- httr2::url_parse(req$url)$path
  if (grepl("/shares$", path)) {
    httr2::response_json(body = list(items = list(
      list(name = "sales", id = "s1"), list(name = "marketing", id = "s2")
    )))
  } else if (grepl("/schemas$", path)) {
    httr2::response_json(body = list(items = list(list(name = "default"))))
  } else if (grepl("/tables$", path)) {
    httr2::response_json(body = list(items = list(
      list(share = "sales", schema = "default", name = "orders"),
      list(share = "sales", schema = "default", name = "returns")
    )))
  } else {
    httr2::response(404)
  }
}
httr2::with_mocked_responses(mock, {
  print(client$list_shares())
  print(client$list_tables(share = "sales", schema = "default"))
})

cat("\n== 3. Real kernel read of a local Delta table ==\n")
stream <- native_snapshot_stream(fixture("local-table"))
df <- sharing_stream_to_data_frame(stream)
cat("rows:", nrow(df), " cols:", paste(names(df), collapse = ", "), "\n")
print(utils::head(df))

cat("\n== 4. Projection + limit through the kernel ==\n")
df2 <- sharing_stream_to_data_frame(
  native_snapshot_stream(fixture("local-table"), columns = c("id", "value"), limit = 3)
)
print(df2)

cat("\n== 5. Logical types round-trip ==\n")
df3 <- sharing_stream_to_data_frame(native_snapshot_stream(fixture("logical-types")))
cat("cols:", paste(names(df3), collapse = ", "), "\n")
utils::str(df3, max.level = 1)

cat("\n== 6. Typed errors ==\n")
err <- tryCatch(
  client$table("only.two"),
  delta_sharing_validation_error = function(e) conditionMessage(e)
)
cat("validation error:", err, "\n")

cat("\nSpin complete.\n")
