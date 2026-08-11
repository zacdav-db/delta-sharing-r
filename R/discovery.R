# Share/schema/table discovery. Each function paginates the relevant REST route
# and returns the server's records as a printable list.

#' @export
print.delta_sharing_listing <- function(x, ...) {
  kind <- attr(x, "kind")
  fields <- switch(
    kind,
    shares = "name",
    schemas = c("share", "name"),
    tables = c("share", "schema", "name")
  )
  cat(sprintf("<Delta Sharing %s> %d\n", kind, length(x)))
  purrr::walk(x, function(record) {
    cat("  ", paste(unlist(record[fields]), collapse = "."), "\n", sep = "")
  })
  invisible(x)
}

sharing_list_shares <- function(profile, auth) {
  records <- sharing_paginate(profile, auth, "shares", "list_shares")
  structure(
    records,
    class = c("delta_sharing_listing", "list"),
    kind = "shares"
  )
}

sharing_list_schemas <- function(profile, auth, share) {
  share <- discovery_name(share, "share", "list_schemas")
  items <- sharing_paginate(
    profile,
    auth,
    c("shares", share, "schemas"),
    "list_schemas"
  )
  records <- purrr::map(items, \(item) {
    c(list(share = share), item)
  })
  structure(
    records,
    class = c("delta_sharing_listing", "list"),
    kind = "schemas"
  )
}

sharing_list_tables <- function(profile, auth, share, schema = NULL) {
  share <- discovery_name(share, "share", "list_tables")
  path <- if (is.null(schema)) {
    c("shares", share, "all-tables")
  } else {
    schema <- discovery_name(schema, "schema", "list_tables")
    c("shares", share, "schemas", schema, "tables")
  }
  records <- sharing_paginate(
    profile,
    auth,
    path,
    "list_tables"
  )
  structure(
    records,
    class = c("delta_sharing_listing", "list"),
    kind = "tables"
  )
}

discovery_name <- function(value, name, operation) {
  if (!is_scalar_character(value) || grepl("[[:cntrl:]]", value)) {
    abort(
      "{.arg {name}} must be one non-empty name without control characters.",
      type = "validation",
      operation = operation
    )
  }
  value
}
