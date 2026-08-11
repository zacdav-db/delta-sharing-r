# Share/schema/table discovery. Each function paginates the relevant REST route
# and returns a printable list of records. Storage locations and other private
# fields are deliberately excluded.

# Keep only the public fields from each discovery record.
discovery_records <- function(items, fields, kind) {
  records <- purrr::map(items, function(item) {
    purrr::map(fields, \(field) item[[field]] %||% NA_character_)
  })
  structure(records, class = c("delta_sharing_listing", "list"), kind = kind)
}

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
  profile |>
    sharing_paginate(auth, "shares", "list_shares") |>
    discovery_records(c(name = "name", id = "id"), "shares")
}

sharing_list_schemas <- function(profile, auth, share = NULL) {
  if (is.null(share)) {
    records <- sharing_list_shares(profile, auth) |>
      purrr::map(\(record) {
        sharing_list_schemas(profile, auth, record$name)
      }) |>
      purrr::list_flatten()
    return(structure(
      records,
      class = c("delta_sharing_listing", "list"),
      kind = "schemas"
    ))
  }
  share <- discovery_name(share, "share", "list_schemas")
  items <- sharing_paginate(
    profile,
    auth,
    c("shares", share, "schemas"),
    "list_schemas"
  )
  records <- purrr::map(items, \(item) {
    list(share = share, name = item$name %||% NA_character_)
  })
  structure(
    records,
    class = c("delta_sharing_listing", "list"),
    kind = "schemas"
  )
}

sharing_list_tables <- function(profile, auth, share = NULL, schema = NULL) {
  if (is.null(share) && is.null(schema)) {
    records <- sharing_list_schemas(profile, auth) |>
      purrr::map(\(record) {
        sharing_list_tables(profile, auth, record$share, record$name)
      }) |>
      purrr::list_flatten()
    return(structure(
      records,
      class = c("delta_sharing_listing", "list"),
      kind = "tables"
    ))
  }
  if (is.null(schema)) {
    return(sharing_list_tables_in_share(profile, auth, share))
  }
  share <- discovery_name(share, "share", "list_tables")
  schema <- discovery_name(schema, "schema", "list_tables")
  profile |>
    sharing_paginate(
      auth,
      c("shares", share, "schemas", schema, "tables"),
      "list_tables"
    ) |>
    table_records()
}

sharing_list_tables_in_share <- function(profile, auth, share) {
  share <- discovery_name(share, "share", "list_tables_in_share")
  profile |>
    sharing_paginate(auth, c("shares", share, "all-tables"), "list_tables") |>
    table_records()
}

table_records <- function(items) {
  discovery_records(
    items,
    c(share = "share", schema = "schema", name = "name"),
    "tables"
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
