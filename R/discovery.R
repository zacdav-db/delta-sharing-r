# Share/schema/table discovery. Each function paginates the relevant REST route
# and returns a tibble with stable character columns. Storage locations and
# other private fields are deliberately excluded.

# Build a tibble from a list of record lists, pulling named fields with a
# missing -> NA fallback so column types stay stable (including across pages
# and for empty results).
records_to_tibble <- function(items, fields) {
  cols <- purrr::map(fields, function(path) {
    purrr::map_chr(items, \(item) item[[path]] %||% NA_character_)
  })
  tibble::as_tibble(rlang::set_names(cols, names(fields)))
}

sharing_list_shares <- function(profile, auth) {
  profile |>
    sharing_paginate(auth, "shares", "list_shares") |>
    records_to_tibble(c(name = "name", id = "id"))
}

sharing_list_schemas <- function(profile, auth, share = NULL) {
  if (is.null(share)) {
    shares <- sharing_list_shares(profile, auth)$name
    return(
      purrr::map(shares, \(s) sharing_list_schemas(profile, auth, s)) |>
        purrr::list_rbind()
    )
  }
  share <- discovery_name(share, "share", "list_schemas")
  items <- sharing_paginate(
    profile,
    auth,
    c("shares", share, "schemas"),
    "list_schemas"
  )
  tibble::tibble(share = share, name = purrr::map_chr(items, "name"))
}

sharing_list_tables <- function(profile, auth, share = NULL, schema = NULL) {
  if (is.null(share) && is.null(schema)) {
    schemas <- sharing_list_schemas(profile, auth)
    return(
      purrr::map2(
        schemas$share,
        schemas$name,
        \(sh, sc) sharing_list_tables(profile, auth, sh, sc)
      ) |>
        purrr::list_rbind()
    )
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
  records_to_tibble(items, c(share = "share", schema = "schema", name = "name"))
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
