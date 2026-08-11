# Table identifier parsing for the public client surface.

sharing_table_identifier <- function(name, schema = NULL, share = NULL) {
  if (is.null(schema) && is.null(share)) {
    if (!is_scalar_character(name)) {
      abort(
        "{.arg name} must be a {.val share.schema.name} string when \\
         {.arg share} and {.arg schema} are omitted.",
        type = "validation",
        operation = "table"
      )
    }
    parts <- strsplit(name, ".", fixed = TRUE)[[1]]
    if (length(parts) != 3L || any(!nzchar(parts))) {
      abort(
        "A compact table name must have exactly three non-empty \\
         dot-separated parts.",
        type = "validation",
        operation = "table"
      )
    }
    share <- parts[[1]]
    schema <- parts[[2]]
    name <- parts[[3]]
  }
  identifier <- list(share = share, schema = schema, table = name)
  if (!purrr::every(identifier, is_scalar_character)) {
    abort(
      "{.arg share}, {.arg schema}, and {.arg name} must each be one \
       non-empty string.",
      type = "validation",
      operation = "table"
    )
  }
  identifier
}
