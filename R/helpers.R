#' Print DeltaShareCredentials
#'
#' @param x Object of class `DeltaShareCredentials`
#' @param ... Additional args
#' @export
print.DeltaShareCredentials <- function(x, ...) {
  x <- paste0("(V", x$shareCredentialsVersion, ") [", x$endpoint, "]\n")
  cat(x)
  invisible(x)
}


#' Print DeltaShareTableVersion
#'
#' @param x Object of class `DeltaShareTableVersion`
#' @param ... Additional args
#' @export
print.DeltaShareTableVersion <- function(x, ...) {
  x <- paste0(x$`delta-table-version`, " [", x$date, "]\n")
  cat(x)
  invisible(x)
}

#' Clean File Metadata
#'
#' @param x File metadata list
clean_file_metadata <- function(x) {
  tibble::tibble(
    id = x$id,
    size = x$size,
    numRecords = purrr::map_int(x$stats, "numRecords"),
    stats = purrr::map(x$stats, ~purrr::transpose(.x[2:4])),
    partitionValues = x$partitionValues,
    url = x$url
  )
}

#' Convert Schema from Delta to Arrow
#' @details
#' Maps types from delta to arrow.
#' Will require parsing `schemaString`.
#'
#' e.g.
#' `jsonlite::fromJSON(self$metadata$metaData$schemaString)$fields`
#' @param schema `schemaString` fields object (parsed from JSON to list).
#' @return A [arrow:schema()] object
delta_to_arrow_schema <- function(schema) {

  # TODO:
  parsed_schema <- split(schema$name) %>%
    purrr::map(~{
      switch(
        .x$type,
        "string" = arrow::string(),
        "integer" = arrow::int64(),
        "double" = arrow::decimal(10, 1)
      )
    })

  do.call(arrow::schema, parsed_schema)

}

#' Clean x-ndjson (New Line Delimited JSON)
#'
#' @param x x-ndjson string.
#' @return List
clean_xndjson <- function(x) {
  x %>%
    httr2::resp_body_string() %>%
    strsplit("\n") %>%
    purrr::pluck(1) %>%
    purrr::map(~{jsonlite::fromJSON(.x)})
}
