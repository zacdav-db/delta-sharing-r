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


#' Map to Arrow Types
#'
#' @details TODO
#'
#' @param type Character, name of type to convert to Arrow.
#' @param settings Named list, additional metadata relevant to mapping.
#'
#' @return ArrowObject
#' @importFrom stats setNames
to_arrow_types <- function(type, settings = list()) {

  # if type was already an arrow object, spit it back out
  if (inherits(type, c("DataType", "ArrowObject"))) {
    return(type)
  }

  # resolve nested types (generally)
  if (is.list(settings$element_type)) {
    settings$element_type <- to_arrow_types(
      type = settings$element_type$type,
      settings = list(element_type = settings$element_type$elementType)
    )
  }

  # resolve nested types (`MapType()` items)
  if (is.list(settings$item_type)) {
    settings$item_type <- to_arrow_types(
      type = settings$item_type$type,
      settings = list(element_type = settings$item_type$elementType)
    )
  }

  # determine arrow type
  switch(
    type,
    "byte"      = arrow::int8(),
    "short"     = arrow::int16(),
    "integer"   = arrow::int32(),
    "long"      = arrow::int64(),
    "float"     = arrow::float32(),
    "double"    = arrow::float64(),
    "decimal"   = arrow::decimal128(
      precision = settings$decimal_precision,
      scale = settings$decimal_scale
    ),
    "string"    = arrow::string(),
    "boolean"   = arrow::boolean(),
    "binary"    = arrow::binary(),
    "timestamp" = arrow::timestamp(unit = "us", timezone = "UTC"),
    "date"      = arrow::date32(),
    "interval"  = arrow::duration(unit = "us"),
    "array"     = arrow::list_of(to_arrow_types(settings$element_type)),
    "map"       = arrow::map_of(
      key_type = to_arrow_types(settings$key_type),
      item_type = to_arrow_types(settings$item_type)
    ),
    "struct"    = do.call(
      arrow::struct,
      setNames(
        create_arrow_type_list(settings$fields),
        purrr::map_chr(settings$fields, "name")
      )
    )
  )
}


# TODO: Document + example
create_arrow_type_list <- function(fields) {

  purrr::map(fields, function(x) {

    settings <- list()

    # detect nested type (type will be a list)
    if (!is.list(x$type)) {

      # get type settings if needed
      type <- x$type

      if (grepl("decimal", type)) {
        settings$decimal_precision <- as.integer(gsub(".*\\((\\d+),(\\d+)\\)", "\\1", type))
        settings$decimal_scale <- as.integer(gsub(".*\\((\\d+),(\\d+)\\)", "\\2", type))
      }

      # remove brackets if they are present
      type <- gsub("(.*?)\\(.*", "\\1", type)

      # convert
      arrow_type <- to_arrow_types(type, settings)


    } else {

      # handle complex types
      type <- x$type$type

      if (type == "array") {
        settings$element_type <- x$type$elementType
      } else if (type == "struct") {
        settings$fields <- x$type$fields
      } else if (type == "map") {
        settings$item_type <- x$type$valueType
        settings$key_type <- x$type$keyType
      }

      arrow_type <- to_arrow_types(type, settings)

    }

    return(arrow_type)

  })

}

#' Convert Schema from Delta to Arrow
#' @details
#' Maps types from delta to arrow.
#' Will require parsing `schemaString`.
#'
#' e.g.
#' `jsonlite::fromJSON(self$metadata$metaData$schemaString, simplifyDataFrame = FALSE)`
#'
#' Partition columns are overrode to be `arrow::string()`.
#'
#' @param schema `schemaString` fields object (parsed from JSON to list).
#' @param partitions character vector of partition names
#' @return A [arrow:schema()] object
convert_to_arrow_schema <- function(schema, partitions = list()) {
  parsed_schema <- create_arrow_type_list(schema$fields)
  names(parsed_schema) <- purrr::map_chr(schema$fields, "name")
  # columns will need to be re-organised (partitions to the right)
  # for now they are always treated as string type
  if (length(partitions) > 1) {
    pcheck <- names(parsed_schema) %in% partitions
    pcols <- purrr::map(seq_along(partitions), ~arrow::string())
    names(pcols) <- partitions
    parsed_schema <- c(parsed_schema[!pcheck], pcols)
  }
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

#TODO Docs
list_to_hive_partition <- function(x) {
  string <- purrr::imap_chr(x, ~paste(.y, .x, sep = "="))
  paste(string, collapse = "/")
}
