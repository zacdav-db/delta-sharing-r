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

# TODO: Documentation
extract_file_metadata <- function(x, changes) {
  file_type <- if (changes) names(x)[1] else "file"
  x <- x[[file_type]]

  file_metadata <- list(
    url = x$url,
    id = x$id,
    partitionValues = list(x$partitionValues),
    size = x$size,
    stats = if (!is.null(x$stats)) list(jsonlite::fromJSON(x$stats)) else x$stats
  )

  if (changes) {
    file_metadata[["change_type"]] <- file_type
    file_metadata[["commit_timestamp"]] <- x$timestamp
    file_metadata[["commit_version"]] <- x$version
  }

  return(file_metadata)
}

# TODO: docs
resolve_query_files <- function(x, table_folder, changes = FALSE) {

  # add metadata to query files
  # - where to save
  # - check if they already exist locally (to skip re-downloading)

  files <- x %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      hivePartitions = delta.sharing:::list_to_hive_partition(partitionValues)
    ) %>%
    dplyr::ungroup()

  if (changes) {
    files <- files %>%
      dplyr::mutate(
        # _change_type, _commit_timestamp, _commit_version
        change_type = dplyr::case_when(
          change_type == "add" ~ "insert",
          change_type == "remove" ~ "delete",
          TRUE ~ change_type
        ),
        file_string = paste(id, "_", change_type, commit_timestamp, commit_version, sep = "_"),
        file = paste0(file_string, ".parquet"),
        destPath = file.path(!!table_folder, "_table_changes", hivePartitions),
        filePath = file.path(destPath, file)
      )
  } else {
    files <- files %>%
      dplyr::mutate(
        files, file = paste0(id, ".parquet"),
        destPath = file.path(!!table_folder, "_table", hivePartitions),
        filePath = file.path(destPath, file)
      )
  }

  # both paths are generated to check if files can be skipped from
  # download step later on, there are two types of skipping:
  # 1. file exists in current directory (_table_changes or root)
  # 2. can be copied between _table_changes and root directory
  existing_files <- list.files(path = table_folder, recursive = TRUE) %>%
    data.frame(file = .) %>%
    dplyr::mutate(
      id = gsub(".*?(.*\\/)?([a-z0-9]*?)(_.*?)?(\\.parquet)", "\\2", file, perl = TRUE),
      is_in_cdf = grepl("^_table_changes", file)
    )

  existing_ids_root <- dplyr::filter(existing_files, !is_in_cdf)$id
  existing_ids_cdf <- dplyr::filter(existing_files, is_in_cdf)$id

  files <- files %>%
    dplyr::mutate(
      relativePath = sub("^\\/", "", file.path(hivePartitions, file)),
      alreadyExistsRoot = id %in% existing_ids_root,
      alreadyExistsInCDF = id %in% existing_ids_cdf,
      alreadyExists = alreadyExistsRoot | alreadyExistsInCDF,
      to_download = !alreadyExists,
      to_copy_to_cdf = !alreadyExistsInCDF & alreadyExistsRoot,
      to_copt_to_root = !alreadyExistsRoot & alreadyExistsInCDF
    )

  files

}

# TODO: Documentation
download_new_files <- function(new_query_files, changes) {

  # create progress bar
  pb <- progress::progress_bar$new(
    format = "  downloading files (:elapsedfull) [:bar] :current/:total (:percent) [:eta]",
    total = nrow(new_query_files),
    clear = FALSE,
    width = 100
  )
  pb$tick(0)

  # download new files
  new_query_files %>%
    purrr::pwalk(function(url, filePath, ...) {
      download.file(url, destfile = filePath, quiet = TRUE)
      pb$tick()
    })

}

# TODO: Documentation
version_is_valid <- function(x) {
  floor(x) == x
}

# TODO: Documentation
timestamp_is_valid <- function(x) {
  # it should be defined as a character
  if (is.character(x)) {
    x <- as.POSIXct(x, format = "%Y-%m-%d %H:%M:%S")
    return(!is.na(x))
  }
  FALSE
}

# TODO: Documentation
validate_cdf_options <- function(starting_version, ending_version,
                                 starting_timestamp, ending_timestamp) {

  # minimum requirement is to have starting_* parameter
  # can only specify either version or timestamp, not both
  if (!xor(is.null(starting_version), is.null(starting_timestamp))) {
    rlang::abort(
      paste(
        "INVALID_PARAMETER_VALUE: Must pass either starting_version or",
        "starting_timestamp using set_cdf_options() before performing CDF read."
      )
    )
  }

  if (all(!is.null(c(ending_version, ending_timestamp)))) {
    rlang::abort(
      paste(
        "INVALID_PARAMETER_VALUE: If specifying an end point use either
        ending_version or ending_timestamp using set_cdf_options()."
      )
    )
  }

  # assert valid version/timestamp parameters
  if (!is.null(starting_version)) {
    # ensure versions are valid values
    if (any(!version_is_valid(c(starting_version, ending_version)))) {
      rlang::abort("INVALID_PARAMETER_VALUE: Versions must be represented as integer values.")
    }
    # ensure start is before end
    if (!is.null(ending_version) && (starting_version > ending_version)) {
      rlang::abort(
        paste(
          "INVALID_PARAMETER_VALUE: `starting_version` must be less than or",
          "equal to `ending_version`"
        )
      )
    }
  } else {
    # ensure timestamps are valid
    if (any(!timestamp_is_valid(c(starting_timestamp, ending_timestamp)))) {
      rlang::abort(
        paste(
          "INVALID_PARAMETER_VALUE: Timestamps must be in a valid",
          "yyyy-mm-dd hh:mm:ss[.fffffffff] string format."
        )
      )
    }
    # ensure start is before end
    if (!is.null(ending_timestamp) && (starting_timestamp > ending_timestamp)) {
      rlang::abort(
        paste(
          "INVALID_PARAMETER_VALUE: `starting_timestamp` must be less than or",
          "equal to `ending_timestamp`"
        )
      )
    }
  }

  # TODO: assert valid version, given table metadata and when CDF was enabled
  # TODO: assert valid timestamp, given table metadata and when CDF was enabled
  NULL
}

# TODO: Documentation
read_with_duckdb <- function(table_folder, changes) {
  con <- DBI::dbConnect(duckdb::duckdb())


  if (changes) {
    tbl_path <- file.path(table_folder, "_table_changes/")
    query <- glue::glue_sql("
      create or replace table {tbl_path} as
      with changes as (
        select *
        from read_parquet({file.path(tbl_path, '*__cdf_*')}, filename=true)
      ),
      other as (
        select * exclude (filename), null as _change_type, filename
        from read_parquet({file.path(tbl_path, '*__[i|d]*_*')}, filename=true)
      ),
      all_data as (
        select * from changes
        union
        select * from other
      ),
      dataset as (
        select
          *,
          str_split(regexp_extract(filename, '.*\\/(.*)\\..*', 1), '_') as metadata,
          to_timestamp(metadata[5]::bigint/1000)::string as _change_timestamp,
          metadata[6]::int as _change_version,
          from all_data
      )
      select
        *
        exclude (filename, metadata)
        replace (coalesce(_change_type, metadata[4]) as _change_type)
      from dataset
      ",
      .con = con
    )
  } else {
    tbl_path <- file.path(table_folder, "_table/")
    query <- glue::glue_sql("
      create or replace table {tbl_path} as
      select *
      from read_parquet({file.path(tbl_path, '*')})
      ",
      .con = con
    )
  }

  # creates table
  DBI::dbSendQuery(con, query)
  arrow::to_arrow(dplyr::tbl(con, tbl_path))
}


