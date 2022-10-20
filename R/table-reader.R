#' Sharing Table Reader
#'
#' @description
#' Sharing Table Reader Description: TODO
#'
#' @details TODO
#' @export
SharingTableReader <- R6::R6Class(
  classname = "SharingTableReader",
  public = list(

    #' @field table Referenced table.
    table = NULL,

    #' @field share Referenced share.
    share = NULL,

    #' @field schema Referenced schema.
    schema = NULL,

    #' @field creds Delta sharing credentials.
    creds = NULL,

    #' @field endpoint_base Endpoint queries are directed to.
    endpoint_base = NULL,

    #' @field limit Hint from the client to tell the server how many rows in the
    #' table the client plans to read.
    limit = NULL,

    #' @field version Return files as of the specified version of the table.
    version = NULL,

    #' @field predicates List of SQL boolean expressions.
    predicates = NULL,

    #' @field path Location used for downloading table data.
    path = NULL,

    #' @field last_query Most recent metadata fetched regarding table.
    last_query = NULL,

    #' @description Create a new `SharingTableReader` object
    #' @details This object should be instantiated via `SharingClient$table(...)`
    #' and not directly.
    #' @param share Share for desired table
    #' @param schema schema for desired table
    #' @param table table name
    #' @param creds Credentials for desired table
    #' @return A new `SharingTableReader` object
    initialize = function(share, schema, table, creds) {
      self$creds <- creds
      self$share <- share
      self$schema <- schema
      self$table <- table
      self$endpoint_base <- paste(
        "shares", share,
        "schemas", schema,
        "tables", table,
        sep = "/"
      )
    },

    #' @description Define Predicate Hints
    #' @details
    #' - Filtering files based on the SQL predicates is **Best Effort**.
    #'The server may return files that don’t satisfy the predicates.
    #' - If the server fails to parse one of the SQL predicates, or fails to
    #' evaluate it, the server may skip it.
    #' - Predicate expressions are conjunctive (AND-ed together).
    #' - When absent, the server will return all of files in the table.
    #' @param hints List of SQL boolean expressions using a [restricted subset](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#sql-expressions-for-filtering)
    #' of SQL
    set_predicate = function(hints) {
      self$predicates <- hints
    },

    #' @description Set Limit
    #' @param limit Integer, hint from the client to tell the server how many
    #' rows in the table the client plans to read.
    set_limit = function(limit) {
      self$limit <- limit
    },

    #' @description Set Version
    #' @details This is only supported on tables with change data feed (cdf)
    #' enabled.
    #' @param version Integer, an optional version number. If set, will return
    #' files as of the specified version of the table.
    set_version = function(version) {
      self$version <- version
    },

    #' @description Load as Tibble
    #' @param infer_schema Boolean (default: `TRUE`). If `FALSE` will use the
    #' schema defined in the tables metadata on the sharing server.
    #' When `TRUE` the schema is inferred via [arrow::open_dataset()] on read.
    #' @return tibble of delta sharing table data
    load_as_tibble = function(infer_schema = TRUE) {
      dataset <- self$load_as_arrow(infer_schema = infer_schema)
      dplyr::collect(dataset)
    },

    #' @description Load as Arrow
    #' @param infer_schema Boolean (default: `TRUE`). If `FALSE` will use the
    #' schema defined in the tables metadata on the sharing server.
    #' When `TRUE` the schema is inferred via [arrow::open_dataset()] on read.
    #' inferring the schema can be very useful for complex types, however it
    #' will assume the local timezone for relevant columns, be cautious.
    #' @return A [arrow::Dataset] R6 object.
    load_as_arrow = function(infer_schema = TRUE) {

      # download table
      # TODO: check if current version is okay and downloads are required
      dataset_meta <- private$download()

      # schema is fetched from self$metadata
      # inferring schema will use {arrow} and the parquet files as-is
      if (!infer_schema) {
        metadata <- self$metadata$metaData
        schema <- jsonlite::fromJSON(
          txt = metadata$schemaString,
          simplifyDataFrame = FALSE
        )
        schema <- convert_to_arrow_schema(schema, metadata$partitionColumns)
      } else {
        schema <- NULL
      }

      dataset <- arrow::open_dataset(
        sources = dataset_meta$path,
        schema = schema,
        hive_style = TRUE,
        partitioning = dataset_meta$partitions,
        format = "parquet"
      )
    },

    # load_as_spark = function() {
    #   NULL
    # },

    # load_as_sparklyr = function() {
    #   NULL
    # },

    #' @description Set Download Path
    #' @param path Location to save table files to
    set_download_path = function(path) {
      self$path <- path
    },

    #' @description Get Table Files
    get_files = function() {

      endpoint <- paste0(self$endpoint_base, "/query")
      body <- list(
        predicateHints = self$predicates,
        limitHint = self$limit,
        version = self$version
      )

      req <- req_share(
        creds = self$creds,
        method = "POST",
        endpoint = endpoint,
        body = body
      )

      res <- req %>%
        httr2::req_retry(max_tries = 3) %>%
        httr2::req_error(body = req_error_body) %>%
        httr2::req_perform() %>%
        clean_xndjson()

      self$last_query <- list(
        protocol = res[[1]][[1]],
        metaData = res[[2]][[1]],
        files = purrr::map_dfr(res[-c(1, 2)], ~{
          .x <- .x$file
          list(
            url = .x$url,
            id = .x$id,
            partitionValues = list(.x$partitionValues),
            size = .x$size,
            stats = list(jsonlite::fromJSON(.x$stats))
          )
        })
      )

      self$last_query

    }

  ),
  private = list(

    #' Download Associated Files
    #' Internal function that downloads table files and stores them
    #' on disk.
    download = function() {

      # download directory
      if (is.null(self$path)) {
        self$path <- tempdir()
      } else {
        if (!dir.exists(self$path)) dir.create(self$path)
      }

      # query files that belong to table
      self$get_files()

      # create a folder for the table, just in-case a directory is shared
      # between other tables / data
      table_folder <- file.path(self$path, self$last_query$metaData$id)
      if (!dir.exists(table_folder)) {
        dir.create(table_folder)
      }

      # determine what files already may be downloaded
      # id is unique - used to avoid re-downloads
      query_files <- self$last_query$files %>%
        dplyr::rowwise() %>%
        dplyr::mutate(
          hivePartitions = list_to_hive_partition(partitionValues),
          destPath = file.path(!!table_folder, hivePartitions),
          file = paste0(id, ".parquet"),
          relativePath = sub("^\\/", "", file.path(hivePartitions, file)),
          filePath = file.path(destPath, file),
          alreadyExists = file.exists(filePath)
        )

      new_query_files <- dplyr::filter(query_files, !alreadyExists)

      # delete files not part of current table state
      # this is required so that arrow plays well with partitioning
      # no effective way to manage state since everything is parquet
      all_existing_files <- list.files(table_folder, recursive = TRUE)
      to_delete <- all_existing_files[!all_existing_files %in% query_files$relativePath]
      if (length(to_delete) > 0) {
        message("deleting ", length(to_delete), " files that are no longer referenced")
        file.remove(file.path(table_folder, to_delete))
      }

      # if there are files to download
      if (nrow(new_query_files) > 0) {

        # create all partition directories required
        dest_paths <- unique(new_query_files$destPath)
        purrr::walk(dest_paths[!dir.exists(dest_paths)], function(path) {
          dir.create(path, recursive = TRUE)
        })

        # create progress bar
        pb <- progress::progress_bar$new(
          format = "  downloading files (:elapsedfull) [:bar] :current/:total (:percent) [:eta]",
          total = nrow(new_query_files),
          clear = FALSE,
          width = 100
        )
        pb$tick(0)

        # download just what is required
        new_query_files %>%
          dplyr::select(url, filePath) %>%
          purrr::pwalk(function(url, filePath) {
            download.file(url, destfile = filePath, quiet = TRUE)
            pb$tick()
          })
      }

      if (length(self$last_query$metaData$partitionColumns) == 0) {
        partitions <- NULL
      } else {
        partitions <- self$last_query$metaData$partitionColumns
      }

      dataset_meta <- list(
        path = table_folder,
        partitions = partitions
      )
      return(dataset_meta)

    }

  ),
  active = list(

    #' @field current_version Get current version of table
    current_version = function() {

      req <- req_share(
        creds = self$creds,
        method = "HEAD",
        endpoint = self$endpoint_base
      )
      version <- make_req(req)
      class(version) <- "DeltaShareTableVersion"
      version

    },

    #' @field metadata Get table metadata
    metadata = function() {

      endpoint <- paste0(self$endpoint_base, "/metadata")

      req <- req_share(
        creds = self$creds,
        method = "GET",
        endpoint = endpoint
      )

      res <- req %>%
        httr2::req_perform() %>%
        clean_xndjson()

      purrr::flatten(res)

    }

  )
)
