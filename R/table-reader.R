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

    #' @field starting_version The starting version of table changes.
    starting_version = NULL,

    #' @field ending_version The ending version of table changes.
    ending_version = NULL,

    #' @field starting_timestamp The starting timestamp of table changes.
    starting_timestamp = NULL,

    #' @field ending_timestamp The ending timestamp of table changes.
    ending_timestamp = NULL,

    #' @description Create a new `SharingTableReader` object
    #' @details This object should be instantiated via `SharingClient$table(...)`
    #' and not directly.
    #' @param share Share for desired table
    #' @param schema schema for desired table
    #' @param table table name
    #' @param conn DuckDB connection object
    #' @param creds Credentials for desired table
    #' @return A new `SharingTableReader` object
    initialize = function(share, schema, table, conn, creds) {
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
      private$duckdb_connection <- conn
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

    #' @description Set Timestamp
    #' @details This is only supported on tables with change data feed (cdf)
    #' enabled.
    #' @param timestamp String, an optional timestamp. If set, will return
    #' files as of the specified timestamp of the table.
    set_timestamp = function(timestamp) {
      self$timestamp <- timestamp
    },

    #' @description Set CDF Options
    #' @details This is only supported on tables with change data feed (cdf)
    #' enabled.
    #' @param starting_version The starting version of table changes.
    #' @param ending_version The ending version of table changes.
    #' @param starting_timestamp The starting timestamp of table changes.
    #' @param ending_timestamp The ending timestamp of table changes.
    set_cdf_options = function(
      starting_version = NULL,
      ending_version = NULL,
      starting_timestamp = NULL,
      ending_timestamp = NULL
    ) {

      # validate passed arguments
      validate_cdf_options(
        starting_version = starting_version,
        ending_version = ending_version,
        starting_timestamp = starting_timestamp,
        ending_timestamp = ending_timestamp
      )

      # TODO: automatically cast anything resembling a valid timestamp to a
      # ZD Note: I'd rather not do this the more I think about it.
      # Extra effort to maintain, and if a package used to do it changes then it
      # breaks things.
      # correctly formatted string

      # set attributes
      self$starting_version <- starting_version
      self$ending_version <- ending_version
      self$starting_timestamp <- starting_timestamp
      self$ending_timestamp <- ending_timestamp

    },

    #' @description Load as Tibble
    #' @param changes Boolean (default: FALSE), determines if table changes are
    #' read instead of returning table as of a given version or time.
    #' @return  A [tibble::tibble] S3 object.
    load_tibble = function(changes = FALSE) {
      dataset <- private$load_dataset(changes = changes)
      dplyr::collect(dataset)
    },

    # @description Load as Arrow Record Batch Reader
    # @param changes Boolean (default: FALSE), determines if table changes are
    # read instead of returning table as of a given version or time.
    # @return A [arrow::RecordBatchReader] R6 object.
    #load_arrow_batch = function(changes = FALSE) {
    #  private$load_dataset(changes = changes)
    #},

    #' @description Load as Arrow Table
    #' @param changes Boolean (default: FALSE), determines if table changes are
    #' read instead of returning table as of a given version or time.
    #' @return A [arrow::Table] or [arrow::Dataset] R6 object.
    load_arrow_table = function(changes = FALSE) {
      dataset <- private$load_dataset(changes = changes)
      dataset$read_table()
    },

    #' @description Load as DuckDB Table
    #' @param changes Boolean (default: FALSE), determines if table changes are
    #' read instead of returning table as of a given version or time.
    #' @return TODO
    load_duckdb = function(changes = FALSE) {
      dataset <- private$load_dataset(changes = changes)
      arrow::to_duckdb(dataset)
    },

    # load_dt = function(changes = FALSE) {
    #   NULL
    # },

    # load_spark = function() {
    #   NULL
    # },

    # load_sparklyr = function() {
    #   NULL
    # },

    #' @description Set Download Path
    #' @param path Location to save table files to
    set_download_path = function(path) {
      self$path <- path
    },

    #' @description Get Table Files
    #' @param changes Boolean (default: FALSE), determines if table changes are
    #' read instead of returning table as of a given version or time.
    get_files = function(changes = FALSE) {

      if (changes) {

        endpoint <- paste0(self$endpoint_base, "/changes")
        params <- list(
          startingVersion = self$starting_version,
          endingVersion = self$ending_version,
          startingTimestamp = self$starting_timestamp,
          endingTimestamp = self$ending_timestamp
        )

        req <- req_share(
          creds = self$creds,
          method = "GET",
          endpoint = endpoint,
          params = params
        )

      } else {

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

      }

      res <- req %>%
        httr2::req_retry(max_tries = 3) %>%
        httr2::req_error(body = req_error_body) %>%
        httr2::req_perform() %>%
        clean_xndjson()

      self$last_query <- list(
        protocol = res[[1]][[1]],
        metaData = res[[2]][[1]],
        files = purrr::map_dfr(
          res[-c(1, 2)],
          ~extract_file_metadata(.x, changes = changes)
        )
      )

      self$last_query

    }

  ),
  private = list(

    # DuckDB connection object
    duckdb_connection = NULL,

    #' Download Associated Files
    #' Internal function that downloads table files and stores them
    #' on disk.
    download = function(changes = FALSE) {

      # query files that belong to table
      self$get_files(changes = changes)

      # download directory, create if doesn't exist
      if (is.null(self$path)) {
        self$path <- tempdir()
      } else {
        if (!dir.exists(self$path)) dir.create(self$path)
      }

      # create a sub-directory for the table within the path
      table_folder <- file.path(self$path, self$last_query$metaData$id)
      if (!dir.exists(table_folder)) dir.create(table_folder)

      # ensure that sub-directories to store data are always present (even if not used)
      # this is to ensure "copy-skipping" resolution will behave
      table_folder_cdf <- file.path(table_folder, "_table_changes")
      table_folder_std <- file.path(table_folder, "_table")
      if (!dir.exists(table_folder_std)) dir.create(table_folder_std)
      if (!dir.exists(table_folder_cdf)) dir.create(table_folder_cdf)

      # determine what files already may be downloaded
      # id is unique - used to avoid re-downloads
      query_files <- self$last_query$files %>%
        resolve_query_files(
          changes = changes,
          table_folder = table_folder
        )

      if (changes) {
        new_query_files <- dplyr::filter(query_files, to_download | to_copy_to_cdf)
      } else {
        new_query_files <- dplyr::filter(query_files, to_download | to_copt_to_root)
      }

      # delete files not part of current table state
      # this is required so that arrow plays well with partitioning
      # no effective way to manage state since everything is parquet

      # determine which files can be deleted
      table_folder_del <- ifelse(changes, table_folder_cdf, table_folder_std)

      all_existing_files <- list.files(table_folder_del, recursive = TRUE)
      to_delete <- all_existing_files[!all_existing_files %in% query_files$relativePath]

      if (length(to_delete) > 0) {
        message("deleting ", length(to_delete), " files that are no longer referenced")
        file.remove(file.path(table_folder_del, to_delete))
      }

      # if there are files to download
      if (nrow(new_query_files) > 0) {

        # create all partition directories required
        dest_paths <- unique(new_query_files$destPath)
        purrr::walk(dest_paths[!dir.exists(dest_paths)], function(path) {
          dir.create(path, recursive = TRUE)
        })

        # if a file exists in either main directory or _table_changes copy it
        # instead of re-downloading
        # TODO: copy data between directory

        # download just what is required
        download_new_files(
          new_query_files = new_query_files,
          changes = changes
        )
      }

      # detect partitioning
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

    },

    # TODO: Documentation
    load_dataset = function(changes) {

      # download table
      # TODO: check if current version is okay and downloads are required
      dataset_meta <- private$download(changes = changes)

      # always load via DuckDB
      read_with_duckdb(
        table_folder = dataset_meta$path,
        changes = changes,
        conn = private$duckdb_connection
      )

    }

  ),
  active = list(

    #' @field current_version Get current version of table
    current_version = function() {

      req <- req_share(
        creds = self$creds,
        method = "HEAD",
        # endpoint = paste0(self$endpoint_base, "/version"),
        endpoint = self$endpoint_base
      )
      version <- make_req(req)
      version <- list(
        version = as.integer(version$`delta-table-version`),
        date = version$date,
        server = version$server
      )
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
        httr2::req_retry(max_tries = 3) %>%
        httr2::req_error(body = req_error_body) %>%
        httr2::req_perform() %>%
        clean_xndjson()

      purrr::flatten(res)

    },

    #' @field table_path Directory where saved data exists for table. This is
    #' an extension of `path` combined with the table ID.
    table_path = function() {
      file.path(self$path, self$metadata$metaData$id)
    }
  )
)
