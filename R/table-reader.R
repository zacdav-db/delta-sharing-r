#' Sharing Table Reader
#'
#' @description
#' Sharing Table Reader Description: TODO
#'
#' @details
#' Sharing Table Reader Details: TODO
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
    #' @param infer_schema Boolean (default: `FALSE`). If `FALSE` will use the
    #' schema defined in the tables metadata on the sharing server.
    #' When `TRUE` the schema is inferred via [arrow::open_dataset()] on read.
    #' @return tibble of delta sharing table data
    load_as_tibble = function(infer_schema = FALSE) {
      dataset <- self$load_as_arrow(infer_schema = infer_schema)
      dplyr::collect(dataset)
    },

    #' @description Load as Arrow
    #' @param infer_schema Boolean (default: `FALSE`). If `FALSE` will use the
    #' schema defined in the tables metadata on the sharing server.
    #' When `TRUE` the schema is inferred via [arrow::open_dataset()] on read.
    #' @return A [arrow::Dataset] R6 object.
    load_as_arrow = function(infer_schema = FALSE) {

      # download table
      # TODO: check if current version is okay and downloads are required
      private$download()

      # schema is fetched from self$metadata
      # inferring schema will use {arrow} and the parquet files as-is
      if (!infer_schema) {
        schema <- jsonlite::fromJSON(self$metadata$metaData$schemaString)
        schema <- delta_to_arrow_schema(schema$fields)
      } else {
        schema <- NULL
      }

      dataset <- arrow::open_dataset(
        sources = file.path(self$path, self$last_query$files$id),
        schema = schema,
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

      if (is.null(self$path)) {
        self$path <- tempdir()
      } else {
        if (!dir.exists(self$path)) dir.create(self$path)
      }

      # query table
      self$get_files()

      # TODO: if there are any partitions create required directories
      # if (length(table$last_query$metaData$partitionColumns) > 0) {
      #
      # }

      # download and name by unique ID
      if (nrow(self$last_query$files) > 1) {
        self$last_query$files %>%
          dplyr::select(url, id) %>%
          purrr::pwalk(function(url, id, partitionValues) {
            download.file(url, destfile = file.path(self$path, id), quiet = TRUE)
          })
      } else {
        stop("There are no files associated to this table")
      }

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
