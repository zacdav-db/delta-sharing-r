#' Create Delta Sharing Client
#'
#' @param credentials Path to delta share credentials
#'
#' @return SharingClient
#' @export
#'
#' @examples
#' sharing_client("config.share")
sharing_client <- function(credentials) {
  delta.sharing::SharingClient$new(credentials)
}

#' Sharing Client
#'
#' @description
#' Sharing Client Description: TODO
#'
#' @details TODO
#' @export
SharingClient <- R6::R6Class(
  classname = "SharingClient",
  public = list(

    #' @field creds Delta sharing credentials.
    creds = NULL,

    #' @description Create a new `DeltaShareCredentials` object
    #' @param credentials Path to delta share credentials
    #' @return A new `DeltaShareCredentials` object
    initialize = function(credentials) {
      creds <- jsonlite::read_json(credentials)
      class(creds) <- "DeltaShareCredentials"
      # convert expiration time and check validity
      creds$expirationTime <- strptime(
        x = creds$expirationTime,
        format = "%Y-%m-%dT%H:%M:%S",
        tz = "UTC"
      )
      if (Sys.time() > creds$expirationTime) {
        stop("Credentials are expired as of ", creds$expirationTime)
      }
      self$creds <- creds
    },

    #' @description Lists available shares
    #' @return tibble of the available shares associated with current delta
    #' sharing credentials
    list_shares = function() {

      params <- list(maxResults = 50)
      req <- req_share(creds = self$creds, method = "GET", endpoint = "shares", params = params)

      shares <- make_req(req)
      dplyr::bind_rows(shares$items)

    },

    #' @description List schemas within share
    #' @param share Name of the share to list schemas
    #' @return tibble of the available schemas associated with given share
    list_schemas = function(share) {

      endpoint <- paste("shares", share, "schemas", sep = "/")

      params <- list(maxResults = 50)
      req <- req_share(creds = self$creds, method = "GET", endpoint = endpoint, params = params)

      tables <- make_req(req)

      dplyr::bind_rows(tables$items) %>%
        dplyr::select(share, schema = name)

    },

    #' @description List all schemas for all shares
    #' @return tibble of the available schemas associated with current delta
    #' sharing credentials
    list_all_schemas = function() {

      share_names <- unique(self$list_shares()$name)
      purrr::map_dfr(share_names, self$list_schemas)

    },

    #' @description List tables within schema
    #' @param schema Name of the scehma to list tables within
    #' @return tibble of the available tables within given schema
    list_tables = function(share, schema) {

      endpoint <- paste("shares", share, "schemas", schema, "tables", sep = "/")

      params <- list(maxResults = 50)
      req <- req_share(creds = self$creds, method = "GET", endpoint = endpoint, params = params)

      tables <- make_req(req)

      dplyr::bind_rows(tables$items) %>%
        dplyr::select(share, schema, name)

    },

    #' @description List tables within share
    #' @param share Name of the share to list tables within
    #' @return tibble of the available tables within given share
    list_tables_in_share = function(share) {

      endpoint <- paste("shares", share, "all-tables", sep = "/")

      params <- list(maxResults = 50)
      req <- req_share(creds = self$creds, method = "GET", endpoint = endpoint, params = params)

      tables <- make_req(req)

      dplyr::bind_rows(tables$items) %>%
        dplyr::select(share, schema, name)

    },

    #' @description Create reference to delta sharing table
    #' @param share Share the schema/table resides within
    #' @param schema Schema the table resides within
    #' @param table Table to query
    #' @return R6 class of `SharingTableReader` for specified table
    table = function(share, schema, table) {
      SharingTableReader$new(
        share = share,
        schema = schema,
        table = table,
        creds = self$creds
      )
    }

    #TODO: update docs
    ##' @description Create reference to delta sharing table changes
    ##' @param share Share the schema/table resides within
    ##' @param schema Schema the table resides within
    ##' @param table Table to query changes of
    ##' @return R6 class of `SharingTableReader` for specified table
    # table_changes = function(share, schema, table) {
    #   SharingTableChangesReader$new(
    #     share = share,
    #     schema = schema,
    #     table = table,
    #     creds = self$creds
    #   )
    # }

  )
)
