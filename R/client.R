# Remove credentials, query parameters, and fragments from a printed endpoint.
redact_endpoint <- function(endpoint) {
  endpoint <- sub("^([^:/?#]+://)[^/@]*@", "\\1", endpoint)
  sub("[?#].*$", "", endpoint)
}

#' Create a Delta Sharing client
#'
#' Constructs a [SharingClient] from a Delta Sharing profile. The profile may be
#' a path to a `.share` file or a parsed profile list. Construction parses and
#' validates the profile but performs no network request or token exchange.
#'
#' @param profile A profile file path or parsed profile `list`. Profile versions
#'   1 (bearer) and 2 (bearer, basic, OAuth client-credentials, and private-key
#'   JWT) are supported.
#' @return A [SharingClient].
#' @examplesIf interactive()
#' client <- sharing_client(demo_profile())
#' client$list_tables("delta_sharing")
#' @export
sharing_client <- function(profile) {
  SharingClient$new(profile)
}

#' Delta Sharing client
#'
#' A reusable client that owns a parsed profile and its authentication context.
#' Discovery and table handles are created from the client. Download
#' concurrency is configured on each table handle; query options live on its
#' snapshot and changes readers.
#'
#' Most users call [sharing_client()] rather than `SharingClient$new()`.
#'
#' @export
SharingClient <- R6::R6Class(
  classname = "SharingClient",
  cloneable = FALSE,
  public = list(
    #' @description Create a client from a profile.
    #' @param profile Profile path or parsed list.
    initialize = function(profile) {
      private$profile <- sharing_profile_parse(profile)
      private$auth <- sharing_auth_context(private$profile)
      invisible(self)
    },

    #' @description The configured profile endpoint.
    #' @return The endpoint URL string.
    endpoint = function() {
      private$profile$endpoint
    },

    #' @description List available shares.
    #' @return A printable list of share records.
    list_shares = function() {
      sharing_list_shares(private$profile, private$auth)
    },

    #' @description List schemas in a share.
    #' @param share Share name.
    #' @return A printable list of schema records.
    list_schemas = function(share) {
      sharing_list_schemas(private$profile, private$auth, share = share)
    },

    #' @description List tables in a share, optionally within one schema.
    #' @param share Share name.
    #' @param schema Optional schema name.
    #' @return A printable list of table records.
    list_tables = function(share, schema = NULL) {
      sharing_list_tables(
        private$profile,
        private$auth,
        share = share,
        schema = schema
      )
    },

    #' @description Create a reusable table handle.
    #' @param name Table name, or a `"share.schema.name"` string when `share`
    #'   and `schema` are omitted.
    #' @param schema Schema name when using explicit components.
    #' @param share Share name when using explicit components.
    #' @param concurrency Maximum number of data files downloaded concurrently.
    #' @return A [SharingTable].
    table = function(
      name,
      schema = NULL,
      share = NULL,
      concurrency = 4L
    ) {
      identifier <- sharing_table_identifier(name, schema, share)
      SharingTable$new(
        private$profile,
        private$auth,
        identifier,
        concurrency
      )
    },

    #' @description Print the client.
    #' @param ... Ignored.
    print = function(...) {
      cat(sprintf(
        "<SharingClient> %s [%s]\n",
        redact_endpoint(private$profile$endpoint),
        private$profile$auth_type
      ))
      invisible(self)
    }
  ),
  private = list(
    profile = NULL,
    auth = NULL
  )
)
