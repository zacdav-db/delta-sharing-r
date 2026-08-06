redact_url_userinfo <- function(url) {
  if (!is.character(url) || length(url) != 1L || is.na(url)) {
    return("<invalid endpoint>")
  }
  sub("^([^:/?#]+://)[^/@]*@", "\\1", url)
}

#' Create a Delta Sharing client
#'
#' Constructs a [SharingClient] from a Delta Sharing profile. The profile may be
#' a path to a `.share` file, a parsed profile list, or an inline JSON string.
#' Construction parses and validates the profile but performs no network request
#' or token exchange.
#'
#' @param profile A profile file path, a parsed profile `list`, or a JSON
#'   string. Profile versions 1 (bearer) and 2 (bearer, basic, OAuth
#'   client-credentials, and private-key JWT) are supported.
#' @return A [SharingClient].
#' @examples
#' client <- sharing_client(list(
#'   shareCredentialsVersion = 2,
#'   type = "bearer_token",
#'   endpoint = "https://sharing.example.test/api",
#'   bearerToken = "example-only-not-a-secret"
#' ))
#' @export
sharing_client <- function(profile) {
  SharingClient$new(profile)
}

#' Delta Sharing client
#'
#' A reusable client that owns a parsed profile and its authentication context.
#' Discovery and table handles are created from the client. Query configuration
#' lives on snapshot/changes reader objects, not on the client or table.
#'
#' Most users call [sharing_client()] rather than `SharingClient$new()`.
#'
#' @export
SharingClient <- R6::R6Class(
  classname = "SharingClient",
  cloneable = FALSE,
  public = list(
    #' @description Create a client from a profile.
    #' @param profile Profile path, parsed list, or JSON string.
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
    #' @return A tibble with `name` and identifier columns.
    list_shares = function() {
      sharing_list_shares(private$profile, private$auth)
    },

    #' @description List schemas. With no `share`, lists schemas in every
    #'   accessible share.
    #' @param share Optional share name.
    #' @return A tibble with `share` and `name` columns.
    list_schemas = function(share = NULL) {
      sharing_list_schemas(private$profile, private$auth, share = share)
    },

    #' @description List tables. With no arguments, lists every accessible
    #'   table; with `share` only, lists all tables in that share.
    #' @param share Optional share name.
    #' @param schema Optional schema name (requires `share`).
    #' @return A tibble with `share`, `schema`, and `name` columns.
    list_tables = function(share = NULL, schema = NULL) {
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
    #' @return A [SharingTable].
    table = function(name, schema = NULL, share = NULL) {
      identifier <- sharing_table_identifier(name, schema, share)
      SharingTable$new(private$profile, private$auth, identifier)
    },

    #' @description Print the client.
    #' @param ... Ignored.
    print = function(...) {
      cat(sprintf(
        "<SharingClient> %s [%s]\n",
        redact_url_userinfo(private$profile$endpoint),
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
