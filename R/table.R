#' Delta Sharing table handle
#'
#' A cheap, reusable reference to a shared table. Metadata methods query the
#' control plane without scanning rows. `snapshot()` and `changes()` create
#' reader objects that carry query options and expose materializers. Downloaded
#' files are cached for the R session and shared by handles for the same table.
#'
#' Created via `SharingClient$table()`, not directly.
#'
#' @export
SharingTable <- R6::R6Class(
  classname = "SharingTable",
  cloneable = FALSE,
  public = list(
    #' @description Create a table handle. Prefer `SharingClient$table()`.
    #' @param profile Parsed profile (internal).
    #' @param auth Authentication context (internal).
    #' @param identifier A `list(share, schema, table)` (internal).
    #' @param concurrency Maximum number of concurrent file downloads.
    initialize = function(
      profile,
      auth,
      identifier,
      concurrency = 4L
    ) {
      rlang::check_number_whole(concurrency, min = 1)
      private$profile <- profile
      private$auth <- auth
      private$id <- identifier
      private$concurrency <- concurrency
      private$cache_directory <- table_download_cache(profile, identifier)
      invisible(self)
    },

    #' @description The structured table identifier.
    #' @return A `list` with `share`, `schema`, and `table`.
    identifier = function() {
      private$id
    },

    #' @description The current table version.
    #' @return A non-negative whole number.
    version = function() {
      sharing_table_version(private$profile, private$auth, private$id)
    },

    #' @description The table protocol capabilities.
    #' @return A safe list of protocol fields.
    protocol = function() {
      sharing_table_protocol(private$profile, private$auth, private$id)
    },

    #' @description The table metadata (no row scan).
    #' @return A safe structured metadata list.
    metadata = function() {
      sharing_table_metadata(private$profile, private$auth, private$id)
    },

    #' @description The table's logical schema.
    #' @return The parsed struct schema.
    schema = function() {
      sharing_table_schema(private$profile, private$auth, private$id)
    },

    #' @description Configure a snapshot read.
    #' @param version Optional non-negative whole-number version.
    #' @param timestamp Optional scalar timestamp string or `POSIXct` value.
    #' @param columns Optional character vector of projected columns.
    #' @param limit Optional non-negative whole-number row limit.
    #' @param predicate Optional structured server-side predicate hint.
    #' @param response_format One of `"auto"`, `"delta"`, or `"parquet"`.
    #' @return A [SharingSnapshot].
    snapshot = function(
      version = NULL,
      timestamp = NULL,
      columns = NULL,
      limit = NULL,
      predicate = NULL,
      response_format = "auto"
    ) {
      SharingSnapshot$new(
        profile = private$profile,
        auth = private$auth,
        identifier = private$id,
        version = version,
        timestamp = timestamp,
        columns = columns,
        limit = limit,
        predicate = predicate,
        response_format = response_format,
        cache_path = private$cache_directory,
        concurrency = private$concurrency
      )
    },

    #' @description Configure a change data feed read.
    #' @param starting_version,ending_version Optional version bounds.
    #' @param starting_timestamp,ending_timestamp Optional timestamp strings or
    #'   `POSIXct` bounds.
    #' @param columns Optional character vector of projected columns.
    #' @param response_format One of `"auto"`, `"delta"`, or `"parquet"`.
    #' @return A [SharingChanges].
    changes = function(
      starting_version = NULL,
      ending_version = NULL,
      starting_timestamp = NULL,
      ending_timestamp = NULL,
      columns = NULL,
      response_format = "auto"
    ) {
      SharingChanges$new(
        profile = private$profile,
        auth = private$auth,
        identifier = private$id,
        starting_version = starting_version,
        ending_version = ending_version,
        starting_timestamp = starting_timestamp,
        ending_timestamp = ending_timestamp,
        columns = columns,
        response_format = response_format,
        cache_path = private$cache_directory,
        concurrency = private$concurrency
      )
    },

    #' @description Print the table handle.
    #' @param ... Ignored.
    print = function(...) {
      cat(sprintf(
        "<SharingTable> %s.%s.%s\n",
        private$id$share,
        private$id$schema,
        private$id$table
      ))
      invisible(self)
    }
  ),
  active = list(
    #' @field cache_path Read-only path to this table's session cache. Removing
    #'   it manually invalidates active lazy readers; the next read recreates it.
    cache_path = function(value) {
      if (!missing(value)) {
        stop("`cache_path` is read-only.", call. = FALSE)
      }
      private$cache_directory
    }
  ),
  private = list(
    profile = NULL,
    auth = NULL,
    id = NULL,
    cache_directory = NULL,
    concurrency = NULL
  )
)
