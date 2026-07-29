# Reader objects returned by SharingTable$snapshot() / $changes(). Query options
# are fixed at construction; the eager materializers (to_arrow, to_data_frame)
# are adapters over the one lazy Arrow stream, so there is a single read path.
# SharingReader holds that shared behaviour; the subclasses differ only in how
# they validate options and open the native stream.

#' Shared Delta Sharing reader
#'
#' Internal base class for snapshot and change readers. Public readers inherit
#' its Arrow and data-frame materializers.
#'
#' @keywords internal
SharingReader <- R6::R6Class(
  "SharingReader",
  cloneable = FALSE,
  public = list(
    #' @description Materialize as an Arrow table (requires `{arrow}`).
    #' @param batch_size Rows per batch.
    #' @param progress Show live rows and, when snapshot statistics are
    #'   complete, an exact percentage. Defaults to `TRUE` in interactive R
    #'   sessions.
    #' @return An `arrow::Table`.
    to_arrow = function(
      batch_size = DEFAULT_BATCH_SIZE,
      progress = interactive()
    ) {
      sharing_stream_to_arrow(
        self$to_arrow_stream(batch_size = batch_size),
        progress = progress
      )
    },

    #' @description Expose a lazy Arrow record batch reader (requires
    #'   `{arrow}`). The reader owns the underlying stream; consume it or call
    #'   its `Close()` method.
    #' @param batch_size Rows per batch.
    #' @return An `arrow::RecordBatchReader`.
    to_arrow_reader = function(batch_size = DEFAULT_BATCH_SIZE) {
      sharing_stream_to_arrow_reader(
        self$to_arrow_stream(batch_size = batch_size)
      )
    },

    #' @description Materialize as a base data frame.
    #' @param batch_size Rows per batch.
    #' @param progress Show live rows and, when snapshot statistics are
    #'   complete, an exact percentage. Defaults to `TRUE` in interactive R
    #'   sessions.
    #' @return A data frame.
    to_data_frame = function(
      batch_size = DEFAULT_BATCH_SIZE,
      progress = interactive()
    ) {
      sharing_stream_to_data_frame(
        self$to_arrow_stream(batch_size = batch_size),
        progress = progress
      )
    },

    #' @description Materialize as a lazy Arrow C stream.
    #' @param batch_size Rows per batch (1..1,000,000; default 65,536).
    #' @return A `nanoarrow_array_stream`.
    to_arrow_stream = function(batch_size = DEFAULT_BATCH_SIZE) {
      private$open_stream(batch_size)
    },

    #' @description Print the reader.
    #' @param ... Ignored.
    print = function(...) {
      id <- private$identifier
      cat(sprintf(
        "<%s> %s.%s.%s\n",
        class(self)[[1]],
        id$share,
        id$schema,
        id$table
      ))
      invisible(self)
    }
  ),
  private = list(
    profile = NULL,
    auth = NULL,
    identifier = NULL,
    spec = NULL,
    open_stream = function(batch_size) {
      stop("`open_stream()` must be implemented by a SharingReader subclass.")
    }
  )
)

#' Delta Sharing snapshot reader
#'
#' An immutable snapshot read specification with Arrow materializers. Created by
#' `SharingTable$snapshot()`. Materialize with `to_arrow_stream()` (lazy),
#' `to_arrow_reader()` (lazy), `to_arrow()`, or `to_data_frame()`.
#'
#' @export
SharingSnapshot <- R6::R6Class(
  "SharingSnapshot",
  inherit = SharingReader,
  cloneable = FALSE,
  public = list(
    #' @description Create a snapshot reader. Prefer `SharingTable$snapshot()`.
    #' @param profile,auth,identifier Internal client state.
    #' @param version,timestamp,columns,limit,predicate,response_format Query
    #'   options; see [SharingTable]'s `snapshot()` method.
    initialize = function(
      profile,
      auth,
      identifier,
      version = NULL,
      timestamp = NULL,
      columns = NULL,
      limit = NULL,
      predicate = NULL,
      response_format = "auto"
    ) {
      version <- normalize_version(version, "version")
      timestamp <- normalize_timestamp(timestamp, "timestamp")
      if (!is.null(version) && !is.null(timestamp)) {
        abort(
          "`version` and `timestamp` are mutually exclusive.",
          type = "validation",
          operation = "snapshot"
        )
      }
      private$profile <- profile
      private$auth <- auth
      private$identifier <- identifier
      private$spec <- list(
        version = version,
        timestamp = timestamp,
        columns = normalize_columns(columns),
        limit = normalize_limit(limit),
        predicate = normalize_predicate(predicate),
        response_format = normalize_response_format(response_format)
      )
      invisible(self)
    }
  ),
  private = list(
    open_stream = function(batch_size) {
      sharing_snapshot_stream(
        private$profile,
        private$auth,
        private$identifier,
        private$spec,
        batch_size = batch_size
      )
    }
  )
)

#' Delta Sharing change data feed reader
#'
#' An immutable change data feed specification with Arrow materializers. Created
#' by `SharingTable$changes()`. Exactly one starting bound is required and an
#' optional ending bound of the same kind; version and timestamp bounds cannot
#' be mixed.
#'
#' @export
SharingChanges <- R6::R6Class(
  "SharingChanges",
  inherit = SharingReader,
  cloneable = FALSE,
  public = list(
    #' @description Create a changes reader. Prefer `SharingTable$changes()`.
    #' @param profile,auth,identifier Internal client state.
    #' @param starting_version,ending_version,starting_timestamp,ending_timestamp,columns,response_format
    #'   Query options; see [SharingTable]'s `changes()` method.
    initialize = function(
      profile,
      auth,
      identifier,
      starting_version = NULL,
      ending_version = NULL,
      starting_timestamp = NULL,
      ending_timestamp = NULL,
      columns = NULL,
      response_format = "auto"
    ) {
      private$profile <- profile
      private$auth <- auth
      private$identifier <- identifier
      private$spec <- sharing_changes_validate(
        starting_version = starting_version,
        ending_version = ending_version,
        starting_timestamp = starting_timestamp,
        ending_timestamp = ending_timestamp,
        columns = columns,
        response_format = response_format
      )
      invisible(self)
    }
  ),
  private = list(
    open_stream = function(batch_size) {
      sharing_changes_stream(
        private$profile,
        private$auth,
        private$identifier,
        private$spec,
        batch_size = batch_size
      )
    }
  )
)
