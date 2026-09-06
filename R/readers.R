# Reader objects returned by SharingTable$snapshot() / $changes(). Query options
# are fixed at construction; the eager materializers (to_arrow, to_tibble,
# to_data_frame) use the same Arrow C stream path. Each materializer call opens
# a new stream.
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
    #' @return An `arrow::Table`.
    to_arrow = function(batch_size = 65536L) {
      sharing_stream_to_arrow(
        self$to_arrow_stream(batch_size = batch_size)
      )
    },

    #' @description Expose a lazy Arrow record batch reader (requires
    #'   `{arrow}`). The reader owns the underlying stream; consume it or call
    #'   its `Close()` method.
    #' @param batch_size Rows per batch.
    #' @return An `arrow::RecordBatchReader`.
    to_arrow_reader = function(batch_size = 65536L) {
      sharing_stream_to_arrow_reader(
        self$to_arrow_stream(batch_size = batch_size)
      )
    },

    #' @description Materialize as a tibble. By default, 64-bit integers become
    #'   doubles, which cannot represent all integers outside -2^53..2^53
    #'   exactly. For lossless integer text, supply a character prototype,
    #'   for example `to = data.frame(id = character())` for an `id`-only
    #'   projection. This preserves the full signed 64-bit range and nulls,
    #'   including integers that `bit64` reserves for missing values.
    #'   Nanoarrow's `integer64` conversion is not recommended: some versions
    #'   misread sliced arrays with nonzero offsets.
    #' @param batch_size Rows per batch.
    #' @param to A data-frame prototype, or a function of the Arrow schema and
    #'   default prototype returning one, passed to
    #'   [nanoarrow::convert_array_stream()]. `NULL` uses nanoarrow's default
    #'   conversion. The prototype must match the projected result columns.
    #' @return A `tibble::tbl_df`.
    to_tibble = function(batch_size = 65536L, to = NULL) {
      sharing_stream_to_tibble(
        self$to_arrow_stream(batch_size = batch_size),
        to = to
      )
    },

    #' @description Materialize as a base data frame by dropping the tibble
    #'   class from `to_tibble()`.
    #' @param batch_size Rows per batch.
    #' @param to Conversion prototype; see `to_tibble()`.
    #' @return A data frame.
    to_data_frame = function(batch_size = 65536L, to = NULL) {
      as.data.frame(
        self$to_tibble(batch_size = batch_size, to = to)
      )
    },

    #' @description Materialize as a lazy Arrow C stream.
    #' @param batch_size Rows per batch (1..1,000,000; default 65,536).
    #' @return A `nanoarrow_array_stream`.
    to_arrow_stream = function(batch_size = 65536L) {
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
      spec <- private$spec
      read <- if (inherits(self, "SharingSnapshot")) {
        read <- "latest snapshot"
        if (!is.null(spec$version)) {
          read <- paste("version", spec$version)
        }
        if (!is.null(spec$timestamp)) {
          read <- paste("at", format_timestamp(spec$timestamp))
        }
        read
      } else {
        start <- if (is.null(spec$starting_version)) {
          format_timestamp(spec$starting_timestamp %||% "?")
        } else {
          spec$starting_version
        }
        end <- if (is.null(spec$ending_version)) {
          format_timestamp(spec$ending_timestamp %||% "latest")
        } else {
          spec$ending_version
        }
        paste("changes", start, "to", end)
      }
      details <- c(
        read,
        if (!is.null(spec$columns)) {
          paste(
            length(spec$columns),
            if (length(spec$columns) == 1L) "column" else "columns"
          )
        },
        if (!is.null(spec$limit)) paste0("limit ", spec$limit)
      )
      cat("  ", paste(details, collapse = " | "), "\n", sep = "")
      invisible(self)
    }
  ),
  private = list(
    profile = NULL,
    auth = NULL,
    identifier = NULL,
    spec = NULL,
    cache_path = NULL,
    concurrency = NULL,
    open_stream = function(batch_size) {
      stop("`open_stream()` must be implemented by a SharingReader subclass.")
    }
  )
)

#' Delta Sharing snapshot reader
#'
#' An immutable snapshot read specification with Arrow materializers. Created by
#' `SharingTable$snapshot()`. Materialize with `to_arrow_stream()` (lazy),
#' `to_arrow_reader()` (lazy), `to_arrow()`, `to_tibble()`, or
#' `to_data_frame()`.
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
    #' @param cache_path,concurrency Internal table execution settings.
    initialize = function(
      profile,
      auth,
      identifier,
      version = NULL,
      timestamp = NULL,
      columns = NULL,
      limit = NULL,
      predicate = NULL,
      response_format = "auto",
      cache_path,
      concurrency
    ) {
      if (
        !is.null(limit) &&
          (!rlang::is_scalar_integerish(limit, finite = TRUE) || limit < 0)
      ) {
        abort(
          "{.arg limit} must be a non-negative whole number.",
          type = "validation",
          operation = "snapshot"
        )
      }
      if (!is.null(columns) && !is.character(columns)) {
        abort(
          "{.arg columns} must be a character vector.",
          type = "validation",
          operation = "snapshot"
        )
      }
      if (!is.null(predicate) && !is.list(predicate)) {
        abort(
          "{.arg predicate} must be a list.",
          type = "validation",
          operation = "snapshot"
        )
      }
      private$profile <- profile
      private$auth <- auth
      private$identifier <- identifier
      private$cache_path <- cache_path
      private$concurrency <- concurrency
      private$spec <- list(
        version = version,
        timestamp = timestamp,
        columns = columns,
        limit = limit,
        predicate = predicate,
        response_format = rlang::arg_match0(
          response_format,
          c("auto", "delta", "parquet")
        )
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
        private$cache_path,
        batch_size = batch_size,
        concurrency = private$concurrency
      )
    }
  )
)

#' Delta Sharing change data feed reader
#'
#' An immutable change data feed specification with Arrow materializers. Created
#' by `SharingTable$changes()`. The sharing server validates the supplied bounds.
#'
#' @export
SharingChanges <- R6::R6Class(
  "SharingChanges",
  inherit = SharingReader,
  cloneable = FALSE,
  public = list(
    #' @description Create a changes reader. Prefer `SharingTable$changes()`.
    #' @param profile,auth,identifier Internal client state.
    #' @param starting_version,ending_version,starting_timestamp,ending_timestamp,columns
    #'   Query options; see [SharingTable]'s `changes()` method.
    #' @param cache_path,concurrency Internal table execution settings.
    initialize = function(
      profile,
      auth,
      identifier,
      starting_version = NULL,
      ending_version = NULL,
      starting_timestamp = NULL,
      ending_timestamp = NULL,
      columns = NULL,
      cache_path,
      concurrency
    ) {
      if (!is.null(columns) && !is.character(columns)) {
        abort(
          "{.arg columns} must be a character vector.",
          type = "validation",
          operation = "changes"
        )
      }
      private$profile <- profile
      private$auth <- auth
      private$identifier <- identifier
      private$cache_path <- cache_path
      private$concurrency <- concurrency
      private$spec <- list(
        starting_version = starting_version,
        ending_version = ending_version,
        starting_timestamp = starting_timestamp,
        ending_timestamp = ending_timestamp,
        columns = columns
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
        private$cache_path,
        batch_size = batch_size,
        concurrency = private$concurrency
      )
    }
  )
)
