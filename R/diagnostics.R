.read_diagnostics_attribute <- ".delta_sharing_read_diagnostics"

.diagnostics_optional_numeric <- S7::new_union(NULL, S7::class_numeric)
.diagnostics_optional_character <- S7::new_union(NULL, S7::class_character)
.diagnostics_optional_timestamp <- S7::new_union(NULL, S7::class_POSIXct)

.normalize_diagnostics_count <- function(value, name) {
  if (!is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 0 ||
      value != floor(value) ||
      value > 2^53) {
    .abort_delta_sharing(
      sprintf("`%s` must be one non-negative whole number.", name),
      type = "validation",
      operation = "read_diagnostics"
    )
  }
  as.double(value)
}

.normalize_diagnostics_batch_size <- function(value) {
  if (!is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 1 ||
      value > 1000000 ||
      value != floor(value)) {
    .abort_delta_sharing(
      "`batch_size` must be one whole number between 1 and 1000000.",
      type = "validation",
      operation = "read_diagnostics"
    )
  }
  as.double(value)
}

.normalize_diagnostics_concurrency <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 1 ||
      value > .Machine$integer.max ||
      value != floor(value)) {
    .abort_delta_sharing(
      "`concurrency` must be NULL or one positive whole number.",
      type = "validation",
      operation = "read_diagnostics"
    )
  }
  as.double(value)
}

.normalize_diagnostics_flag <- function(value, name) {
  if (!is.logical(value) || length(value) != 1L || is.na(value)) {
    .abort_delta_sharing(
      sprintf("`%s` must be TRUE or FALSE.", name),
      type = "validation",
      operation = "read_diagnostics"
    )
  }
  value
}

.normalize_diagnostics_expiry_seconds <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (!is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 0) {
    .abort_delta_sharing(
      "`url_expires_in_seconds` must be NULL or one non-negative number.",
      type = "validation",
      operation = "read_diagnostics"
    )
  }
  as.double(value)
}

#' Immutable diagnostics for one Delta Sharing read
#'
#' `SharingReadDiagnostics` is a redacted snapshot of safe planning and
#' selection facts attached to one stream by [read_arrow_stream()]. It never
#' owns the stream or any execution resource. The object deliberately excludes
#' credentials, URLs, paths, query strings, page and refresh tokens, predicate
#' values, protocol actions, and private temporary locations.
#'
#' Diagnostics are immutable and remain available after stream exhaustion or
#' explicit release. They do not report an active/released flag because the
#' standard nanoarrow stream surface does not expose that state reliably.
#'
#' @param read_kind `"snapshot"` or `"cdf"`.
#' @param response_format The response format selected for the read.
#' @param table_version Resolved snapshot table version, or `NULL` for CDF.
#' @param starting_version,ending_version Inclusive CDF version bounds, or
#'   `NULL` for a snapshot.
#' @param page_count Number of protocol response pages consumed during
#'   planning.
#' @param file_count Number of file actions selected for the read.
#' @param columns Projected columns, or `NULL` for all columns.
#' @param limit Exact row limit, or `NULL` when unbounded.
#' @param batch_size Maximum Arrow batch size requested from the reader.
#' @param concurrency Selected concurrency, or `NULL` for the reader default.
#' @param predicate_hint_sent Whether a server-side predicate hint was sent.
#'   Predicate expressions and values are never retained.
#' @param server_limit_hint Numeric limit hint sent to the server, or `NULL`.
#' @param min_url_expiration Earliest signed-file URL expiration selected while
#'   planning, or `NULL` when the server supplied no expiration.
#' @param url_expires_in_seconds Non-negative time-to-expiry observed when
#'   planning completed, or `NULL`. This is an immutable observation, not a
#'   live countdown.
#' @return A read-only `SharingReadDiagnostics` object.
#' @examples
#' diagnostics <- SharingReadDiagnostics(
#'   read_kind = "snapshot",
#'   response_format = "delta",
#'   table_version = 42,
#'   page_count = 2,
#'   file_count = 8,
#'   columns = c("order_id", "amount"),
#'   limit = 100,
#'   batch_size = 65536
#' )
#' diagnostics@file_count
#' @export
SharingReadDiagnostics <- S7::new_class(
  "SharingReadDiagnostics",
  package = "delta.sharing",
  properties = list(
    read_kind = .readonly_property(S7::class_character, ".read_kind"),
    response_format = .readonly_property(
      S7::class_character,
      ".response_format"
    ),
    table_version = .readonly_property(
      .diagnostics_optional_numeric,
      ".table_version"
    ),
    starting_version = .readonly_property(
      .diagnostics_optional_numeric,
      ".starting_version"
    ),
    ending_version = .readonly_property(
      .diagnostics_optional_numeric,
      ".ending_version"
    ),
    page_count = .readonly_property(S7::class_numeric, ".page_count"),
    file_count = .readonly_property(S7::class_numeric, ".file_count"),
    columns = .readonly_property(
      .diagnostics_optional_character,
      ".columns"
    ),
    limit = .readonly_property(.diagnostics_optional_numeric, ".limit"),
    batch_size = .readonly_property(S7::class_numeric, ".batch_size"),
    concurrency = .readonly_property(
      .diagnostics_optional_numeric,
      ".concurrency"
    ),
    predicate_hint_sent = .readonly_property(
      S7::class_logical,
      ".predicate_hint_sent"
    ),
    server_limit_hint = .readonly_property(
      .diagnostics_optional_numeric,
      ".server_limit_hint"
    ),
    min_url_expiration = .readonly_property(
      .diagnostics_optional_timestamp,
      ".min_url_expiration"
    ),
    url_expires_in_seconds = .readonly_property(
      .diagnostics_optional_numeric,
      ".url_expires_in_seconds"
    )
  ),
  constructor = function(
    read_kind,
    response_format,
    table_version = NULL,
    starting_version = NULL,
    ending_version = NULL,
    page_count,
    file_count,
    columns = NULL,
    limit = NULL,
    batch_size,
    concurrency = NULL,
    predicate_hint_sent = FALSE,
    server_limit_hint = NULL,
    min_url_expiration = NULL,
    url_expires_in_seconds = NULL
  ) {
    if (!.is_scalar_character(read_kind) ||
        !read_kind %in% c("snapshot", "cdf")) {
      .abort_delta_sharing(
        "`read_kind` must be \"snapshot\" or \"cdf\".",
        type = "validation",
        operation = "read_diagnostics"
      )
    }
    if (!.is_scalar_character(response_format) ||
        !response_format %in% c("delta", "parquet")) {
      .abort_delta_sharing(
        "`response_format` must be \"delta\" or \"parquet\".",
        type = "validation",
        operation = "read_diagnostics"
      )
    }
    table_version <- .normalize_version(table_version, "table_version")
    starting_version <- .normalize_version(
      starting_version,
      "starting_version"
    )
    ending_version <- .normalize_version(ending_version, "ending_version")
    if (identical(read_kind, "snapshot") &&
        (is.null(table_version) ||
          !is.null(starting_version) ||
          !is.null(ending_version))) {
      .abort_delta_sharing(
        "Snapshot diagnostics require `table_version` and no CDF bounds.",
        type = "validation",
        operation = "read_diagnostics"
      )
    }
    if (identical(read_kind, "cdf") &&
        (!is.null(table_version) ||
          is.null(starting_version) ||
          (!is.null(ending_version) &&
            ending_version < starting_version))) {
      .abort_delta_sharing(
        "CDF diagnostics require a valid inclusive version range.",
        type = "validation",
        operation = "read_diagnostics"
      )
    }

    page_count <- .normalize_diagnostics_count(page_count, "page_count")
    file_count <- .normalize_diagnostics_count(file_count, "file_count")
    columns <- .normalize_columns(columns)
    limit <- .normalize_limit(limit)
    batch_size <- .normalize_diagnostics_batch_size(batch_size)
    concurrency <- .normalize_diagnostics_concurrency(concurrency)
    predicate_hint_sent <- .normalize_diagnostics_flag(
      predicate_hint_sent,
      "predicate_hint_sent"
    )
    server_limit_hint <- if (is.null(server_limit_hint)) {
      NULL
    } else {
      .normalize_diagnostics_count(
        server_limit_hint,
        "server_limit_hint"
      )
    }
    min_url_expiration <- .normalize_timestamp(
      min_url_expiration,
      "min_url_expiration"
    )
    url_expires_in_seconds <- .normalize_diagnostics_expiry_seconds(
      url_expires_in_seconds
    )
    if (xor(
      is.null(min_url_expiration),
      is.null(url_expires_in_seconds)
    )) {
      .abort_delta_sharing(
        "URL expiration timestamp and time-to-expiry must both be present or both be NULL.",
        type = "validation",
        operation = "read_diagnostics"
      )
    }

    S7::new_object(
      S7::S7_object(),
      .read_kind = read_kind,
      .response_format = response_format,
      .table_version = table_version,
      .starting_version = starting_version,
      .ending_version = ending_version,
      .page_count = page_count,
      .file_count = file_count,
      .columns = columns,
      .limit = limit,
      .batch_size = batch_size,
      .concurrency = concurrency,
      .predicate_hint_sent = predicate_hint_sent,
      .server_limit_hint = server_limit_hint,
      .min_url_expiration = min_url_expiration,
      .url_expires_in_seconds = url_expires_in_seconds
    )
  }
)

.new_snapshot_read_diagnostics <- function(
  specification,
  planning,
  batch_size,
  concurrency
) {
  if (!.object_is(specification, SharingRead) || !is.list(planning)) {
    .abort_delta_sharing(
      "Snapshot diagnostics could not be constructed.",
      type = "native",
      operation = "read_arrow_stream"
    )
  }
  SharingReadDiagnostics(
    read_kind = "snapshot",
    response_format = planning$response_format,
    table_version = planning$table_version,
    page_count = planning$page_count,
    file_count = planning$file_count,
    columns = specification@columns,
    limit = specification@limit,
    batch_size = batch_size,
    concurrency = concurrency,
    predicate_hint_sent = planning$predicate_hint_sent,
    server_limit_hint = planning$server_limit_hint,
    min_url_expiration = planning$min_url_expiration,
    url_expires_in_seconds = planning$url_expires_in_seconds
  )
}

.new_cdf_read_diagnostics <- function(
  specification,
  planning,
  batch_size,
  concurrency
) {
  if (!.object_is(specification, SharingChanges) || !is.list(planning)) {
    .abort_delta_sharing(
      "CDF diagnostics could not be constructed.",
      type = "native",
      operation = "read_arrow_stream"
    )
  }
  SharingReadDiagnostics(
    read_kind = "cdf",
    response_format = planning$response_format,
    starting_version = planning$start_version,
    ending_version = planning$end_version,
    page_count = planning$page_count,
    file_count = planning$file_count,
    columns = specification@columns,
    limit = NULL,
    batch_size = batch_size,
    concurrency = concurrency,
    predicate_hint_sent = FALSE,
    server_limit_hint = NULL,
    min_url_expiration = planning$min_url_expiration,
    url_expires_in_seconds = planning$url_expires_in_seconds
  )
}

.attach_read_diagnostics <- function(stream, diagnostics) {
  if (!.object_is(diagnostics, SharingReadDiagnostics)) {
    .abort_delta_sharing(
      "The read diagnostics state is invalid.",
      type = "native",
      operation = "read_arrow_stream"
    )
  }
  attached <- tryCatch(
    {
      attr(stream, .read_diagnostics_attribute) <- diagnostics
      stream
    },
    error = function(condition) NULL
  )
  if (is.null(attached)) {
    .abort_delta_sharing(
      "The native stream could not retain read diagnostics.",
      type = "native",
      operation = "read_arrow_stream"
    )
  }
  attached
}

.stream_read_diagnostics <- function(stream) {
  diagnostics <- attr(
    stream,
    .read_diagnostics_attribute,
    exact = TRUE
  )
  if (!inherits(stream, "nanoarrow_array_stream") ||
      !.object_is(diagnostics, SharingReadDiagnostics)) {
    .abort_delta_sharing(
      "`stream` must be a stream returned by `read_arrow_stream()`.",
      type = "validation",
      operation = "read_diagnostics"
    )
  }
  diagnostics
}
