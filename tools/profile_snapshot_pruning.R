# Compare the signed-file manifest selected by a plain snapshot, a limit hint,
# a structured predicate hint, and both hints together. This profiles only the
# R control plane and synthetic-log preparation; it does not download Parquet.
#
# Usage:
#   Rscript tools/profile_snapshot_pruning.R \
#     PROFILE TABLE COLUMN VALUE VALUE_TYPE LIMIT

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 6L) {
  stop(
    paste(
      "Usage: profile_snapshot_pruning.R",
      "PROFILE TABLE COLUMN VALUE VALUE_TYPE LIMIT"
    ),
    call. = FALSE
  )
}

profile_path <- fs::path_expand(args[[1L]])
table_name <- args[[2L]]
column <- args[[3L]]
value <- args[[4L]]
value_type <- args[[5L]]
limit <- suppressWarnings(as.numeric(args[[6L]]))

if (
  !fs::file_exists(profile_path) ||
    any(!nzchar(c(table_name, column, value, value_type))) ||
    !rlang::is_scalar_integerish(limit, finite = TRUE) ||
    limit < 0
) {
  stop("The profile, predicate, or limit argument is invalid.", call. = FALSE)
}

pkgload::load_all(".", quiet = TRUE)

profile <- delta.sharing:::sharing_profile_parse(profile_path)
auth <- delta.sharing:::sharing_auth_context(profile)
identifier <- delta.sharing:::sharing_table_identifier(table_name)
predicate <- list(
  op = "equal",
  children = list(
    list(op = "column", name = column, valueType = value_type),
    list(op = "literal", value = value, valueType = value_type)
  )
)

format_started <- proc.time()[["elapsed"]]
response_format <- delta.sharing:::resolve_query_format(
  profile,
  auth,
  identifier,
  "auto"
)
format_seconds <- unname(proc.time()[["elapsed"]] - format_started)

cases <- list(
  baseline = list(predicate = NULL, limit = NULL),
  limit = list(predicate = NULL, limit = limit),
  predicate = list(predicate = predicate, limit = NULL),
  predicate_and_limit = list(predicate = predicate, limit = limit)
)

measure <- function(options, name) {
  spec <- list(
    predicate = options$predicate,
    limit = options$limit,
    version = NULL,
    timestamp = NULL
  )
  started <- proc.time()[["elapsed"]]
  log <- delta.sharing:::prepare_snapshot_query_log(
    profile,
    auth,
    identifier,
    spec,
    response_format
  )
  withr::defer(log$cleanup())
  elapsed <- unname(proc.time()[["elapsed"]] - started)
  commit <- fs::path(log$path, "_delta_log", delta.sharing:::log_commit_name)

  list(
    case = name,
    predicate_hint = !is.null(options$predicate),
    limit_hint = options$limit,
    elapsed_seconds = elapsed,
    pages = log$page_count,
    selected_files = log$file_count,
    synthetic_log_bytes = as.double(fs::file_info(commit)$size)
  )
}

measurements <- purrr::imap(cases, measure)
cat(
  jsonlite::toJSON(
    list(
      table = table_name,
      response_format = response_format,
      format_negotiation_seconds = format_seconds,
      measurements = measurements
    ),
    auto_unbox = TRUE,
    null = "null",
    pretty = TRUE
  ),
  "\n"
)
