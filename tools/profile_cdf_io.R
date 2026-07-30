# Profile a credentialed CDF read by phase without exposing credentials or
# signed URLs. `plan` stops after preparing the synthetic log; `full` also
# drains the native Arrow stream and summarizes remote batch-pull latency.
#
# Usage:
#   Rscript tools/profile_cdf_io.R PROFILE TABLE START END plan|full

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 5L) {
  stop(
    "Usage: profile_cdf_io.R PROFILE TABLE START END plan|full",
    call. = FALSE
  )
}

profile_path <- fs::path_expand(args[[1L]])
table_name <- args[[2L]]
start_version <- suppressWarnings(as.numeric(args[[3L]]))
end_version <- suppressWarnings(as.numeric(args[[4L]]))
mode <- args[[5L]]

if (
  !fs::file_exists(profile_path) ||
    !nzchar(table_name) ||
    !rlang::is_scalar_integerish(start_version, finite = TRUE) ||
    !rlang::is_scalar_integerish(end_version, finite = TRUE) ||
    start_version < 0 ||
    end_version < start_version ||
    !mode %in% c("plan", "full")
) {
  stop("The profile, table, version range, or mode is invalid.", call. = FALSE)
}

pkgload::load_all(".", quiet = TRUE)

elapsed <- function(code) {
  started <- proc.time()[["elapsed"]]
  value <- force(code)
  list(
    value = value,
    seconds = unname(proc.time()[["elapsed"]] - started)
  )
}

action_kind <- function(action) {
  kind <- purrr::detect(
    c("cdc", "add", "remove", "metaData"),
    ~ !is.null(action[[.x]])
  )
  if (is.null(kind)) "other" else kind
}

result <- local({
  profile <- delta.sharing:::sharing_profile_parse(profile_path)
  auth <- delta.sharing:::sharing_auth_context(profile)
  identifier <- delta.sharing:::sharing_table_identifier(table_name)
  spec <- delta.sharing:::sharing_changes_validate(
    starting_version = start_version,
    ending_version = end_version,
    starting_timestamp = NULL,
    ending_timestamp = NULL,
    columns = NULL,
    response_format = "delta"
  )

  query <- elapsed(
    delta.sharing:::sharing_query_changes(profile, auth, identifier, spec)
  )
  parsed <- query$value
  version_actions <- purrr::map(parsed$by_version, "actions")
  actions <- purrr::list_flatten(version_actions)
  action_kinds <- purrr::map_chr(actions, action_kind)
  kind_counts <- as.list(table(factor(
    action_kinds,
    levels = c("cdc", "add", "remove", "metaData", "other")
  )))
  kind_counts <- purrr::discard(kind_counts, ~ identical(as.integer(.x), 0L))
  kind_counts <- purrr::map(kind_counts, as.integer)

  log_result <- elapsed(delta.sharing:::prepare_cdf_log(
    parsed$protocol,
    parsed$by_version,
    parsed$start_version,
    parsed$end_version
  ))
  log <- log_result$value
  withr::defer(log$cleanup())
  log_dir <- fs::path(log$path, "_delta_log")
  log_files <- fs::dir_ls(log_dir, type = "file")
  log_bytes <- sum(as.double(fs::file_info(log_files)$size))

  planning <- list(
    query_seconds = query$seconds,
    log_preparation_seconds = log_result$seconds,
    represented_versions = length(parsed$by_version),
    effective_start_version = parsed$start_version,
    effective_end_version = parsed$end_version,
    actions = length(actions),
    action_kinds = kind_counts,
    synthetic_log_files = length(log_files),
    synthetic_log_bytes = log_bytes
  )

  scan <- NULL
  if (identical(mode, "full")) {
    native <- elapsed(delta.sharing:::native_cdf_stream(
      table_location = log$path,
      start_version = log$start_version,
      end_version = log$end_version,
      batch_size = delta.sharing:::DEFAULT_BATCH_SIZE,
      cleanup_root = log$root
    ))
    stream <- native$value
    # Native ownership has transferred, so the R-side cleanup callback must no
    # longer remove the root.
    log$cleanup <- function() invisible(NULL)
    withr::defer(delta.sharing:::release_materializer_stream(stream))

    rows <- 0
    batches <- 0L
    pull_seconds <- numeric()
    repeat {
      pull <- elapsed(stream$get_next())
      pull_seconds <- c(pull_seconds, pull$seconds)
      batch <- pull$value
      if (is.null(batch)) {
        break
      }
      rows <- rows + batch$length
      batches <- batches + 1L
    }

    data_pulls <- if (batches == 0L) {
      numeric()
    } else {
      pull_seconds[seq_len(batches)]
    }
    quantiles <- if (length(data_pulls) == 0L) {
      c(p50 = 0, p90 = 0, p99 = 0)
    } else {
      stats::quantile(
        data_pulls,
        probs = c(0.5, 0.9, 0.99),
        names = FALSE,
        type = 8
      ) |>
        stats::setNames(c("p50", "p90", "p99"))
    }
    scan <- list(
      native_construction_seconds = native$seconds,
      rows = rows,
      batches = batches,
      first_batch_seconds = if (length(data_pulls)) data_pulls[[1L]] else 0,
      data_pull_seconds = sum(data_pulls),
      terminal_pull_seconds = pull_seconds[[length(pull_seconds)]],
      pull_seconds = c(
        as.list(quantiles),
        list(
          maximum = if (length(data_pulls)) max(data_pulls) else 0,
          over_one_second = sum(data_pulls > 1),
          over_five_seconds = sum(data_pulls > 5)
        )
      )
    )
  }

  list(
    table = table_name,
    mode = mode,
    planning = planning,
    scan = scan
  )
})

cat(
  jsonlite::toJSON(
    result,
    auto_unbox = TRUE,
    null = "null",
    pretty = TRUE
  ),
  "\n"
)
