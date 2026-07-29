# Time one source through the development R connector. LIMIT is `none`, a row
# count, or `cdf:START:END`. Run this in a fresh process so remote timings do
# not share connector state.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 4L) {
  stop(
    "Usage: compare_connector.R PROFILE TABLE LIMIT_OR_CDF ITERATIONS",
    call. = FALSE
  )
}
profile <- fs::path_expand(args[[1L]])
table_name <- args[[2L]]
limit_text <- args[[3L]]
iterations <- suppressWarnings(as.integer(args[[4L]]))
cdf_bounds <- if (startsWith(limit_text, "cdf:")) {
  suppressWarnings(as.numeric(strsplit(limit_text, ":", fixed = TRUE)[[1L]][
    -1L
  ]))
} else {
  NULL
}
limit <- if (identical(limit_text, "none") || !is.null(cdf_bounds)) {
  NULL
} else {
  suppressWarnings(as.numeric(limit_text))
}
if (
  !fs::file_exists(profile) ||
    is.na(iterations) ||
    iterations < 1L ||
    (!is.null(cdf_bounds) &&
      (length(cdf_bounds) != 2L || anyNA(cdf_bounds))) ||
    (!is.null(limit) && (!is.finite(limit) || limit < 0))
) {
  stop("PROFILE, LIMIT, or ITERATIONS is invalid.", call. = FALSE)
}

pkgload::load_all(".", quiet = TRUE)
options(cli.progress_show_after = Inf)
table <- sharing_client(profile)$table(table_name)

measurements <- purrr::map(seq_len(iterations), function(iteration) {
  started <- proc.time()[["elapsed"]]
  data <- if (is.null(cdf_bounds)) {
    table$snapshot(
      limit = limit,
      response_format = "delta"
    )$to_data_frame(progress = FALSE)
  } else {
    table$changes(
      starting_version = cdf_bounds[[1L]],
      ending_version = cdf_bounds[[2L]]
    )$to_data_frame(progress = FALSE)
  }
  list(
    iteration = iteration,
    elapsed_seconds = unname(proc.time()[["elapsed"]] - started),
    rows = nrow(data),
    columns = ncol(data)
  )
})

cat(
  jsonlite::toJSON(
    list(
      connector = "delta.sharing R development",
      table = table_name,
      mode = if (is.null(cdf_bounds)) "snapshot" else "changes",
      limit = limit,
      cdf_bounds = cdf_bounds,
      measurements = measurements
    ),
    auto_unbox = TRUE,
    null = "null",
    pretty = TRUE
  ),
  "\n"
)
