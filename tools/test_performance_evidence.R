#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
script <- normalizePath(
  sub("^--file=", "", file_arg[[1L]]),
  winslash = "/",
  mustWork = TRUE
)
repo_root <- dirname(dirname(script))
source(file.path(repo_root, "tools", "performance_harness.R"))
source(file.path(repo_root, "tools", "performance_evidence.R"))
source(file.path(repo_root, "tools", "performance_peak_rss_worker.R"))

expect_error <- function(expression, pattern) {
  condition <- tryCatch(
    {
      force(expression)
      NULL
    },
    error = identity
  )
  stopifnot(inherits(condition, "error"))
  stopifnot(grepl(pattern, conditionMessage(condition), fixed = TRUE))
}

parsed <- pe_parse_cli(
  c(
    "--base",
    "base.json",
    "--mode",
    "standard",
    "--output",
    "evidence.json"
  ),
  repo_root
)
stopifnot(identical(parsed$base, "base.json"))
stopifnot(identical(parsed$mode, "standard"))
stopifnot(identical(parsed$output, "evidence.json"))
expect_error(
  pe_parse_cli(c("--mode", "slow", "--base", "x"), repo_root),
  "`--mode` must be"
)
expect_error(
  pe_parse_cli(character(), repo_root),
  "`--base` is required"
)

worker <- worker_parse_cli(c(
  "--workload",
  "synthetic",
  "--batches",
  "8",
  "--rows-per-batch",
  "4096",
  "--output",
  "worker.json"
))
stopifnot(identical(worker$batches, 8L))
stopifnot(identical(worker$rows_per_batch, 4096L))
expect_error(
  worker_parse_cli(c(
    "--workload",
    "synthetic",
    "--batches",
    "0",
    "--rows-per-batch",
    "1",
    "--output",
    "worker.json"
  )),
  "`--batches` must be"
)

darwin <- list(name = "darwin-time-l")
darwin_rss <- pe_parse_peak_rss(
  "            12345678  maximum resident set size",
  darwin
)
stopifnot(identical(darwin_rss, 12345678))
gnu <- list(name = "gnu-time-v")
gnu_rss <- pe_parse_peak_rss(
  "\tMaximum resident set size (kbytes): 54321",
  gnu
)
stopifnot(identical(gnu_rss, 54321 * 1024))
expect_error(
  pe_parse_peak_rss("no memory here", darwin),
  "did not contain one peak-RSS value"
)

kernel_samples <- list(
  list(
    case = "all",
    batch_size = 2,
    rows = 7,
    total_seconds = 2,
    rows_per_second = 3.5
  ),
  list(
    case = "all",
    batch_size = 2,
    rows = 7,
    total_seconds = 1,
    rows_per_second = 7
  )
)
base_kernel <- pe_kernel_samples(list(
  measurements = list(kernel = kernel_samples)
))
stopifnot(identical(base_kernel$batch_size, 2L))
stopifnot(identical(base_kernel$rows, 7))

comparable <- list(
  fixture = list(rows = 7, parquet_bytes = 100),
  order = "test",
  r_samples = lapply(kernel_samples, function(sample) {
    c(sample, list(maximum_batch_rows = 2))
  }),
  rust_samples = list(
    list(
      rows = 7,
      maximum_batch_rows = 2,
      total_seconds = 1,
      rows_per_second = 7
    ),
    list(
      rows = 7,
      maximum_batch_rows = 2,
      total_seconds = 0.5,
      rows_per_second = 14
    )
  )
)
comparison <- pe_comparisons(comparable)
stopifnot(identical(comparison$r_observed_maximum_batch_rows, 2))
stopifnot(identical(comparison$rust_observed_maximum_batch_rows, 2))
stopifnot(identical(comparison$median_total_time_overhead_fraction, 1))
stopifnot(identical(comparison$median_throughput_ratio, 0.5))

config <- pe_config("standard")
rss <- list(
  backend = list(available = TRUE),
  kernel_scaling = lapply(
    seq_along(config$rss_kernel_row_counts),
    function(index) {
      list(
        table_rows = as.double(config$rss_kernel_row_counts[[index]]),
        parquet_bytes = index * 100,
        peak_rss_bytes = 100 * 1024^2 + index * 1024^2
      )
    }
  )
)
rss_scaling <- pe_rss_scaling(rss, config)
stopifnot(isTRUE(rss_scaling$evaluable))
stopifnot(isTRUE(rss_scaling$pass))

gates <- pe_evaluate_gates(
  comparison,
  rss_scaling,
  list(pass = TRUE),
  config
)
stopifnot(identical(gates[[1L]]$status, "pass"))
stopifnot(identical(gates[[2L]]$status, "pass"))
stopifnot(identical(gates[[3L]]$status, "not_evaluable"))
stopifnot(identical(gates[[4L]]$status, "fail"))
stopifnot(identical(gates[[5L]]$status, "pass"))

artifact <- list(
  schema_version = 1L,
  environment = list(),
  base_artifact = list(),
  configuration = list(),
  source_identity = list(),
  measurements = list(),
  comparisons = list(),
  gates = gates,
  limitations = "Test."
)
pe_validate_artifact(artifact)
duplicate <- artifact
duplicate$gates <- c(duplicate$gates, list(duplicate$gates[[1L]]))
expect_error(
  pe_validate_artifact(duplicate),
  "duplicate gate IDs"
)

backend <- pe_time_backend()
if (isTRUE(backend$available)) {
  measured <- pe_timed_worker(
    repo_root,
    backend,
    c("--workload", "baseline")
  )
  stopifnot(is.finite(measured$peak_rss_bytes))
  stopifnot(measured$peak_rss_bytes > 0)
  stopifnot(identical(measured$worker$workload, "baseline"))
  stopifnot(identical(
    as.double(measured$worker$result$pending_cleanups_delta),
    0
  ))
}

cat("performance evidence tests: PASS\n")
