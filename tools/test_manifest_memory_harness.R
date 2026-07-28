#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
script <- normalizePath(
  sub("^--file=", "", file_arg[[1L]]),
  winslash = "/",
  mustWork = TRUE
)
repo_root <- dirname(dirname(script))
source(file.path(repo_root, "tools", "manifest_memory_harness.R"))
source(file.path(repo_root, "tools", "manifest_memory_worker.R"))

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

parsed <- mm_parse_cli(
  c("--mode", "standard", "--repetitions", "2", "--output", "result.json"),
  repo_root
)
stopifnot(identical(parsed$mode, "standard"))
stopifnot(identical(parsed$repetitions, 2L))
stopifnot(identical(parsed$output, "result.json"))
expect_error(mm_parse_cli(c("--mode", "slow"), repo_root), "`--mode`")
expect_error(mm_parse_cli(c("--repetitions", "0"), repo_root), "`--repetitions`")

quick <- mm_config("quick")
standard <- mm_config("standard")
stopifnot(identical(quick$file_counts, c(100L, 1000L, 10000L)))
stopifnot(identical(standard$file_counts, c(1000L, 10000L, 100000L)))

worker_options <- mmw_parse_cli(c(
  "--files", "25",
  "--chunk-files", "7",
  "--outcome", "explicit_release",
  "--output", "unused.json"
))
worker <- mmw_run(worker_options)
stopifnot(identical(worker$result$status, "pass"))
stopifnot(identical(worker$result$roots_after_cleanup, 0L))
stopifnot(identical(worker$closes, 1L))
stopifnot(worker$result$commit_bytes > 0)
stopifnot(worker$wire_bytes > worker$result$commit_bytes)

error_options <- worker_options
error_options$outcome <- "write_error"
failed <- mmw_run(error_options)
stopifnot(identical(failed$result$status, "pass"))
stopifnot(identical(failed$result$roots_after_cleanup, 0L))

finalizer_options <- worker_options
finalizer_options$outcome <- "finalizer"
finalized <- mmw_run(finalizer_options)
stopifnot(identical(finalized$result$status, "pass"))
stopifnot(identical(finalized$result$roots_after_cleanup, 0L))

darwin <- list(name = "darwin-time-l")
stopifnot(identical(
  mm_parse_peak_rss("  123456  maximum resident set size", darwin),
  123456
))
gnu <- list(name = "gnu-time-v")
stopifnot(identical(
  mm_parse_peak_rss("Maximum resident set size (kbytes): 1024", gnu),
  1024 * 1024
))

evidence_paths <- file.path(
  repo_root,
  "design",
  "evidence",
  c(
    "manifest-memory-darwin-arm64-0c88b9e.json",
    "manifest-memory-darwin-arm64-09dbd9b.json"
  )
)
stopifnot(all(file.exists(evidence_paths)))
evidence <- lapply(
  evidence_paths,
  jsonlite::read_json,
  simplifyVector = FALSE
)
stopifnot(identical(evidence[[1L]]$environment$git_worktree_dirty, TRUE))
stopifnot(identical(evidence[[2L]]$environment$git_worktree_dirty, FALSE))
for (artifact in evidence) {
  stopifnot(identical(
    artifact$gates$temporary_root_lifecycle,
    "pass"
  ))
  stopifnot(identical(
    artifact$gates$adr_003_rust_scope_expansion,
    "not_met"
  ))
  stopifnot(identical(
    artifact$lifecycle$explicit_release$response_closes,
    1L
  ))
  stopifnot(identical(
    artifact$lifecycle$explicit_release$roots_after_cleanup,
    0L
  ))
  stopifnot(identical(
    artifact$lifecycle$write_error_100000$roots_after_cleanup,
    0L
  ))
  stopifnot(identical(
    artifact$lifecycle$finalizer_100000$roots_after_cleanup,
    0L
  ))
}

cat("manifest memory harness tests: PASS\n")
