#!/usr/bin/env Rscript

all_args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", all_args, value = TRUE)
if (length(file_arg) != 1L) {
  stop("Could not resolve the benchmark script path.", call. = FALSE)
}
script_path <- normalizePath(
  sub("^--file=", "", file_arg),
  winslash = "/",
  mustWork = TRUE
)
repo_root <- dirname(dirname(script_path))
source(file.path(repo_root, "tools", "performance_harness.R"))

options <- ph_parse_cli(commandArgs(trailingOnly = TRUE), repo_root)
if (isTRUE(options$help)) {
  cat(
    paste(
      "Usage:",
      "  Rscript bench/vnext-performance.R [options]",
      "",
      "Options:",
      "  --mode quick|standard       Workload size (default: quick)",
      "  --repetitions N             Override timed repetitions (1..100)",
      "  --output PATH               JSON artifact path",
      "  --help                      Show this help",
      sep = "\n"
    ),
    "\n"
  )
  quit(status = 0L)
}

suppressPackageStartupMessages(library(delta.sharing))
config <- ph_config(options$mode, options$repetitions)
artifact <- ph_run(options$repo_root, config, options$output)

cat("artifact:", normalizePath(options$output, winslash = "/"), "\n")
cat("gates:\n")
for (gate in artifact$gates) {
  cat(sprintf("  %-39s %s\n", gate$id, gate$status))
}
failed <- vapply(
  artifact$gates,
  function(gate) identical(gate$status, "fail"),
  logical(1)
)
if (any(failed)) {
  quit(status = 1L)
}
