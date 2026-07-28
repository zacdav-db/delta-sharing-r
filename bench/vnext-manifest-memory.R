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
source(file.path(repo_root, "tools", "manifest_memory_harness.R"))

options <- mm_parse_cli(commandArgs(trailingOnly = TRUE), repo_root)
config <- mm_config(options$mode, options$repetitions)
suppressPackageStartupMessages(library(delta.sharing))
artifact <- mm_run(repo_root, config, options$output)

cat("artifact:", normalizePath(options$output, winslash = "/"), "\n")
for (summary in artifact$summaries) {
  cat(sprintf(
    "%7d files  %.3f s  %.1f MiB peak  %.1f MiB incremental\n",
    summary$files,
    summary$median_elapsed_seconds,
    summary$median_peak_rss_bytes / 1024^2,
    summary$median_incremental_peak_rss_bytes / 1024^2
  ))
}
cat("gates:\n")
for (gate in artifact$gates) {
  cat(sprintf("  %-31s %s\n", gate$id, gate$status))
}
if (any(vapply(
  artifact$gates,
  function(gate) identical(gate$status, "fail"),
  logical(1)
))) {
  quit(status = 1L)
}
