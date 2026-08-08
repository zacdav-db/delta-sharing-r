coverage_dir <- fs::path("coverage")
fs::dir_create(coverage_dir)

# This report measures R line coverage only. Native behavior is exercised by
# package checks and focused Rust tests, but is outside covr's report.
reviewed_exclusions <- list(
  "src/native.c" = seq_along(readLines("src/native.c")),
  "R/zzz.R" = seq_along(readLines("R/zzz.R"))
)

coverage <- covr::package_coverage(
  type = "tests",
  quiet = FALSE,
  clean = TRUE,
  line_exclusions = reviewed_exclusions
)
percent <- as.numeric(covr::percent_coverage(coverage))

covr::to_cobertura(
  coverage,
  filename = fs::path(coverage_dir, "cobertura.xml")
)
saveRDS(coverage, fs::path(coverage_dir, "coverage.rds"))
writeLines(
  sprintf("%.2f", percent),
  fs::path(coverage_dir, "percent.txt")
)

minimum <- as.numeric(Sys.getenv(
  "DELTA_SHARING_MIN_COVERAGE",
  unset = "90"
))

message(sprintf(
  "R line coverage: %.2f%% (required: %.2f%%)",
  percent,
  minimum
))

if (!is.finite(percent) || percent < minimum) {
  stop(
    sprintf(
      "R line coverage %.2f%% is below the %.2f%% gate",
      percent,
      minimum
    ),
    call. = FALSE
  )
}
