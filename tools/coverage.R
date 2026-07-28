coverage_dir <- "coverage"
dir.create(coverage_dir, showWarnings = FALSE)

coverage <- covr::package_coverage(
  type = "tests",
  quiet = FALSE,
  clean = TRUE
)
percent <- as.numeric(covr::percent_coverage(coverage))

covr::to_cobertura(
  coverage,
  filename = file.path(coverage_dir, "cobertura.xml")
)
saveRDS(coverage, file.path(coverage_dir, "coverage.rds"))
writeLines(
  sprintf("%.2f", percent),
  file.path(coverage_dir, "percent.txt")
)

minimum <- as.numeric(Sys.getenv(
  "DELTA_SHARING_MIN_COVERAGE",
  unset = "80"
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
