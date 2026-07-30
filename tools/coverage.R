coverage_dir <- "coverage"
dir.create(coverage_dir, showWarnings = FALSE)

# This job measures the R implementation. The C shim is exercised by the
# native FFI, installed-package lifecycle, and sanitizer gates instead.
# `.onUnload()` is also exercised only from an installed package because
# unloading a pkgload namespace does not reproduce the installed DLL lifecycle.
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
  filename = file.path(coverage_dir, "cobertura.xml")
)
saveRDS(coverage, file.path(coverage_dir, "coverage.rds"))
writeLines(
  sprintf("%.2f", percent),
  file.path(coverage_dir, "percent.txt")
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
