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

parsed <- ph_parse_cli(
  c(
    "--mode",
    "standard",
    "--repetitions",
    "7",
    "--output",
    "result.json"
  ),
  repo_root
)
stopifnot(identical(parsed$mode, "standard"))
stopifnot(identical(parsed$repetitions, 7L))
stopifnot(identical(parsed$output, "result.json"))
expect_error(
  ph_parse_cli(c("--mode", "slow"), repo_root),
  "`--mode` must be"
)
expect_error(
  ph_parse_cli(c("--repetitions", "0"), repo_root),
  "`--repetitions` must be"
)
expect_error(
  ph_parse_cli("--unknown", repo_root),
  "Unknown benchmark argument"
)

allocation_bytes <- ph_rprofmem_bytes(c(
  "128 :\"foo\"",
  "new page:",
  "2048: benchmark",
  "not an allocation"
))
stopifnot(identical(allocation_bytes, c(128, 2048)))

gc_baseline_6 <- matrix(
  c(
    10, 2, 100, 10, 10, 2,
    20, 3, 200, 20, 20, 3
  ),
  nrow = 2L,
  byrow = TRUE,
  dimnames = list(
    c("Ncells", "Vcells"),
    c("used", "(Mb)", "gc trigger", "(Mb)", "max used", "(Mb)")
  )
)
gc_high_water_6 <- gc_baseline_6
gc_high_water_6[, 6L] <- c(4, 5)
stopifnot(identical(
  ph_gc_heap_peak_proxy_bytes(gc_baseline_6, gc_high_water_6),
  4 * 1024^2
))

if (isTRUE(unname(capabilities("profmem")))) {
  interrupted <- structure(
    list(message = "performance harness test interrupt"),
    class = c("interrupt", "condition")
  )
  caught <- tryCatch(
    ph_profile_r(function() {
      signalCondition(interrupted)
    }),
    interrupt = identity
  )
  stopifnot(inherits(caught, "interrupt"))

  # A fresh profile must be usable immediately after the non-error condition
  # exits ph_profile_r(). This exercises the on.exit Rprofmem(NULL) cleanup.
  followup_profile <- tempfile(
    "delta-sharing-r-rprofmem-followup-",
    fileext = ".out"
  )
  on.exit(unlink(followup_profile, force = TRUE), add = TRUE)
  Rprofmem(followup_profile, threshold = 0L)
  invisible(raw(4096L))
  Rprofmem(NULL)
  stopifnot(file.exists(followup_profile))
}

minimal_measurements <- list(
  action_staging = list(),
  manifest = list(),
  ffi = list(),
  kernel = list(),
  release = list(),
  backpressure = list()
)
artifact <- list(
  schema_version = 1L,
  environment = list(r_version = R.version.string),
  configuration = list(mode = "test"),
  measurements = minimal_measurements,
  summaries = list(),
  gates = list(ph_gate(
    "test-gate",
    "Parser round-trip.",
    "pass",
    list(value = 1),
    "value = 1"
  )),
  metric_classes = list(
    controlled = "test-gate",
    trend = "test-trend"
  ),
  limitations = "Test fixture."
)
ph_validate_artifact(artifact)

path <- tempfile("delta-sharing-r-performance-", fileext = ".json")
on.exit(unlink(path, force = TRUE), add = TRUE)
ph_write_artifact(artifact, path)
round_trip <- ph_read_artifact(path)
stopifnot(identical(round_trip$schema_version, 1L))
stopifnot(identical(round_trip$gates[[1L]]$id, "test-gate"))
stopifnot(identical(round_trip$gates[[1L]]$status, "pass"))

duplicate <- artifact
duplicate$gates <- c(duplicate$gates, duplicate$gates)
expect_error(
  ph_validate_artifact(duplicate),
  "duplicate gate identifiers"
)
invalid <- artifact
invalid$gates[[1L]]$status <- "maybe"
expect_error(
  ph_validate_artifact(invalid),
  "invalid gate"
)
missing <- artifact
missing$measurements$ffi <- NULL
expect_error(
  ph_validate_artifact(missing),
  "missing measurement groups"
)

cat("performance harness parser/validation tests: PASS\n")
