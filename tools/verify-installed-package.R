#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) {
  stop("usage: verify-installed-package.R <library>", call. = FALSE)
}

library_path <- normalizePath(args[[1L]], winslash = "/", mustWork = TRUE)
.libPaths(c(library_path, .libPaths()))

if (!requireNamespace("delta.sharing", quietly = TRUE)) {
  stop("delta.sharing is not installed in the verification library.", call. = FALSE)
}

package_path <- system.file(package = "delta.sharing")
expected_package_path <- file.path(library_path, "delta.sharing")
if (!dir.exists(expected_package_path)) {
  stop("delta.sharing is not installed in the verification library.", call. = FALSE)
}
expected_package_path <- normalizePath(
  expected_package_path,
  winslash = "/",
  mustWork = TRUE
)
actual_package_path <- normalizePath(
  package_path,
  winslash = "/",
  mustWork = TRUE
)
if (identical(.Platform$OS.type, "windows")) {
  expected_package_path <- tolower(expected_package_path)
  actual_package_path <- tolower(actual_package_path)
}
if (!identical(actual_package_path, expected_package_path)) {
  stop("delta.sharing was loaded from outside the verification library.", call. = FALSE)
}

diagnostics <- delta.sharing:::.native_diagnostics()
description <- read.dcf("DESCRIPTION", fields = "Version")
if (nrow(description) != 1L || is.na(description[[1L, "Version"]])) {
  stop("The repository DESCRIPTION has no package version.", call. = FALSE)
}
expected_version <- description[[1L, "Version"]]
stopifnot(
  identical(
    as.character(utils::packageVersion("delta.sharing")),
    expected_version
  ),
  identical(diagnostics$delta_kernel_version, "0.26.0"),
  identical(diagnostics$arrow_rs_version, "58.3.0"),
  isTRUE(diagnostics$kernel_smoke_ok),
  diagnostics$active_streams == 0,
  diagnostics$pending_cleanups == 0
)

native_libraries <- list.files(
  file.path(package_path, "libs"),
  pattern = paste0("\\", .Platform$dynlib.ext, "$"),
  recursive = TRUE,
  full.names = TRUE
)
if (length(native_libraries) != 1L) {
  stop("The installed package must contain exactly one native library.", call. = FALSE)
}

license_files <- file.path(
  package_path,
  "dependency-licenses",
  c("dependency-inventory.json", "rust-license-texts.tar.xz")
)
if (!all(file.exists(license_files))) {
  stop("The installed package is missing dependency-license materials.", call. = FALSE)
}

installed_directories <- list.dirs(
  package_path,
  recursive = TRUE,
  full.names = FALSE
)
if (any(basename(installed_directories) %in% c(".cargo", "target", "vendor"))) {
  stop("The installed package retained a native build directory.", call. = FALSE)
}

message(
  "Verified delta.sharing ",
  utils::packageVersion("delta.sharing"),
  " in ",
  library_path
)
