args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 4L) {
  stop(
    "Expected mode, ready-file, result-file, and package-path arguments.",
    call. = FALSE
  )
}

mode <- args[[1L]]
ready_path <- args[[2L]]
result_path <- args[[3L]]
package_path <- args[[4L]]

load_test_package <- function() {
  installed_marker <- fs::path(package_path, "Meta", "package.rds")
  if (fs::file_exists(installed_marker)) {
    suppressPackageStartupMessages(
      library(delta.sharing, lib.loc = fs::path_dir(package_path))
    )
  } else {
    if (!requireNamespace("pkgload", quietly = TRUE)) {
      stop("Source-tree lifecycle tests require pkgload.", call. = FALSE)
    }
    suppressPackageStartupMessages(
      pkgload::load_all(package_path, helpers = FALSE, quiet = TRUE)
    )
  }
}

reload_test_package <- function() {
  load_test_package()
  stream <- delta.sharing:::native_test_stream(
    batches = 2L,
    rows_per_batch = 3L
  )
  data <- delta.sharing:::sharing_stream_to_data_frame(stream)
  identical(nrow(data), 6L)
}

unload_test_package <- function() {
  warnings <- character()
  error <- NULL
  withCallingHandlers(
    tryCatch(
      unloadNamespace("delta.sharing"),
      error = function(condition) {
        error <<- conditionMessage(condition)
      }
    ),
    warning = function(condition) {
      warnings <<- c(warnings, conditionMessage(condition))
      invokeRestart("muffleWarning")
    }
  )
  list(warnings = warnings, error = error)
}

load_test_package()
withr::local_options(list(cli.progress_show_after = Inf))

if (identical(mode, "clean_reload")) {
  stream <- delta.sharing:::native_test_stream(
    batches = 2L,
    rows_per_batch = 3L
  )
  data <- delta.sharing:::sharing_stream_to_data_frame(
    stream,
    progress = TRUE
  )
  active_before_unload <- delta.sharing:::native_collect_active()
  unload <- unload_test_package()
  dll_present_after_unload <- "delta.sharing" %in% names(getLoadedDLLs())
  reload_error <- NULL
  reload_ok <- tryCatch(
    reload_test_package(),
    error = function(condition) {
      reload_error <<- conditionMessage(condition)
      FALSE
    }
  )
  saveRDS(
    list(
      rows = nrow(data),
      active_before_unload = active_before_unload,
      unload_warnings = unload$warnings,
      unload_error = unload$error,
      dll_present_after_unload = dll_present_after_unload,
      reload_ok = reload_ok,
      reload_error = reload_error
    ),
    result_path
  )
  quit(save = "no", status = 0L)
}

if (!identical(mode, "interrupt")) {
  stop("Unknown lifecycle subprocess mode.", call. = FALSE)
}

# This stream is intentionally far larger than the child can collect before the
# parent sends an interrupt. The native Rust unit gate separately uses a
# condition-variable reader to prove the worker's behaviour inside a blocked
# Arrow callback; this subprocess proves the R polling and condition boundary.
stream <- delta.sharing:::native_test_stream(
  batches = 10000L,
  rows_per_batch = 100000L
)
attr(stream, "delta_sharing_progress") <- list(total_rows = 1000000000)
writeLines("ready", ready_path, useBytes = TRUE)

started <- proc.time()[["elapsed"]]
condition <- tryCatch(
  delta.sharing:::sharing_stream_to_data_frame(stream, progress = TRUE),
  delta_sharing_cancelled = identity,
  error = identity,
  interrupt = identity
)
elapsed <- proc.time()[["elapsed"]] - started
active_after_interrupt <- delta.sharing:::native_collect_active()
pointer_valid <- nanoarrow::nanoarrow_pointer_is_valid(stream)

unload <- unload_test_package()
dll_present_after_unload <- "delta.sharing" %in% names(getLoadedDLLs())
reload_error <- NULL
reload_ok <- tryCatch(
  reload_test_package(),
  error = function(condition) {
    reload_error <<- conditionMessage(condition)
    FALSE
  }
)

saveRDS(
  list(
    classes = class(condition),
    message = if (inherits(condition, "condition")) {
      conditionMessage(condition)
    } else {
      "The eager read completed before interruption."
    },
    operation = if (inherits(condition, "condition")) {
      condition$operation
    } else {
      NULL
    },
    elapsed = elapsed,
    active_after_interrupt = active_after_interrupt,
    pointer_valid = pointer_valid,
    unload_warnings = unload$warnings,
    unload_error = unload$error,
    dll_present_after_unload = dll_present_after_unload,
    reload_ok = reload_ok,
    reload_error = reload_error
  ),
  result_path
)
