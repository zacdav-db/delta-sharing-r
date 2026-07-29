run_progress_lifecycle_subprocess <- function(mode) {
  skip_if_not_installed("processx")

  directory <- withr::local_tempdir(pattern = "delta-sharing-lifecycle-")
  ready_path <- fs::path(directory, "ready")
  result_path <- fs::path(directory, "result.rds")
  log_path <- fs::path(directory, "child.log")
  script_path <- fs::path_real(
    test_path("fixtures", "progress-interrupt-subprocess.R")
  )
  package_path <- fs::path_real(
    getNamespaceInfo(asNamespace("delta.sharing"), "path")
  )
  process_class <- getExportedValue("processx", "process")
  process <- process_class$new(
    fs::path(R.home("bin"), "Rscript"),
    args = c(
      "--vanilla",
      script_path,
      mode,
      ready_path,
      result_path,
      package_path
    ),
    stdout = log_path,
    stderr = log_path,
    env = c(R_LIBS = paste(.libPaths(), collapse = .Platform$path.sep)),
    cleanup = TRUE,
    cleanup_tree = TRUE
  )
  withr::defer({
    if (process$is_alive()) {
      process$kill_tree()
    }
  })

  if (identical(mode, "interrupt")) {
    deadline <- Sys.time() + 20
    while (
      !fs::file_exists(ready_path) &&
        process$is_alive() &&
        Sys.time() < deadline
    ) {
      Sys.sleep(0.005)
    }
    if (!fs::file_exists(ready_path)) {
      stop(
        paste(
          "Progress interrupt subprocess did not become ready:",
          paste(readLines(log_path, warn = FALSE), collapse = "\n")
        ),
        call. = FALSE
      )
    }

    # Allow the child to enter the polling loop before delivering a genuine
    # process interrupt (SIGINT on Unix, CTRL+BREAK through processx on Windows).
    Sys.sleep(0.02)
    process$interrupt()
  }

  process$wait(timeout = 10000)
  if (process$is_alive()) {
    stop("Progress lifecycle subprocess did not stop promptly.", call. = FALSE)
  }
  if (process$get_exit_status() != 0L || !fs::file_exists(result_path)) {
    stop(
      paste(
        "Progress lifecycle subprocess failed:",
        paste(readLines(log_path, warn = FALSE), collapse = "\n")
      ),
      call. = FALSE
    )
  }
  readRDS(result_path)
}

test_that("a real interrupt cancels eager progress without blocking R", {
  skip_on_os("windows")

  result <- run_progress_lifecycle_subprocess("interrupt")

  expect_type(result, "list")
  expect_true("delta_sharing_cancelled" %in% result$classes)
  expect_identical(
    result$message,
    "The Delta Sharing read was interrupted."
  )
  expect_identical(result$operation, "read_arrow_stream")
  expect_false(result$pointer_valid)
  expect_lt(result$elapsed, 5)

  # Cancelling while a native callback is in flight detaches the worker. The
  # DLL must remain resident, and loading the package again must remain safe.
  expect_gte(result$active_after_interrupt, 1)
  expect_null(result$unload_error)
  expect_true(any(grepl(
    "native library remains loaded",
    result$unload_warnings,
    fixed = TRUE
  )))
  expect_true(result$dll_present_after_unload)
  expect_true(result$reload_ok, info = result$reload_error)
})

test_that("completed eager progress permits clean unload and reload", {
  result <- run_progress_lifecycle_subprocess("clean_reload")

  expect_identical(result$rows, 6L)
  expect_equal(result$active_before_unload, 0)
  expect_length(result$unload_warnings, 0L)
  expect_null(result$unload_error)
  expect_false(result$dll_present_after_unload)
  expect_true(result$reload_ok, info = result$reload_error)
})

test_that("success and failure release completed collection jobs", {
  withr::local_options(list(cli.progress_show_after = Inf))
  expect_equal(native_collect_active(), 0)

  stream <- native_test_stream(batches = 3L, rows_per_batch = 2L)
  data <- sharing_stream_to_data_frame(stream, progress = TRUE)
  expect_equal(nrow(data), 6L)
  expect_equal(native_collect_active(), 0)

  stream <- native_test_stream(
    batches = 3L,
    rows_per_batch = 2L,
    error_after = 1L
  )
  expect_error(
    sharing_stream_to_data_frame(stream, progress = TRUE),
    class = "delta_sharing_kernel_error"
  )
  expect_equal(native_collect_active(), 0)
})
