run_interrupt_subprocess <- function(kind) {
  testthat::skip_if_not_installed("processx")
  script <- normalizePath(
    test_path("fixtures", "interrupt-subprocess.R"),
    winslash = "/",
    mustWork = TRUE
  )
  fixtures <- normalizePath(
    test_path("fixtures"),
    winslash = "/",
    mustWork = TRUE
  )
  directory <- tempfile("delta-sharing-interrupt-result-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE, force = TRUE), add = TRUE)
  ready <- file.path(directory, "ready")
  result <- file.path(directory, "result.rds")
  log <- file.path(directory, "child.log")
  libraries <- paste(.libPaths(), collapse = .Platform$path.sep)
  process <- processx::process$new(
    file.path(R.home("bin"), "Rscript"),
    args = c(script, kind, ready, result, fixtures),
    stdout = log,
    stderr = log,
    env = c(R_LIBS = libraries),
    cleanup = TRUE,
    cleanup_tree = TRUE
  )
  on.exit({
    if (process$is_alive()) {
      process$kill_tree()
    }
  }, add = TRUE)

  deadline <- Sys.time() + 20
  while (!file.exists(ready) && process$is_alive() && Sys.time() < deadline) {
    Sys.sleep(0.005)
  }
  if (!file.exists(ready)) {
    stop(
      paste(
        "Interrupt subprocess did not become ready:",
        paste(readLines(log, warn = FALSE), collapse = "\n")
      ),
      call. = FALSE
    )
  }

  # Let the child enter the native conversion or pull before delivering SIGINT.
  Sys.sleep(0.1)
  process$interrupt()
  process$wait(timeout = 5000)
  if (process$is_alive()) {
    stop("Interrupt subprocess did not stop promptly.", call. = FALSE)
  }
  exit_status <- process$get_exit_status()
  if (exit_status != 0L || !file.exists(result)) {
    stop(
      paste(
        "Interrupt subprocess failed:",
        paste(readLines(log, warn = FALSE), collapse = "\n")
      ),
      call. = FALSE
    )
  }
  readRDS(result)
}

test_that("a real R interrupt cancels synthetic, snapshot, and CDF streams", {
  skip_on_os("windows")

  for (kind in c("synthetic", "snapshot", "cdf")) {
    result <- run_interrupt_subprocess(kind)
    expect_true(
      "delta_sharing_cancelled" %in% result$classes,
      info = kind
    )
    expect_identical(
      result$message,
      "The Delta Sharing read was interrupted.",
      info = kind
    )
    expect_identical(result$operation, "read_data_frame", info = kind)
    expect_equal(result$active_delta, 0, info = kind)
    expect_equal(result$cancelled_delta, 1, info = kind)
    expect_equal(result$pending, 0, info = kind)
    expect_equal(result$final_active_delta, 0, info = kind)
    expect_equal(result$final_cancelled_delta, 1, info = kind)
    expect_false(result$root_exists, info = kind)
    expect_false(result$pointer_valid, info = kind)
    expect_lt(result$elapsed, 5)
    expect_false(grepl("file://|sig=|secret", result$message), info = kind)
  }
})

test_that("direct get_next maps a real interrupt to public cancellation", {
  skip_on_os("windows")

  result <- run_interrupt_subprocess("direct")
  expect_true("delta_sharing_cancelled" %in% result$classes)
  expect_identical(
    result$message,
    "The Delta Sharing read was interrupted."
  )
  expect_identical(result$operation, "read_arrow_stream")
  expect_equal(result$active_delta, 0)
  expect_equal(result$cancelled_delta, 1)
  expect_equal(result$final_cancelled_delta, 1)
  expect_false(result$pointer_valid)
  expect_lt(result$elapsed, 5)
})

test_that("read_arrow maps a real interrupt to public cancellation", {
  skip_on_os("windows")
  skip_if_not_installed("arrow")

  result <- run_interrupt_subprocess("arrow")
  expect_true("delta_sharing_cancelled" %in% result$classes)
  expect_identical(
    result$message,
    "The Delta Sharing read was interrupted."
  )
  expect_identical(result$operation, "read_arrow")
  expect_equal(result$active_delta, 0)
  expect_equal(result$cancelled_delta, 1)
  expect_equal(result$final_cancelled_delta, 1)
  expect_false(result$pointer_valid)
  expect_lt(result$elapsed, 5)
})

test_that("R interrupt mapping releases once and preserves other errors", {
  start <- delta.sharing:::.native_diagnostics()
  sentinel_stream <- delta.sharing:::.native_test_stream(batches = 10L)
  sentinel <- simpleError(
    paste(
      "nanoarrow pull failed:",
      delta.sharing:::.native_stream_interrupt_message
    )
  )
  condition <- expect_error(
    delta.sharing:::.with_native_stream_interrupt(
      stop(sentinel),
      operation = "read_arrow_stream",
      stream = sentinel_stream
    ),
    class = "delta_sharing_cancelled"
  )
  expect_identical(condition$operation, "read_arrow_stream")
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(sentinel_stream))

  r_interrupt_stream <- delta.sharing:::.native_test_stream(batches = 10L)
  r_interrupt <- structure(
    list(message = "test interrupt", call = NULL),
    class = c("interrupt", "condition")
  )
  condition <- expect_error(
    delta.sharing:::.with_native_stream_interrupt(
      signalCondition(r_interrupt),
      operation = "read_arrow",
      stream = r_interrupt_stream
    ),
    class = "delta_sharing_cancelled"
  )
  expect_identical(condition$operation, "read_arrow")
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(r_interrupt_stream))

  after <- delta.sharing:::.native_diagnostics()
  expect_equal(after$active_streams, start$active_streams)
  expect_equal(after$cancelled_streams, start$cancelled_streams + 2)
  expect_error(
    delta.sharing:::.with_native_stream_interrupt(
      stop("ordinary adapter failure"),
      operation = "read_data_frame"
    ),
    "ordinary adapter failure",
    fixed = TRUE
  )
})

test_that("interruptible streams preserve normal errors and returned arrays", {
  stream <- delta.sharing:::.native_test_stream(
    batches = 2L,
    error_after = 1L
  )
  first <- stream$get_next()
  expect_equal(first$length, 3L)
  condition <- expect_error(
    stream$get_next(),
    "synthetic reader error after 1 batches",
    fixed = TRUE
  )
  expect_false(inherits(condition, "delta_sharing_cancelled"))
  first_data <- nanoarrow::convert_array(
    first,
    to = nanoarrow::infer_nanoarrow_ptype(stream$get_schema())
  )
  expect_equal(nrow(first_data), 3L)
  stream$release()
})
