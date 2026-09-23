test_that("only failed snapshot and CDF preparation removes private logs", {
  state <- new.env(parent = emptyenv())
  state$roots <- character()
  state$failure <- NULL
  withr::defer(purrr::walk(
    state$roots[fs::dir_exists(state$roots)],
    fs::dir_delete
  ))

  # Track real log preparation; inject failures only at the native boundary.
  prepare <- prepare_log
  snapshot <- native_snapshot_stream
  cdf <- native_cdf_stream
  testthat::local_mocked_bindings(
    prepare_log = function(write) {
      result <- prepare(write)
      state$roots <- c(state$roots, result$root)
      result
    },
    native_snapshot_stream = function(...) {
      if (!is.null(state$failure)) stop(state$failure)
      snapshot(...)
    },
    native_cdf_stream = function(...) {
      if (!is.null(state$failure)) stop(state$failure)
      cdf(...)
    },
    .package = "delta.sharing"
  )
  actions <- list(
    snapshot = local_snapshot_actions(),
    cdf = local_cdf_actions()
  )
  httr2::local_mocked_responses(function(req) {
    kind <- if (endsWith(httr2::url_parse(req$url)$path, "/changes")) {
      "cdf"
    } else {
      "snapshot"
    }
    httr2::response(200, body = charToRaw(ndjson_body(actions[[kind]])))
  })
  table <- test_client()$table("sales.default.failed-log")
  withr::defer(fs::dir_delete(table$cache_path))
  cached <- fs::path(table$cache_path, "existing.parquet")
  writeBin(charToRaw("cached"), cached)
  reads <- list(
    table$snapshot(response_format = "delta"),
    table$changes(starting_version = 1, ending_version = 2)
  )
  failures <- list(
    simpleError("native constructor failure"),
    structure(
      list(message = "interrupted"),
      class = c("interrupt", "condition")
    )
  )
  for (failure in failures) {
    state$failure <- failure
    for (read in reads) {
      caught <- tryCatch(
        read$to_arrow_reader(),
        error = identity,
        interrupt = identity
      )
      expect_identical(caught, failure)
      expect_false(fs::dir_exists(tail(state$roots, 1L)))
      expect_identical(readBin(cached, "raw", 6L), charToRaw("cached"))
    }
  }
  expect_length(state$roots, 4L)

  # Retrying can use the same cache, and successful readers keep their logs.
  cached_files <- fs::dir_ls(table$cache_path)
  expect_gt(length(cached_files), 1L)
  state$failure <- NULL
  # Also exercise genuine Kernel constructor failures, not just injected ones.
  invalid_reads <- list(
    table$snapshot(columns = "missing", response_format = "delta"),
    table$changes(starting_version = 1, ending_version = 2, columns = "missing")
  )
  for (read in invalid_reads) {
    expect_error(read$to_arrow_reader(), "projection validation failed")
    expect_false(fs::dir_exists(tail(state$roots, 1L)))
  }
  rows <- purrr::map_dbl(reads, function(read) {
    reader <- read$to_arrow_reader(batch_size = 2L)
    withr::defer(reader$Close())
    expect_true(fs::dir_exists(tail(state$roots, 1L)))
    nrow(reader$read_table())
  })
  expect_equal(rows[[1L]], 7L)
  expect_gt(rows[[2L]], 0L)
  expect_true(all(fs::dir_exists(tail(state$roots, 2L))))
  expect_identical(fs::dir_ls(table$cache_path), cached_files)
})

test_that("cleanup failures preserve the original error or interrupt", {
  root <- withr::local_tempdir()
  testthat::local_mocked_bindings(
    dir_delete = function(...) stop("cleanup failure"),
    .package = "fs"
  )
  failures <- list(
    simpleError("original failure"),
    structure(
      list(message = "interrupted"),
      class = c("interrupt", "condition")
    )
  )
  for (failure in failures) {
    caught <- tryCatch(
      with_failed_log_cleanup(root, stop(failure)),
      error = identity,
      interrupt = identity
    )
    expect_identical(caught, failure)
  }
  expect_true(fs::dir_exists(root))
})

test_that("interrupted log writing removes partial output", {
  state <- new.env(parent = emptyenv())
  state$root <- NULL
  withr::defer({
    if (!is.null(state$root) && fs::dir_exists(state$root)) {
      fs::dir_delete(state$root)
    }
  })
  interrupt <- structure(
    list(message = "interrupted"),
    class = c("interrupt", "condition")
  )
  condition <- tryCatch(
    prepare_log(function(log_dir) {
      state$root <- fs::path_dir(fs::path_dir(log_dir))
      writeLines("partial", fs::path(log_dir, log_commit_name))
      stop(interrupt)
    }),
    interrupt = identity
  )
  expect_identical(condition, interrupt)
  expect_false(fs::dir_exists(state$root))
})
