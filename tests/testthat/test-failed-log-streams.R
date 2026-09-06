test_that("failed stream constructors remove their prepared logs", {
  roots <- character()
  log <- function(...) {
    result <- prepare_log(function(log_dir) {
      writeLines("partial", fs::path(log_dir, log_commit_name))
      list(start_version = 0, end_version = 0)
    })
    roots <<- c(roots, result$root)
    result
  }
  testthat::local_mocked_bindings(
    prepare_snapshot_query_log = log,
    sharing_query_changes = function(...) NULL,
    prepare_cdf_query_log = log,
    native_snapshot_stream = function(...) stop("snapshot constructor failure"),
    native_cdf_stream = function(...) stop("CDF constructor failure"),
    .package = "delta.sharing"
  )
  table <- test_client()$table("sales.default.failed-log")
  expect_error(
    table$snapshot(response_format = "delta")$to_tibble(),
    "snapshot constructor failure",
    fixed = TRUE
  )
  expect_error(
    table$changes(starting_version = 0)$to_tibble(),
    "CDF constructor failure",
    fixed = TRUE
  )
  expect_length(roots, 2L)
  expect_false(any(fs::dir_exists(roots)))
})

test_that("interrupted log writing removes partial output", {
  root <- NULL
  condition <- tryCatch(
    prepare_log(function(log_dir) {
      root <<- fs::path_dir(fs::path_dir(log_dir))
      writeLines("partial", fs::path(log_dir, log_commit_name))
      stop(structure(
        list(message = "interrupted"),
        class = c("interrupt", "condition")
      ))
    }),
    interrupt = identity
  )
  expect_s3_class(condition, "interrupt")
  expect_false(fs::dir_exists(root))
})
