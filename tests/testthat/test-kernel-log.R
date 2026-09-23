test_that("delta format writes protocol, metadata, and verbatim actions", {
  proto <- list(
    deltaProtocol = list(minReaderVersion = 1L, minWriterVersion = 2L)
  )
  meta <- list(
    deltaMetadata = list(
      id = "t",
      schemaString = "{}",
      partitionColumns = list()
    )
  )
  files <- list(
    list(
      deltaSingleAction = list(
        add = list(path = "https://s/f1", size = 10, dataChange = TRUE)
      )
    ),
    list(
      deltaSingleAction = list(
        add = list(path = "https://s/f2", size = 20, dataChange = TRUE)
      )
    )
  )
  lines <- c(
    synthetic_log_header("delta", proto, meta, "read"),
    purrr::map_chr(
      files,
      \(file) log_json_line(synthetic_file_action(file, "delta", "read"))
    )
  )

  expect_length(lines, 4L)
  purrr::walk(lines, \(line) expect_no_error(jsonlite::fromJSON(line)))
  expect_equal(jsonlite::fromJSON(lines[[1]])$protocol$minReaderVersion, 1L)
  expect_equal(jsonlite::fromJSON(lines[[3]])$add$path, "https://s/f1")
})

test_that("parquet format synthesizes a flat add with object-valued maps", {
  proto <- list(minReaderVersion = 1L, minWriterVersion = 2L)
  meta <- list(
    id = "t",
    schemaString = "{\"type\":\"struct\",\"fields\":[]}",
    partitionColumns = list()
  )
  files <- list(
    list(
      file = list(
        url = "https://s/p1",
        id = "a",
        size = 100,
        partitionValues = list(year = "2020")
      )
    )
  )
  lines <- c(
    synthetic_log_header("parquet", proto, meta, "read"),
    purrr::map_chr(
      files,
      \(file) log_json_line(synthetic_file_action(file, "parquet", "read"))
    )
  )

  add <- jsonlite::fromJSON(lines[[3]])
  expect_equal(add$add$path, "https://s/p1")
  expect_equal(add$add$size, 100)
  expect_equal(add$add$partitionValues$year, "2020")
  # empty configuration must serialize as an object, not an array
  metaline <- jsonlite::fromJSON(lines[[2]], simplifyVector = FALSE)
  expect_true(is.list(metaline$metaData$configuration))
})

test_that("prepare_log writes the session-temporary layout", {
  lines <- c(
    '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
    '{"metaData":{"id":"t"}}'
  )
  log <- prepare_log(function(log_dir) {
    writeLines(lines, fs::path(log_dir, log_commit_name), useBytes = TRUE)
    invisible(NULL)
  })
  withr::defer(fs::dir_delete(log$root))

  # root is a .delta-sharing-snapshot-* dir; table location is <root>/table
  expect_match(fs::path_file(log$root), "^\\.delta-sharing-snapshot-")
  expect_equal(fs::path_file(log$path), "table")

  # the commit itself
  commit <- fs::path(
    log$path,
    "_delta_log",
    "00000000000000000000.json"
  )
  content <- readLines(commit)
  expect_length(content, 2L)
  expect_match(content[[1]], "protocol")
})

test_that("prepare_log removes failed work without replacing the error", {
  state <- new.env(parent = emptyenv())
  state$root <- NULL
  withr::defer({
    if (!is.null(state$root) && fs::dir_exists(state$root)) {
      fs::dir_delete(state$root)
    }
  })

  expect_error(
    prepare_log(function(log_dir) {
      state$root <- fs::path_dir(fs::path_dir(log_dir))
      writeLines("partial log", fs::path(log_dir, log_commit_name))
      stop("synthetic write failure")
    }),
    "synthetic write failure"
  )

  expect_false(fs::dir_exists(state$root))
})
