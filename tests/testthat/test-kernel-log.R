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
  lines <- synthetic_log_lines("delta", proto, meta, files, "read")

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
  lines <- synthetic_log_lines("parquet", proto, meta, files, "read")

  add <- jsonlite::fromJSON(lines[[3]])
  expect_equal(add$add$path, "https://s/p1")
  expect_equal(add$add$size, 100)
  expect_equal(add$add$partitionValues$year, "2020")
  # empty configuration must serialize as an object, not an array
  metaline <- jsonlite::fromJSON(lines[[2]], simplifyVector = FALSE)
  expect_true(is.list(metaline$metaData$configuration))
})

test_that("prepare_synthetic_log writes the private ownership-marked layout", {
  lines <- c(
    '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}',
    '{"metaData":{"id":"t"}}'
  )
  log <- prepare_synthetic_log(lines)
  withr::defer(log$cleanup())

  # root is a private .delta-sharing-snapshot-* dir; table location is <root>/table
  expect_match(fs::path_file(log$root), "^\\.delta-sharing-snapshot-")
  expect_equal(fs::path_file(log$path), "table")

  # ownership marker the native cleanup guard checks
  marker <- fs::path(log$root, ".delta-sharing-r-prepared-log")
  expect_true(fs::file_exists(marker))
  expect_equal(readChar(marker, 100L), "delta-sharing-r:vnext\n")

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

test_that("prepare_log removes an incomplete log after a write failure", {
  root <- NULL
  withr::defer({
    if (!is.null(root) && fs::dir_exists(root)) {
      fs::dir_delete(root)
    }
  })

  expect_error(
    prepare_log(function(log_dir) {
      root <<- fs::path_dir(fs::path_dir(log_dir))
      stop("synthetic write failure")
    }),
    "synthetic write failure"
  )

  expect_false(fs::dir_exists(root))
})
