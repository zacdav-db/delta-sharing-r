test_that("CDF requests include versioned metadata and CDF capabilities", {
  spec <- list(
    starting_version = 1,
    ending_version = 4,
    starting_timestamp = NULL,
    ending_timestamp = NULL
  )
  query <- changes_query(spec, "next")

  expect_equal(query$startingVersion, 1)
  expect_equal(query$endingVersion, 4)
  expect_equal(query$includeHistoricalMetadata, "true")
  expect_equal(query$pageToken, "next")

  capabilities <- query_capabilities("delta", for_cdf = TRUE)
  expect_match(capabilities, "responseformat=delta", fixed = TRUE)
  expect_match(capabilities, "deletionvectors,columnmapping", fixed = TRUE)
  expect_false(grepl("timestampntz", capabilities, fixed = TRUE))
})

test_that("CDF requests support open version and timestamp ranges", {
  open_versions <- changes_query(
    list(
      starting_version = 3,
      ending_version = NULL,
      starting_timestamp = NULL,
      ending_timestamp = NULL
    ),
    NULL
  )
  expect_equal(open_versions$startingVersion, 3)
  expect_null(open_versions$endingVersion)

  start <- as.POSIXct("2026-07-01 00:00:00", tz = "UTC")
  end <- as.POSIXct("2026-07-02 00:00:00", tz = "UTC")
  timestamps <- changes_query(
    list(
      starting_version = NULL,
      ending_version = NULL,
      starting_timestamp = start,
      ending_timestamp = end
    ),
    NULL
  )
  expect_equal(timestamps$startingTimestamp, format_timestamp(start))
  expect_equal(timestamps$endingTimestamp, format_timestamp(end))
  expect_null(timestamps$startingVersion)
})

test_that("CDF actions retain versioned metadata and response bounds", {
  actions <- list(
    list(protocol = list(deltaProtocol = list(minReaderVersion = 3))),
    list(metaData = list(deltaMetadata = list(id = "table"))),
    list(
      file = list(
        version = 1,
        timestamp = 1000,
        deltaSingleAction = list(add = list(path = "one.parquet"))
      )
    ),
    list(
      file = list(
        version = 3,
        timestamp = 3000,
        deltaSingleAction = list(cdc = list(path = "three.parquet"))
      )
    ),
    list(
      metaData = list(
        version = 4,
        deltaMetadata = list(id = "table", name = "renamed")
      )
    )
  )

  parsed <- bucket_cdf_actions(actions, 1, 4)

  expect_equal(parsed$start_version, 1)
  expect_equal(parsed$end_version, 4)
  expect_setequal(names(parsed$by_version), c("1", "3", "4"))
  expect_equal(parsed$by_version[["1"]]$actions[[1]]$metaData$id, "table")
  expect_equal(
    parsed$by_version[["4"]]$actions[[1]]$metaData$name,
    "renamed"
  )
})

test_that("CDF effective end contracts to the last returned version", {
  actions <- list(
    list(protocol = list(deltaProtocol = list(minReaderVersion = 1))),
    list(metaData = list(deltaMetadata = list(id = "table"))),
    list(
      file = list(
        version = 3,
        timestamp = 3000,
        deltaSingleAction = list(add = list(path = "three.parquet"))
      )
    )
  )

  parsed <- bucket_cdf_actions(actions, 1, 4)

  expect_equal(parsed$start_version, 1)
  expect_equal(parsed$end_version, 3)
})

test_that("CDF response versions define timestamp-bounded log bounds", {
  actions <- list(
    list(protocol = list(deltaProtocol = list(minReaderVersion = 1))),
    list(
      metaData = list(
        version = 2,
        deltaMetadata = list(id = "table")
      )
    ),
    list(
      file = list(
        version = 3,
        timestamp = 3000,
        deltaSingleAction = list(add = list(path = "three.parquet"))
      )
    )
  )

  parsed <- bucket_cdf_actions(actions, NULL, NULL)

  expect_equal(parsed$start_version, 2)
  expect_equal(parsed$end_version, 3)
})

test_that("timestamp-bounded CDF metadata must identify its version", {
  actions <- list(
    list(protocol = list(deltaProtocol = list(minReaderVersion = 1))),
    list(metaData = list(deltaMetadata = list(id = "table")))
  )

  expect_error(
    bucket_cdf_actions(actions, NULL, NULL),
    class = "delta_sharing_protocol_error"
  )
})

test_that("CDF responses cannot widen the requested range", {
  actions <- list(
    list(protocol = list(deltaProtocol = list(minReaderVersion = 1))),
    list(metaData = list(deltaMetadata = list(id = "table"))),
    list(
      file = list(
        version = 5,
        timestamp = 5000,
        deltaSingleAction = list(add = list(path = "five.parquet"))
      )
    )
  )

  expect_error(
    bucket_cdf_actions(actions, 1, 4),
    class = "delta_sharing_protocol_error"
  )
})

test_that("prepared CDF logs span the effective response range", {
  protocol <- list(minReaderVersion = 1, minWriterVersion = 2)
  by_version <- list(
    "1" = list(
      version = 1,
      timestamp_ms = 1000,
      actions = list(list(metaData = list(id = "table")))
    ),
    "3" = list(
      version = 3,
      timestamp_ms = 3000,
      actions = list(list(add = list(path = "three.parquet")))
    )
  )

  log <- prepare_cdf_log(protocol, by_version, 1, 3)
  withr::defer(log$cleanup())
  log_dir <- fs::path(log$path, "_delta_log")
  entries <- fs::dir_ls(log_dir, type = "file") |>
    fs::path_file() |>
    as.character()

  expect_equal(log$start_version, 1)
  expect_equal(log$end_version, 3)
  expect_setequal(
    entries,
    c(
      "00000000000000000000.checkpoint.parquet",
      "_last_checkpoint",
      "00000000000000000001.json",
      "00000000000000000002.json",
      "00000000000000000003.json"
    )
  )
  expect_length(
    readLines(fs::path(log_dir, cdf_commit_name(2))),
    0L
  )
  expect_equal(
    readLines(fs::path(log_dir, cdf_commit_name(1))),
    c(
      log_json_line(list(protocol = protocol)),
      log_json_line(list(metaData = list(id = "table")))
    )
  )
  expect_equal(
    readLines(fs::path(log_dir, cdf_commit_name(3))),
    '{"add":{"path":"three.parquet"}}'
  )

  commit_times <- fs::file_info(
    fs::path(log_dir, cdf_commit_name(c(1, 3)))
  )
  expect_equal(
    as.numeric(commit_times$modification_time),
    c(1, 3),
    tolerance = 0.01
  )
})

test_that("CDF action buckets require protocol and represented versions", {
  expect_error(
    bucket_cdf_actions(
      list(list(metaData = list(deltaMetadata = list(id = "table")))),
      1,
      2
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    bucket_cdf_actions(
      list(list(protocol = list(deltaProtocol = list(minReaderVersion = 1)))),
      1,
      2
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_null(find_next_page_token(list(list(protocol = list()))))
})
