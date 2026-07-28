copy_native_cdf_fixture <- function(target) {
  source <- test_path("fixtures", "delta", "cdf")
  copy_directory <- function(from, to) {
    dir.create(to, recursive = TRUE, showWarnings = FALSE)
    entries <- list.files(
      from,
      all.files = TRUE,
      no.. = TRUE,
      full.names = TRUE
    )
    for (entry in entries) {
      destination <- file.path(to, basename(entry))
      if (dir.exists(entry)) {
        copy_directory(entry, destination)
      } else {
        file.copy(entry, destination, copy.mode = FALSE, copy.date = FALSE)
      }
    }
  }
  copy_directory(source, target)
  Sys.setFileTime(
    file.path(target, "_delta_log", "00000000000000000001.json"),
    as.POSIXct(1734480105.872, origin = "1970-01-01", tz = "UTC")
  )
  Sys.setFileTime(
    file.path(target, "_delta_log", "00000000000000000002.json"),
    as.POSIXct(1734480106.177, origin = "1970-01-01", tz = "UTC")
  )
  target
}

native_cdf_parse_actions <- function(lines) {
  decoder <- delta.sharing:::.new_ndjson_decoder("query_table_changes")
  actions <- delta.sharing:::.ndjson_decoder_push(
    decoder,
    charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
  )
  c(actions, delta.sharing:::.ndjson_decoder_finish(decoder))
}

native_cdf_guard <- function() {
  root <- tempfile(".delta-sharing-snapshot-", tmpdir = tempdir())
  dir.create(root, mode = "0700")
  writeLines(
    "delta-sharing-r:vnext",
    file.path(root, ".delta-sharing-r-prepared-log"),
    useBytes = TRUE
  )
  table <- file.path(root, "table")
  log_dir <- file.path(table, "_delta_log")
  dir.create(log_dir, recursive = TRUE)
  fixture <- test_path("fixtures", "delta", "cdf")
  log_entries <- c(
    "00000000000000000000.checkpoint.parquet",
    "_last_checkpoint",
    "00000000000000000001.json",
    "00000000000000000002.json"
  )
  expect_true(all(file.copy(
    file.path(fixture, "_delta_log", log_entries),
    file.path(log_dir, log_entries),
    copy.mode = FALSE,
    copy.date = FALSE
  )))
  for (version in 1:2) {
    commit <- file.path(log_dir, sprintf("%020d.json", version))
    lines <- readLines(commit, warn = FALSE)
    rewritten <- vapply(lines, function(line) {
      action <- jsonlite::fromJSON(line, simplifyVector = FALSE)
      file_type <- intersect(c("remove", "cdc"), names(action))
      if (length(file_type) == 1L) {
        local_path <- normalizePath(
          file.path(
            fixture,
            utils::URLdecode(action[[file_type]]$path)
          ),
          winslash = "/",
          mustWork = TRUE
        )
        action[[file_type]]$path <- paste0(
          "file://",
          utils::URLencode(
            local_path,
            reserved = FALSE,
            repeated = TRUE
          )
        )
      }
      jsonlite::toJSON(action, auto_unbox = TRUE, null = "null")
    }, character(1))
    writeLines(rewritten, commit, useBytes = TRUE)
  }
  Sys.setFileTime(
    file.path(log_dir, "00000000000000000001.json"),
    as.POSIXct(1734480105.872, origin = "1970-01-01", tz = "UTC")
  )
  Sys.setFileTime(
    file.path(log_dir, "00000000000000000002.json"),
    as.POSIXct(1734480106.177, origin = "1970-01-01", tz = "UTC")
  )
  guard <- delta.sharing:::.new_snapshot_log_guard(root, table, 4L)
  state <- guard$state
  state$read_kind <- "cdf"
  state$start_version <- 1
  state$end_version <- 2
  guard
}

test_that("native CDF preserves exact metadata-only projection and bounds", {
  guard <- native_cdf_guard()
  root <- guard$state$root
  stream <- delta.sharing:::.native_cdf_stream(
    guard,
    start_version = 1,
    end_version = 2,
    columns = c(
      "_change_type",
      "_commit_version",
      "_commit_timestamp"
    ),
    batch_size = 3L
  )
  expect_true(guard$state$released)
  expect_named(
    stream$get_schema()$children,
    c("_change_type", "_commit_version", "_commit_timestamp")
  )
  data <- delta.sharing:::.materialize_data_frame_stream(stream)

  expect_identical(
    names(data),
    c("_change_type", "_commit_version", "_commit_timestamp")
  )
  expect_setequal(unique(data$`_commit_version`), c(1, 2))
  expect_setequal(unique(data$`_change_type`), c("delete", "insert"))
  timestamps <- vapply(
    split(
      as.numeric(data$`_commit_timestamp`) * 1000,
      data$`_commit_version`
    ),
    function(value) unique(value),
    numeric(1)
  )
  expect_equal(unname(timestamps), c(1734480105872, 1734480106177))
  expect_false(file.exists(root))
})

test_that("native CDF validates inclusive bounds and redacts failures", {
  fixture <- copy_native_cdf_fixture(tempfile("native-cdf-"))
  on.exit(unlink(fixture, recursive = TRUE, force = TRUE), add = TRUE)

  stream <- delta.sharing:::.native_cdf_stream(
    fixture,
    start_version = 1,
    end_version = 1,
    columns = c("id", "_commit_version"),
    batch_size = 64L
  )
  data <- delta.sharing:::.materialize_data_frame_stream(stream)
  expect_true(nrow(data) > 0L)
  expect_identical(unique(data$`_commit_version`), 1)

  expect_error(
    delta.sharing:::.native_cdf_stream(
      fixture,
      start_version = 2,
      end_version = 1
    ),
    class = "delta_sharing_validation_error"
  )

  malformed <- tempfile("native-cdf-secret-")
  dir.create(file.path(malformed, "_delta_log"), recursive = TRUE)
  writeLines(
    "not valid Delta JSON",
    file.path(malformed, "_delta_log", "00000000000000000001.json"),
    useBytes = TRUE
  )
  on.exit(unlink(malformed, recursive = TRUE, force = TRUE), add = TRUE)
  condition <- expect_error(
    delta.sharing:::.native_cdf_stream(
      malformed,
      start_version = 1,
      end_version = 1
    ),
    "Delta Kernel CDF preparation failed"
  )
  expect_false(grepl(malformed, conditionMessage(condition), fixed = TRUE))
  expect_false(grepl("secret", conditionMessage(condition), fixed = TRUE))
})

test_that("public native ownership supports an exact zero-start CDF log", {
  data_path <- normalizePath(
    test_path("fixtures", "delta", "local-table", "part-00000.parquet"),
    winslash = "/",
    mustWork = TRUE
  )
  data_url <- paste0(
    "file://",
    utils::URLencode(data_path, reserved = FALSE, repeated = TRUE)
  )
  schema <- jsonlite::toJSON(
    list(
      type = "struct",
      fields = list(
        list(
          name = "id",
          type = "long",
          nullable = FALSE,
          metadata = structure(list(), names = character())
        ),
        list(
          name = "group",
          type = "string",
          nullable = TRUE,
          metadata = structure(list(), names = character())
        ),
        list(
          name = "value",
          type = "double",
          nullable = TRUE,
          metadata = structure(list(), names = character())
        ),
        list(
          name = "active",
          type = "boolean",
          nullable = FALSE,
          metadata = structure(list(), names = character())
        )
      )
    ),
    auto_unbox = TRUE
  )
  actions <- native_cdf_parse_actions(vapply(
    list(
      list(protocol = list(
        version = 0,
        deltaProtocol = list(
          minReaderVersion = 1,
          minWriterVersion = 7,
          writerFeatures = list("changeDataFeed")
        )
      )),
      list(metaData = list(
        version = 0,
        deltaMetadata = list(
          id = "zero-start",
          format = list(
            provider = "parquet",
            options = structure(list(), names = character())
          ),
          schemaString = schema,
          partitionColumns = list(),
          configuration = list(delta.enableChangeDataFeed = "true")
        )
      )),
      list(file = list(
        id = "zero-add",
        version = 0,
        timestamp = 1734480100000,
        deltaSingleAction = list(add = list(
          path = "https://storage.example.test/zero-start.parquet",
          partitionValues = structure(list(), names = character()),
          size = unname(file.info(data_path)$size),
          modificationTime = 1734480100000,
          dataChange = TRUE
        ))
      ))
    ),
    jsonlite::toJSON,
    character(1),
    auto_unbox = TRUE,
    null = "null"
  ))
  guard <- delta.sharing:::.prepare_cdf_log(
    protocol = actions[[1L]]$value,
    metadata = actions[[2L]]$value,
    files = list(actions[[3L]]$value),
    start_version = 0,
    end_version = 0
  )
  root <- guard$state$root
  expect_identical(
    list.files(file.path(guard$state$table_path, "_delta_log")),
    "00000000000000000000.json"
  )
  commit <- file.path(
    guard$state$table_path,
    "_delta_log",
    "00000000000000000000.json"
  )
  lines <- readLines(commit, warn = FALSE)
  rewritten <- vapply(lines, function(line) {
    action <- jsonlite::fromJSON(line, simplifyVector = FALSE)
    if ("add" %in% names(action)) {
      action$add$path <- data_url
    }
    jsonlite::toJSON(action, auto_unbox = TRUE, null = "null")
  }, character(1))
  original_mtime <- file.info(commit)$mtime
  writeLines(rewritten, commit, useBytes = TRUE)
  Sys.setFileTime(commit, original_mtime)

  stream <- delta.sharing:::.native_cdf_stream(
    guard,
    start_version = 0,
    end_version = 0,
    columns = c("id", "_change_type", "_commit_version"),
    batch_size = 64L
  )
  data <- delta.sharing:::.materialize_data_frame_stream(stream)
  expect_true(nrow(data) > 0L)
  expect_identical(unique(data$`_change_type`), "insert")
  expect_identical(unique(data$`_commit_version`), 0)
  expect_false(file.exists(root))
})

test_that("native CDF rejects invalid paths, bounds, projections, and guards", {
  for (table_location in list(NULL, 1, "s3://private-bucket/table")) {
    expect_error(
      delta.sharing:::.native_cdf_stream(
        table_location,
        start_version = 1,
        end_version = 2
      ),
      class = "delta_sharing_validation_error"
    )
  }
  expect_error(
    delta.sharing:::.native_cdf_stream(
      tempfile("missing-cdf-table-"),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_validation_error"
  )

  fixture <- copy_native_cdf_fixture(tempfile("native-cdf-validation-"))
  on.exit(unlink(fixture, recursive = TRUE, force = TRUE), add = TRUE)
  expect_error(
    delta.sharing:::.native_cdf_stream(
      fixture,
      start_version = 2,
      end_version = 1
    ),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.native_cdf_stream(
      fixture,
      start_version = 1,
      end_version = 2,
      columns = c("id", "ID")
    ),
    class = "delta_sharing_validation_error"
  )
  for (batch_size in list(0, 1000001, 1.5, NA_real_)) {
    expect_error(
      delta.sharing:::.native_cdf_stream(
        fixture,
        start_version = 1,
        end_version = 2,
        batch_size = batch_size
      ),
      class = "delta_sharing_validation_error"
    )
  }

  guard <- native_cdf_guard()
  on.exit(unlink(guard$state$root, recursive = TRUE, force = TRUE), add = TRUE)
  expect_error(
    delta.sharing:::.native_cdf_stream(
      guard,
      start_version = 1,
      end_version = 3
    ),
    class = "delta_sharing_validation_error"
  )
})
