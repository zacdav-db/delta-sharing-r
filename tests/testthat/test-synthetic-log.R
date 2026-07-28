snapshot_fixture_components <- function() {
  path <- test_path("fixtures", "protocol", "snapshot-delta.ndjson")
  bytes <- readBin(path, what = "raw", n = file.info(path)$size)
  decoder <- delta.sharing:::.new_ndjson_decoder("snapshot fixture")
  actions <- c(
    delta.sharing:::.ndjson_decoder_push(decoder, bytes),
    delta.sharing:::.ndjson_decoder_finish(decoder)
  )
  list(
    protocol = actions[[1L]]$value,
    metadata = actions[[2L]]$value,
    files = lapply(actions[3:4], `[[`, "value")
  )
}

snapshot_commit_path <- function(guard) {
  file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log",
    "00000000000000000000.json"
  )
}

snapshot_read_lines <- function(guard) {
  readLines(snapshot_commit_path(guard), warn = FALSE, encoding = "UTF-8")
}

snapshot_condition_text <- function(condition) {
  paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )
}

test_that("Delta snapshot wrappers normalize into private file state", {
  components <- snapshot_fixture_components()
  add <- delta.sharing:::.snapshot_file_state(components$files[[2L]])
  rendered <- paste(
    capture.output(print(components$files[[2L]])),
    capture.output(str(components$files[[2L]])),
    collapse = "\n"
  )

  expect_identical(
    components$metadata$location,
    paste0(
      "s3://private-root/table?credential=",
      "storage-location-secret"
    )
  )
  expect_identical(
    components$metadata$auxiliary_locations,
    "s3://private-root/dv?credential=aux-location-secret"
  )
  expect_identical(add$id, "active-a")
  expect_identical(add$action_type, "add")
  expect_identical(add$expiration_timestamp, 1700003600000)
  expect_identical(
    delta.sharing:::.snapshot_file_expiration_timestamp(
      components$files[[2L]]
    ),
    1700003600000
  )
  expect_identical(
    add$delta_action$add$deletionVector$storageType,
    "p"
  )
  expect_false(grepl("add-url-secret", rendered, fixed = TRUE))
  expect_false(grepl("dv-url-secret", rendered, fixed = TRUE))
})

test_that("snapshot preparation mirrors the official Delta action mapping", {
  components <- snapshot_fixture_components()
  guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    rev(components$files)
  )
  on.exit(delta.sharing:::.release_snapshot_log(guard), add = TRUE)

  lines <- snapshot_read_lines(guard)
  actions <- lapply(
    lines,
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )

  expect_length(actions, 4L)
  expect_identical(
    actions[[1L]]$protocol$minReaderVersion,
    3L
  )
  expect_identical(
    unlist(actions[[1L]]$protocol$readerFeatures, use.names = FALSE),
    c("deletionVectors", "columnMapping")
  )
  expect_identical(actions[[2L]]$metaData$id, "snapshot-table")
  expect_identical(
    unlist(actions[[2L]]$metaData$partitionColumns, use.names = FALSE),
    "region"
  )
  expect_identical(names(actions[[3L]]), "add")
  expect_identical(names(actions[[4L]]), "remove")
  expect_identical(
    actions[[3L]]$add$path,
    "https://objects.example.test/current.parquet?sig=add-url-secret"
  )
  expect_identical(
    actions[[3L]]$add$deletionVector$pathOrInlineDv,
    "https://objects.example.test/current.dv?sig=dv-url-secret"
  )
  expect_identical(
    actions[[4L]]$remove$path,
    "https://objects.example.test/old.parquet?sig=remove-url-secret"
  )
  expect_false(any(
    c(
      "version",
      "size",
      "numFiles",
      "location",
      "auxiliaryLocations",
      "accessModes"
    ) %in%
      names(actions[[2L]]$metaData)
  ))
  expect_false(any(
    c(
      "id",
      "version",
      "timestamp",
      "expirationTimestamp",
      "deltaSingleAction"
    ) %in%
      names(actions[[3L]])
  ))
})

test_that("snapshot preparation is deterministic and publishes atomically", {
  components <- snapshot_fixture_components()
  first <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    components$files
  )
  second <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    rev(components$files)
  )
  on.exit(delta.sharing:::.release_snapshot_log(first), add = TRUE)
  on.exit(delta.sharing:::.release_snapshot_log(second), add = TRUE)

  expect_identical(
    readBin(
      snapshot_commit_path(first),
      what = "raw",
      n = file.info(snapshot_commit_path(first))$size
    ),
    readBin(
      snapshot_commit_path(second),
      what = "raw",
      n = file.info(snapshot_commit_path(second))$size
    )
  )
  expect_identical(
    list.files(
      delta.sharing:::.snapshot_log_path(first),
      all.files = TRUE,
      no.. = TRUE
    ),
    "_delta_log"
  )
  expect_identical(
    list.files(
      file.path(delta.sharing:::.snapshot_log_path(first), "_delta_log"),
      all.files = TRUE,
      no.. = TRUE
    ),
    "00000000000000000000.json"
  )
  expect_false(any(grepl(
    "staging",
    list.files(first$state$root, all.files = TRUE, no.. = TRUE),
    fixed = TRUE
  )))
  if (.Platform$OS.type != "windows") {
    expect_identical(
      as.integer(file.info(snapshot_commit_path(first))$mode),
      as.integer(as.octmode("0600"))
    )
  }
})

test_that("storage locations never enter the synthetic log or printable guard", {
  components <- snapshot_fixture_components()
  guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    components$files
  )
  on.exit(delta.sharing:::.release_snapshot_log(guard), add = TRUE)
  commit <- paste(snapshot_read_lines(guard), collapse = "\n")
  rendered <- paste(
    capture.output(print(guard)),
    capture.output(str(guard)),
    collapse = "\n"
  )

  expect_false(grepl("storage-location-secret", commit, fixed = TRUE))
  expect_false(grepl("aux-location-secret", commit, fixed = TRUE))
  for (secret in c(
    "storage-location-secret",
    "aux-location-secret",
    "add-url-secret",
    "remove-url-secret",
    "dv-url-secret"
  )) {
    expect_false(grepl(secret, rendered, fixed = TRUE))
  }
  expect_match(rendered, "active")
})

test_that("the Kernel invocation receives an encoded local file URI", {
  components <- snapshot_fixture_components()
  parent <- tempfile("snapshot parent ")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    components$files,
    temp_parent = parent
  )
  on.exit(delta.sharing:::.release_snapshot_log(guard), add = TRUE)
  uri <- delta.sharing:::.snapshot_log_uri(guard)

  expect_match(uri, "^file:///")
  expect_match(uri, "%20")
  expect_false(grepl(" ", uri, fixed = TRUE))
  expect_false(grepl("url-secret", uri, fixed = TRUE))
})

test_that("explicit release is deterministic and idempotent", {
  components <- snapshot_fixture_components()
  guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    components$files
  )
  root <- guard$state$root

  expect_true(dir.exists(root))
  expect_true(delta.sharing:::.release_snapshot_log(guard))
  expect_false(file.exists(root))
  expect_true(delta.sharing:::.release_snapshot_log(guard))
  expect_error(
    delta.sharing:::.snapshot_log_path(guard),
    "released",
    class = "delta_sharing_protocol_error"
  )
  expect_match(capture.output(print(guard)), "released")
})

test_that("failed release remains retryable and the finalizer stays armed", {
  components <- snapshot_fixture_components()
  guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    components$files
  )
  root <- guard$state$root
  cleanup_attempts <- 0L

  expect_error(
    delta.sharing:::.release_snapshot_log(
      guard,
      cleanup = function(path) {
        cleanup_attempts <<- cleanup_attempts + 1L
        FALSE
      }
    ),
    "could not be released",
    class = "delta_sharing_protocol_error"
  )
  expect_identical(cleanup_attempts, 1L)
  expect_false(guard$state$released)
  expect_true(dir.exists(root))
  expect_true(delta.sharing:::.release_snapshot_log(guard))
  expect_true(guard$state$released)
  expect_false(file.exists(root))

  finalizer_root <- local({
    finalizer_guard <- delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      components$files
    )
    root <- finalizer_guard$state$root
    expect_error(
      delta.sharing:::.release_snapshot_log(
        finalizer_guard,
        cleanup = function(path) FALSE
      ),
      class = "delta_sharing_protocol_error"
    )
    expect_false(finalizer_guard$state$released)
    root
  })
  gc()
  expect_false(file.exists(finalizer_root))
})

test_that("the lifetime finalizer removes an abandoned private root", {
  components <- snapshot_fixture_components()
  root <- local({
    guard <- delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      components$files
    )
    guard$state$root
  })

  gc()
  expect_false(file.exists(root))
})

test_that("a failed write removes every unpublished artifact", {
  components <- snapshot_fixture_components()
  parent <- tempfile("snapshot-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  secret <- "writer-error-must-not-leak"
  saw_unpublished <- FALSE

  condition <- expect_error(
    delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      components$files,
      temp_parent = parent,
      write_commit = function(path, lines) {
        root <- dirname(dirname(dirname(path)))
        saw_unpublished <<- !dir.exists(file.path(root, "table"))
        writeBin(charToRaw("partial signed url"), path)
        stop(secret)
      }
    ),
    class = "delta_sharing_protocol_error"
  )

  expect_true(saw_unpublished)
  expect_identical(
    list.files(parent, all.files = TRUE, no.. = TRUE),
    character()
  )
  expect_false(grepl(
    secret,
    snapshot_condition_text(condition),
    fixed = TRUE
  ))
})

test_that("a writer that omits the commit cannot publish a table", {
  components <- snapshot_fixture_components()
  parent <- tempfile("snapshot-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  expect_error(
    delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      components$files,
      temp_parent = parent,
      write_commit = function(path, lines) invisible(path)
    ),
    "not written",
    class = "delta_sharing_protocol_error"
  )
  expect_identical(
    list.files(parent, all.files = TRUE, no.. = TRUE),
    character()
  )
})

test_that("malformed paths, actions, and JSON fail without leaking input", {
  secret <- "malformed-secret-must-not-leak"
  valid <- jsonlite::fromJSON(
    paste0(
      "{\"id\":\"file-a\",\"deltaSingleAction\":{\"add\":{",
      "\"path\":\"https://objects.example.test/data?sig=",
      secret,
      "\",",
      "\"partitionValues\":{},\"size\":1,\"modificationTime\":1,",
      "\"dataChange\":true}}}"
    ),
    simplifyVector = FALSE
  )
  cases <- list(
    within(valid, {
      deltaSingleAction$add$path <- paste0(
        "http://objects.example.test/data?sig=",
        secret
      )
    }),
    within(valid, {
      deltaSingleAction$add$path <- paste0(
        "https://objects.example.test/data%ZZ?sig=",
        secret
      )
    }),
    within(valid, {
      deltaSingleAction$add$unknown <- secret
    }),
    within(valid, {
      deltaSingleAction$remove <- deltaSingleAction$add
    }),
    within(valid, {
      deltaSingleAction$add$stats <- paste0("{\"", secret, "\":")
    })
  )

  for (value in cases) {
    condition <- expect_error(
      delta.sharing:::.normalize_snapshot_file_action(value),
      class = "delta_sharing_protocol_error"
    )
    expect_false(grepl(
      secret,
      snapshot_condition_text(condition),
      fixed = TRUE
    ))
  }
})

test_that("deletion-vector adds require valid record-count statistics", {
  secret <- "dv-stats-secret-must-not-leak"
  value <- jsonlite::fromJSON(
    paste0(
      "{\"id\":\"file-a\",\"deltaSingleAction\":{\"add\":{",
      "\"path\":\"https://objects.example.test/data?sig=",
      secret,
      "\",",
      "\"partitionValues\":{},\"size\":1,\"modificationTime\":1,",
      "\"dataChange\":true,\"deletionVector\":{\"storageType\":\"i\",",
      "\"pathOrInlineDv\":\"inline-bitmap\",\"sizeInBytes\":10,",
      "\"cardinality\":1}}}}"
    ),
    simplifyVector = FALSE
  )

  missing <- expect_error(
    delta.sharing:::.normalize_snapshot_file_action(value),
    "require file statistics",
    class = "delta_sharing_protocol_error"
  )
  value$deltaSingleAction$add$stats <- "{\"notNumRecords\":1}"
  invalid <- expect_error(
    delta.sharing:::.normalize_snapshot_file_action(value),
    "numRecords",
    class = "delta_sharing_protocol_error"
  )

  expect_false(grepl(secret, snapshot_condition_text(missing), fixed = TRUE))
  expect_false(grepl(secret, snapshot_condition_text(invalid), fixed = TRUE))
})

test_that("snapshot scope rejects CDF and unresolved relative deletion vectors", {
  cdf <- jsonlite::fromJSON(
    paste0(
      "{\"id\":\"cdf-a\",\"deltaSingleAction\":{\"cdc\":{",
      "\"path\":\"https://objects.example.test/cdf.parquet\",",
      "\"partitionValues\":{},\"size\":1}}}"
    ),
    simplifyVector = FALSE
  )
  relative_dv <- jsonlite::fromJSON(
    paste0(
      "{\"id\":\"file-a\",\"deltaSingleAction\":{\"add\":{",
      "\"path\":\"https://objects.example.test/data.parquet\",",
      "\"partitionValues\":{},\"size\":1,\"modificationTime\":1,",
      "\"dataChange\":true,\"deletionVector\":{\"storageType\":\"u\",",
      "\"pathOrInlineDv\":\"encoded-relative-id\",\"offset\":1,",
      "\"sizeInBytes\":10,\"cardinality\":1}}}}"
    ),
    simplifyVector = FALSE
  )

  expect_error(
    delta.sharing:::.normalize_snapshot_file_action(cdf),
    class = "delta_sharing_unsupported_error"
  )
  expect_error(
    delta.sharing:::.normalize_snapshot_file_action(relative_dv),
    "storage type",
    class = "delta_sharing_protocol_error"
  )
})

test_that("empty snapshots retain schema in a two-action Delta commit", {
  components <- snapshot_fixture_components()
  guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    list()
  )
  on.exit(delta.sharing:::.release_snapshot_log(guard), add = TRUE)

  expect_length(snapshot_read_lines(guard), 2L)
  expect_match(capture.output(print(guard)), "0 file actions")
})

test_that("deletion vectors require matching Delta protocol features", {
  components <- snapshot_fixture_components()
  protocol <- components$protocol
  protocol$reader_features <- character()

  expect_error(
    delta.sharing:::.prepare_snapshot_log(
      protocol,
      components$metadata,
      components$files
    ),
    "inconsistent",
    class = "delta_sharing_protocol_error"
  )

  protocol <- components$protocol
  protocol$min_writer_version <- NULL
  expect_error(
    delta.sharing:::.prepare_snapshot_log(
      protocol,
      components$metadata,
      components$files
    ),
    "min_writer_version",
    class = "delta_sharing_protocol_error"
  )
})

test_that("invalid metadata is redacted as snapshot preparation failure", {
  components <- snapshot_fixture_components()
  secret <- "schema-content-must-not-leak"
  components$metadata$schema_string <- paste0(
    "{\"type\":\"",
    secret,
    "\",\"fields\":[]}"
  )

  condition <- expect_error(
    delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      list()
    ),
    "metadata schema",
    class = "delta_sharing_protocol_error"
  )

  expect_identical(condition$operation, "prepare_snapshot_log")
  expect_false(grepl(
    secret,
    snapshot_condition_text(condition),
    fixed = TRUE
  ))
})

test_that("duplicate and unvalidated file actions are rejected safely", {
  components <- snapshot_fixture_components()

  expect_error(
    delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      list(components$files[[1L]], components$files[[1L]])
    ),
    "duplicate",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      list(list(path = "https://objects.example.test/private"))
    ),
    class = "delta_sharing_protocol_error"
  )
})
