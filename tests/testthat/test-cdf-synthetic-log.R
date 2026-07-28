cdf_parse_actions <- function(lines) {
  decoder <- delta.sharing:::.new_ndjson_decoder("query_table_changes")
  actions <- delta.sharing:::.ndjson_decoder_push(
    decoder,
    charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
  )
  c(actions, delta.sharing:::.ndjson_decoder_finish(decoder))
}

cdf_log_components <- function(second_timestamp = 1734480106177) {
  schema <- paste0(
    "{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[",
    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",",
    "\\\"nullable\\\":true,\\\"metadata\\\":{}}]}"
  )
  actions <- cdf_parse_actions(c(
    paste0(
      '{"protocol":{"version":1,"deltaProtocol":',
      '{"minReaderVersion":1,"minWriterVersion":7,',
      '"writerFeatures":["changeDataFeed"]}}}'
    ),
    paste0(
      '{"metaData":{"version":1,"deltaMetadata":{',
      '"id":"table-id","format":{"provider":"parquet","options":{}},',
      '"schemaString":"', schema, '","partitionColumns":[],',
      '"configuration":{"delta.enableChangeDataFeed":"true"}}}}'
    ),
    paste0(
      '{"file":{"id":"one","version":1,"timestamp":1734480105872,',
      '"deltaSingleAction":{"remove":{',
      '"path":"https://storage.example/one.parquet",',
      '"deletionTimestamp":1734480105000,"dataChange":true,',
      '"extendedFileMetadata":true,"partitionValues":{},"size":10}}}}'
    ),
    paste0(
      '{"file":{"id":"two","version":2,"timestamp":',
      second_timestamp,
      ',"deltaSingleAction":{"cdc":{',
      '"path":"https://storage.example/two.parquet",',
      '"partitionValues":{},"size":20,"dataChange":false}}}}'
    )
  ))
  list(
    protocol = actions[[1L]]$value,
    metadata = actions[[2L]]$value,
    files = lapply(actions[3:4], `[[`, "value")
  )
}

test_that("bounded CDF logs retain provider versions and millisecond mtimes", {
  components <- cdf_log_components()
  checkpoint <- test_path(
    "fixtures",
    "delta",
    "cdf",
    "_delta_log",
    "00000000000000000000.checkpoint.parquet"
  )
  guard <- delta.sharing:::.prepare_cdf_log(
    protocol = components$protocol,
    metadata = components$metadata,
    files = components$files,
    start_version = 1,
    end_version = 2,
    checkpoint_asset = checkpoint
  )
  on.exit(delta.sharing:::.release_snapshot_log(guard), add = TRUE)

  log_dir <- file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log"
  )
  expect_setequal(
    list.files(log_dir),
    c(
      "_last_checkpoint",
      "00000000000000000000.checkpoint.parquet",
      "00000000000000000001.json",
      "00000000000000000002.json"
    )
  )
  expect_true(any(grepl(
    '"remove"',
    readLines(file.path(log_dir, "00000000000000000001.json")),
    fixed = TRUE
  )))
  expect_match(
    readLines(file.path(log_dir, "00000000000000000002.json")),
    '"cdc"',
    fixed = TRUE
  )
  mtimes <- file.info(file.path(
    log_dir,
    c("00000000000000000001.json", "00000000000000000002.json")
  ))$mtime
  expect_equal(
    as.double(mtimes) * 1000,
    c(1734480105872, 1734480106177),
    tolerance = 0.001
  )
  state <- delta.sharing:::.validate_snapshot_log_guard(guard)
  expect_identical(state$read_kind, "cdf")
  expect_identical(state$start_version, 1)
  expect_identical(state$end_version, 2)
})

test_that("CDF log preparation rejects unprovable wire metadata", {
  components <- cdf_log_components(second_timestamp = 1734480106178)
  state <- delta.sharing:::.snapshot_file_state(components$files[[2L]])
  components$files[[2L]] <- delta.sharing:::.new_private_snapshot_file(
    id = state$id,
    action_type = state$action_type,
    delta_action = state$delta_action,
    timestamp = state$timestamp
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = list(),
      historical_metadata = list(),
      files = components$files,
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )

  components <- cdf_log_components()
  duplicate <- components$files[[1L]]
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = list(),
      historical_metadata = list(),
      files = c(components$files, list(duplicate)),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )
})

test_that("CDF action planning rejects invalid ranges and action placement", {
  components <- cdf_log_components()
  call_plan <- function(...) {
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = list(),
      historical_metadata = list(),
      files = components$files,
      start_version = 1,
      end_version = 2,
      ...
    )
  }

  for (version in list(-1, 1.5, NA_real_, Inf, "1")) {
    expect_error(
      delta.sharing:::.cdf_whole_version(version, "test version"),
      class = "delta_sharing_protocol_error"
    )
  }
  expect_error(
    delta.sharing:::.cdf_validate_range(2, 1),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.cdf_validate_range(
      0,
      delta.sharing:::.cdf_log_max_versions
    ),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = list(),
      metadata = components$metadata,
      historical_protocols = list(),
      historical_metadata = list(),
      files = list(),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = list(),
      historical_protocols = list(),
      historical_metadata = list(),
      files = list(),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = NULL,
      historical_metadata = list(),
      files = list(),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = list(components$protocol),
      historical_metadata = list(),
      files = list(),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = list(),
      historical_metadata = list(components$metadata),
      files = list(),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = list(),
      historical_metadata = list(),
      files = list(components$files[[2L]]),
      start_version = 1,
      end_version = 1
    ),
    class = "delta_sharing_protocol_error"
  )

  state <- delta.sharing:::.snapshot_file_state(components$files[[2L]])
  conflicting <- delta.sharing:::.new_private_snapshot_file(
    id = "conflicting-file",
    action_type = state$action_type,
    delta_action = state$delta_action,
    expiration_timestamp = state$expiration_timestamp,
    version = 1,
    timestamp = 1734480105999
  )
  expect_error(
    delta.sharing:::.cdf_action_plan(
      protocol = components$protocol,
      metadata = components$metadata,
      historical_protocols = list(),
      historical_metadata = list(),
      files = list(components$files[[1L]], conflicting),
      start_version = 1,
      end_version = 2
    ),
    class = "delta_sharing_protocol_error"
  )
})

test_that("CDF log preparation fails closed on invalid control hooks", {
  components <- cdf_log_components()
  checkpoint <- test_path(
    "fixtures",
    "delta",
    "cdf",
    "_delta_log",
    "00000000000000000000.checkpoint.parquet"
  )
  base_args <- list(
    protocol = components$protocol,
    metadata = components$metadata,
    files = components$files,
    start_version = 1,
    end_version = 2,
    checkpoint_asset = checkpoint
  )

  expect_error(
    do.call(
      delta.sharing:::.prepare_cdf_log,
      c(base_args, list(write_commit = "not a function"))
    ),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    do.call(
      delta.sharing:::.prepare_cdf_log,
      utils::modifyList(base_args, list(checkpoint_asset = tempfile()))
    ),
    class = "delta_sharing_protocol_error"
  )
  not_a_directory <- tempfile("cdf-parent-file-")
  writeLines("file", not_a_directory)
  on.exit(unlink(not_a_directory), add = TRUE)
  expect_error(
    do.call(
      delta.sharing:::.prepare_cdf_log,
      c(base_args, list(temp_parent = not_a_directory))
    ),
    class = "delta_sharing_validation_error"
  )

  private_parent <- tempfile("cdf-failure-parent-")
  dir.create(private_parent)
  on.exit(unlink(private_parent, recursive = TRUE, force = TRUE), add = TRUE)
  expect_error(
    do.call(
      delta.sharing:::.prepare_cdf_log,
      c(base_args, list(
        temp_parent = private_parent,
        write_commit = function(path, lines) {
          stop("private writer failure")
        }
      ))
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_length(list.files(private_parent, all.files = TRUE, no.. = TRUE), 0L)

  expect_error(
    delta.sharing:::.cdf_secure_file(
      tempfile("missing-secure-cdf-"),
      "missing CDF file"
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.cdf_set_commit_timestamp(
      tempfile("missing-cdf-timestamp-"),
      1734480105000
    ),
    class = "delta_sharing_protocol_error"
  )
})
