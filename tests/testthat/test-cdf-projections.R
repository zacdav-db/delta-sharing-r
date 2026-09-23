# Put partitions first so choosing the first user column cannot fix the scan.
partitioned_cdf_actions <- function(actions) {
  partitions <- list(region = "apac", country = "au")
  purrr::map(actions, function(action) {
    if (!is.null(action$metaData)) {
      metadata <- action$metaData$deltaMetadata
      schema <- jsonlite::fromJSON(
        metadata$schemaString,
        simplifyVector = FALSE
      )
      fields <- purrr::map(names(partitions), function(name) {
        list(
          name = name,
          type = "string",
          nullable = TRUE,
          metadata = as_json_map(NULL)
        )
      })
      schema$fields <- c(fields, schema$fields)
      metadata$schemaString <- log_json_line(schema)
      metadata$partitionColumns <- as.list(names(partitions))
      action$metaData$deltaMetadata <- metadata
    }
    if (!is.null(action$file)) {
      action$file$deltaSingleAction <- purrr::map(
        action$file$deltaSingleAction,
        function(file_action) {
          file_action$partitionValues <- partitions
          file_action
        }
      )
    }
    action
  })
}

# Reuse snapshot files to represent inserts followed by a whole-file deletion.
inferred_cdf_actions <- function() {
  actions <- local_snapshot_actions()
  actions[[1L]]$protocol$deltaProtocol$minWriterVersion <- 4L
  metadata <- actions[[2L]]$metaData$deltaMetadata
  metadata$configuration <- list(delta.enableChangeDataFeed = "true")
  actions[[2L]]$metaData <- list(version = 0L, deltaMetadata = metadata)
  for (index in c(3L, 4L)) {
    actions[[index]]$file$version <- 0L
    actions[[index]]$file$timestamp <- 1000
  }
  removed <- actions[[3L]]$file
  removed$version <- 1L
  removed$timestamp <- 2000
  add <- removed$deltaSingleAction$add
  removed$deltaSingleAction <- list(
    remove = list(
      path = add$path,
      size = add$size,
      partitionValues = add$partitionValues,
      dataChange = TRUE,
      extendedFileMetadata = TRUE
    )
  )
  c(actions, list(list(file = removed)))
}

test_that("CDF partition and metadata projections preserve inferred changes", {
  actions <- partitioned_cdf_actions(inferred_cdf_actions())
  httr2::local_mocked_responses(function(req) {
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  })
  table <- test_client()$table("tests.default.partitioned-cdf-projections")
  full <- table$changes(starting_version = 0, ending_version = 1)$to_tibble()
  expect_identical(nrow(full), 10L)
  expect_equal(sum(full$`_change_type` == "insert"), 7)
  expect_equal(sum(full$`_change_type` == "delete"), 3)
  expect_s3_class(full$id, "integer64")
  expect_s3_class(full$`_commit_version`, "integer64")
  expect_true(all(full$region == "apac" & full$country == "au"))

  for (columns in list(
    "region",
    "country",
    c("country", "region"),
    "_commit_version",
    "_commit_timestamp",
    "_change_type",
    c("region", "_change_type", "_commit_version"),
    c("_commit_version", "country", "_change_type", "region"),
    c("region", "id")
  )) {
    projected <- table$changes(
      starting_version = 0,
      ending_version = 1,
      columns = columns
    )$to_tibble(batch_size = 2L)
    expect_identical(names(projected), columns)
    expect_equal(projected, full[columns])
  }
})

test_that("CDF projections also preserve explicit change-data files", {
  actions <- partitioned_cdf_actions(local_cdf_actions())
  httr2::local_mocked_responses(function(req) {
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  })
  table <- test_client()$table("tests.default.explicit-cdf-projections")
  full <- table$changes(starting_version = 1, ending_version = 2)$to_tibble()
  expect_gt(nrow(full), 0L)
  expect_s3_class(full$id, "integer64")
  expect_s3_class(full$`_commit_version`, "integer64")
  expect_true(all(full$region == "apac" & full$country == "au"))

  for (columns in list(
    "region",
    c("country", "region"),
    "_commit_version",
    "_commit_timestamp",
    "_change_type",
    c("_commit_version", "country", "_change_type", "region"),
    c("region", "id")
  )) {
    projected <- table$changes(
      starting_version = 1,
      ending_version = 2,
      columns = columns
    )$to_tibble(batch_size = 2L)
    expect_identical(names(projected), columns)
    expect_equal(projected, full[columns])
  }
})
