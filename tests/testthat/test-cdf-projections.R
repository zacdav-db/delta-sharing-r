partitioned_cdf_actions <- function() {
  actions <- local_snapshot_actions()
  actions[[1L]]$protocol$deltaProtocol$minWriterVersion <- 4L
  metadata <- actions[[2L]]$metaData$deltaMetadata
  schema <- jsonlite::fromJSON(metadata$schemaString, simplifyVector = FALSE)
  # Put the partition first so selecting the first user column is insufficient.
  schema$fields <- c(
    list(list(
      name = "region",
      type = "string",
      nullable = TRUE,
      metadata = as_json_map(NULL)
    )),
    schema$fields
  )
  metadata$schemaString <- log_json_line(schema)
  metadata$partitionColumns <- list("region")
  metadata$configuration <- list(delta.enableChangeDataFeed = "true")
  actions[[2L]]$metaData <- list(version = 0L, deltaMetadata = metadata)
  for (index in c(3L, 4L)) {
    actions[[index]]$file$version <- 0L
    actions[[index]]$file$timestamp <- 1000
    actions[[index]]$file$deltaSingleAction$add$partitionValues <- list(
      region = "apac"
    )
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
  actions <- partitioned_cdf_actions()
  httr2::local_mocked_responses(function(req) {
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  })
  table <- test_client()$table("tests.default.partitioned-cdf-projections")
  full <- table$changes(starting_version = 0, ending_version = 1)$to_tibble()
  expect_identical(nrow(full), 10L)
  expect_equal(sum(full$`_change_type` == "insert"), 7)
  expect_equal(sum(full$`_change_type` == "delete"), 3)

  for (columns in list(
    "region",
    "_commit_version",
    "_commit_timestamp",
    "_change_type",
    c("region", "_change_type", "_commit_version"),
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
