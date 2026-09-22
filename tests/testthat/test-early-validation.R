test_that("invalid batch sizes fail before requests for every materializer", {
  httr2::local_mocked_responses(function(req) {
    stop("Unexpected HTTP request for invalid options")
  })
  table <- test_client()$table("sales.default.validation")
  readers <- list(table$snapshot(), table$changes(starting_version = 0))
  methods <- c(
    "to_arrow_stream", "to_arrow_reader", "to_arrow", "to_tibble", "to_data_frame"
  )
  for (batch in list(0, -1, 1.5, NA, Inf, TRUE, "1", 1000001, numeric(), c(1, 2))) {
    for (reader in readers) {
      for (method in methods) {
        expect_error(
          reader[[method]](batch_size = batch),
          class = "delta_sharing_validation_error"
        )
      }
    }
  }
  expect_length(fs::dir_ls(table$cache_path, all = TRUE), 0L)
})

test_that("invalid projection shapes fail before requests", {
  httr2::local_mocked_responses(function(req) {
    stop("Unexpected HTTP request for invalid options")
  })
  table <- test_client()$table("sales.default.invalid-columns")
  for (columns in list(1, list("id"), character(), NA_character_, "", c("id", "ID"))) {
    expect_error(
      table$snapshot(columns = columns),
      class = "delta_sharing_validation_error"
    )
    expect_error(
      table$changes(columns = columns),
      class = "delta_sharing_validation_error"
    )
  }
  # Column existence is checked against the schema at read time, not here.
  expect_s3_class(table$snapshot(columns = "unknown"), "SharingSnapshot")
  expect_s3_class(table$changes(columns = "unknown"), "SharingChanges")
})

test_that("zero-row snapshots preserve projected types without downloading", {
  missing <- local_file_url(fs::path(withr::local_tempdir(), "missing.parquet"))
  actions <- purrr::map(local_snapshot_actions(), function(action) {
    if (!is.null(action$file)) {
      action$file$deltaSingleAction$add$path <- missing
    }
    action
  })
  # Both wire formats describe the same schema and unavailable data files.
  parquet_actions <- purrr::map(actions, function(action) {
    if (!is.null(action$protocol)) {
      return(list(protocol = action$protocol$deltaProtocol))
    }
    if (!is.null(action$metaData)) {
      return(list(metaData = action$metaData$deltaMetadata))
    }
    file <- action$file
    list(file = list(id = file$id, size = file$size, url = missing))
  })
  responses <- list(delta = actions, parquet = parquet_actions)
  state <- new.env(parent = emptyenv())
  state$requests <- 0L
  httr2::local_mocked_responses(function(req) {
    state$requests <- state$requests + 1L
    expect_equal(req$body$data$limitHint, 0)
    expect_equal(req$body$data$version, 0)
    httr2::response(200, body = charToRaw(ndjson_body(responses[[format]])))
  })

  for (format in names(responses)) {
    table <- test_client()$table(paste0("sales.default.zero-rows-", format))
    for (method in c("to_tibble", "to_data_frame")) {
      result <- table$snapshot(
        version = 0,
        limit = 0,
        columns = c("group", "id"),
        response_format = format
      )[[method]]()
      expect_equal(nrow(result), 0L)
      expect_identical(names(result), c("group", "id"))
      expect_identical(result$group, character())
      expect_identical(result$id, bit64::integer64())
      expect_identical(inherits(result, "tbl_df"), method == "to_tibble")
    }
    expect_length(fs::dir_ls(table$cache_path, all = TRUE), 0L)
  }
  expect_identical(state$requests, 4L)
})

test_that("zero-row reads still report sharing query failures", {
  httr2::local_mocked_responses(list(httr2::response(
    404,
    body = charToRaw('{"errorCode":"RESOURCE_DOES_NOT_EXIST","message":"Missing table"}')
  )))
  table <- test_client()$table("sales.default.missing-table")
  expect_error(
    table$snapshot(limit = 0, response_format = "delta")$to_tibble(),
    class = "httr2_http_404"
  )
})
