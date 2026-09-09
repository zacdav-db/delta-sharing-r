test_that("invalid batch sizes and projections fail before requests", {
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    httr2::response(
      200,
      body = charToRaw(ndjson_body(local_snapshot_actions()))
    )
  })
  table <- test_client()$table("sales.default.validation")
  for (batch in list(0, 1.5, NA, Inf, TRUE, 1000001, numeric())) {
    for (reader in list(
      table$snapshot(),
      table$changes(starting_version = 0)
    )) {
      expect_error(
        reader$to_tibble(batch_size = batch),
        class = "delta_sharing_validation_error"
      )
    }
  }
  for (columns in list(character(), NA_character_, "", c("id", "ID"))) {
    expect_error(
      table$snapshot(columns = columns),
      class = "delta_sharing_validation_error"
    )
    expect_error(
      table$changes(columns = columns),
      class = "delta_sharing_validation_error"
    )
  }
  expect_equal(requests, 0L)
})

test_that("zero-row snapshots preserve schema without downloading assets", {
  requests <- 0L
  httr2::local_mocked_responses(function(req) {
    requests <<- requests + 1L
    actions <- local_snapshot_actions()
    # A correct zero-row read never opens either of these unavailable files.
    for (i in 3:4) {
      actions[[i]]$file$deltaSingleAction$add$path <- local_file_url(
        fs::path(tempdir(), "unavailable-zero-row-file.parquet")
      )
    }
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  })
  table <- test_client()$table("sales.default.zero-rows")
  result <- table$snapshot(
    limit = 0,
    columns = c("group", "id"),
    response_format = "delta"
  )$to_tibble()
  expect_equal(nrow(result), 0L)
  expect_identical(names(result), c("group", "id"))
  expect_type(result$group, "character")
  expect_type(result$id, "double")
  expect_equal(requests, 1L)
  expect_length(fs::dir_ls(table$cache_path, all = TRUE), 0L)
})
