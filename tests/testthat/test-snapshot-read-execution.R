snapshot_identifier <- function() {
  sharing_table_identifier("sales.default.events")
}

snapshot_file_url <- function(name = "part-00000.parquet") {
  local_file_url(fs::path(fixture_table("local-table"), name))
}

snapshot_delta_actions <- function() {
  list(
    list(
      protocol = list(
        deltaProtocol = list(
          minReaderVersion = 3L,
          minWriterVersion = 7L
        )
      )
    ),
    list(
      metaData = list(
        deltaMetadata = list(
          id = "events",
          schemaString = "{\"type\":\"struct\",\"fields\":[]}",
          partitionColumns = list()
        )
      )
    ),
    list(
      file = list(
        deltaSingleAction = list(
          add = list(
            path = snapshot_file_url(),
            size = as.numeric(fs::file_size(
              local_file_path(snapshot_file_url())
            )),
            dataChange = TRUE,
            stats = "{\"numRecords\":10}"
          )
        )
      )
    )
  )
}

test_that("snapshot pages append to one private commit", {
  state <- new.env(parent = emptyenv())
  state$page <- 0L
  mock <- function(req) {
    state$page <- state$page + 1L
    if (state$page == 1L) {
      expect_null(req$body$data$pageToken)
      actions <- c(
        snapshot_delta_actions(),
        list(list(endStreamAction = list(nextPageToken = "page-two")))
      )
    } else {
      expect_equal(req$body$data$pageToken, "page-two")
      actions <- list(list(
        file = list(
          deltaSingleAction = list(
            add = list(
              path = snapshot_file_url("part-00001.parquet"),
              size = as.numeric(fs::file_size(
                local_file_path(snapshot_file_url("part-00001.parquet"))
              )),
              dataChange = TRUE,
              stats = "{\"numRecords\":7}",
              deletionVector = list(cardinality = 2)
            )
          )
        )
      ))
    }
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  }
  httr2::local_mocked_responses(mock)
  profile <- test_profile()
  log <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    snapshot_identifier(),
    list(
      predicate = NULL,
      limit = NULL,
      version = NULL,
      timestamp = NULL,
      cache = FALSE
    ),
    "delta"
  )
  withr::defer(log$cleanup())

  log_dir <- fs::path(log$path, "_delta_log")
  commit <- fs::path(log_dir, log_commit_name)
  lines <- readLines(commit)

  expect_equal(state$page, 2L)
  expect_identical(log$page_count, 2L)
  expect_identical(log$file_count, 2L)
  expect_identical(log$response_format, "delta")
  expect_setequal(fs::path_file(fs::dir_ls(log_dir)), log_commit_name)
  expect_length(lines, 4L)
  expect_equal(jsonlite::fromJSON(lines[[1L]])$protocol$minReaderVersion, 3L)
  expect_equal(
    httr2::url_parse(jsonlite::fromJSON(lines[[3L]])$add$path)$scheme,
    "file"
  )
  expect_equal(
    httr2::url_parse(jsonlite::fromJSON(lines[[4L]])$add$path)$scheme,
    "file"
  )
  expect_equal(length(fs::dir_ls(fs::path(log$path, "data"))), 2L)
})

test_that("parquet snapshot pages use the same preparation path", {
  actions <- list(
    list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
    list(
      metaData = list(
        id = "events",
        schemaString = "{\"type\":\"struct\",\"fields\":[]}",
        partitionColumns = list()
      )
    ),
    list(
      file = list(
        url = snapshot_file_url(),
        size = as.numeric(fs::file_size(
          local_file_path(snapshot_file_url())
        )),
        stats = "{\"numRecords\":4}"
      )
    )
  )
  httr2::local_mocked_responses(
    function(req) {
      httr2::response(200, body = charToRaw(ndjson_body(actions)))
    }
  )
  profile <- test_profile()
  log <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    snapshot_identifier(),
    list(
      predicate = NULL,
      limit = NULL,
      version = NULL,
      timestamp = NULL,
      cache = FALSE
    ),
    "parquet"
  )
  withr::defer(log$cleanup())

  lines <- readLines(fs::path(log$path, "_delta_log", log_commit_name))
  expect_identical(log$page_count, 1L)
  expect_identical(log$file_count, 1L)
  expect_identical(log$response_format, "parquet")
  expect_equal(
    httr2::url_parse(jsonlite::fromJSON(lines[[3L]])$add$path)$scheme,
    "file"
  )
})

test_that("a malformed later page removes incomplete snapshot staging", {
  roots_before <- as.character(fs::dir_ls(
    fs::path_temp(),
    regexp = paste0("/", log_root_prefix),
    type = "directory",
    fail = FALSE
  ))
  state <- new.env(parent = emptyenv())
  state$page <- 0L
  httr2::local_mocked_responses(function(req) {
    state$page <- state$page + 1L
    if (state$page == 1L) {
      actions <- c(
        snapshot_delta_actions(),
        list(list(endStreamAction = list(nextPageToken = "broken-page")))
      )
      return(httr2::response(
        200,
        body = charToRaw(ndjson_body(actions))
      ))
    }
    httr2::response(200, body = charToRaw("{not-json"))
  })
  profile <- test_profile()

  expect_error(
    prepare_snapshot_query_log(
      profile,
      sharing_auth_context(profile),
      snapshot_identifier(),
      list(
        predicate = NULL,
        limit = NULL,
        version = NULL,
        timestamp = NULL,
        cache = FALSE
      ),
      "delta"
    ),
    class = "delta_sharing_protocol_error"
  )

  roots_after <- as.character(fs::dir_ls(
    fs::path_temp(),
    regexp = paste0("/", log_root_prefix),
    type = "directory",
    fail = FALSE
  ))
  expect_setequal(roots_after, roots_before)
})

test_that("snapshot responses require protocol and metadata", {
  httr2::local_mocked_responses(function(req) {
    httr2::response(
      200,
      body = charToRaw(ndjson_body(list(list(
        endStreamAction = list(nextPageToken = "")
      )))
    ))
  })
  profile <- test_profile()

  expect_error(
    prepare_snapshot_query_log(
      profile,
      sharing_auth_context(profile),
      snapshot_identifier(),
      list(
        predicate = NULL,
        limit = NULL,
        version = NULL,
        timestamp = NULL,
        cache = FALSE
      ),
      "delta"
    ),
    class = "delta_sharing_protocol_error"
  )
})
