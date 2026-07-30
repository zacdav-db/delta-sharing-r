snapshot_identifier <- function() {
  sharing_table_identifier("sales.default.events")
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
            path = "https://storage.example.test/one.parquet",
            size = 100,
            dataChange = TRUE,
            stats = "{\"numRecords\":10}"
          )
        )
      )
    )
  )
}

streaming_test_response <- function(text) {
  bytes <- charToRaw(text)
  position <- 1L
  open <- TRUE
  body <- new.env(parent = emptyenv())
  body$read <- function(size) {
    if (!open || position > length(bytes)) {
      return(raw())
    }
    end <- min(position + size - 1L, length(bytes))
    value <- bytes[seq.int(position, end)]
    position <<- end + 1L
    value
  }
  body$is_complete <- function() position > length(bytes)
  body$is_open <- function() open
  body$close <- function() {
    open <<- FALSE
    invisible(NULL)
  }
  class(body) <- c("StreamingBody", "R6")

  resp <- httr2::response(
    200,
    headers = list(`content-type` = "application/x-ndjson")
  )
  resp$body <- body
  resp
}

test_that("streaming responses deliver bounded groups of lines to R", {
  body <- paste(sprintf('{"value":%d}', 1:5), collapse = "\n")
  httr2::local_mocked_responses(
    function(req) httr2::response(200, body = charToRaw(body))
  )
  req <- sharing_request(
    test_profile(),
    sharing_auth_context(test_profile()),
    "stream",
    operation = "read"
  )
  chunks <- list()

  sharing_stream_lines(
    req,
    function(lines) {
      chunks[[length(chunks) + 1L]] <<- lines
    },
    lines_per_chunk = 2L
  )

  expect_equal(purrr::list_c(chunks), strsplit(body, "\n", fixed = TRUE)[[1L]])
  expect_true(all(purrr::map_int(chunks, length) <= 2L))
})

test_that("streaming responses exercise httr2's connection-body path", {
  body <- paste(sprintf('{"value":%d}', 1:5), collapse = "\n")
  httr2::local_mocked_responses(
    function(req) streaming_test_response(body)
  )
  req <- sharing_request(
    test_profile(),
    sharing_auth_context(test_profile()),
    "stream",
    operation = "read"
  )
  chunks <- list()

  sharing_stream_lines(
    req,
    function(lines) {
      chunks[[length(chunks) + 1L]] <<- lines
    },
    lines_per_chunk = 2L
  )

  expect_equal(purrr::list_c(chunks), strsplit(body, "\n", fixed = TRUE)[[1L]])
  expect_true(all(purrr::map_int(chunks, length) <= 2L))
})

test_that("streaming responses reject an oversized NDJSON line", {
  body <- '{"value":"too-large"}'
  httr2::local_mocked_responses(
    function(req) streaming_test_response(body)
  )
  req <- sharing_request(
    test_profile(),
    sharing_auth_context(test_profile()),
    "stream",
    operation = "read"
  )

  expect_error(
    sharing_stream_lines(
      req,
      function(lines) invisible(lines),
      max_line_bytes = 8L
    ),
    class = "delta_sharing_protocol_error"
  )
})

test_that("snapshot pages stream directly into one private commit", {
  page <- 0L
  mock <- function(req) {
    page <<- page + 1L
    if (page == 1L) {
      expect_null(req$body$data$pageToken)
      actions <- c(
        snapshot_delta_actions(),
        list(list(nextPageToken = "page-two"))
      )
    } else {
      expect_equal(req$body$data$pageToken, "page-two")
      actions <- list(list(
        file = list(
          deltaSingleAction = list(
            add = list(
              path = "https://storage.example.test/two.parquet",
              size = 70,
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
      timestamp = NULL
    ),
    "delta"
  )
  withr::defer(log$cleanup())

  log_dir <- fs::path(log$path, "_delta_log")
  commit <- fs::path(log_dir, log_commit_name)
  lines <- readLines(commit)

  expect_equal(page, 2L)
  expect_identical(log$page_count, 2L)
  expect_identical(log$file_count, 2L)
  expect_identical(log$response_format, "delta")
  expect_setequal(fs::path_file(fs::dir_ls(log_dir)), log_commit_name)
  expect_length(lines, 4L)
  expect_equal(jsonlite::fromJSON(lines[[1L]])$protocol$minReaderVersion, 3L)
  expect_equal(
    jsonlite::fromJSON(lines[[3L]])$add$path,
    "https://storage.example.test/one.parquet"
  )
  expect_equal(
    jsonlite::fromJSON(lines[[4L]])$add$path,
    "https://storage.example.test/two.parquet"
  )
})

test_that("parquet snapshot pages use the same bounded preparation path", {
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
        url = "https://storage.example.test/events.parquet",
        size = 100,
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
      timestamp = NULL
    ),
    "parquet"
  )
  withr::defer(log$cleanup())

  lines <- readLines(fs::path(log$path, "_delta_log", log_commit_name))
  expect_identical(log$page_count, 1L)
  expect_identical(log$file_count, 1L)
  expect_identical(log$response_format, "parquet")
  expect_equal(
    jsonlite::fromJSON(lines[[3L]])$add$path,
    "https://storage.example.test/events.parquet"
  )
})

test_that("a malformed later page removes incomplete snapshot staging", {
  roots_before <- as.character(fs::dir_ls(
    fs::path_temp(),
    regexp = paste0("/", log_root_prefix),
    type = "directory",
    fail = FALSE
  ))
  page <- 0L
  httr2::local_mocked_responses(function(req) {
    page <<- page + 1L
    if (page == 1L) {
      actions <- c(
        snapshot_delta_actions(),
        list(list(nextPageToken = "broken-page"))
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
        timestamp = NULL
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
