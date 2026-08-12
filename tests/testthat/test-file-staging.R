staging_snapshot_actions <- function(url, size) {
  list(
    list(protocol = list(deltaProtocol = list(
      minReaderVersion = 1L,
      minWriterVersion = 2L
    ))),
    list(metaData = list(deltaMetadata = list(
      id = "events",
      schemaString = "{\"type\":\"struct\",\"fields\":[]}",
      partitionColumns = list()
    ))),
    list(file = list(deltaSingleAction = list(add = list(
      path = url,
      size = size,
      dataChange = TRUE
    ))))
  )
}

staging_identifier <- function() {
  sharing_table_identifier("sales.default.events")
}

snapshot_staging_spec <- function(cache = FALSE) {
  list(
    predicate = NULL,
    limit = NULL,
    version = NULL,
    timestamp = NULL,
    cache = cache
  )
}

test_that("session table caching ignores rotated signatures", {
  clear_session_download_cache()
  withr::defer(clear_session_download_cache())
  state <- new.env(parent = emptyenv())
  state$query_count <- 0L
  bytes <- charToRaw("parquet-data")
  source <- withr::local_tempfile(fileext = ".parquet")
  writeBin(bytes, source)

  httr2::local_mocked_responses(function(req) {
    state$query_count <- state$query_count + 1L
    token <- c("first", "rotated")[[state$query_count]]
    actions <- staging_snapshot_actions(
      paste0(local_file_url(source), "?token=", token),
      length(bytes)
    )
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  })

  profile <- test_profile()
  identifier <- staging_identifier()
  first <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    identifier,
    snapshot_staging_spec(cache = TRUE),
    "delta"
  )
  withr::defer(first$cleanup())
  second <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    identifier,
    snapshot_staging_spec(cache = TRUE),
    "delta"
  )
  withr::defer(second$cleanup())

  expect_equal(state$query_count, 2L)
  expect_equal(first$downloaded, 1L)
  expect_equal(first$cache_hits, 0L)
  expect_equal(second$downloaded, 0L)
  expect_equal(second$cache_hits, 1L)
  expect_true(fs::file_exists(fs::dir_ls(fs::path(first$path, "data"))))
  expect_true(fs::file_exists(fs::dir_ls(fs::path(second$path, "data"))))
})

test_that("table clear_cache removes only that table cache", {
  clear_session_download_cache()
  withr::defer(clear_session_download_cache())
  profile <- test_profile()
  first_id <- sharing_table_identifier("sales.default.first")
  second_id <- sharing_table_identifier("sales.default.second")
  first <- table_download_cache(profile, first_id)
  second <- table_download_cache(profile, second_id)
  fs::file_create(fs::path(first, "entry"))
  fs::file_create(fs::path(second, "entry"))

  table <- SharingTable$new(
    profile,
    sharing_auth_context(profile),
    first_id
  )
  expect_invisible(table$clear_cache())

  expect_false(fs::dir_exists(first))
  expect_true(fs::dir_exists(second))
})

test_that("clearing a table cache does not invalidate staged read files", {
  clear_session_download_cache()
  withr::defer(clear_session_download_cache())
  root <- withr::local_tempdir(pattern = "delta-sharing-cache-read-")
  table_dir <- fs::path(root, "table")
  fs::dir_create(table_dir)
  source <- fs::path(root, "source.parquet")
  writeBin(charToRaw("immutable-data"), source)
  profile <- test_profile()
  identifier <- staging_identifier()
  context <- new_staging_context(
    profile,
    identifier,
    table_dir,
    cache = TRUE
  )
  action <- list(add = list(
    path = local_file_url(source),
    size = fs::file_size(source)
  ))
  staged <- stage_delta_actions(list(action), context, threads = 4L)[[1L]]
  staged_path <- local_file_path(staged$add$path)

  clear_table_download_cache(profile, identifier)

  expect_true(fs::file_exists(staged_path))
  expect_equal(readChar(staged_path, fs::file_size(staged_path)), "immutable-data")
})

test_that("a cached file with the wrong size is downloaded again", {
  clear_session_download_cache()
  withr::defer(clear_session_download_cache())
  root <- withr::local_tempdir(pattern = "delta-sharing-cache-size-")
  table_dir <- fs::path(root, "table")
  fs::dir_create(table_dir)
  profile <- test_profile()
  identifier <- staging_identifier()
  context <- new_staging_context(
    profile,
    identifier,
    table_dir,
    cache = TRUE
  )
  bytes <- charToRaw("complete-parquet")
  source <- fs::path(root, "source.parquet")
  writeBin(bytes, source)
  url <- paste0(local_file_url(source), "?token=secret")
  asset <- staged_asset("data", url, length(bytes))
  cache_path <- fs::path(context$cache_dir, asset$name)
  writeBin(charToRaw("short"), cache_path)

  ensure_staged_assets(list(asset), context, threads = 4L)

  expect_equal(context$downloaded, 1L)
  expect_equal(as.numeric(fs::file_size(cache_path)), length(bytes))
})

test_that("uncached reads neither create nor reuse a session cache", {
  clear_session_download_cache()
  withr::defer(clear_session_download_cache())
  bytes <- charToRaw("parquet-data")
  source <- withr::local_tempfile(fileext = ".parquet")
  writeBin(bytes, source)
  actions <- staging_snapshot_actions(
    paste0(local_file_url(source), "?token=secret"),
    length(bytes)
  )

  httr2::local_mocked_responses(function(req) {
    if (grepl("/query", req$url, fixed = TRUE)) {
      return(httr2::response(200, body = charToRaw(ndjson_body(actions))))
    }
  })
  profile <- test_profile()
  logs <- purrr::map(1:2, function(index) {
    prepare_snapshot_query_log(
      profile,
      sharing_auth_context(profile),
      staging_identifier(),
      snapshot_staging_spec(),
      "delta"
    )
  })
  withr::defer(purrr::walk(logs, "cleanup"))

  expect_equal(purrr::map_int(logs, "downloaded"), c(1L, 1L))
  expect_null(session_cache_root(create = FALSE))
})

test_that("staging rewrites data and absolute deletion-vector paths", {
  root <- withr::local_tempdir(pattern = "delta-sharing-staging-")
  table_dir <- fs::path(root, "table")
  fs::dir_create(table_dir)
  data <- fs::path(root, "source.parquet")
  dv <- fs::path(root, "source.bin")
  writeBin(charToRaw("parquet"), data)
  writeBin(charToRaw("deletion-vector"), dv)
  profile <- test_profile()
  context <- new_staging_context(
    profile,
    staging_identifier(),
    table_dir,
    cache = FALSE
  )
  action <- list(add = list(
    path = local_file_url(data),
    size = fs::file_size(data),
    deletionVector = list(
      storageType = "p",
      pathOrInlineDv = local_file_url(dv),
      offset = 1,
      sizeInBytes = 10,
      cardinality = 2
    )
  ))

  staged <- stage_delta_actions(list(action), context, threads = 4L)[[1L]]

  expect_equal(httr2::url_parse(staged$add$path)$scheme, "file")
  expect_equal(
    httr2::url_parse(staged$add$deletionVector$pathOrInlineDv)$scheme,
    "file"
  )
  expect_false(identical(staged$add$path, action$add$path))
  expect_equal(length(fs::dir_ls(context$data_dir)), 2L)
})

test_that("download failures are redacted and remove partial staging", {
  signed_url <- paste0(
    "https://storage.example.test/missing.parquet",
    "?X-Amz-Credential=secret&X-Amz-Signature=private"
  )
  actions <- staging_snapshot_actions(signed_url, 100L)
  httr2::local_mocked_responses(function(req) {
    if (grepl("/query", req$url, fixed = TRUE)) {
      return(httr2::response(200, body = charToRaw(ndjson_body(actions))))
    }
    httr2::response(403, body = charToRaw("denied"))
  })
  roots_before <- fs::dir_ls(
    fs::path_temp(),
    regexp = paste0("/", log_root_prefix),
    type = "directory",
    fail = FALSE
  )
  profile <- test_profile()

  condition <- expect_error(
    prepare_snapshot_query_log(
      profile,
      sharing_auth_context(profile),
      staging_identifier(),
      snapshot_staging_spec(),
      "delta"
    ),
    class = "httr2_error"
  )
  roots_after <- fs::dir_ls(
    fs::path_temp(),
    regexp = paste0("/", log_root_prefix),
    type = "directory",
    fail = FALSE
  )

  expect_equal(conditionMessage(condition), "A shared data file could not be downloaded.")
  expect_false(grepl("secret", conditionMessage(condition), fixed = TRUE))
  expect_false(grepl("private", conditionMessage(condition), fixed = TRUE))
  expect_setequal(as.character(roots_after), as.character(roots_before))
})

test_that("incomplete downloads are not published", {
  root <- withr::local_tempdir(pattern = "delta-sharing-short-download-")
  table_dir <- fs::path(root, "table")
  fs::dir_create(table_dir)
  context <- new_staging_context(
    test_profile(),
    staging_identifier(),
    table_dir,
    cache = FALSE
  )
  source <- fs::path(root, "short.parquet")
  writeBin(charToRaw("short"), source)
  asset <- staged_asset("data", local_file_url(source), size = 100)

  expect_error(
    ensure_staged_assets(list(asset), context, threads = 4L),
    class = "httr2_error"
  )
  expect_false(fs::file_exists(fs::path(context$data_dir, asset$name)))
  expect_length(fs::dir_ls(context$data_dir, fail = FALSE), 0L)
})
