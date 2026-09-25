staging_identifier <- function(name = "events") {
  sharing_table_identifier(paste0("sales.default.", name))
}

local_empty_cache <- function(identifier) {
  path <- table_download_cache(test_profile(), identifier)
  if (fs::dir_exists(path)) {
    fs::dir_delete(path)
  }
  fs::dir_create(path, mode = "u=rwx,go=")
  withr::defer(
    if (fs::dir_exists(path)) fs::dir_delete(path),
    envir = parent.frame()
  )
  path
}

staging_snapshot_actions <- function(id, url, size) {
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
    list(file = list(
      id = id,
      size = size,
      deltaSingleAction = list(add = list(
        path = url,
        size = size,
        dataChange = TRUE
      ))
    ))
  )
}

snapshot_staging_spec <- function() {
  list(
    predicate = NULL,
    limit = NULL,
    version = NULL,
    timestamp = NULL
  )
}

test_that("table handles create and share their session cache", {
  identifier <- staging_identifier("shared-cache")
  path <- local_empty_cache(identifier)
  client <- test_client()

  first <- client$table("sales.default.shared-cache")
  second <- client$table("sales.default.shared-cache", concurrency = 8)

  expect_true(fs::dir_exists(first$cache_path))
  expect_identical(first$cache_path, second$cache_path)
  expect_identical(first$cache_path, fs::path_real(path))
  expect_error(first$cache_path <- "elsewhere", "read-only")
})

test_that("rotated URLs reuse the server file ID", {
  identifier <- staging_identifier("rotated-urls")
  cache <- local_empty_cache(identifier)
  state <- new.env(parent = emptyenv())
  state$query_count <- 0L
  bytes <- charToRaw("parquet-data")
  source <- withr::local_tempfile(fileext = ".parquet")
  writeBin(bytes, source)
  file_id <- paste(rep("a", 64), collapse = "")

  httr2::local_mocked_responses(function(req) {
    state$query_count <- state$query_count + 1L
    token <- c("first", "rotated")[[state$query_count]]
    actions <- staging_snapshot_actions(
      file_id,
      paste0(local_file_url(source), "?token=", token),
      length(bytes)
    )
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  })

  profile <- test_profile()
  first <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    identifier,
    snapshot_staging_spec(),
    "delta",
    cache_path = cache
  )
  withr::defer(fs::dir_delete(first$root))
  second <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    identifier,
    snapshot_staging_spec(),
    "delta",
    cache_path = cache
  )
  withr::defer(fs::dir_delete(second$root))

  expect_equal(state$query_count, 2L)
  expect_equal(first$downloaded, 1L)
  expect_equal(first$cache_hits, 0L)
  expect_equal(second$downloaded, 0L)
  expect_equal(second$cache_hits, 1L)
  expect_true(fs::file_exists(fs::path(cache, paste0(file_id, ".parquet"))))
})

test_that("a manually removed cache is recreated on the next read", {
  identifier <- staging_identifier("recreated-cache")
  cache <- local_empty_cache(identifier)
  bytes <- charToRaw("parquet-data")
  source <- withr::local_tempfile(fileext = ".parquet")
  writeBin(bytes, source)
  file_id <- paste(rep("b", 64), collapse = "")
  actions <- staging_snapshot_actions(
    file_id,
    local_file_url(source),
    length(bytes)
  )
  httr2::local_mocked_responses(function(req) {
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  })

  fs::dir_delete(cache)
  profile <- test_profile()
  log <- prepare_snapshot_query_log(
    profile,
    sharing_auth_context(profile),
    identifier,
    snapshot_staging_spec(),
    "delta",
    cache_path = cache
  )
  withr::defer(fs::dir_delete(log$root))

  expect_true(fs::dir_exists(cache))
  expect_equal(log$downloaded, 1L)
})

test_that("a cached file with the wrong size is downloaded again", {
  identifier <- staging_identifier("wrong-size")
  cache <- local_empty_cache(identifier)
  bytes <- charToRaw("complete-parquet")
  source <- withr::local_tempfile(fileext = ".parquet")
  writeBin(bytes, source)
  file_id <- paste(rep("c", 64), collapse = "")
  cache_path <- fs::path(cache, paste0(file_id, ".parquet"))
  writeBin(charToRaw("short"), cache_path)
  asset <- staged_asset("data", file_id, local_file_url(source), length(bytes))

  result <- ensure_staged_assets(list(asset), cache, concurrency = 4L)

  expect_equal(result$downloaded, 1L)
  expect_equal(as.numeric(fs::file_size(cache_path)), length(bytes))
})

test_that("staging uses file and deletion-vector IDs directly", {
  identifier <- staging_identifier("deletion-vector")
  cache <- local_empty_cache(identifier)
  data <- withr::local_tempfile(fileext = ".parquet")
  dv <- withr::local_tempfile(fileext = ".bin")
  writeBin(charToRaw("parquet"), data)
  writeBin(charToRaw("deletion-vector"), dv)
  file_id <- paste(rep("d", 64), collapse = "")
  dv_id <- paste(rep("e", 64), collapse = "")
  wrapper <- list(
    id = file_id,
    deletionVectorFileId = dv_id,
    size = as.numeric(fs::file_size(data)),
    deltaSingleAction = list(add = list(
      path = local_file_url(data),
      deletionVector = list(
        storageType = "p",
        pathOrInlineDv = local_file_url(dv),
        offset = 1,
        sizeInBytes = as.numeric(fs::file_size(dv)) - 9,
        cardinality = 2
      )
    ))
  )

  assets <- file_wrapper_assets(wrapper, "delta", "read")
  expect_null(assets[[2L]]$size)

  result <- stage_file_wrappers(
    list(wrapper),
    "delta",
    cache,
    concurrency = 4L,
    operation = "read"
  )
  staged <- result$actions[[1L]]$add

  expect_true(fs::file_exists(fs::path(cache, paste0(file_id, ".parquet"))))
  expect_true(fs::file_exists(fs::path(cache, paste0(dv_id, ".bin"))))
  expect_identical(
    local_file_path(staged$path),
    as.character(fs::path_abs(fs::path(cache, paste0(file_id, ".parquet"))))
  )
  expect_identical(
    local_file_path(staged$deletionVector$pathOrInlineDv),
    as.character(fs::path_abs(fs::path(cache, paste0(dv_id, ".bin"))))
  )
})

test_that("a missing server file ID is a protocol error", {
  wrapper <- list(
    size = 1,
    deltaSingleAction = list(add = list(path = "file:///missing"))
  )

  expect_error(
    file_wrapper_assets(wrapper, "delta", "read"),
    class = "delta_sharing_protocol_error"
  )
})

test_that("file and deletion-vector IDs cannot contain path syntax", {
  root <- withr::local_tempdir()
  cache <- fs::path(root, "cache")
  source <- fs::path(root, "source")
  writeBin(charToRaw("data"), source)
  ids <- c(
    "../outside", "..\\outside", as.character(fs::path(root, "absolute")),
    "nested/file", "nested\\file", "file:stream"
  )

  purrr::walk(c("data", "deletion-vector"), function(kind) {
    purrr::walk(ids, function(id) {
      asset <- staged_asset(kind, id, local_file_url(source), size = 4)
      expect_error(
        ensure_staged_assets(list(asset), cache, 4L),
        "path separator or colon",
        class = "delta_sharing_protocol_error"
      )
    })
  })
  expect_length(fs::dir_ls(cache, all = TRUE), 0L)
})

test_that("invalid IDs are rejected before cache files are touched", {
  root <- withr::local_tempdir()
  cache <- fs::path(root, "cache")
  fs::dir_create(cache)
  source <- fs::path(root, "source")
  writeBin(charToRaw("download"), source)
  kept <- fs::path(cache, "kept.parquet")
  outside <- fs::path(root, c("outside.parquet", "outside.bin"))
  sentinel <- charToRaw("keep")
  purrr::walk(c(kept, outside), function(path) writeBin(sentinel, path))

  purrr::walk(c("data", "deletion-vector"), function(kind) {
    # The first asset would replace a short cached file if staging began.
    assets <- list(
      staged_asset("data", "kept", local_file_url(source), size = 8),
      staged_asset(kind, "../outside", local_file_url(source), size = 8)
    )
    expect_error(
      ensure_staged_assets(assets, cache, 4L),
      class = "delta_sharing_protocol_error"
    )
    purrr::walk(c(kept, outside), function(path) {
      expect_identical(readBin(path, "raw", 100), sentinel)
    })
    expect_identical(fs::path_file(fs::dir_ls(cache, all = TRUE)), "kept.parquet")
  })
})

test_that("ordinary IDs retain their filenames and cache reuse", {
  cache <- withr::local_tempdir()
  source <- withr::local_tempfile()
  writeBin(charToRaw("data"), source)
  ids <- c(
    "591723a8-6a27-4240-a90e-57426f4736d2",
    paste(rep("d", 64), collapse = ""),
    "file_123-abc", "part.001"
  )
  assets <- purrr::list_flatten(purrr::map(c("data", "deletion-vector"), function(kind) {
    purrr::map(ids, function(id) staged_asset(kind, id, local_file_url(source), 4))
  }))
  first <- ensure_staged_assets(assets, cache, 4L)
  fs::file_delete(source)
  second <- ensure_staged_assets(assets, cache, 4L)

  expect_setequal(
    fs::path_file(fs::dir_ls(cache)),
    c(paste0(ids, ".parquet"), paste0(ids, ".bin"))
  )
  expect_equal(first$downloaded, 8L)
  expect_equal(second$downloaded, 0L)
  expect_equal(second$cache_hits, 8L)
  expect_identical(first$paths, second$paths)
})

test_that("HTTP failures do not publish partial cache files or credentials", {
  identifier <- staging_identifier("failed-download")
  cache <- local_empty_cache(identifier)
  file_id <- paste(rep("f", 64), collapse = "")
  signed_url <- paste0(
    "https://storage.example.test/missing.parquet",
    "?X-Amz-Credential=secret&X-Amz-Signature=private"
  )
  actions <- staging_snapshot_actions(file_id, signed_url, 100L)
  httr2::local_mocked_responses(function(req) {
    if (grepl("/query", req$url, fixed = TRUE)) {
      return(httr2::response(200, body = charToRaw(ndjson_body(actions))))
    }
    httr2::response(403, body = charToRaw("denied"))
  })
  profile <- test_profile()

  condition <- expect_error(
    prepare_snapshot_query_log(
      profile,
      sharing_auth_context(profile),
      identifier,
      snapshot_staging_spec(),
      "delta",
      cache_path = cache
    ),
    class = "httr2_error"
  )

  expect_false(grepl("secret", conditionMessage(condition), fixed = TRUE))
  expect_false(grepl("private", conditionMessage(condition), fixed = TRUE))
  expect_false(fs::file_exists(fs::path(cache, paste0(file_id, ".parquet"))))
  expect_length(fs::dir_ls(cache, fail = FALSE), 0L)
})

test_that("incomplete downloads are not published", {
  identifier <- staging_identifier("short-download")
  cache <- local_empty_cache(identifier)
  source <- withr::local_tempfile(fileext = ".parquet")
  writeBin(charToRaw("short"), source)
  file_id <- paste(rep("1", 64), collapse = "")
  asset <- staged_asset("data", file_id, local_file_url(source), size = 100)

  expect_error(
    ensure_staged_assets(list(asset), cache, concurrency = 4L),
    class = "delta_sharing_protocol_error"
  )
  expect_false(fs::file_exists(fs::path(cache, paste0(file_id, ".parquet"))))
  expect_length(fs::dir_ls(cache, fail = FALSE), 0L)
})

test_that("indexed staging handles empty file lists", {
  cache <- withr::local_tempdir()
  for (format in c("delta", "parquet")) {
    expect_identical(
      stage_file_wrappers(list(), format, cache, 4L, "read"),
      list(actions = list(), downloaded = 0L, cache_hits = 0L)
    )
  }
})

test_that("indexed Delta paths preserve actions, duplicates and deletion vectors", {
  cache <- withr::local_tempdir()
  source <- withr::local_tempfile()
  writeBin(charToRaw("data"), source)
  original_action <- list(
    path = local_file_url(source),
    size = 4,
    partitionValues = list(region = "west"),
    stats = '{"numRecords":1}',
    deletionVector = list(
      storageType = "p",
      pathOrInlineDv = local_file_url(source),
      offset = 0,
      sizeInBytes = 4,
      cardinality = 1
    )
  )
  # Repeated data/DV IDs share files; their distinct extensions stay distinct.
  files <- purrr::map(c("add", "remove", "cdc"), function(field) {
    list(
      id = if (field == "cdc") "other" else "shared",
      deletionVectorFileId = "shared",
      size = 4,
      deltaSingleAction = stats::setNames(list(original_action), field)
    )
  })
  files <- c(files, files[1], list(list(
    deltaSingleAction = list(commitInfo = list(operation = "WRITE"))
  )))
  original_files <- files
  asset_names <- c("shared.parquet", "shared.bin", "other.parquet")
  paths <- stats::setNames(
    purrr::map(fs::path(cache, asset_names), local_file_url),
    asset_names
  )
  # The old named-list lookup is the reference for exact output equality.
  expected <- purrr::map(
    files, rewrite_staged_file, "delta", "changes", paths = paths
  )
  result <- stage_file_wrappers(files, "delta", cache, 4L, "changes")

  expect_identical(result$actions, expected)
  expect_identical(files, original_files)
  expect_equal(result$downloaded, 3L)
  expect_equal(result$cache_hits, 0L)
  expect_setequal(fs::path_file(fs::dir_ls(cache)), asset_names)
  cached <- stage_file_wrappers(files, "delta", cache, 4L, "changes")
  expect_identical(cached$actions, expected)
  expect_equal(cached$downloaded, 0L)
  expect_equal(cached$cache_hits, 3L)
})

test_that("indexed Parquet paths preserve synthesized actions and order", {
  cache <- withr::local_tempdir()
  source <- withr::local_tempfile()
  writeBin(charToRaw("data"), source)
  files <- purrr::map(c("second", "first", "second"), function(id) {
    list(
      id = id,
      url = local_file_url(source),
      size = 4,
      partitionValues = list(region = "east"),
      stats = '{"numRecords":1}'
    )
  })
  asset_names <- c("second.parquet", "first.parquet")
  paths <- stats::setNames(
    purrr::map(fs::path(cache, asset_names), local_file_url),
    asset_names
  )
  expected <- purrr::map(files, rewrite_staged_file, "parquet", "read", paths)
  result <- stage_file_wrappers(files, "parquet", cache, 4L, "read")

  expect_identical(result$actions, expected)
  expect_equal(result$downloaded, 2L)
  expect_equal(result$cache_hits, 0L)
})
