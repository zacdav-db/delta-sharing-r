test_that("asset IDs cannot create, reuse, or replace files outside the cache", {
  for (kind in c("data", "deletion-vector")) {
    for (existing in c("absent", "same-size", "wrong-size")) {
      root <- withr::local_tempdir()
      cache <- fs::path(root, "cache")
      fs::dir_create(cache)
      source <- fs::path(root, "source")
      bytes <- charToRaw("provider")
      writeBin(bytes, source)
      extension <- if (kind == "data") ".parquet" else ".bin"
      outside <- fs::path(root, paste0("outside", extension))
      sentinel <- if (existing == "same-size") charToRaw("sentinel") else as.raw(1)
      if (existing != "absent") writeBin(sentinel, outside)

      wrapper <- list(
        id = if (kind == "data") "../outside" else "data-id",
        deletionVectorFileId = if (kind == "deletion-vector") "../outside" else "dv-id",
        size = length(bytes),
        deltaSingleAction = list(add = list(
          path = local_file_url(source),
          deletionVector = list(
            storageType = "p",
            pathOrInlineDv = local_file_url(source)
          )
        ))
      )
      result <- stage_file_wrappers(list(wrapper), "delta", cache, 1L, "read")
      action <- result$actions[[1L]]$add
      url <- if (kind == "data") action$path else action$deletionVector$pathOrInlineDv
      staged_path <- local_file_path(url)

      expect_equal(result$downloaded, 2L)
      expect_equal(result$cache_hits, 0L)
      expect_identical(fs::path_real(fs::path_dir(staged_path)), fs::path_real(cache))
      expect_identical(readBin(staged_path, "raw", length(bytes)), bytes)
      if (existing == "absent") {
        expect_false(fs::file_exists(outside))
      } else {
        expect_identical(readBin(outside, "raw", 100), sentinel)
      }
    }
  }
})

test_that("failed downloads with traversal IDs leave outside files untouched", {
  for (kind in c("data", "deletion-vector")) {
    root <- withr::local_tempdir()
    cache <- fs::path(root, "cache")
    fs::dir_create(cache)
    extension <- if (kind == "data") ".parquet" else ".bin"
    outside <- fs::path(root, paste0("outside", extension))
    sentinel <- charToRaw("keep")
    writeBin(sentinel, outside)
    asset <- staged_asset(
      kind,
      "../outside",
      local_file_url(fs::path(root, "missing")),
      size = if (kind == "data") 100 else NULL
    )

    expect_error(ensure_staged_assets(list(asset), cache, 1L))
    expect_true(fs::file_exists(outside))
    if (fs::file_exists(outside)) {
      expect_identical(readBin(outside, "raw", 100), sentinel)
    }
    expect_length(fs::dir_ls(cache, all = TRUE), 0L)
  }
})

test_that("opaque IDs produce portable filenames for both asset types", {
  ids <- c(
    "../outside", "..\\outside", "/absolute/path", "C:\\absolute\\path",
    "//server/share", "file:stream", ".", "..", "CON", "trailing. ",
    "a/b", "a\\b", "a_b", "a%2fb", "ID", "id", "caf\u00e9",
    paste(rep("x", 300), collapse = "")
  )
  for (kind in c("data", "deletion-vector")) {
    assets <- purrr::map(ids, ~ staged_asset(kind, .x, "file:///unused"))
    names <- purrr::map_chr(assets, "name")
    expect_true(all(grepl("^[a-f0-9]{64}\\.(parquet|bin)$", names)))
    expect_identical(purrr::map_chr(assets, "id"), ids)
    expect_length(unique(names), length(ids))
  }
})

test_that("data and deletion vectors with the same ID remain distinct and reusable", {
  cache <- withr::local_tempdir()
  data <- withr::local_tempfile()
  dv <- withr::local_tempfile()
  writeBin(charToRaw("parquet"), data)
  writeBin(charToRaw("bitmap"), dv)
  wrapper <- list(
    id = "591723a8-6a27-4240-a90e-57426f4736d2",
    deletionVectorFileId = "591723a8-6a27-4240-a90e-57426f4736d2",
    size = 7,
    deltaSingleAction = list(add = list(
      path = local_file_url(data),
      deletionVector = list(storageType = "p", pathOrInlineDv = local_file_url(dv))
    ))
  )
  first <- stage_file_wrappers(list(wrapper, wrapper), "delta", cache, 1L, "read")
  # No source remains to copy; changed URLs must still reuse each immutable ID.
  fs::file_delete(c(data, dv))
  wrapper$deltaSingleAction$add$path <- paste0(local_file_url(data), "?rotated=data")
  wrapper$deltaSingleAction$add$deletionVector$pathOrInlineDv <- paste0(
    local_file_url(dv), "?rotated=dv"
  )
  second <- stage_file_wrappers(list(wrapper), "delta", cache, 1L, "read")
  action <- second$actions[[1L]]$add

  expect_equal(first$downloaded, 2L)
  expect_equal(second$downloaded, 0L)
  expect_equal(second$cache_hits, 2L)
  expect_identical(second$actions[[1L]], first$actions[[1L]])
  expect_identical(
    readBin(local_file_path(action$path), "raw", 100),
    charToRaw("parquet")
  )
  expect_identical(
    readBin(local_file_path(action$deletionVector$pathOrInlineDv), "raw", 100),
    charToRaw("bitmap")
  )
})
