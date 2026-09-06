local_mock_download_bodies <- function(.env = parent.frame()) {
  perform <- httr2::req_perform_parallel
  # httr2's mocked queue returns response bodies without writing `paths`.
  # Supply that transport side effect while retaining the real queue's error
  # handling and response ordering.
  testthat::local_mocked_bindings(
    req_perform_parallel = function(reqs, paths, ...) {
      responses <- perform(reqs, paths = paths, ...)
      for (i in seq_along(responses)) {
        if (inherits(responses[[i]], "httr2_response")) {
          writeBin(httr2::resp_body_raw(responses[[i]]), paths[[i]])
        }
      }
      responses
    },
    .package = "httr2",
    .env = .env
  )
}

test_that("completed HTTP assets survive a sibling failure and are reused", {
  local_mock_download_bodies()
  cache <- withr::local_tempdir()
  attempts <- c(good = 0L, bad = 0L)
  httr2::local_mocked_responses(function(req) {
    name <- basename(httr2::url_parse(req$url)$path)
    attempts[[name]] <<- attempts[[name]] + 1L
    status <- if (name == "bad" && attempts[[name]] == 1L) 403L else 200L
    # The failed response deliberately has the expected byte count.
    httr2::response(status, body = charToRaw("data"))
  })
  assets <- lapply(c("good", "bad"), function(name) {
    staged_asset("data", name, paste0("https://storage.example.test/", name), 4)
  })
  expect_error(ensure_staged_assets(assets, cache, 1L), class = "httr2_error")
  expect_true(fs::file_exists(fs::path(cache, assets[[1]]$name)))
  expect_false(fs::file_exists(fs::path(cache, assets[[2]]$name)))
  expect_length(fs::dir_ls(cache, all = TRUE), 1L)

  result <- ensure_staged_assets(assets, cache, 1L)
  expect_identical(attempts, c(good = 1L, bad = 2L))
  expect_equal(result$downloaded, 1L)
  expect_equal(result$cache_hits, 1L)
  expect_length(fs::dir_ls(cache, all = TRUE), 2L)
})

test_that("valid assets survive a later size validation failure", {
  local_mock_download_bodies()
  for (remote in c(FALSE, TRUE)) {
    cache <- withr::local_tempdir()
    source <- withr::local_tempfile()
    writeBin(charToRaw("data"), source)
    httr2::local_mocked_responses(function(req) {
      httr2::response(200L, body = charToRaw("data"))
    })
    url <- if (remote) "https://storage.example.test/data" else
      local_file_url(source)
    assets <- list(
      staged_asset("data", "complete", url, 4),
      staged_asset("data", "incomplete", url, 5)
    )
    expect_error(
      ensure_staged_assets(assets, cache, 1L),
      class = "delta_sharing_protocol_error"
    )
    expect_true(fs::file_exists(fs::path(cache, assets[[1]]$name)))
    expect_false(fs::file_exists(fs::path(cache, assets[[2]]$name)))
    expect_length(fs::dir_ls(cache, all = TRUE), 1L)
  }
})
