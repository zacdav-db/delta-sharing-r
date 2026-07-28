test_that("pagination collects every page in order", {
  requested <- list()
  responses <- list(
    list(
      items = list(list(name = "sales")),
      nextPageToken = "page-2"
    ),
    list(
      items = list(list(name = "operations")),
      nextPageToken = ""
    )
  )

  result <- delta.sharing:::.collect_pages(function(token) {
    requested <<- c(requested, list(token))
    responses[[length(requested)]]
  })

  expect_identical(
    vapply(result, `[[`, character(1), "name"),
    c("sales", "operations")
  )
  expect_null(requested[[1L]])
  expect_identical(requested[[2L]], "page-2")
})

test_that("pagination accepts missing items and completion tokens", {
  expect_identical(
    delta.sharing:::.collect_pages(function(token) list()),
    list()
  )
})

test_that("pagination rejects repeated tokens", {
  condition <- expect_error(
    delta.sharing:::.collect_pages(
      function(token) {
        list(items = list(), nextPageToken = "same")
      }
    ),
    class = "delta_sharing_protocol_error"
  )

  expect_identical(condition$operation, "paginate")
  expect_false(any(grepl("same", condition$message, fixed = TRUE)))
})

test_that("pagination enforces a page ceiling", {
  counter <- 0L
  condition <- expect_error(
    delta.sharing:::.collect_pages(
      function(token) {
        counter <<- counter + 1L
        list(items = list(), nextPageToken = paste0("page-", counter))
      },
      max_pages = 2L
    ),
    class = "delta_sharing_protocol_error"
  )

  expect_identical(counter, 2L)
  expect_identical(condition$operation, "paginate")
})

test_that("pagination validates callbacks, fields, and page shapes", {
  expect_error(
    delta.sharing:::.collect_pages(NULL),
    "`fetch_page` must be a function",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.collect_pages(function(token) list(), max_pages = 0),
    "`max_pages` must be one positive whole number",
    fixed = TRUE
  )
  expect_error(
    delta.sharing:::.collect_pages(function(token) "not a page"),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.collect_pages(
      function(token) list(items = "not a list")
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.collect_pages(
      function(token) list(items = list(), nextPageToken = 2L)
    ),
    class = "delta_sharing_protocol_error"
  )
})
