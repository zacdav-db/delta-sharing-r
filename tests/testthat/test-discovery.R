mock_discovery <- function(req) {
  path <- httr2::url_parse(req$url)$path
  if (grepl("/shares$", path)) {
    return(httr2::response_json(
      body = list(
        items = list(
          list(name = "sales", id = "s1"),
          list(name = "mktg", id = "s2")
        )
      )
    ))
  }
  if (grepl("/shares/sales/schemas$", path)) {
    return(httr2::response_json(
      body = list(items = list(list(name = "default")))
    ))
  }
  if (grepl("/tables$", path)) {
    return(httr2::response_json(
      body = list(
        items = list(
          list(share = "sales", schema = "default", name = "orders")
        )
      )
    ))
  }
  if (grepl("/all-tables$", path)) {
    return(httr2::response_json(
      body = list(
        items = list(
          list(share = "sales", schema = "default", name = "orders")
        )
      )
    ))
  }
  httr2::response(404)
}

test_that("list_shares returns a tibble of share names", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  shares <- client$list_shares()
  expect_s3_class(shares, "tbl_df")
  expect_equal(shares$name, c("sales", "mktg"))
})

test_that("pagination follows nextPageToken across pages", {
  client <- test_client()
  page <- 0L
  mock <- function(req) {
    page <<- page + 1L
    if (page == 1L) {
      httr2::response_json(
        body = list(
          items = list(list(name = "a", id = "1")),
          nextPageToken = "tok2"
        )
      )
    } else {
      httr2::response_json(
        body = list(items = list(list(name = "b", id = "2")))
      )
    }
  }
  httr2::local_mocked_responses(mock)
  shares <- client$list_shares()
  expect_equal(shares$name, c("a", "b"))
  expect_equal(page, 2L)
})

test_that("list_schemas scopes to a share", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  schemas <- client$list_schemas(share = "sales")
  expect_equal(schemas$share, "sales")
  expect_equal(schemas$name, "default")
})

test_that("list_tables returns share/schema/name columns", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  tables <- client$list_tables(share = "sales", schema = "default")
  expect_equal(names(tables), c("share", "schema", "name"))
  expect_equal(tables$name, "orders")
})

test_that("list_tables with only a share uses the all-tables route", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  tables <- client$list_tables(share = "sales")
  expect_equal(tables$name, "orders")
})
