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

test_that("list_shares returns a printable list of share records", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  shares <- client$list_shares()
  expect_s3_class(shares, "delta_sharing_listing")
  expect_type(shares, "list")
  expect_equal(purrr::map_chr(shares, "name"), c("sales", "mktg"))
  expect_output(print(shares), "<Delta Sharing shares> 2", fixed = TRUE)
  expect_output(print(shares), "sales", fixed = TRUE)
})

test_that("pagination follows nextPageToken across pages", {
  client <- test_client()
  state <- new.env(parent = emptyenv())
  state$page <- 0L
  mock <- function(req) {
    state$page <- state$page + 1L
    if (state$page == 1L) {
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
  expect_equal(purrr::map_chr(shares, "name"), c("a", "b"))
  expect_equal(state$page, 2L)
})

test_that("list_schemas scopes to a share", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  schemas <- client$list_schemas(share = "sales")
  expect_equal(schemas[[1]], list(share = "sales", name = "default"))
  expect_output(print(schemas), "sales.default", fixed = TRUE)
})

test_that("list_tables returns qualified table records", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  tables <- client$list_tables(share = "sales", schema = "default")
  expect_equal(names(tables[[1]]), c("share", "schema", "name"))
  expect_equal(tables[[1]]$name, "orders")
  expect_output(print(tables), "sales.default.orders", fixed = TRUE)
})

test_that("list_tables with only a share uses the all-tables route", {
  client <- test_client()
  httr2::local_mocked_responses(mock_discovery)
  tables <- client$list_tables(share = "sales")
  expect_equal(tables[[1]]$name, "orders")
})

test_that("schema and table listings require a share", {
  client <- test_client()

  expect_error(client$list_schemas(), "share.*missing")
  expect_error(client$list_tables(), "share.*missing")
  expect_error(client$list_tables(schema = "default"), "share.*missing")
})

test_that("empty discovery results remain printable lists", {
  httr2::local_mocked_responses(function(req) {
    httr2::response_json(body = list(items = list()))
  })
  empty <- test_client()$list_shares()

  expect_s3_class(empty, "delta_sharing_listing")
  expect_length(empty, 0L)
  expect_output(print(empty), "<Delta Sharing shares> 0", fixed = TRUE)
})

test_that("discovery names reject empty and control-character values", {
  purrr::walk(
    list("", NA_character_, c("a", "b"), "bad\nname"),
    function(value) {
      expect_error(
        discovery_name(value, "share", "list_schemas"),
        class = "delta_sharing_validation_error"
      )
    }
  )
})
