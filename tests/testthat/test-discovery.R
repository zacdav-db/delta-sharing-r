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
  expect_equal(shares$name, c("a", "b"))
  expect_equal(state$page, 2L)
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

test_that("unscoped discovery expands shares and schemas", {
  mock <- function(req) {
    path <- httr2::url_parse(req$url)$path
    body <- switch(
      path,
      "/api/shares" = list(
        items = list(
          list(name = "sales", id = "s1"),
          list(name = "marketing", id = "s2")
        )
      ),
      "/api/shares/sales/schemas" = list(
        items = list(list(name = "default"))
      ),
      "/api/shares/marketing/schemas" = list(
        items = list(list(name = "analytics"))
      ),
      "/api/shares/sales/schemas/default/tables" = list(
        items = list(
          list(share = "sales", schema = "default", name = "orders")
        )
      ),
      "/api/shares/marketing/schemas/analytics/tables" = list(
        items = list(
          list(share = "marketing", schema = "analytics", name = "events")
        )
      ),
      NULL
    )
    if (is.null(body)) {
      return(httr2::response(404))
    }
    httr2::response_json(body = body)
  }
  client <- test_client()
  httr2::local_mocked_responses(mock)

  schemas <- client$list_schemas()
  tables <- client$list_tables()

  expect_equal(schemas$share, c("sales", "marketing"))
  expect_equal(tables$name, c("orders", "events"))
})

test_that("discovery records stay typed when empty or incomplete", {
  empty <- records_to_tibble(list(), c(name = "name", id = "id"))
  incomplete <- records_to_tibble(
    list(list(name = "sales")),
    c(name = "name", id = "id")
  )

  expect_s3_class(empty, "tbl_df")
  expect_identical(names(empty), c("name", "id"))
  expect_identical(nrow(empty), 0L)
  expect_true(is.na(incomplete$id))
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
  expect_error(
    test_client()$list_tables(schema = "default"),
    class = "delta_sharing_validation_error"
  )
})
