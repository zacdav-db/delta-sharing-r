discovery_fixture <- function(name) {
  jsonlite::fromJSON(
    test_path("fixtures", "discovery", name),
    simplifyVector = FALSE
  )
}

fixture_discovery_fetcher <- function(recorder = NULL) {
  function(path_segments, page_token) {
    path <- paste0(
      "/",
      paste(
        vapply(
          path_segments,
          utils::URLencode,
          character(1),
          reserved = TRUE,
          repeated = TRUE,
          USE.NAMES = FALSE
        ),
        collapse = "/"
      )
    )
    if (!is.null(recorder)) {
      recorder$calls <- c(
        recorder$calls,
        list(list(
          path_segments = path_segments,
          path = path,
          page_token = page_token
        ))
      )
    }
    key <- paste0(path, "|", if (is.null(page_token)) "" else page_token)
    switch(
      key,
      "/shares|" = discovery_fixture("shares-page-1.json"),
      "/shares|page-2" = discovery_fixture("shares-page-2.json"),
      "/shares/sales/schemas|" =
        discovery_fixture("schemas-sales.json"),
      "/shares/operations/schemas|" =
        discovery_fixture("schemas-operations.json"),
      "/shares/sales/all-tables|" =
        discovery_fixture("tables-sales.json"),
      "/shares/operations/all-tables|" =
        discovery_fixture("tables-operations.json"),
      stop("Unexpected fixture route: ", key)
    )
  }
}

test_that("discovery routes encode each provider name as one path segment", {
  expect_identical(
    delta.sharing:::.discovery_route("shares"),
    "/shares"
  )
  expect_identical(
    delta.sharing:::.discovery_route(
      "schemas",
      share = "Share Name/100%"
    ),
    "/shares/Share%20Name%2F100%25/schemas"
  )
  expect_identical(
    delta.sharing:::.discovery_route(
      "tables",
      share = "café",
      schema = "schema?x#"
    ),
    "/shares/caf%C3%A9/schemas/schema%3Fx%23/tables"
  )
  expect_identical(
    delta.sharing:::.discovery_route(
      "all_tables",
      share = "schema.with.dot"
    ),
    "/shares/schema.with.dot/all-tables"
  )
})

test_that("discovery route validation is typed and secret-safe", {
  condition <- expect_error(
    delta.sharing:::.discovery_route("schemas", share = "bad\nsecret"),
    class = "delta_sharing_validation_error"
  )
  expect_identical(condition$operation, "list_schemas")
  expect_false(grepl("bad", conditionMessage(condition), fixed = TRUE))

  expect_error(
    delta.sharing:::.discovery_route("tables", share = "sales"),
    class = "delta_sharing_validation_error"
  )
})

test_that("discovery route contracts reject inapplicable filter combinations", {
  expect_error(
    delta.sharing:::.discovery_route_segments("unknown"),
    "Unknown discovery resource"
  )
  expect_error(
    delta.sharing:::.discovery_route_segments("shares", share = "sales"),
    "do not apply to the shares route"
  )
  expect_error(
    delta.sharing:::.discovery_route_segments("schemas"),
    class = "delta_sharing_validation_error"
  )
  expect_error(
    delta.sharing:::.discovery_route_segments(
      "schemas",
      share = "sales",
      schema = "default"
    ),
    "does not apply to the schemas route"
  )
  expect_error(
    delta.sharing:::.discovery_route_segments(
      "all_tables",
      share = "sales",
      schema = "default"
    ),
    "does not apply to the all-tables route"
  )
})

test_that("share normalization has stable base-data-frame columns", {
  records <- c(
    discovery_fixture("shares-page-1.json")$items,
    discovery_fixture("shares-page-2.json")$items
  )
  records[[1L]]$displayName <- "Sales"
  records[[1L]]$comment <- "Provider-visible description"
  records[[1L]]$credentials <- list(bearerToken = "must-not-leak")

  result <- delta.sharing:::.normalize_share_records(records)

  expect_s3_class(result, "data.frame")
  expect_identical(
    names(result),
    c("name", "id", "display_name", "comment")
  )
  expect_identical(result$name, c("sales", "operations"))
  expect_identical(result$display_name, c("Sales", NA_character_))
  expect_false(any(grepl("must-not-leak", capture.output(str(result)), fixed = TRUE)))
})

test_that("schema normalization preserves exact names and excludes unknown data", {
  records <- discovery_fixture("schemas-sales.json")$items
  result <- delta.sharing:::.normalize_schema_records(
    records,
    expected_share = "sales"
  )

  expect_identical(names(result), c("share", "name"))
  expect_identical(result$share, c("sales", "sales"))
  expect_identical(result$name, c("default", "analytics"))
  expect_false(any(grepl("private-bucket", capture.output(str(result)), fixed = TRUE)))
})

test_that("table normalization excludes locations, credentials, and unknown fields", {
  records <- discovery_fixture("tables-sales.json")$items
  result <- delta.sharing:::.normalize_table_records(
    records,
    expected_share = "sales"
  )
  rendered <- capture.output(str(result))

  expect_identical(
    names(result),
    c("share", "schema", "name", "share_id", "id", "access_modes")
  )
  expect_identical(result$name, c("orders", "forecast"))
  expect_identical(result$access_modes[[1L]], c("url", "dir"))
  expect_identical(result$access_modes[[2L]], character())
  expect_false(any(grepl("location", rendered, fixed = TRUE)))
  expect_false(any(grepl("private-bucket", rendered, fixed = TRUE)))
  expect_false(any(grepl("must-not-leak", rendered, fixed = TRUE)))
})

test_that("empty normalized discovery results retain stable types", {
  shares <- delta.sharing:::.normalize_share_records(list())
  schemas <- delta.sharing:::.normalize_schema_records(list())
  tables <- delta.sharing:::.normalize_table_records(list())

  expect_identical(names(shares), c("name", "id", "display_name", "comment"))
  expect_identical(names(schemas), c("share", "name"))
  expect_identical(
    names(tables),
    c("share", "schema", "name", "share_id", "id", "access_modes")
  )
  expect_identical(nrow(shares), 0L)
  expect_identical(nrow(schemas), 0L)
  expect_identical(nrow(tables), 0L)
  expect_true(is.list(tables$access_modes))
})

test_that("empty discovery fan-out keeps stable route and result schemas", {
  shares <- data.frame(name = character(), stringsAsFactors = FALSE)
  schema_routes <- delta.sharing:::.plan_schema_routes(shares = shares)
  table_routes <- delta.sharing:::.plan_table_routes(shares = shares)

  expect_identical(nrow(schema_routes), 0L)
  expect_identical(nrow(table_routes), 0L)
  expect_identical(
    names(schema_routes),
    c("operation", "share", "schema", "path_segments")
  )
  expect_identical(
    names(table_routes),
    c("operation", "share", "schema", "path_segments")
  )
  expect_error(
    delta.sharing:::.plan_table_routes(schema = "default", shares = shares),
    class = "delta_sharing_validation_error"
  )
  expect_identical(
    delta.sharing:::.plan_table_routes(share = "sales")$path_segments[[1L]],
    c("shares", "sales", "all-tables")
  )
  expect_identical(
    delta.sharing:::.bind_discovery_frames(
      list(),
      delta.sharing:::.empty_table_records
    ),
    delta.sharing:::.empty_table_records()
  )
})

test_that("invalid records fail without exposing record contents", {
  secret <- "record-secret-must-not-leak"
  condition <- expect_error(
    delta.sharing:::.normalize_table_records(list(list(
      share = "sales",
      schema = "default",
      name = list(secret),
      location = paste0("s3://bucket/?sig=", secret)
    ))),
    class = "delta_sharing_protocol_error"
  )
  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )

  expect_identical(condition$operation, "list_tables")
  expect_false(grepl(secret, rendered, fixed = TRUE))
  expect_null(condition$location)
})

test_that("discovery record validation enforces shape and requested scope", {
  for (normalizer in list(
    delta.sharing:::.normalize_share_records,
    delta.sharing:::.normalize_schema_records,
    delta.sharing:::.normalize_table_records
  )) {
    expect_error(
      normalizer("not-a-record-list"),
      class = "delta_sharing_protocol_error"
    )
  }

  expect_error(
    delta.sharing:::.normalize_share_records(list(unname(list(name = "x")))),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.normalize_share_records(list(list(id = "missing-name"))),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.normalize_schema_records(
      list(list(share = "operations", name = "default")),
      expected_share = "sales"
    ),
    "outside the requested share",
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.normalize_table_records(
      list(list(
        share = "sales",
        schema = "analytics",
        name = "events"
      )),
      expected_share = "sales",
      expected_schema = "default"
    ),
    "outside the requested scope",
    class = "delta_sharing_protocol_error"
  )

  table <- delta.sharing:::.normalize_table_records(list(list(
    share = "sales",
    schema = "default",
    name = "events",
    accessModes = list()
  )))
  expect_identical(table$access_modes[[1L]], character())
})

test_that("schema and table planners fan out omitted filters in provider order", {
  shares <- delta.sharing:::.normalize_share_records(c(
    discovery_fixture("shares-page-1.json")$items,
    discovery_fixture("shares-page-2.json")$items
  ))

  schemas <- delta.sharing:::.plan_schema_routes(shares = shares)
  tables <- delta.sharing:::.plan_table_routes(shares = shares)

  expect_identical(
    unclass(schemas$path_segments),
    list(
      c("shares", "sales", "schemas"),
      c("shares", "operations", "schemas")
    )
  )
  expect_identical(
    unclass(tables$path_segments),
    list(
      c("shares", "sales", "all-tables"),
      c("shares", "operations", "all-tables")
    )
  )
  expect_identical(
    delta.sharing:::.plan_table_routes(
      share = "Sales Share",
      schema = "schema/name"
    )$path_segments[[1L]],
    c("shares", "Sales Share", "schemas", "schema/name", "tables")
  )
})

test_that("fan-out planning rejects duplicate share records", {
  shares <- data.frame(
    name = c("sales", "sales"),
    stringsAsFactors = FALSE
  )

  expect_error(
    delta.sharing:::.plan_schema_routes(shares = shares),
    class = "delta_sharing_protocol_error"
  )
  expect_error(
    delta.sharing:::.plan_table_routes(shares = shares),
    class = "delta_sharing_protocol_error"
  )
})

test_that("discovery collectors reuse pagination and omitted-filter fan-out", {
  recorder <- new.env(parent = emptyenv())
  recorder$calls <- list()
  fetch_page <- fixture_discovery_fetcher(recorder)

  schemas <- delta.sharing:::.collect_schema_records(fetch_page)
  schema_calls <- recorder$calls

  expect_identical(
    schemas$name,
    c("default", "analytics", "reporting")
  )
  expect_identical(
    vapply(schema_calls, `[[`, character(1), "path"),
    c(
      "/shares",
      "/shares",
      "/shares/sales/schemas",
      "/shares/operations/schemas"
    )
  )
  expect_null(schema_calls[[1L]]$page_token)
  expect_identical(schema_calls[[2L]]$page_token, "page-2")

  recorder$calls <- list()
  tables <- delta.sharing:::.collect_table_records(fetch_page)

  expect_identical(tables$name, c("orders", "forecast", "incidents"))
  expect_identical(
    vapply(recorder$calls, `[[`, character(1), "path"),
    c(
      "/shares",
      "/shares",
      "/shares/sales/all-tables",
      "/shares/operations/all-tables"
    )
  )
})

test_that("discovery collection wraps untyped callback errors safely", {
  secret <- "transport-secret-must-not-leak"
  condition <- expect_error(
    delta.sharing:::.collect_share_records(function(path_segments, page_token) {
      stop(secret)
    }),
    class = "delta_sharing_protocol_error"
  )

  expect_identical(condition$operation, "list_shares")
  expect_false(grepl(secret, conditionMessage(condition), fixed = TRUE))
})

test_that("discovery collection validates hooks and forwards route scope", {
  route <- delta.sharing:::.new_discovery_route_record(
    operation = "list_tables",
    share = NA_character_,
    schema = "default",
    path_segments = c("shares", "sales", "all-tables")
  )
  scope <- NULL
  result <- delta.sharing:::.collect_discovery_route(
    fetch_page = function(path_segments, page_token) {
      list(items = list())
    },
    route = route,
    normalizer = function(records, expected_share, expected_schema) {
      scope <<- list(
        share = expected_share,
        schema = expected_schema
      )
      delta.sharing:::.empty_table_records()
    }
  )
  expect_identical(nrow(result), 0L)
  expect_null(scope$share)
  expect_identical(scope$schema, "default")

  expect_error(
    delta.sharing:::.collect_discovery_route(
      fetch_page = "not-a-function",
      route = route,
      normalizer = identity
    ),
    "hooks must be functions"
  )
  expect_error(
    delta.sharing:::.collect_discovery_route(
      fetch_page = function(...) list(items = list()),
      route = data.frame(),
      normalizer = identity
    ),
    "one discovery route record"
  )

  table <- delta.sharing:::.collect_table_records(
    fixture_discovery_fetcher(),
    share = "sales"
  )
  expect_identical(table$name, c("orders", "forecast"))
})
