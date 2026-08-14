cdfx_fixture <- function(name) {
  directory <- c(
    basic = "b",
    `column-mapping-name` = "n",
    `column-mapping-id` = "i",
    `schema-transition` = "s"
  )[[name]]
  fs::path(test_path("fixtures", "cdfx"), directory)
}

cdfx_file_action_type <- function(action) {
  purrr::detect(c("add", "remove", "cdc"), \(type) {
    !is.null(action[[type]])
  })
}

cdfx_commits <- function(name) {
  root <- cdfx_fixture(name)
  paths <- fs::dir_ls(
    fs::path(root, "l"),
    regexp = "[0-9]+\\.json$",
    type = "file"
  )
  commits <- purrr::map(paths, function(path) {
    actions <- purrr::map(
      readLines(path, warn = FALSE),
      jsonlite::fromJSON,
      simplifyVector = FALSE
    )
    commit_info <- purrr::detect(actions, \(action) !is.null(action$commitInfo))
    list(
      version = as.numeric(fs::path_ext_remove(fs::path_file(path))),
      timestamp = commit_info$commitInfo$timestamp,
      actions = actions
    )
  })
  payload_paths <- commits |>
    purrr::map("actions") |>
    purrr::list_flatten() |>
    purrr::keep(\(action) !is.null(cdfx_file_action_type(action))) |>
    purrr::map_chr(function(action) {
      type <- cdfx_file_action_type(action)
      action[[type]]$path
    }) |>
    unique() |>
    sort()
  list(root = root, commits = commits, payload_paths = payload_paths)
}

cdfx_active_action <- function(commits, type, version) {
  eligible <- purrr::keep(commits, \(commit) commit$version <= version)
  action <- purrr::detect(
    rev(eligible),
    \(commit) purrr::some(commit$actions, \(action) !is.null(action[[type]]))
  )
  purrr::detect(action$actions, \(action) !is.null(action[[type]]))[[type]]
}

cdfx_wire_actions <- function(commit, fixture, start_version) {
  purrr::map(commit$actions, function(action) {
    if (commit$version > start_version && !is.null(action$protocol)) {
      return(list(protocol = list(
        version = commit$version,
        deltaProtocol = action$protocol
      )))
    }
    if (commit$version > start_version && !is.null(action$metaData)) {
      return(list(metaData = list(
        version = commit$version,
        deltaMetadata = action$metaData
      )))
    }

    type <- cdfx_file_action_type(action)
    if (is.null(type)) {
      return(NULL)
    }
    payload_index <- match(action[[type]]$path, fixture$payload_paths)
    source <- fs::path(
      fixture$root,
      "p",
      sprintf("%02d.parquet", payload_index)
    )
    local_action <- action[[type]]
    local_action$path <- local_file_url(source)
    list(file = list(
      id = fixture_file_id(source),
      size = as.numeric(fs::file_size(source)),
      version = commit$version,
      timestamp = commit$timestamp,
      deltaSingleAction = rlang::set_names(list(local_action), type)
    ))
  }) |>
    purrr::compact()
}

cdfx_actions <- function(name, start_version, end_version) {
  fixture <- cdfx_commits(name)
  lines <- list(
    list(protocol = list(
      version = start_version,
      deltaProtocol = cdfx_active_action(
        fixture$commits,
        "protocol",
        start_version
      )
    )),
    list(metaData = list(
      version = start_version,
      deltaMetadata = cdfx_active_action(
        fixture$commits,
        "metaData",
        start_version
      )
    ))
  )
  selected_commits <- purrr::keep(
    fixture$commits,
    \(commit) {
      commit$version >= start_version && commit$version <= end_version
    }
  )
  wire_actions <- selected_commits |>
    purrr::map(
      cdfx_wire_actions,
      fixture = fixture,
      start_version = start_version
    ) |>
    purrr::list_flatten()

  c(lines, wire_actions, list(list(endStreamAction = list())))
}

cdfx_table <- function(name, actions) {
  httr2::local_mocked_responses(function(req) {
    httr2::response(200, body = charToRaw(ndjson_body(actions)))
  }, env = parent.frame())
  shared_table <- test_client()$table(
    paste("fixtures", "default", name, sep = ".")
  )
  if (fs::dir_exists(shared_table$cache_path)) {
    fs::dir_delete(shared_table$cache_path)
  }
  withr::defer(
    if (fs::dir_exists(shared_table$cache_path)) {
      fs::dir_delete(shared_table$cache_path)
    },
    envir = parent.frame()
  )
  shared_table
}

test_that("CDF preserves all four change types", {
  shared_table <- cdfx_table("cdfx-basic", cdfx_actions("basic", 0, 3))
  data <- shared_table$changes(
    starting_version = 0,
    ending_version = 3,
    columns = c(
      "id",
      "name",
      "birthday",
      "_change_type",
      "_commit_version",
      "_commit_timestamp"
    )
  )$to_tibble(batch_size = 3L)

  expect_identical(nrow(data), 23L)
  counts <- table(data$`_change_type`)
  expect_equal(
    as.numeric(counts[c(
      "insert",
      "delete",
      "update_preimage",
      "update_postimage"
    )]),
    c(10L, 1L, 6L, 6L)
  )
  expect_setequal(as.numeric(data$`_commit_version`), 0:3)
})

test_that("CDF retains logical values for both column mapping modes", {
  fixtures <- list(
    list(
      name = "column-mapping-name",
      updated = "Bob",
      before = 200,
      after = 250,
      inserted = "David",
      deleted = "Alice"
    ),
    list(
      name = "column-mapping-id",
      updated = "Frank",
      before = 250,
      after = 275,
      inserted = "Henry",
      deleted = "Grace"
    )
  )

  purrr::walk(fixtures, function(fixture) {
    shared_table <- cdfx_table(
      paste0("cdfx-", fixture$name),
      cdfx_actions(fixture$name, 1, 4)
    )
    data <- shared_table$changes(
      starting_version = 1,
      ending_version = 4,
      columns = c("id", "name", "value", "_change_type", "_commit_version")
    )$to_tibble(batch_size = 2L)

    expect_identical(nrow(data), 4L)
    expect_setequal(
      data$`_change_type`,
      c("update_preimage", "update_postimage", "insert", "delete")
    )
    update <- data[data$name == fixture$updated, ]
    expect_setequal(
      update$`_change_type`,
      c("update_preimage", "update_postimage")
    )
    expect_setequal(as.numeric(update$value), c(fixture$before, fixture$after))
    expect_equal(
      as.numeric(data$`_commit_version`[data$name == fixture$inserted]),
      3
    )
    expect_equal(
      as.numeric(data$`_commit_version`[data$name == fixture$deleted]),
      4
    )
  })
})

test_that("CDF bounds isolate an incompatible schema transition", {
  compatible <- cdfx_table(
    "cdfx-schema-compatible",
    cdfx_actions("schema-transition", 3, 3)
  )
  data <- compatible$changes(
    starting_version = 3,
    ending_version = 3
  )$to_tibble()
  expect_identical(nrow(data), 0L)

  incompatible <- cdfx_table(
    "cdfx-schema-incompatible",
    cdfx_actions("schema-transition", 3, 4)
  )
  expect_error(
    incompatible$changes(
      starting_version = 3,
      ending_version = 4
    )$to_tibble(),
    "Delta Kernel CDF preparation failed",
    fixed = TRUE
  )
})
