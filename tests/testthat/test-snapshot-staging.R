staging_fixture_components <- function(name = "snapshot-delta.ndjson") {
  path <- test_path("fixtures", "protocol", name)
  bytes <- readBin(path, what = "raw", n = file.info(path)$size)
  decoder <- delta.sharing:::.new_ndjson_decoder("snapshot staging fixture")
  actions <- c(
    delta.sharing:::.ndjson_decoder_push(decoder, bytes),
    delta.sharing:::.ndjson_decoder_finish(decoder)
  )
  list(
    protocol = actions[[1L]]$value,
    metadata = actions[[2L]]$value,
    files = lapply(
      Filter(function(action) identical(action$type, "file"), actions),
      `[[`,
      "value"
    )
  )
}

staging_commit_path <- function(guard) {
  file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log",
    "00000000000000000000.json"
  )
}

staging_commit_raw <- function(guard) {
  path <- staging_commit_path(guard)
  readBin(path, what = "raw", n = file.info(path)$size)
}

staging_clone_add <- function(file, id, path = NULL) {
  state <- delta.sharing:::.snapshot_file_state(file)
  add <- state$delta_action$add
  add$path <- if (is.null(path)) {
    sprintf(
      "https://objects.example.test/%s.parquet?fixture=staging",
      id
    )
  } else {
    path
  }
  add$deletionVector <- NULL
  delta.sharing:::.new_private_snapshot_file(
    id = id,
    action_type = "add",
    delta_action = list(add = add),
    expiration_timestamp = 4102444800000,
    response_format = state$response_format
  )
}

staging_actions_bytes <- function(actions) {
  lines <- vapply(actions, function(action) {
    unclass(jsonlite::toJSON(
      action,
      auto_unbox = TRUE,
      null = "null",
      digits = NA
    ))
  }, character(1))
  charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
}

staging_parent <- function(prefix) {
  parent <- tempfile(prefix)
  dir.create(parent)
  parent
}

staging_parent_entries <- function(parent) {
  list.files(parent, all.files = TRUE, no.. = TRUE)
}

staging_parquet_headers <- function(version = "42") {
  planned_snapshot_headers(
    version = version,
    capabilities = "responseformat=parquet"
  )
}

test_that("disk-backed runs preserve exact synthetic commit bytes", {
  components <- staging_fixture_components()
  seed <- components$files[[2L]]
  files <- rev(lapply(seq_len(40L), function(index) {
    staging_clone_add(seed, sprintf("staged-%03d", index))
  }))
  parent <- staging_parent("snapshot-staging-equivalence-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  expected <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    files,
    temp_parent = parent
  )
  stage <- delta.sharing:::.new_snapshot_stage(
    parent,
    run_files = 3L
  )
  delta.sharing:::.initialize_snapshot_stage(
    stage,
    components$protocol,
    components$metadata
  )
  for (file in files) {
    delta.sharing:::.snapshot_stage_add_file(stage, file)
  }
  actual <- delta.sharing:::.publish_snapshot_stage(stage)
  on.exit(delta.sharing:::.release_snapshot_log(expected), add = TRUE)
  on.exit(delta.sharing:::.release_snapshot_log(actual), add = TRUE)

  expect_identical(staging_commit_raw(actual), staging_commit_raw(expected))
  actual_state <- delta.sharing:::.validate_snapshot_log_guard(actual)
  expect_false(dir.exists(file.path(actual_state$root, ".runs")))
  expect_false(dir.exists(file.path(actual_state$root, ".merge")))
  if (.Platform$OS.type != "windows") {
    expect_identical(
      as.integer(file.info(actual_state$root)$mode),
      as.integer(as.octmode("0700"))
    )
    expect_identical(
      as.integer(file.info(staging_commit_path(actual))$mode),
      as.integer(as.octmode("0600"))
    )
  }
})

test_that("multi-page production staging is byte-equivalent and cleans changes", {
  page_one_bytes <- planned_snapshot_bytes("snapshot-page-1.ndjson")
  page_two_bytes <- planned_snapshot_bytes("snapshot-page-2.ndjson")
  page_one <- delta.sharing:::.consume_snapshot_page(
    planned_pull_response(page_one_bytes)
  )
  page_two <- delta.sharing:::.consume_snapshot_page(
    planned_pull_response(page_two_bytes)
  )
  parent <- staging_parent("snapshot-staging-pages-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  expected <- delta.sharing:::.prepare_snapshot_log(
    page_one$protocol,
    page_one$metadata,
    c(page_one$files, page_two$files),
    temp_parent = parent
  )
  recorders <- list()
  prepared <- delta.sharing:::.prepare_snapshot_read(
    sharing_read(test_table()),
    fetch = function(request) {
      recorder <- new.env(parent = emptyenv())
      recorders[[length(recorders) + 1L]] <<- recorder
      planned_pull_response(
        if (request$page_number == 1L) page_one_bytes else page_two_bytes,
        recorder = recorder
      )
    },
    temp_parent = parent,
    stage_run_files = 1L
  )
  guard <- delta.sharing:::.prepared_snapshot_state(prepared)$guard
  expect_identical(staging_commit_raw(guard), staging_commit_raw(expected))
  expect_true(all(vapply(
    recorders,
    function(recorder) identical(recorder$closes, 1L),
    logical(1)
  )))
  delta.sharing:::.release_prepared_snapshot(prepared)
  delta.sharing:::.release_snapshot_log(expected)
  expect_identical(staging_parent_entries(parent), character())

  changed <- sub(
    '"id":"paged-table"',
    '"id":"changed-after-staging"',
    rawToChar(page_two_bytes),
    fixed = TRUE
  )
  changed_recorders <- list()
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) {
        recorder <- new.env(parent = emptyenv())
        changed_recorders[[length(changed_recorders) + 1L]] <<- recorder
        planned_pull_response(
          if (request$page_number == 1L) {
            page_one_bytes
          } else {
            charToRaw(changed)
          },
          recorder = recorder
        )
      },
      temp_parent = parent,
      stage_run_files = 1L
    ),
    "changed across pages",
    class = "delta_sharing_protocol_error"
  )
  expect_identical(staging_parent_entries(parent), character())
  expect_true(all(vapply(
    changed_recorders,
    function(recorder) identical(recorder$closes, 1L),
    logical(1)
  )))

  page_two_lines <- readLines(
    test_path("fixtures", "protocol", "snapshot-page-2.ndjson"),
    warn = FALSE
  )
  mixed <- page_two_lines
  mixed[[3L]] <- unclass(jsonlite::toJSON(
    list(file = list(
      url = "https://objects.example.test/mixed.parquet",
      id = "mixed-format-file",
      partitionValues = structure(list(), names = character()),
      size = 50,
      expirationTimestamp = 4102444700000
    )),
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  ))
  malformed <- page_two_lines
  malformed[[3L]] <- paste0(
    '{"file":{"id":"malformed-file",',
    '"deltaSingleAction":{"future":{}}}}'
  )
  for (invalid_lines in list(mixed, malformed)) {
    expect_error(
      delta.sharing:::.prepare_snapshot_read(
        sharing_read(test_table()),
        fetch = function(request) {
          planned_pull_response(
            if (request$page_number == 1L) {
              page_one_bytes
            } else {
              charToRaw(paste0(
                paste(invalid_lines, collapse = "\n"),
                "\n"
              ))
            }
          )
        },
        temp_parent = parent,
        stage_run_files = 1L
      ),
      class = "delta_sharing_protocol_error"
    )
    expect_identical(staging_parent_entries(parent), character())
  }
})

test_that("duplicate IDs and paths fail within and beyond merge fan-in", {
  components <- staging_fixture_components()
  seed <- components$files[[2L]]

  exercise_duplicate <- function(files, run_files) {
    parent <- staging_parent("snapshot-staging-duplicate-")
    on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
    stage <- delta.sharing:::.new_snapshot_stage(parent, run_files)
    delta.sharing:::.initialize_snapshot_stage(
      stage,
      components$protocol,
      components$metadata
    )
    for (file in files) {
      delta.sharing:::.snapshot_stage_add_file(stage, file)
    }
    condition <- expect_error(
      delta.sharing:::.publish_snapshot_stage(stage),
      "duplicate",
      class = "delta_sharing_protocol_error"
    )
    root <- delta.sharing:::.snapshot_stage_state(stage)$root
    open_connections <- showConnections(all = TRUE)
    descriptions <- if (nrow(open_connections) == 0L) {
      character()
    } else {
      open_connections[, "description"]
    }
    expect_false(any(startsWith(descriptions, root)))
    expect_true(delta.sharing:::.release_snapshot_stage(stage))
    expect_false(file.exists(root))
    expect_identical(staging_parent_entries(parent), character())
    condition
  }

  first <- staging_clone_add(seed, "duplicate-id")
  same_id <- staging_clone_add(seed, "duplicate-id")
  exercise_duplicate(list(first, same_id), run_files = 10L)
  same_run_path <- staging_clone_add(
    seed,
    "different-id",
    path = delta.sharing:::.snapshot_file_state(first)$delta_action$add$path
  )
  exercise_duplicate(list(first, same_run_path), run_files = 10L)

  unique <- lapply(seq_len(34L), function(index) {
    staging_clone_add(seed, sprintf("unique-%03d", index))
  })
  id_state <- delta.sharing:::.snapshot_file_state(unique[[1L]])
  id_action <- id_state$delta_action$add
  id_action$path <- "https://objects.example.test/duplicate-id-tail.parquet"
  duplicate_id <- delta.sharing:::.new_private_snapshot_file(
    id_state$id,
    "add",
    list(add = id_action),
    expiration_timestamp = 4102444800000
  )
  exercise_duplicate(c(unique, list(duplicate_id)), run_files = 1L)

  path_state <- delta.sharing:::.snapshot_file_state(unique[[1L]])
  duplicate_path <- staging_clone_add(
    seed,
    "unique-tail-path",
    path = path_state$delta_action$add$path
  )
  exercise_duplicate(c(unique, list(duplicate_path)), run_files = 1L)
})

test_that("commit-source writers release on errors and early returns", {
  bytes <- planned_snapshot_bytes("snapshot-page-2.ndjson")
  parent <- staging_parent("snapshot-staging-writer-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  seen_source <- NULL
  condition <- expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) planned_pull_response(bytes),
      temp_parent = parent,
      stage_run_files = 1L,
      write_commit = function(path, lines) {
        expect_s3_class(lines, "delta_sharing_snapshot_commit_source")
        seen_source <<- lines
        expect_true(dir.exists(file.path(
          dirname(dirname(dirname(path))),
          ".runs"
        )))
        lines$next_line()
        lines$next_line()
        lines$next_line()
        stop("injected writer error private text", call. = FALSE)
      }
    ),
    class = "delta_sharing_protocol_error"
  )
  expect_false(grepl(
    "injected writer error private text",
    planned_condition_text(condition),
    fixed = TRUE
  ))
  expect_null(seen_source$next_line())
  expect_identical(staging_parent_entries(parent), character())

  early_source <- NULL
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) planned_pull_response(bytes),
      temp_parent = parent,
      write_commit = function(path, lines) {
        early_source <<- lines
        writeLines(lines$next_line(), path, useBytes = TRUE)
        invisible(path)
      }
    ),
    "complete staged manifest",
    class = "delta_sharing_protocol_error"
  )
  expect_null(early_source$next_line())
  expect_identical(staging_parent_entries(parent), character())
})

test_that("run-record delimiters fail closed during wire normalization", {
  components <- staging_fixture_components()
  state <- delta.sharing:::.snapshot_file_state(components$files[[2L]])
  add <- state$delta_action$add
  wrappers <- list(
    list(
      id = "bad\tid",
      deltaSingleAction = list(add = add)
    ),
    list(
      id = "bad\nid",
      deltaSingleAction = list(add = add)
    ),
    list(
      id = "safe-id",
      deltaSingleAction = list(add = within(add, {
        path <- "https://objects.example.test/bad\tpath"
      }))
    ),
    list(
      id = "safe-id",
      deltaSingleAction = list(add = within(add, {
        path <- "https://objects.example.test/bad\npath"
      }))
    )
  )
  for (wrapper in wrappers) {
    expect_error(
      delta.sharing:::.normalize_snapshot_file_action(wrapper),
      class = "delta_sharing_protocol_error"
    )
  }
})

test_that("Parquet totals, versions, and Delta DV protocol stay enforced", {
  parquet_actions <- lapply(
    readLines(
      test_path(
        "fixtures",
        "protocol",
        "snapshot-parquet-kernel-proof.ndjson"
      ),
      warn = FALSE
    ),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
  parent <- staging_parent("snapshot-staging-features-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  prepare <- function(actions, headers = staging_parquet_headers()) {
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table(), response_format = "parquet"),
      fetch = function(request) {
        planned_pull_response(
          staging_actions_bytes(actions),
          headers = headers
        )
      },
      temp_parent = parent,
      stage_run_files = 1L
    )
  }
  valid <- prepare(parquet_actions)
  expect_true(delta.sharing:::.release_prepared_snapshot(valid))
  expect_identical(staging_parent_entries(parent), character())

  invalid_count <- parquet_actions
  invalid_count[[2L]]$metaData$numFiles <- 2
  expect_error(
    prepare(invalid_count),
    "file count",
    class = "delta_sharing_protocol_error"
  )
  invalid_size <- parquet_actions
  invalid_size[[2L]]$metaData$size <- 1
  expect_error(
    prepare(invalid_size),
    "total size",
    class = "delta_sharing_protocol_error"
  )
  invalid_version <- parquet_actions
  invalid_version[[3L]]$file$version <- 41
  expect_error(
    prepare(invalid_version),
    "versions",
    class = "delta_sharing_protocol_error"
  )
  expect_identical(staging_parent_entries(parent), character())

  delta_lines <- readLines(
    test_path("fixtures", "protocol", "snapshot-delta.ndjson"),
    warn = FALSE
  )
  invalid_protocol <- jsonlite::fromJSON(
    delta_lines[[1L]],
    simplifyVector = FALSE
  )
  invalid_protocol$protocol$deltaProtocol <- list(
    minReaderVersion = 1,
    minWriterVersion = 2
  )
  delta_lines[[1L]] <- unclass(jsonlite::toJSON(
    invalid_protocol,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  ))
  expect_error(
    delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table(), response_format = "delta"),
      fetch = function(request) {
        planned_pull_response(
          charToRaw(paste0(paste(delta_lines, collapse = "\n"), "\n")),
          headers = planned_snapshot_headers(
            capabilities = "responseformat=delta"
          )
        )
      },
      temp_parent = parent,
      stage_run_files = 1L
    ),
    "deletion vectors",
    class = "delta_sharing_protocol_error"
  )
  expect_identical(staging_parent_entries(parent), character())
})

test_that("abandoned staging and prepared roots finalize exactly", {
  parent <- staging_parent("snapshot-staging-finalizer-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  stage_root <- local({
    stage <- delta.sharing:::.new_snapshot_stage(parent, run_files = 1L)
    delta.sharing:::.snapshot_stage_state(stage)$root
  })
  gc()
  expect_false(file.exists(stage_root))

  prepared_root <- local({
    prepared <- delta.sharing:::.prepare_snapshot_read(
      sharing_read(test_table()),
      fetch = function(request) {
        planned_pull_response(
          planned_snapshot_bytes("snapshot-page-2.ndjson")
        )
      },
      temp_parent = parent,
      stage_run_files = 1L
    )
    delta.sharing:::.prepared_snapshot_state(prepared)$guard$state$root
  })
  attempts <- 0L
  while (file.exists(prepared_root) && attempts < 10L) {
    gc()
    attempts <- attempts + 1L
  }
  expect_false(file.exists(prepared_root))
  expect_identical(staging_parent_entries(parent), character())
})
