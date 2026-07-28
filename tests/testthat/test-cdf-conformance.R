cdf_conformance_root <- function(name) {
  directory <- c(
    basic = "b",
    `column-mapping-name` = "n",
    `column-mapping-id` = "i",
    `schema-transition` = "s"
  )[[name]]
  stopifnot(!is.null(directory))
  normalizePath(
    test_path("fixtures", "cdfx", directory),
    winslash = "/",
    mustWork = TRUE
  )
}

cdf_conformance_commits <- function(name) {
  root <- cdf_conformance_root(name)
  paths <- list.files(
    file.path(root, "l"),
    pattern = "^[0-9]+\\.json$",
    full.names = TRUE
  )
  commits <- lapply(paths, function(path) {
    actions <- lapply(
      readLines(path, warn = FALSE, encoding = "UTF-8"),
      jsonlite::fromJSON,
      simplifyVector = FALSE
    )
    commit_info <- Filter(
      function(action) "commitInfo" %in% names(action),
      actions
    )
    stopifnot(length(commit_info) == 1L)
    list(
      version = as.double(sub("\\.json$", "", basename(path))),
      timestamp = commit_info[[1L]]$commitInfo$timestamp,
      actions = actions
    )
  })
  payload_paths <- sort(unique(unlist(lapply(commits, function(commit) {
    vapply(
      Filter(
        function(action) {
          length(intersect(c("add", "remove", "cdc"), names(action))) == 1L
        },
        commit$actions
      ),
      function(action) {
        type <- intersect(c("add", "remove", "cdc"), names(action))
        action[[type]]$path
      },
      character(1)
    )
  }), use.names = FALSE)))
  list(root = root, commits = commits, payload_paths = payload_paths)
}

cdf_conformance_active_action <- function(commits, type, version) {
  active <- NULL
  for (commit in commits) {
    if (commit$version > version) {
      break
    }
    candidates <- Filter(
      function(action) type %in% names(action),
      commit$actions
    )
    if (length(candidates) > 0L) {
      stopifnot(length(candidates) == 1L)
      active <- candidates[[1L]][[type]]
    }
  }
  stopifnot(!is.null(active))
  active
}

cdf_conformance_response <- function(name, start_version, end_version) {
  fixture <- cdf_conformance_commits(name)
  commits <- fixture$commits
  protocol <- cdf_conformance_active_action(
    commits,
    "protocol",
    start_version
  )
  metadata <- cdf_conformance_active_action(
    commits,
    "metaData",
    start_version
  )
  lines <- list(
    list(protocol = list(
      version = start_version,
      deltaProtocol = protocol
    )),
    list(metaData = list(
      version = start_version,
      size = 0,
      numFiles = 0,
      deltaMetadata = metadata
    ))
  )
  file_paths <- list()
  file_count <- 0L

  for (commit in commits) {
    if (commit$version < start_version ||
        commit$version > end_version) {
      next
    }
    for (action in commit$actions) {
      type <- intersect(c("protocol", "metaData"), names(action))
      if (length(type) == 1L && commit$version > start_version) {
        envelope <- list(version = commit$version)
        envelope[[if (identical(type, "protocol")) {
          "deltaProtocol"
        } else {
          "deltaMetadata"
        }]] <- action[[type]]
        lines[[length(lines) + 1L]] <- stats::setNames(
          list(envelope),
          type
        )
        next
      }

      type <- intersect(c("add", "remove", "cdc"), names(action))
      if (length(type) != 1L) {
        next
      }
      file_count <- file_count + 1L
      payload_index <- match(
        action[[type]]$path,
        fixture$payload_paths
      )
      stopifnot(!is.na(payload_index))
      local_path <- normalizePath(
        file.path(
          fixture$root,
          "p",
          sprintf("%02d.parquet", payload_index)
        ),
        winslash = "/",
        mustWork = TRUE
      )
      wire_path <- paste0(
        "https://fixture.invalid/cdf/",
        name,
        "/",
        file_count,
        ".parquet?signature=cdf-conformance"
      )
      wire_action <- action[[type]]
      wire_action$path <- wire_path
      file_paths[[wire_path]] <- local_path
      delta_action <- stats::setNames(list(wire_action), type)
      lines[[length(lines) + 1L]] <- list(file = list(
        id = paste(name, commit$version, type, file_count, sep = "-"),
        version = commit$version,
        timestamp = commit$timestamp,
        expirationTimestamp = 4102444800000,
        deltaSingleAction = delta_action
      ))
    }
  }
  lines[[length(lines) + 1L]] <- list(endStreamAction = list(
    minUrlExpirationTimestamp = 4102444800000
  ))

  encoded <- vapply(
    lines,
    jsonlite::toJSON,
    character(1),
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  list(
    bytes = charToRaw(paste0(paste(encoded, collapse = "\n"), "\n")),
    start_version = start_version,
    end_version = end_version,
    file_paths = file_paths,
    file_count = file_count
  )
}

cdf_conformance_transport <- function(response, recorder) {
  recorder$opens <- 0L
  recorder$closed <- 0L
  recorder$requests <- list()
  list(
    open = function(request) {
      recorder$opens <- recorder$opens + 1L
      recorder$requests[[recorder$opens]] <- request
      state <- new.env(parent = emptyenv())
      state$status <- 200L
      state$headers <- list(
        "content-type" = "application/x-ndjson",
        "delta-table-version" = as.character(response$start_version),
        "delta-sharing-capabilities" =
          "responseformat=delta;includeendstreamaction=true",
        fileidhash = "delta"
      )
      state$chunks <- split(
        response$bytes,
        ceiling(seq_along(response$bytes) / 23L)
      )
      state$offset <- 1L
      state
    },
    status = function(state) state$status,
    headers = function(state) state$headers,
    pull = function(state) {
      if (state$offset > length(state$chunks)) {
        return(NULL)
      }
      chunk <- state$chunks[[state$offset]]
      state$offset <- state$offset + 1L
      chunk
    },
    close = function(state) {
      recorder$closed <- recorder$closed + 1L
      invisible(NULL)
    },
    retry_after = function(state) NULL
  )
}

cdf_conformance_native_factory <- function(response, recorder) {
  force(response)
  force(recorder)
  function(table_location,
           start_version,
           end_version,
           columns,
           batch_size) {
    state <- delta.sharing:::.validate_snapshot_log_guard(table_location)
    recorder$prepared_root <- state$root
    recorder$validated_https <- character()
    log_dir <- file.path(
      delta.sharing:::.snapshot_log_path(table_location),
      "_delta_log"
    )
    for (version in start_version:end_version) {
      commit <- file.path(log_dir, sprintf("%020.0f.json", version))
      lines <- readLines(commit, warn = FALSE)
      if (length(lines) == 0L) {
        next
      }
      original_mtime <- file.info(commit)$mtime
      rewritten <- vapply(lines, function(line) {
        action <- jsonlite::fromJSON(line, simplifyVector = FALSE)
        type <- intersect(c("add", "remove", "cdc"), names(action))
        if (length(type) == 1L) {
          wire_path <- action[[type]]$path
          stopifnot(startsWith(wire_path, "https://"))
          local_path <- response$file_paths[[wire_path]]
          stopifnot(
            is.character(local_path),
            length(local_path) == 1L,
            file.exists(local_path)
          )
          recorder$validated_https <- c(
            recorder$validated_https,
            wire_path
          )
          prefix <- if (grepl("^[A-Za-z]:/", local_path)) {
            "file:///"
          } else {
            "file://"
          }
          action[[type]]$path <- paste0(
            prefix,
            utils::URLencode(
              local_path,
              reserved = FALSE,
              repeated = TRUE
            )
          )
        }
        jsonlite::toJSON(
          action,
          auto_unbox = TRUE,
          null = "null",
          digits = NA
        )
      }, character(1))
      writeLines(rewritten, commit, useBytes = TRUE)
      Sys.setFileTime(commit, original_mtime)
    }
    tryCatch(
      delta.sharing:::.native_cdf_stream(
        table_location = table_location,
        start_version = start_version,
        end_version = end_version,
        columns = columns,
        batch_size = batch_size
      ),
      error = function(condition) {
        recorder$native_condition <- condition
        stop(condition)
      }
    )
  }
}

cdf_conformance_interface <- function(response, recorder) {
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("static bearer authentication must not perform auth HTTP")
    }),
    snapshot_transport = cdf_conformance_transport(response, recorder),
    clock = function() as.POSIXct("2026-07-29", tz = "UTC"),
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    native_cdf_stream_factory =
      cdf_conformance_native_factory(response, recorder)
  )
  delta.sharing:::.new_execution_interface(callbacks)
}

test_that("production CDF path preserves all four change types", {
  response <- cdf_conformance_response("basic", 0, 3)
  recorder <- new.env(parent = emptyenv())
  changes <- sharing_changes(
    test_table(),
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
  )
  data <- delta.sharing:::.with_execution_interface(
    cdf_conformance_interface(response, recorder),
    read_data_frame(changes, batch_size = 3L)
  )

  expect_identical(recorder$opens, 1L)
  expect_identical(recorder$closed, 1L)
  expect_identical(length(recorder$validated_https), response$file_count)
  expect_identical(response$file_count, 36L)
  expect_false(file.exists(recorder$prepared_root))
  expect_identical(
    names(data),
    c(
      "id",
      "name",
      "birthday",
      "_change_type",
      "_commit_version",
      "_commit_timestamp"
    )
  )
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
  expect_setequal(as.numeric(unique(data$`_commit_version`)), 0:3)

  bob <- data[
    as.numeric(data$id) == 2 &
      as.numeric(data$`_commit_version`) == 1,
    ,
    drop = FALSE
  ]
  expect_setequal(
    bob$`_change_type`,
    c("update_preimage", "update_postimage")
  )
  expect_setequal(
    as.character(bob$birthday),
    c("2023-12-22", "2023-12-23")
  )
  timestamps <- vapply(
    split(
      as.numeric(data$`_commit_timestamp`) * 1000,
      as.numeric(data$`_commit_version`)
    ),
    function(value) unique(value),
    numeric(1)
  )
  expect_equal(
    unname(timestamps),
    c(1703265018828, 1703265021675, 1703886093785, 1704559499570)
  )
})

test_that("production CDF diagnostics survive deterministic early release", {
  gc()
  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  response <- cdf_conformance_response("basic", 1, 1)
  recorder <- new.env(parent = emptyenv())
  changes <- sharing_changes(
    test_table(),
    starting_version = 1,
    ending_version = 1,
    columns = c("id", "_change_type", "_commit_version")
  )
  stream <- delta.sharing:::.with_execution_interface(
    cdf_conformance_interface(response, recorder),
    read_arrow_stream(changes, batch_size = 1L)
  )
  diagnostics <- read_diagnostics(stream)

  expect_true(S7::S7_inherits(diagnostics, SharingReadDiagnostics))
  expect_identical(diagnostics@read_kind, "cdf")
  expect_identical(diagnostics@response_format, "delta")
  expect_identical(diagnostics@starting_version, 1)
  expect_identical(diagnostics@ending_version, 1)
  expect_identical(diagnostics@page_count, 1)
  expect_identical(diagnostics@file_count, 12)
  expect_identical(
    diagnostics@columns,
    c("id", "_change_type", "_commit_version")
  )
  expect_identical(diagnostics@batch_size, 1)
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before + 1
  )
  expect_equal(stream$get_next()$length, 1L)
  expect_true(file.exists(recorder$prepared_root))

  stream$release()
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_false(file.exists(recorder$prepared_root))
  expect_identical(read_diagnostics(stream), diagnostics)
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before
  )
})

test_that("name and ID column mapping retain logical CDF values", {
  fixtures <- list(
    list(
      name = "column-mapping-name",
      update_name = "Bob",
      before = 200,
      after = 250,
      inserted_name = "David",
      deleted_name = "Alice"
    ),
    list(
      name = "column-mapping-id",
      update_name = "Frank",
      before = 250,
      after = 275,
      inserted_name = "Henry",
      deleted_name = "Grace"
    )
  )

  for (fixture in fixtures) {
    response <- cdf_conformance_response(fixture$name, 1, 4)
    recorder <- new.env(parent = emptyenv())
    changes <- sharing_changes(
      test_table(),
      starting_version = 1,
      ending_version = 4,
      columns = c(
        "id",
        "name",
        "value",
        "_change_type",
        "_commit_version"
      )
    )
    data <- delta.sharing:::.with_execution_interface(
      cdf_conformance_interface(response, recorder),
      read_data_frame(changes, batch_size = 2L)
    )

    expect_false(file.exists(recorder$prepared_root))
    expect_identical(nrow(data), 4L)
    expect_setequal(
      data$`_change_type`,
      c("update_preimage", "update_postimage", "insert", "delete")
    )
    update <- data[data$name == fixture$update_name, , drop = FALSE]
    expect_setequal(update$`_change_type`, c(
      "update_preimage",
      "update_postimage"
    ))
    expect_setequal(as.numeric(update$value), c(
      fixture$before,
      fixture$after
    ))
    expect_identical(
      as.numeric(data$`_commit_version`[data$name == fixture$inserted_name]),
      3
    )
    expect_identical(
      as.numeric(data$`_commit_version`[data$name == fixture$deleted_name]),
      4
    )
  }
})

test_that("inclusive bounds isolate an incompatible schema transition", {
  compatible <- cdf_conformance_response("schema-transition", 3, 3)
  compatible_recorder <- new.env(parent = emptyenv())
  compatible_changes <- sharing_changes(
    test_table(),
    starting_version = 3,
    ending_version = 3
  )
  data <- delta.sharing:::.with_execution_interface(
    cdf_conformance_interface(compatible, compatible_recorder),
    read_data_frame(compatible_changes)
  )
  expect_identical(nrow(data), 0L)
  expect_false(file.exists(compatible_recorder$prepared_root))

  incompatible <- cdf_conformance_response("schema-transition", 3, 4)
  incompatible_recorder <- new.env(parent = emptyenv())
  incompatible_changes <- sharing_changes(
    test_table(),
    starting_version = 3,
    ending_version = 4
  )
  condition <- expect_error(
    delta.sharing:::.with_execution_interface(
      cdf_conformance_interface(incompatible, incompatible_recorder),
      read_arrow_stream(incompatible_changes)
    ),
    class = "delta_sharing_native_error"
  )
  expect_match(
    conditionMessage(incompatible_recorder$native_condition),
    "Delta Kernel CDF preparation failed",
    fixed = TRUE
  )
  expect_false(grepl(
    "nullable",
    conditionMessage(condition),
    fixed = TRUE
  ))
  expect_false(file.exists(incompatible_recorder$prepared_root))
})
