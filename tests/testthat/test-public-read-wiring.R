public_snapshot_transport <- function(specifications,
                                      recorder = new.env(parent = emptyenv())) {
  recorder$opens <- 0L
  recorder$requests <- list()
  recorder$closed <- integer(length(specifications))

  list(
    open = function(request) {
      recorder$opens <- recorder$opens + 1L
      recorder$requests[[recorder$opens]] <- request
      specification <- specifications[[recorder$opens]]
      chunks <- split(
        specification$bytes,
        ceiling(seq_along(specification$bytes) / specification$chunk_bytes)
      )
      response <- new.env(parent = emptyenv())
      response$status <- specification$status
      response$headers <- specification$headers
      response$chunks <- unname(chunks)
      response$offset <- 1L
      response$index <- recorder$opens
      response
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    pull = function(response) {
      if (response$offset > length(response$chunks)) {
        return(NULL)
      }
      chunk <- response$chunks[[response$offset]]
      response$offset <- response$offset + 1L
      chunk
    },
    close = function(response) {
      recorder$closed[[response$index]] <-
        recorder$closed[[response$index]] + 1L
      invisible(NULL)
    },
    retry_after = function(response) NULL
  )
}

public_snapshot_specification <- function(
  name = "snapshot-page-2.ndjson",
  status = 200L,
  chunk_bytes = 19L,
  headers = planned_snapshot_headers()
) {
  list(
    status = status,
    headers = headers,
    bytes = planned_snapshot_bytes(name),
    chunk_bytes = chunk_bytes
  )
}

public_local_snapshot_bytes <- function() {
  table_path <- normalizePath(
    test_path("fixtures", "delta", "local-table"),
    winslash = "/",
    mustWork = TRUE
  )
  commit <- readLines(
    file.path(
      table_path,
      "_delta_log",
      "00000000000000000000.json"
    ),
    warn = FALSE
  )
  actions <- lapply(commit, function(line) {
    jsonlite::fromJSON(line, simplifyVector = FALSE)
  })
  protocol <- actions[[1L]]$protocol
  metadata <- actions[[2L]]$metaData
  adds <- lapply(actions[3:4], `[[`, "add")
  lines <- c(
    jsonlite::toJSON(
      list(protocol = list(deltaProtocol = protocol)),
      auto_unbox = TRUE
    ),
    jsonlite::toJSON(
      list(metaData = list(
        version = 42,
        size = sum(vapply(adds, `[[`, numeric(1), "size")),
        numFiles = length(adds),
        deltaMetadata = metadata
      )),
      auto_unbox = TRUE
    ),
    vapply(seq_along(adds), function(index) {
      add <- adds[[index]]
      add$path <- paste0(
        "https://fixture.invalid/",
        basename(add$path),
        "?signature=public-local-fixture"
      )
      jsonlite::toJSON(
        list(file = list(
          id = paste0("local-", index),
          expirationTimestamp = 4102444800000,
          deltaSingleAction = list(add = add)
        )),
        auto_unbox = TRUE
      )
    }, character(1)),
    jsonlite::toJSON(
      list(minUrlExpirationTimestamp = 4102444800000),
      auto_unbox = TRUE
    )
  )
  charToRaw(paste0(paste(lines, collapse = "\n"), "\n"))
}

public_local_native_factory <- function(recorder) {
  force(recorder)
  function(table_location, columns, limit, batch_size) {
    root <- delta.sharing:::.validate_snapshot_log_guard(
      table_location
    )$root
    commit_path <- file.path(
      delta.sharing:::.snapshot_log_path(table_location),
      "_delta_log",
      "00000000000000000000.json"
    )
    lines <- readLines(commit_path, warn = FALSE)
    actions <- lapply(lines, function(line) {
      jsonlite::fromJSON(line, simplifyVector = FALSE)
    })
    add_indexes <- which(vapply(
      actions,
      function(action) "add" %in% names(action),
      logical(1)
    ))
    recorder$normalized_https <- vapply(
      actions[add_indexes],
      function(action) startsWith(action$add$path, "https://"),
      logical(1)
    )
    fixture_root <- normalizePath(
      test_path("fixtures", "delta", "local-table"),
      winslash = "/",
      mustWork = TRUE
    )
    for (index in add_indexes) {
      file_name <- basename(sub(
        "\\?.*$",
        "",
        actions[[index]]$add$path
      ))
      local_path <- file.path(fixture_root, file_name)
      actions[[index]]$add$path <- paste0(
        "file://",
        utils::URLencode(
          local_path,
          reserved = FALSE,
          repeated = TRUE
        )
      )
      lines[[index]] <- jsonlite::toJSON(
        actions[[index]],
        auto_unbox = TRUE
      )
    }
    writeLines(lines, commit_path, useBytes = TRUE)
    recorder$root <- root
    delta.sharing:::.native_snapshot_stream(
      table_location,
      columns = columns,
      limit = limit,
      batch_size = batch_size
    )
  }
}

public_read_interface <- function(snapshot_transport,
                                  temp_parent,
                                  native_stream_factory =
                                    delta.sharing:::.native_snapshot_stream,
                                  auth_handler = function(request) {
                                    stop("unexpected auth request")
                                  },
                                  clock = function() {
                                    as.POSIXct(
                                      "2026-07-29 00:00:00",
                                      tz = "UTC"
                                    )
                                  }) {
  delta.sharing:::.new_execution_interface(
    delta.sharing:::.new_control_execution_callbacks(
      transport = delta.sharing:::.fake_http_transport(auth_handler),
      snapshot_transport = snapshot_transport,
      clock = clock,
      sleeper = function(seconds) NULL,
      random = function(...) 0,
      max_attempts = 1L,
      snapshot_temp_parent = temp_parent,
      native_stream_factory = native_stream_factory
    )
  )
}

test_that("public snapshot reads forward exact limits and batch size once", {
  parent <- tempfile("public-read-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  recorder <- new.env(parent = emptyenv())
  transport <- public_snapshot_transport(
    list(
      public_snapshot_specification("snapshot-page-1.ndjson", chunk_bytes = 7L),
      public_snapshot_specification("snapshot-page-2.ndjson", chunk_bytes = 5L)
    ),
    recorder
  )
  native_calls <- list()
  native_factory <- function(table_location,
                             columns,
                             limit,
                             batch_size) {
    native_calls[[length(native_calls) + 1L]] <<- list(
      table_location = table_location,
      columns = columns,
      limit = limit,
      batch_size = batch_size
    )
    delta.sharing:::.native_snapshot_stream(
      table_location,
      columns = columns,
      limit = 0,
      batch_size = batch_size
    )
  }
  interface <- public_read_interface(
    transport,
    temp_parent = parent,
    native_stream_factory = native_factory
  )

  stream <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(
      sharing_read(
        test_table(),
        columns = "id",
        limit = 2^40
      ),
      batch_size = 7
    )
  })
  on.exit(stream$release(), add = TRUE)

  expect_length(native_calls, 1L)
  expect_s3_class(
    native_calls[[1L]]$table_location,
    "delta_sharing_snapshot_log"
  )
  expect_identical(native_calls[[1L]]$columns, "id")
  expect_identical(native_calls[[1L]]$limit, 2^40)
  expect_identical(native_calls[[1L]]$batch_size, 7L)
  expect_identical(recorder$opens, 2L)
  expect_identical(recorder$closed, c(1L, 1L))
  expect_false("limitHint" %in% names(recorder$requests[[1L]]$body))
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 1L)
  stream$release()
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
})

test_that("public Kernel reads enforce exact rows, batches, and projection", {
  parent <- tempfile("public-kernel-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  transport_recorder <- new.env(parent = emptyenv())
  native_recorder <- new.env(parent = emptyenv())
  interface <- public_read_interface(
    public_snapshot_transport(
      list(list(
        status = 200L,
        headers = planned_snapshot_headers(),
        bytes = public_local_snapshot_bytes(),
        chunk_bytes = 11L
      )),
      transport_recorder
    ),
    temp_parent = parent,
    native_stream_factory = public_local_native_factory(native_recorder)
  )

  stream <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(
      sharing_read(
        test_table(),
        columns = c("group", "id"),
        limit = 5
      ),
      batch_size = 2
    )
  })
  on.exit(stream$release(), add = TRUE)

  expect_true(all(native_recorder$normalized_https))
  expect_named(stream$get_schema()$children, c("group", "id"))
  batches <- list(
    stream$get_next(),
    stream$get_next(),
    stream$get_next()
  )
  batch_rows <- vapply(batches, `[[`, integer(1), "length")
  expect_length(batch_rows, 3L)
  expect_true(all(batch_rows <= 2L))
  expect_identical(sum(batch_rows), 5L)
  expect_null(stream$get_next())
  expect_false(file.exists(native_recorder$root))
  expect_identical(transport_recorder$closed, 1L)
  stream$release()

  early_parent <- tempfile("public-kernel-early-parent-")
  dir.create(early_parent)
  on.exit(
    unlink(early_parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  early_native <- new.env(parent = emptyenv())
  early_interface <- public_read_interface(
    public_snapshot_transport(
      list(list(
        status = 200L,
        headers = planned_snapshot_headers(),
        bytes = public_local_snapshot_bytes(),
        chunk_bytes = 13L
      ))
    ),
    temp_parent = early_parent,
    native_stream_factory = public_local_native_factory(early_native)
  )
  early <- delta.sharing:::.with_execution_interface(early_interface, {
    read_arrow_stream(
      sharing_read(test_table(), columns = c("group", "id")),
      batch_size = 2
    )
  })
  expect_identical(early$get_next()$length, 2L)
  expect_true(file.exists(early_native$root))
  early$release()
  expect_false(file.exists(early_native$root))
})

test_that("public stream release and exhaustion own synthetic-log cleanup", {
  run_read <- function(limit) {
    parent <- tempfile("public-lifecycle-parent-")
    dir.create(parent)
    recorder <- new.env(parent = emptyenv())
    interface <- public_read_interface(
      public_snapshot_transport(
        list(public_snapshot_specification()),
        recorder
      ),
      temp_parent = parent
    )
    stream <- delta.sharing:::.with_execution_interface(interface, {
      read_arrow_stream(sharing_read(test_table(), limit = limit))
    })
    list(parent = parent, recorder = recorder, stream = stream)
  }

  early <- run_read(3)
  on.exit(
    unlink(early$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  expect_length(list.files(
    early$parent,
    all.files = TRUE,
    no.. = TRUE
  ), 1L)
  early$stream$release()
  expect_length(list.files(
    early$parent,
    all.files = TRUE,
    no.. = TRUE
  ), 0L)
  expect_identical(early$recorder$closed, 1L)

  exhausted <- run_read(0)
  on.exit(
    unlink(exhausted$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  expect_null(exhausted$stream$get_next())
  expect_length(list.files(
    exhausted$parent,
    all.files = TRUE,
    no.. = TRUE
  ), 0L)
  exhausted$stream$release()
  expect_identical(exhausted$recorder$closed, 1L)
})

test_that("native construction failure closes and removes every R resource", {
  parent <- tempfile("public-failure-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  recorder <- new.env(parent = emptyenv())
  secret <- "native-construction-private-secret"
  native_calls <- 0L
  interface <- public_read_interface(
    public_snapshot_transport(
      list(public_snapshot_specification()),
      recorder
    ),
    temp_parent = parent,
    native_stream_factory = function(...) {
      native_calls <<- native_calls + 1L
      stop(secret)
    }
  )

  condition <- expect_error(
    delta.sharing:::.with_execution_interface(interface, {
      read_arrow_stream(sharing_read(
        test_table(),
        predicate = list(
          op = "equal",
          column = "region",
          value = "predicate-private-secret"
        )
      ))
    }),
    class = "delta_sharing_native_error"
  )

  expect_identical(native_calls, 1L)
  expect_identical(recorder$closed, 1L)
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
  rendered <- paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )
  expect_false(grepl(secret, rendered, fixed = TRUE))
  expect_false(grepl("predicate-private-secret", rendered, fixed = TRUE))
})

test_that("a factory that declines cleanup ownership fails closed", {
  parent <- tempfile("public-transfer-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  recorder <- new.env(parent = emptyenv())
  dummy <- NULL
  captured_root <- NULL
  interface <- public_read_interface(
    public_snapshot_transport(
      list(public_snapshot_specification()),
      recorder
    ),
    temp_parent = parent,
    native_stream_factory = function(table_location, ...) {
      captured_root <<-
        delta.sharing:::.validate_snapshot_log_guard(table_location)$root
      dummy <<- delta.sharing:::.native_test_stream()
      dummy
    }
  )

  condition <- expect_error(
    delta.sharing:::.with_execution_interface(interface, {
      read_arrow_stream(sharing_read(test_table()))
    }),
    class = "delta_sharing_native_error"
  )
  on.exit(if (!is.null(dummy)) dummy$release(), add = TRUE)

  expect_match(
    conditionMessage(condition),
    "cleanup ownership",
    fixed = TRUE
  )
  expect_false(file.exists(captured_root))
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
  expect_identical(recorder$closed, 1L)
  dummy$release()
})

test_that("public read controls fail before snapshot I/O", {
  parent <- tempfile("public-controls-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  recorder <- new.env(parent = emptyenv())
  interface <- public_read_interface(
    public_snapshot_transport(
      list(public_snapshot_specification()),
      recorder
    ),
    temp_parent = parent
  )

  delta.sharing:::.with_execution_interface(interface, {
    expect_error(
      read_arrow_stream(sharing_read(test_table()), batch_size = 0),
      class = "delta_sharing_validation_error"
    )
    expect_error(
      read_arrow_stream(sharing_read(test_table()), concurrency = 1),
      class = "delta_sharing_unsupported_error"
    )
    expect_error(
      read_arrow_stream(sharing_read(test_table()), concurrency = 0),
      class = "delta_sharing_validation_error"
    )
    expect_error(
      read_arrow_stream(sharing_changes(
        test_table(),
        starting_version = 1
      )),
      class = "delta_sharing_unsupported_error"
    )
  })

  expect_identical(recorder$opens, 0L)
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
})

public_private_key_profile <- function(private_key_file) {
  profile <- jsonlite::fromJSON(
    test_path("fixtures", "profiles", "private-key-v2.json"),
    simplifyVector = FALSE
  )
  profile$auth$privateKey$privateKeyFile <- private_key_file
  profile
}

public_decode_base64url <- function(value) {
  padding <- (4L - nchar(value) %% 4L) %% 4L
  jsonlite::base64_dec(paste0(
    chartr("-_", "+/", value),
    strrep("=", padding)
  ))
}

test_that("public Query Table uses one real private-key JWT 401 replay", {
  key <- openssl::rsa_keygen(2048)
  key_path <- tempfile("public-read-rsa-", fileext = ".pem")
  openssl::write_pem(key, key_path)
  on.exit(unlink(key_path), add = TRUE)
  parent <- tempfile("public-jwt-parent-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  auth <- new.env(parent = emptyenv())
  auth$requests <- list()
  auth_handler <- function(request) {
    auth$requests[[length(auth$requests) + 1L]] <- request
    list(
      status = 200L,
      body = list(
        access_token = paste0("JWT-QUERY-TOKEN-", length(auth$requests)),
        token_type = "Bearer",
        expires_in = 3600
      )
    )
  }
  query <- new.env(parent = emptyenv())
  transport <- public_snapshot_transport(
    list(
      public_snapshot_specification(status = 401L),
      public_snapshot_specification()
    ),
    query
  )
  interface <- public_read_interface(
    transport,
    temp_parent = parent,
    auth_handler = auth_handler
  )
  client <- sharing_client(public_private_key_profile(key_path))

  stream <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(
      sharing_read(
        sharing_table(client, "sales.default.orders"),
        limit = 0
      )
    )
  })
  on.exit(stream$release(), add = TRUE)

  expect_length(auth$requests, 2L)
  expect_identical(query$opens, 2L)
  expect_identical(query$closed, c(1L, 1L))
  expect_identical(
    vapply(
      query$requests,
      function(request) request$headers[["Authorization"]],
      character(1)
    ),
    c("Bearer JWT-QUERY-TOKEN-1", "Bearer JWT-QUERY-TOKEN-2")
  )
  assertions <- vapply(
    auth$requests,
    function(request) request$body$client_assertion,
    character(1)
  )
  expect_length(unique(assertions), 2L)
  public_key <- as.list(key)$pubkey
  for (assertion in assertions) {
    parts <- strsplit(assertion, ".", fixed = TRUE)[[1L]]
    expect_true(openssl::signature_verify(
      charToRaw(paste(parts[1:2], collapse = ".")),
      public_decode_base64url(parts[[3L]]),
      hash = openssl::sha256,
      pubkey = public_key
    ))
  }
  expect_null(stream$get_next())
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)
})
