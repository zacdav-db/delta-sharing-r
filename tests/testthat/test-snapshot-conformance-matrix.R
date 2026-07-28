snapshot_matrix_fixture <- function(name) {
  normalizePath(
    test_path("fixtures", "delta", name),
    winslash = "/",
    mustWork = TRUE
  )
}

snapshot_matrix_actions <- function(name) {
  path <- file.path(
    snapshot_matrix_fixture(name),
    "_delta_log",
    "00000000000000000000.json"
  )
  lapply(
    readLines(path, warn = FALSE, encoding = "UTF-8"),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
}

snapshot_matrix_mapped_actions <- function() {
  actions <- snapshot_matrix_actions("feature-column-mapping")
  metadata <- actions[[2L]]$metaData
  schema <- jsonlite::fromJSON(
    metadata$schemaString,
    simplifyVector = FALSE
  )
  schema$fields[[4L]] <- list(
    name = "bucket",
    type = "integer",
    nullable = FALSE,
    metadata = list(
      "delta.columnMapping.id" = 4L,
      "delta.columnMapping.physicalName" = "col-bucket"
    )
  )
  metadata$schemaString <- jsonlite::toJSON(
    schema,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  metadata$partitionColumns <- list("region", "bucket")
  metadata$configuration[["delta.columnMapping.maxColumnId"]] <- "4"
  actions[[2L]]$metaData <- metadata
  actions[[3L]]$add$partitionValues[["col-bucket"]] <- "7"
  actions
}

snapshot_matrix_wire <- function(actions,
                                 version,
                                 file_indexes = NULL) {
  stopifnot(
    length(actions) >= 2L,
    identical(names(actions[[1L]]), "protocol"),
    identical(names(actions[[2L]]), "metaData")
  )
  add_actions <- lapply(actions[-c(1L, 2L)], function(action) action$add)
  add_actions <- Filter(Negate(is.null), add_actions)
  if (!is.null(file_indexes)) {
    add_actions <- add_actions[file_indexes]
  }

  wire_files <- lapply(seq_along(add_actions), function(index) {
    add <- add_actions[[index]]
    add$path <- paste0(
      "https://fixture.invalid/",
      basename(add$path),
      "?signature=snapshot-matrix"
    )
    list(file = list(
      id = paste0("snapshot-matrix-", index),
      expirationTimestamp = 4102444800000,
      deltaSingleAction = list(add = add)
    ))
  })
  size <- if (length(add_actions) == 0L) {
    0
  } else {
    sum(vapply(add_actions, `[[`, numeric(1), "size"))
  }
  wire <- c(
    list(list(protocol = list(
      deltaProtocol = actions[[1L]]$protocol
    ))),
    list(list(metaData = list(
      version = version,
      size = size,
      numFiles = length(add_actions),
      deltaMetadata = actions[[2L]]$metaData
    ))),
    wire_files,
    list(list(minUrlExpirationTimestamp = 4102444800000))
  )
  bytes <- charToRaw(paste0(
    paste(
      vapply(
        wire,
        jsonlite::toJSON,
        character(1),
        auto_unbox = TRUE,
        null = "null",
        digits = NA
      ),
      collapse = "\n"
    ),
    "\n"
  ))

  features <- actions[[1L]]$protocol$readerFeatures
  feature_capability <- if (is.null(features)) {
    character()
  } else {
    paste0(
      "readerfeatures=",
      paste(tolower(unlist(features, use.names = FALSE)), collapse = ",")
    )
  }
  capabilities <- paste(
    c(
      "responseformat=delta",
      feature_capability,
      "includeendstreamaction=true"
    ),
    collapse = ";"
  )
  list(
    bytes = bytes,
    version = version,
    headers = c(
      "Content-Type" = "application/x-ndjson; charset=utf-8",
      "Delta-Table-Version" = as.character(version),
      fileidhash = "delta",
      "delta-sharing-capabilities" = capabilities
    )
  )
}

snapshot_matrix_transport <- function(specification, recorder) {
  recorder$opens <- 0L
  recorder$closed <- 0L
  recorder$requests <- list()
  list(
    open = function(request) {
      recorder$opens <- recorder$opens + 1L
      recorder$requests[[recorder$opens]] <- request
      response <- new.env(parent = emptyenv())
      response$status <- 200L
      response$headers <- specification$headers
      response$bytes <- specification$bytes
      response$offset <- 1L
      response
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    pull = function(response) {
      if (response$offset > length(response$bytes)) {
        return(NULL)
      }
      end <- min(length(response$bytes), response$offset + 10L)
      chunk <- response$bytes[seq.int(response$offset, end)]
      response$offset <- end + 1L
      chunk
    },
    close = function(response) {
      recorder$closed <- recorder$closed + 1L
      invisible(NULL)
    },
    retry_after = function(response) NULL
  )
}

snapshot_matrix_file_uri <- function(path) {
  prefix <- if (grepl("^[A-Za-z]:/", path)) "file:///" else "file://"
  paste0(
    prefix,
    utils::URLencode(path, reserved = FALSE, repeated = TRUE)
  )
}

.absolute_dv_kernel_commit <-
  "1c876015bb16902ae94f10916c7e78d7e6ced25e"
.absolute_dv_object_name <-
  "deletion_vector_ae7177f2-6d17-4ea8-819b-8d62fa2c5469.bin"
.absolute_dv_sha256 <-
  "a4e7e6964f4d5271a10b9caae795508bfb293c1be8f74ad0f0aa1a200419a233"

absolute_dv_https_url <- function(secret) {
  paste0(
    "https://raw.githubusercontent.com/delta-io/delta-kernel-rs/",
    .absolute_dv_kernel_commit,
    "/kernel/tests/data/with-short-dv/",
    .absolute_dv_object_name,
    "?X-Amz-Signature=",
    secret
  )
}

absolute_dv_actions <- function(url, size_in_bytes = 38L) {
  actions <- snapshot_matrix_actions("feature-deletion-vectors")
  actions[[3L]]$add$deletionVector <- list(
    storageType = "p",
    pathOrInlineDv = url,
    offset = 1L,
    sizeInBytes = size_in_bytes,
    cardinality = 3L
  )
  actions
}

absolute_dv_response_bytes <- function(url) {
  response <- httr2::request(url) |>
    httr2::req_timeout(30) |>
    httr2::req_perform()
  httr2::resp_body_raw(response)
}

test_that("absolute deletion vectors remain outside production capabilities", {
  expect_false(
    "deletionvectors" %in%
      delta.sharing:::.snapshot_reader_features
  )
})

snapshot_matrix_native_factory <- function(source_root, recorder) {
  source_files <- normalizePath(
    list.files(
      source_root,
      pattern = "\\.parquet$",
      recursive = TRUE,
      full.names = TRUE
    ),
    winslash = "/",
    mustWork = TRUE
  )
  force(source_files)
  function(table_location, columns, limit, batch_size) {
    state <- delta.sharing:::.validate_snapshot_log_guard(table_location)
    recorder$root <- state$root
    commit_path <- file.path(
      delta.sharing:::.snapshot_log_path(table_location),
      "_delta_log",
      "00000000000000000000.json"
    )
    lines <- readLines(commit_path, warn = FALSE, encoding = "UTF-8")
    actions <- lapply(
      lines,
      jsonlite::fromJSON,
      simplifyVector = FALSE
    )
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
    recorder$normalized_deletion_vectors <- lapply(
      actions[add_indexes],
      function(action) action$add$deletionVector
    )
    for (index in add_indexes) {
      file_name <- basename(sub("\\?.*$", "", actions[[index]]$add$path))
      source <- source_files[basename(source_files) == file_name]
      if (length(source) != 1L) {
        stop("Fixture source file could not be resolved.", call. = FALSE)
      }
      actions[[index]]$add$path <- snapshot_matrix_file_uri(source)
      lines[[index]] <- jsonlite::toJSON(
        actions[[index]],
        auto_unbox = TRUE,
        null = "null",
        digits = NA
      )
    }
    writeLines(lines, commit_path, useBytes = TRUE)
    delta.sharing:::.native_snapshot_stream(
      table_location,
      columns = columns,
      limit = limit,
      batch_size = batch_size
    )
  }
}

snapshot_matrix_interface <- function(specification,
                                      source_root,
                                      parent,
                                      transport_recorder,
                                      native_recorder) {
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("unexpected authentication request")
    }),
    snapshot_transport = snapshot_matrix_transport(
      specification,
      transport_recorder
    ),
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    },
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    snapshot_temp_parent = parent,
    native_stream_factory = snapshot_matrix_native_factory(
      source_root,
      native_recorder
    )
  )
  delta.sharing:::.new_execution_interface(callbacks)
}

snapshot_matrix_open <- function(specification,
                                 source_root,
                                 read,
                                 batch_size = 2L) {
  parent <- tempfile("snapshot-matrix-")
  dir.create(parent)
  transport <- new.env(parent = emptyenv())
  native <- new.env(parent = emptyenv())
  interface <- snapshot_matrix_interface(
    specification,
    source_root,
    parent,
    transport,
    native
  )
  stream <- delta.sharing:::.with_execution_interface(interface, {
    read_arrow_stream(read, batch_size = batch_size)
  })
  list(
    parent = parent,
    transport = transport,
    native = native,
    stream = stream
  )
}

snapshot_matrix_expect_balanced <- function(before) {
  after <- delta.sharing:::.native_diagnostics()
  expect_identical(after$active_streams, before$active_streams)
  expect_identical(after$pending_cleanups, before$pending_cleanups)
}

test_that("snapshot type fixture is reproducible package-owned data", {
  fixture <- snapshot_matrix_fixture("snapshot-types")
  paths <- file.path(
    fixture,
    c("part-00000.parquet", "_delta_log/00000000000000000000.json")
  )
  hashes <- vapply(paths, function(path) {
    bytes <- readBin(path, what = "raw", n = file.info(path)$size)
    as.character(openssl::sha256(bytes))
  }, character(1))
  expect_identical(
    unname(hashes),
    c(
      "cd27bfcaa70eeb6d1fbd66d74f49e7428b8ca36439156f5419cf7e8b38105f00",
      "92de1457b01815cf789428ef83d5173d88cb75b54ab2c1a19306ab639df91645"
    )
  )
})

test_that("public empty snapshots retain schema and release on exhaustion", {
  gc()
  before <- delta.sharing:::.native_diagnostics()
  actions <- snapshot_matrix_actions("snapshot-types")
  opened <- snapshot_matrix_open(
    snapshot_matrix_wire(actions, version = 3, file_indexes = integer()),
    snapshot_matrix_fixture("snapshot-types"),
    sharing_read(test_table()),
    batch_size = 2L
  )
  on.exit(
    unlink(opened$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(opened$stream$release(), add = TRUE)

  expect_named(
    opened$stream$get_schema()$children,
    c(
      "id", "flag", "tiny", "small", "count", "ratio", "measure",
      "label", "payload", "event_date", "event_at", "tags", "details"
    )
  )
  diagnostics <- read_diagnostics(opened$stream)
  expect_identical(diagnostics@table_version, 3)
  expect_identical(diagnostics@file_count, 0)
  expect_identical(diagnostics@page_count, 1)
  expect_null(opened$stream$get_next())
  expect_false(file.exists(opened$native$root))
  expect_identical(opened$transport$closed, 1L)
  expect_identical(read_diagnostics(opened$stream), diagnostics)
  snapshot_matrix_expect_balanced(before)
})

test_that("public Kernel snapshots preserve primitive and nested Arrow types", {
  gc()
  before <- delta.sharing:::.native_diagnostics()
  actions <- snapshot_matrix_actions("snapshot-types")
  opened <- snapshot_matrix_open(
    snapshot_matrix_wire(actions, version = 8),
    snapshot_matrix_fixture("snapshot-types"),
    sharing_read(test_table()),
    batch_size = 2L
  )
  on.exit(
    unlink(opened$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(opened$stream$release(), add = TRUE)

  schema <- opened$stream$get_schema()
  expect_identical(
    unname(vapply(schema$children, `[[`, character(1), "format")),
    c(
      "l", "b", "c", "s", "i", "f", "g", "u", "z", "tdD",
      "tsu:UTC", "+l", "+s"
    )
  )
  expect_named(schema$children$tags$children, "element")
  expect_named(schema$children$details$children, c("score", "note"))
  diagnostics <- read_diagnostics(opened$stream)
  data <- delta.sharing:::.materialize_data_frame_stream(opened$stream)

  expect_equal(data$id, c(1, 2, 3))
  expect_identical(data$flag, c(TRUE, FALSE, NA))
  expect_identical(data$tiny, c(-128L, 0L, 127L))
  expect_identical(data$small, c(-32768L, 0L, 32767L))
  expect_identical(data$count, c(10L, 20L, 30L))
  expect_equal(data$ratio, c(1.25, NA, 3.5))
  expect_equal(data$measure, c(1.5, 2.5, NA))
  expect_identical(data$label, c("alpha", "beta", NA))
  expect_identical(data$payload[[1L]], charToRaw("A"))
  expect_identical(data$payload[[2L]], as.raw(c(0, 255)))
  expect_null(data$payload[[3L]])
  expect_identical(
    data$event_date,
    as.Date(c("2025-01-02", "2025-06-07", NA))
  )
  expect_identical(attr(data$event_at, "tzone"), "UTC")
  expect_identical(data$tags[[1L]], c("red", "blue"))
  expect_identical(data$tags[[2L]], character())
  expect_null(data$tags[[3L]])
  expect_identical(data$details$score, c(7L, 8L, NA))
  expect_identical(data$details$note, c("left", NA, "right"))
  expect_identical(diagnostics@table_version, 8)
  expect_identical(diagnostics@file_count, 1)
  expect_identical(diagnostics@batch_size, 2)
  expect_identical(read_diagnostics(opened$stream), diagnostics)
  expect_false(file.exists(opened$native$root))
  snapshot_matrix_expect_balanced(before)
})

test_that("mapped multi-column partitions are restored by Delta Kernel", {
  gc()
  before <- delta.sharing:::.native_diagnostics()
  actions <- snapshot_matrix_mapped_actions()
  opened <- snapshot_matrix_open(
    snapshot_matrix_wire(actions, version = 11),
    snapshot_matrix_fixture("feature-column-mapping"),
    sharing_read(
      test_table(),
      columns = c("bucket", "region", "id", "value")
    ),
    batch_size = 2L
  )
  on.exit(
    unlink(opened$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(opened$stream$release(), add = TRUE)

  expect_named(
    opened$stream$get_schema()$children,
    c("bucket", "region", "id", "value")
  )
  data <- delta.sharing:::.materialize_data_frame_stream(opened$stream)
  expect_identical(data$bucket, rep(7L, 3L))
  expect_identical(data$region, rep("apac", 3L))
  expect_equal(data$id, c(10, 11, 12))
  expect_identical(data$value, c("alpha", "beta", "gamma"))
  expect_true(all(opened$native$normalized_https))
  expect_identical(read_diagnostics(opened$stream)@table_version, 11)
  expect_false(file.exists(opened$native$root))
  snapshot_matrix_expect_balanced(before)
})

test_that("absolute deletion vectors pass trusted HTTPS through Kernel", {
  if (!identical(
    Sys.getenv("DELTA_SHARING_HTTPS_DV_PROOF"),
    "true"
  )) {
    skip("set DELTA_SHARING_HTTPS_DV_PROOF=true to run the trusted HTTPS proof")
  }

  testthat::local_mocked_bindings(
    .snapshot_reader_features = c(
      "columnmapping",
      "deletionvectors",
      "timestampntz"
    ),
    .package = "delta.sharing"
  )
  secret <- "absolute-dv-proof-query-sentinel"
  url <- absolute_dv_https_url(secret)
  object <- absolute_dv_response_bytes(url)
  expect_identical(
    unclass(as.character(openssl::sha256(object))),
    .absolute_dv_sha256
  )

  gc()
  before <- delta.sharing:::.native_diagnostics()
  opened <- snapshot_matrix_open(
    snapshot_matrix_wire(
      absolute_dv_actions(url),
      version = 12
    ),
    snapshot_matrix_fixture("feature-deletion-vectors"),
    sharing_read(test_table()),
    batch_size = 2L
  )
  on.exit(
    unlink(opened$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(opened$stream$release(), add = TRUE)

  request_capabilities <-
    opened$transport$requests[[1L]]$headers[[
      "delta-sharing-capabilities"
    ]]
  expect_match(
    request_capabilities,
    "readerfeatures=columnmapping,deletionvectors,timestampntz",
    fixed = TRUE
  )
  committed_dv <- opened$native$normalized_deletion_vectors[[1L]]
  expect_identical(committed_dv$storageType, "p")
  expect_identical(committed_dv$pathOrInlineDv, url)
  expect_identical(committed_dv$offset, 1L)
  expect_identical(committed_dv$sizeInBytes, 38L)
  expect_identical(committed_dv$cardinality, 3L)
  expect_named(opened$stream$get_schema()$children, c("id", "value"))

  diagnostics <- read_diagnostics(opened$stream)
  safe_output <- c(
    capture.output(print(opened$stream)),
    capture.output(print(diagnostics))
  )
  expect_false(any(grepl(url, safe_output, fixed = TRUE)))
  expect_false(any(grepl(secret, safe_output, fixed = TRUE)))

  data <- delta.sharing:::.materialize_data_frame_stream(opened$stream)
  expect_equal(data$id, c(3, 4))
  expect_identical(data$value, c("three", "four"))
  expect_false(any(data$id %in% c(0, 1, 2)))
  expect_false(file.exists(opened$native$root))
  snapshot_matrix_expect_balanced(before)

  failed <- snapshot_matrix_open(
    snapshot_matrix_wire(
      absolute_dv_actions(url, size_in_bytes = 37L),
      version = 13
    ),
    snapshot_matrix_fixture("feature-deletion-vectors"),
    sharing_read(test_table()),
    batch_size = 2L
  )
  on.exit(
    unlink(failed$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(failed$stream$release(), add = TRUE)

  condition <- tryCatch(
    {
      delta.sharing:::.materialize_data_frame_stream(failed$stream)
      NULL
    },
    error = identity
  )
  expect_s3_class(condition, "delta_sharing_kernel_error")
  expect_identical(
    conditionMessage(condition),
    "Delta Kernel could not produce the requested Arrow data."
  )
  expect_identical(condition$operation, "read_data_frame")
  expect_identical(condition$kernel_category, "data_scan")
  failure_output <- c(
    conditionMessage(condition),
    capture.output(str(condition))
  )
  expect_false(any(grepl(url, failure_output, fixed = TRUE)))
  expect_false(any(grepl(secret, failure_output, fixed = TRUE)))
  expect_false(file.exists(failed$native$root))
  snapshot_matrix_expect_balanced(before)

  missing_secret <- "absolute-dv-download-failure-sentinel"
  missing_url <- sub(
    .absolute_dv_object_name,
    "missing-deletion-vector.bin",
    absolute_dv_https_url(missing_secret),
    fixed = TRUE
  )
  download_failed <- snapshot_matrix_open(
    snapshot_matrix_wire(
      absolute_dv_actions(missing_url),
      version = 14
    ),
    snapshot_matrix_fixture("feature-deletion-vectors"),
    sharing_read(test_table()),
    batch_size = 2L
  )
  on.exit(
    unlink(download_failed$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(download_failed$stream$release(), add = TRUE)

  download_condition <- tryCatch(
    {
      delta.sharing:::.materialize_data_frame_stream(
        download_failed$stream
      )
      NULL
    },
    error = identity
  )
  expect_s3_class(download_condition, "delta_sharing_kernel_error")
  expect_identical(
    conditionMessage(download_condition),
    "Delta Kernel could not produce the requested Arrow data."
  )
  expect_identical(download_condition$operation, "read_data_frame")
  expect_identical(download_condition$kernel_category, "data_scan")
  download_failure_output <- c(
    conditionMessage(download_condition),
    capture.output(str(download_condition))
  )
  expect_false(any(grepl(
    missing_url,
    download_failure_output,
    fixed = TRUE
  )))
  expect_false(any(grepl(
    missing_secret,
    download_failure_output,
    fixed = TRUE
  )))
  expect_false(file.exists(download_failed$native$root))
  snapshot_matrix_expect_balanced(before)
})

test_that("explicit version and latest select distinct server snapshots", {
  actions <- snapshot_matrix_actions("local-table")
  source <- snapshot_matrix_fixture("local-table")

  versioned <- snapshot_matrix_open(
    snapshot_matrix_wire(actions, version = 6, file_indexes = 1L),
    source,
    sharing_read(test_table(), version = 6),
    batch_size = 2L
  )
  on.exit(
    unlink(versioned$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(versioned$stream$release(), add = TRUE)
  versioned_diagnostics <- read_diagnostics(versioned$stream)
  versioned_data <- delta.sharing:::.materialize_data_frame_stream(
    versioned$stream
  )

  latest <- snapshot_matrix_open(
    snapshot_matrix_wire(actions, version = 7),
    source,
    sharing_read(test_table()),
    batch_size = 3L
  )
  on.exit(
    unlink(latest$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(latest$stream$release(), add = TRUE)
  latest_diagnostics <- read_diagnostics(latest$stream)
  latest_data <- delta.sharing:::.materialize_data_frame_stream(latest$stream)

  expect_identical(versioned$transport$requests[[1L]]$body$version, 6)
  expect_false(
    "version" %in% names(latest$transport$requests[[1L]]$body)
  )
  expect_equal(versioned_data$id, c(1, 2, 3))
  expect_equal(sort(latest_data$id), 1:7)
  expect_identical(versioned_diagnostics@table_version, 6)
  expect_identical(latest_diagnostics@table_version, 7)
  expect_identical(versioned_diagnostics@file_count, 1)
  expect_identical(latest_diagnostics@file_count, 2)
  expect_false(file.exists(versioned$native$root))
  expect_false(file.exists(latest$native$root))
})

test_that("malformed wire and Delta metadata fail closed before data", {
  gc()
  before <- delta.sharing:::.native_diagnostics()
  source <- snapshot_matrix_fixture("snapshot-types")
  actions <- snapshot_matrix_actions("snapshot-types")
  valid <- snapshot_matrix_wire(actions, version = 4)
  malformed_wire <- valid
  malformed_wire$bytes <- charToRaw(paste0(
    "{\"protocol\":{\"deltaProtocol\":{\"minReaderVersion\":1,",
    "\"minWriterVersion\":2}}}\n",
    "{\"metaData\":"
  ))

  parent <- tempfile("snapshot-matrix-malformed-wire-")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  transport <- new.env(parent = emptyenv())
  native <- new.env(parent = emptyenv())
  interface <- snapshot_matrix_interface(
    malformed_wire,
    source,
    parent,
    transport,
    native
  )
  expect_error(
    delta.sharing:::.with_execution_interface(interface, {
      read_arrow_stream(sharing_read(test_table()), batch_size = 1L)
    }),
    class = "delta_sharing_protocol_error"
  )
  expect_identical(transport$closed, 1L)
  expect_false(exists("root", native, inherits = FALSE))
  expect_length(list.files(parent, all.files = TRUE, no.. = TRUE), 0L)

  malformed_actions <- actions
  malformed_schema <- jsonlite::fromJSON(
    malformed_actions[[2L]]$metaData$schemaString,
    simplifyVector = FALSE
  )
  malformed_schema$fields[[1L]]$type <- "not_a_delta_type"
  malformed_actions[[2L]]$metaData$schemaString <- jsonlite::toJSON(
    malformed_schema,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  malformed <- snapshot_matrix_wire(malformed_actions, version = 5)
  malformed_parent <- tempfile("snapshot-matrix-malformed-delta-")
  dir.create(malformed_parent)
  on.exit(
    unlink(malformed_parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  malformed_transport <- new.env(parent = emptyenv())
  malformed_native <- new.env(parent = emptyenv())
  malformed_interface <- snapshot_matrix_interface(
    malformed,
    source,
    malformed_parent,
    malformed_transport,
    malformed_native
  )
  expect_error(
    delta.sharing:::.with_execution_interface(malformed_interface, {
      read_arrow_stream(sharing_read(test_table()), batch_size = 1L)
    }),
    class = "delta_sharing_native_error"
  )
  expect_identical(malformed_transport$closed, 1L)
  expect_true(exists("root", malformed_native, inherits = FALSE))
  expect_false(file.exists(malformed_native$root))
  expect_length(
    list.files(malformed_parent, all.files = TRUE, no.. = TRUE),
    0L
  )
  snapshot_matrix_expect_balanced(before)
})

test_that("mid-stream adapter failure releases the real Kernel stream", {
  gc()
  before <- delta.sharing:::.native_diagnostics()
  actions <- snapshot_matrix_actions("snapshot-types")
  opened <- snapshot_matrix_open(
    snapshot_matrix_wire(actions, version = 9),
    snapshot_matrix_fixture("snapshot-types"),
    sharing_read(test_table()),
    batch_size = 1L
  )
  on.exit(
    unlink(opened$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  on.exit(opened$stream$release(), add = TRUE)
  diagnostics <- read_diagnostics(opened$stream)
  rows_seen <- 0L
  expect_true(file.exists(opened$native$root))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    before$active_streams + 1
  )

  condition <- expect_error(
    delta.sharing:::.materialize_data_frame_stream(
      opened$stream,
      converter = function(stream) {
        rows_seen <<- stream$get_next()$length
        stop("deterministic adapter failure", call. = FALSE)
      }
    ),
    class = "delta_sharing_kernel_error"
  )
  expect_false(grepl(
    "deterministic adapter failure",
    conditionMessage(condition),
    fixed = TRUE
  ))
  expect_identical(rows_seen, 1L)
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(opened$stream))
  expect_false(file.exists(opened$native$root))
  expect_identical(read_diagnostics(opened$stream), diagnostics)
  snapshot_matrix_expect_balanced(before)
})
