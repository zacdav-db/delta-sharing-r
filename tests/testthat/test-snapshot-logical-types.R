logical_type_fixture <- function() {
  normalizePath(
    test_path("fixtures", "delta", "snapshot-logical-types"),
    winslash = "/",
    mustWork = TRUE
  )
}

logical_type_actions <- function() {
  path <- file.path(
    logical_type_fixture(),
    "_delta_log",
    "00000000000000000000.json"
  )
  lapply(
    readLines(path, warn = FALSE, encoding = "UTF-8"),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
}

logical_type_wire <- function(actions, version = 17L) {
  add <- actions[[3L]]$add
  add$path <- paste0(
    "https://logical-types.invalid/",
    basename(add$path),
    "?signature=logical-type-sentinel"
  )
  wire <- list(
    list(protocol = list(
      deltaProtocol = actions[[1L]]$protocol
    )),
    list(metaData = list(
      version = version,
      size = add$size,
      numFiles = 1L,
      deltaMetadata = actions[[2L]]$metaData
    )),
    list(file = list(
      id = "snapshot-logical-types-1",
      expirationTimestamp = 4102444800000,
      deltaSingleAction = list(add = add)
    )),
    list(minUrlExpirationTimestamp = 4102444800000)
  )
  lines <- vapply(
    wire,
    jsonlite::toJSON,
    character(1),
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  list(
    bytes = charToRaw(paste0(paste(lines, collapse = "\n"), "\n")),
    headers = c(
      "Content-Type" = "application/x-ndjson; charset=utf-8",
      "Delta-Table-Version" = as.character(version),
      fileidhash = "delta",
      "delta-sharing-capabilities" = paste(
        "responseformat=delta",
        "readerfeatures=columnmapping,timestampntz",
        "includeendstreamaction=true",
        sep = ";"
      )
    )
  )
}

logical_type_transport <- function(specification, recorder) {
  recorder$opens <- 0L
  recorder$closes <- 0L
  list(
    open = function(request) {
      recorder$opens <- recorder$opens + 1L
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
      end <- min(length(response$bytes), response$offset + 16L)
      value <- response$bytes[seq.int(response$offset, end)]
      response$offset <- end + 1L
      value
    },
    close = function(response) {
      recorder$closes <- recorder$closes + 1L
      invisible(NULL)
    },
    retry_after = function(response) NULL
  )
}

logical_type_file_uri <- function(path) {
  prefix <- if (grepl("^[A-Za-z]:/", path)) "file:///" else "file://"
  paste0(
    prefix,
    utils::URLencode(path, reserved = FALSE, repeated = TRUE)
  )
}

logical_type_native_factory <- function(recorder) {
  source <- normalizePath(
    file.path(logical_type_fixture(), "part-00000.parquet"),
    winslash = "/",
    mustWork = TRUE
  )
  force(source)
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
    add_index <- which(vapply(
      actions,
      function(action) "add" %in% names(action),
      logical(1)
    ))
    stopifnot(length(add_index) == 1L)
    recorder$staged_url <- actions[[add_index]]$add$path
    actions[[add_index]]$add$path <- logical_type_file_uri(source)
    lines[[add_index]] <- jsonlite::toJSON(
      actions[[add_index]],
      auto_unbox = TRUE,
      null = "null",
      digits = NA
    )
    writeLines(lines, commit_path, useBytes = TRUE)
    delta.sharing:::.native_snapshot_stream(
      table_location,
      columns = columns,
      limit = limit,
      batch_size = batch_size
    )
  }
}

logical_type_context <- function(actions = logical_type_actions()) {
  specification <- logical_type_wire(actions)
  parent <- tempfile("snapshot-logical-types-")
  dir.create(parent)
  transport <- new.env(parent = emptyenv())
  native <- new.env(parent = emptyenv())
  callbacks <- delta.sharing:::.new_control_execution_callbacks(
    transport = delta.sharing:::.fake_http_transport(function(request) {
      stop("unexpected authentication request")
    }),
    snapshot_transport = logical_type_transport(specification, transport),
    clock = function() {
      as.POSIXct("2026-07-29 00:00:00", tz = "UTC")
    },
    sleeper = function(seconds) NULL,
    random = function(...) 0,
    max_attempts = 1L,
    snapshot_temp_parent = parent,
    native_stream_factory = logical_type_native_factory(native)
  )
  list(
    interface = delta.sharing:::.new_execution_interface(callbacks),
    parent = parent,
    transport = transport,
    native = native
  )
}

logical_type_expect_balanced <- function(before) {
  after <- delta.sharing:::.native_diagnostics()
  expect_identical(after$active_streams, before$active_streams)
  expect_identical(after$pending_cleanups, before$pending_cleanups)
}

test_that("snapshot logical-type fixture has reproducible provenance", {
  fixture <- logical_type_fixture()
  checksums <- readLines(
    file.path(fixture, "SHA256SUMS"),
    warn = FALSE,
    encoding = "UTF-8"
  )
  pieces <- strsplit(checksums, "  ", fixed = TRUE)
  expect_true(all(lengths(pieces) == 2L))
  expected <- vapply(pieces, `[[`, character(1), 1L)
  paths <- file.path(
    fixture,
    vapply(pieces, `[[`, character(1), 2L)
  )
  actual <- vapply(paths, function(path) {
    bytes <- readBin(path, what = "raw", n = file.info(path)$size)
    unclass(as.character(openssl::sha256(bytes)))
  }, character(1))
  expect_identical(unname(actual), unname(expected))

  actions <- logical_type_actions()
  expect_identical(
    unlist(actions[[1L]]$protocol$readerFeatures, use.names = FALSE),
    c("columnMapping", "timestampNtz")
  )
  metadata <- actions[[2L]]$metaData
  schema <- jsonlite::fromJSON(
    metadata$schemaString,
    simplifyVector = FALSE
  )
  expect_identical(
    vapply(schema$fields, `[[`, character(1), "name"),
    c("id", "amount", "metrics", "observed_at", "local_at", "profile")
  )
  expect_identical(
    schema$fields[[3L]]$type$type,
    "map"
  )
  expect_identical(
    schema$fields[[6L]]$type$fields[[3L]]$type$elementType$type,
    "struct"
  )
  expect_identical(
    metadata$configuration[["delta.columnMapping.maxColumnId"]],
    "13"
  )
})

test_that("public Kernel path preserves extended logical types and mapping", {
  gc()
  before <- delta.sharing:::.native_diagnostics()
  context <- logical_type_context()
  on.exit(
    unlink(context$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  stream <- delta.sharing:::.with_execution_interface(
    context$interface,
    {
      read_arrow_stream(
        sharing_read(
          test_table(),
          columns = c(
            "profile",
            "local_at",
            "amount",
            "metrics",
            "observed_at",
            "id"
          )
        ),
        batch_size = 2L
      )
    }
  )
  on.exit(stream$release(), add = TRUE)

  schema <- stream$get_schema()
  expect_named(
    schema$children,
    c("profile", "local_at", "amount", "metrics", "observed_at", "id")
  )
  expect_identical(schema$children$local_at$format, "tsu:")
  expect_identical(schema$children$amount$format, "d:18,4")
  expect_identical(schema$children$metrics$format, "+m")
  expect_identical(schema$children$observed_at$format, "tsu:UTC")
  expect_named(
    schema$children$profile$children,
    c("score", "contact", "events")
  )
  expect_identical(
    schema$children$profile$children$score$format,
    "d:12,3"
  )
  expect_named(
    schema$children$profile$children$contact$children,
    c("label", "seen")
  )
  expect_identical(
    schema$children$profile$children$contact$children$seen$format,
    "tsu:"
  )
  expect_named(
    schema$children$profile$children$events$children$element$children,
    c("code", "at")
  )
  expect_identical(
    schema$children$profile$children$events$children$element$children$at$format,
    "tsu:UTC"
  )

  diagnostics <- read_diagnostics(stream)
  data <- delta.sharing:::.materialize_data_frame_stream(stream)
  expect_equal(data$id, c(101, 102, 103))
  expect_equal(data$amount, c(12345.6789, -0.0100, NA))
  expect_identical(
    attr(data$observed_at, "tzone"),
    "UTC"
  )
  expect_equal(
    as.numeric(data$observed_at),
    c(1735787045.123456, 1749283750.654321, 1767225599.999999),
    tolerance = 1e-6
  )
  # Base R POSIXct has no timezone-free logical type. The Arrow schema above
  # remains timezone-free; the data-frame adapter uses UTC as a stable display
  # timezone without changing the microsecond values.
  expect_identical(attr(data$local_at, "tzone"), "UTC")
  expect_equal(
    as.numeric(data$local_at),
    c(1709254923.000001, 1726395072.100001, 1740774082.200001),
    tolerance = 1e-6
  )
  expect_identical(data$metrics[[1L]]$key, c("alpha", "beta"))
  expect_equal(data$metrics[[1L]]$value, c(1.25, -2.50))
  expect_identical(data$metrics[[2L]]$key, "gamma")
  expect_equal(data$metrics[[2L]]$value, 999.99)
  expect_null(data$metrics[[3L]])
  expect_equal(data$profile$score, c(7.125, 8.500, NA))
  expect_identical(
    data$profile$contact$label,
    c("first", NA, "third")
  )
  expect_identical(
    attr(data$profile$contact$seen, "tzone"),
    "UTC"
  )
  expect_identical(
    data$profile$events[[1L]]$code,
    c("open", "close")
  )
  expect_identical(data$profile$events[[2L]]$code, "review")
  expect_null(data$profile$events[[3L]])
  expect_identical(
    attr(data$profile$events[[1L]]$at, "tzone"),
    "UTC"
  )

  expect_true(startsWith(context$native$staged_url, "https://"))
  expect_identical(context$transport$closes, 1L)
  expect_identical(diagnostics@table_version, 17)
  expect_identical(diagnostics@file_count, 1)
  expect_false(file.exists(context$native$root))
  logical_type_expect_balanced(before)
})

test_that("unsupported Delta interval metadata fails typed and redacted", {
  gc()
  before <- delta.sharing:::.native_diagnostics()
  actions <- logical_type_actions()
  schema <- jsonlite::fromJSON(
    actions[[2L]]$metaData$schemaString,
    simplifyVector = FALSE
  )
  schema$fields[[2L]]$type <- "interval day to second"
  actions[[2L]]$metaData$schemaString <- jsonlite::toJSON(
    schema,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  )
  context <- logical_type_context(actions)
  on.exit(
    unlink(context$parent, recursive = TRUE, force = TRUE),
    add = TRUE
  )

  condition <- tryCatch(
    delta.sharing:::.with_execution_interface(context$interface, {
      read_arrow_stream(sharing_read(test_table()), batch_size = 2L)
    }),
    error = identity
  )
  expect_s3_class(condition, "delta_sharing_native_error")
  expect_identical(
    conditionMessage(condition),
    "Execution operation `read_arrow_stream` failed."
  )
  rendered <- c(
    conditionMessage(condition),
    capture.output(str(condition))
  )
  expect_false(any(grepl(
    "interval day to second",
    rendered,
    fixed = TRUE
  )))
  expect_false(any(grepl(
    "logical-type-sentinel",
    rendered,
    fixed = TRUE
  )))
  expect_identical(context$transport$closes, 1L)
  expect_true(exists("root", context$native, inherits = FALSE))
  expect_false(file.exists(context$native$root))
  expect_length(
    list.files(context$parent, all.files = TRUE, no.. = TRUE),
    0L
  )
  logical_type_expect_balanced(before)
})
