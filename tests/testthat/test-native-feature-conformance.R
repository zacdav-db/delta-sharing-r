native_feature_fixture <- function(feature) {
  normalizePath(
    test_path("fixtures", "delta", feature),
    winslash = "/",
    mustWork = TRUE
  )
}

native_feature_actions <- function(feature) {
  log_path <- file.path(
    native_feature_fixture(feature),
    "_delta_log",
    "00000000000000000000.json"
  )
  lapply(
    readLines(log_path, warn = FALSE, encoding = "UTF-8"),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
}

native_feature_components <- function(feature) {
  actions <- native_feature_actions(feature)
  stopifnot(
    length(actions) == 3L,
    identical(names(actions[[1L]]), "protocol"),
    identical(names(actions[[2L]]), "metaData"),
    identical(names(actions[[3L]]), "add")
  )

  add <- actions[[3L]]$add
  source_file <- normalizePath(
    list.files(
      native_feature_fixture(feature),
      pattern = "\\.parquet$",
      recursive = TRUE,
      full.names = TRUE
    ),
    winslash = "/",
    mustWork = TRUE
  )
  stopifnot(length(source_file) == 1L)
  add$path <- paste0(
    "https://objects.example.test/",
    feature,
    ".parquet?sig=feature-conformance"
  )
  wire_actions <- list(
    list(protocol = list(deltaProtocol = actions[[1L]]$protocol)),
    list(metaData = list(
      version = 0L,
      size = file.info(source_file)$size,
      numFiles = 1L,
      deltaMetadata = actions[[2L]]$metaData
    )),
    list(file = list(
      id = paste0("feature-", feature),
      expirationTimestamp = 4102444800000,
      deltaSingleAction = list(add = add)
    ))
  )
  wire <- paste0(
    vapply(
      wire_actions,
      jsonlite::toJSON,
      character(1),
      auto_unbox = TRUE,
      null = "null",
      digits = NA
    ),
    collapse = "\n"
  )
  decoder <- delta.sharing:::.new_ndjson_decoder(
    "feature conformance response"
  )
  decoded <- c(
    delta.sharing:::.ndjson_decoder_push(
      decoder,
      charToRaw(paste0(wire, "\n"))
    ),
    delta.sharing:::.ndjson_decoder_finish(decoder)
  )
  stopifnot(
    identical(
      vapply(decoded, `[[`, character(1), "type"),
      c("protocol", "metadata", "file")
    )
  )
  list(
    protocol = decoded[[1L]]$value,
    metadata = decoded[[2L]]$value,
    file = decoded[[3L]]$value,
    source_file = source_file
  )
}

native_feature_prepared_log <- function(feature) {
  components <- native_feature_components(feature)
  state <- delta.sharing:::.snapshot_file_state(components$file)
  local_action <- state$delta_action
  file_prefix <- if (grepl("^[A-Za-z]:/", components$source_file)) {
    "file:///"
  } else {
    "file://"
  }
  local_action$add$path <- paste0(
    file_prefix,
    utils::URLencode(
      components$source_file,
      reserved = FALSE,
      repeated = TRUE
    )
  )
  local_file <- delta.sharing:::.new_private_snapshot_file(
    id = state$id,
    action_type = state$action_type,
    delta_action = local_action,
    expiration_timestamp = state$expiration_timestamp,
    version = state$version,
    timestamp = state$timestamp
  )
  list(
    components = components,
    guard = delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      list(local_file)
    )
  )
}

native_feature_commit <- function(guard) {
  path <- file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log",
    "00000000000000000000.json"
  )
  lapply(
    readLines(path, warn = FALSE, encoding = "UTF-8"),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
}

expect_native_streams_balanced <- function(before) {
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    before
  )
}

test_that("column mapping restores logical data and partition names", {
  gc()
  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  prepared <- native_feature_prepared_log("column-mapping")
  state <- delta.sharing:::.snapshot_file_state(prepared$components$file)
  expect_identical(
    prepared$components$protocol$reader_features,
    "columnMapping"
  )
  expect_match(
    prepared$components$metadata$schema_string,
    "delta.columnMapping.physicalName",
    fixed = TRUE
  )
  expect_identical(
    state$delta_action$add$partitionValues,
    list("col-region" = "apac")
  )
  stream <- delta.sharing:::.native_snapshot_stream(
    prepared$guard,
    columns = c("region", "id", "value"),
    batch_size = 2L
  )

  expect_named(stream$get_schema()$children, c("region", "id", "value"))
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before + 1
  )

  data <- delta.sharing:::.materialize_data_frame_stream(stream)
  expect_named(data, c("region", "id", "value"))
  expect_identical(data$region, rep("apac", 3L))
  expect_equal(as.numeric(data$id), c(10, 11, 12))
  expect_identical(data$value, c("alpha", "beta", "gamma"))
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_native_streams_balanced(active_before)
})

test_that("column mapping by ID follows Parquet field IDs", {
  gc()
  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  prepared <- native_feature_prepared_log("column-mapping-id")
  state <- delta.sharing:::.snapshot_file_state(prepared$components$file)
  expect_identical(
    prepared$components$metadata$configuration[[
      "delta.columnMapping.mode"
    ]],
    "id"
  )
  expect_identical(
    state$delta_action$add$partitionValues,
    list("phys-region" = "latam")
  )
  stream <- delta.sharing:::.native_snapshot_stream(
    prepared$guard,
    columns = c("value", "region", "id"),
    batch_size = 2L
  )

  expect_named(stream$get_schema()$children, c("value", "region", "id"))
  data <- delta.sharing:::.materialize_data_frame_stream(stream)
  expect_identical(data$value, c("delta", "kernel", "arrow"))
  expect_identical(data$region, rep("latam", 3L))
  expect_equal(as.numeric(data$id), c(31, 32, 33))
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_native_streams_balanced(active_before)
})

test_that("deletion vectors remove physical rows before Arrow handoff", {
  gc()
  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  prepared <- native_feature_prepared_log("deletion-vectors")
  state <- delta.sharing:::.snapshot_file_state(prepared$components$file)
  descriptor <- state$delta_action$add$deletionVector
  expect_identical(
    prepared$components$protocol$reader_features,
    "deletionVectors"
  )
  expect_identical(descriptor$storageType, "i")
  expect_null(descriptor$offset)
  expect_identical(descriptor$sizeInBytes, 36)
  expect_identical(descriptor$cardinality, 2)
  commit <- native_feature_commit(prepared$guard)
  committed_descriptor <- commit[[3L]]$add$deletionVector
  expect_identical(committed_descriptor$storageType, descriptor$storageType)
  expect_identical(
    committed_descriptor$pathOrInlineDv,
    descriptor$pathOrInlineDv
  )
  expect_equal(committed_descriptor$sizeInBytes, descriptor$sizeInBytes)
  expect_equal(committed_descriptor$cardinality, descriptor$cardinality)
  stream <- delta.sharing:::.native_snapshot_stream(
    prepared$guard,
    batch_size = 2L
  )

  expect_named(stream$get_schema()$children, c("id", "value"))
  data <- delta.sharing:::.materialize_data_frame_stream(stream)
  expect_equal(as.numeric(data$id), c(0, 2, 4))
  expect_identical(data$value, c("zero", "two", "four"))
  expect_false(any(as.numeric(data$id) %in% c(1, 3)))
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_native_streams_balanced(active_before)
})

test_that("timestampNtz remains timezone-free and partitions are injected", {
  skip_if_not_installed("arrow")
  gc()
  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  prepared <- native_feature_prepared_log("timestamp-ntz")
  state <- delta.sharing:::.snapshot_file_state(prepared$components$file)
  expect_identical(
    prepared$components$protocol$reader_features,
    "timestampNtz"
  )
  expect_match(
    prepared$components$metadata$schema_string,
    "\"type\":\"timestamp_ntz\""
  )
  expect_identical(
    state$delta_action$add$partitionValues,
    list(region = "emea")
  )
  stream <- delta.sharing:::.native_snapshot_stream(
    prepared$guard,
    columns = c("observed_at", "region", "id"),
    batch_size = 1L
  )

  expect_named(
    stream$get_schema()$children,
    c("observed_at", "region", "id")
  )
  reader <- arrow::as_record_batch_reader(stream)
  table <- reader$read_table()
  reader$Close()

  expect_identical(table$num_rows, 2L)
  expect_identical(
    table$schema$names,
    c("observed_at", "region", "id")
  )
  schema_text <- table$schema$ToString()
  expect_match(schema_text, "observed_at: timestamp\\[us\\]")
  expect_false(grepl("observed_at: timestamp\\[us, tz=", schema_text))
  data <- as.data.frame(table)
  expect_identical(data$region, rep("emea", 2L))
  expect_equal(as.numeric(data$id), c(21, 22))
  expect_equal(
    as.numeric(data$observed_at),
    c(1767323045.123456, 1780819750.654321),
    tolerance = 1e-6
  )
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_native_streams_balanced(active_before)
})

test_that("feature streams release cleanly before exhaustion", {
  gc()
  active_before <- delta.sharing:::.native_diagnostics()$active_streams
  prepared <- native_feature_prepared_log("column-mapping")
  stream <- delta.sharing:::.native_snapshot_stream(
    prepared$guard,
    batch_size = 1L
  )

  expect_equal(stream$get_next()$length, 1L)
  expect_identical(
    delta.sharing:::.native_diagnostics()$active_streams,
    active_before + 1
  )
  stream$release()
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
  expect_native_streams_balanced(active_before)
})
