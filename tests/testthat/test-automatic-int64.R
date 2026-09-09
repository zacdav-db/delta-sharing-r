automatic_int64_array <- function(values) {
  arrow::Array$create(values, type = arrow::utf8())$cast(arrow::int64())
}

automatic_int64_reader <- function(batches, schema = NULL, state = NULL) {
  LocalReader <- R6::R6Class(
    inherit = SharingReader,
    cloneable = FALSE,
    private = list(open_stream = function(batch_size) {
      stream <- nanoarrow::basic_array_stream(batches, schema = schema)
      if (!is.null(state)) {
        stream <- nanoarrow::array_stream_set_finalizer(stream, function() {
          state$released <- state$released + 1L
        })
      }
      stream
    })
  )
  LocalReader$new()
}

expect_automatic_int64 <- function(value, expected) {
  expect_s3_class(value, "integer64")
  expect_identical(as.character(value), expected)
}

test_that("public R materializers automatically preserve BIGINT values and operations", {
  values <- c(
    "9007199254740992",
    "9007199254740993",
    "-9007199254740993",
    "9223372036854775807",
    "-9223372036854775807",
    NA_character_,
    "42"
  )
  batch <- arrow::RecordBatch$create(
    id = automatic_int64_array(values),
    ordinary = seq_along(values)
  )
  slices <- lapply(seq_along(values), function(i) {
    nanoarrow::as_nanoarrow_array(batch$Slice(i - 1L, 1L))
  })
  for (method in c("to_tibble", "to_data_frame")) {
    result <- automatic_int64_reader(slices)[[method]](batch_size = 1L)
    expect_automatic_int64(result$id, values)
    expect_identical(result$ordinary, seq_along(values))
    expect_identical(inherits(result, "tbl_df"), method == "to_tibble")
    expect_automatic_int64(
      result$id[1:2] + 1L,
      c("9007199254740993", "9007199254740994")
    )
    expect_automatic_int64(
      dplyr::filter(result, id == bit64::as.integer64(values[2]))$id,
      values[2]
    )
    keys <- data.frame(
      id = bit64::as.integer64(values[1:2]),
      label = c("a", "b")
    )
    joined <- dplyr::left_join(result[1:2, ], keys, by = "id")
    expect_identical(joined$label, c("a", "b"))
  }
})

test_that("small, null and empty BIGINT columns have stable types and restore options", {
  batch <- arrow::RecordBatch$create(
    small = automatic_int64_array(c("1", "2", NA_character_)),
    missing = automatic_int64_array(rep(NA_character_, 3)),
    integer = 1:3
  )
  for (setting in list(NULL, TRUE, FALSE)) {
    withr::with_options(list(arrow.int64_downcast = setting), {
      for (method in c("to_tibble", "to_data_frame")) {
        reader <- automatic_int64_reader(list(nanoarrow::as_nanoarrow_array(
          batch
        )))
        result <- reader[[method]]()
        expect_automatic_int64(result$small, c("1", "2", NA_character_))
        expect_automatic_int64(result$missing, rep(NA_character_, 3))
        expect_identical(result$integer, 1:3)
        empty <- automatic_int64_reader(
          list(),
          nanoarrow::as_nanoarrow_schema(batch$schema)
        )[[method]]()
        expect_equal(nrow(empty), 0L)
        expect_automatic_int64(empty$small, character())
        expect_automatic_int64(empty$missing, character())
        expect_identical(getOption("arrow.int64_downcast"), setting)
      }
    })
  }
})

test_that("nested BIGINT structs and lists preserve sliced rows and empty types", {
  values <- c("9007199254740992", "9007199254740993", NA_character_, "42")
  lists <- arrow::Array$create(
    list(values[1:2], NULL, character(), values[3:4]),
    type = arrow::list_of(arrow::utf8())
  )$cast(arrow::list_of(arrow::int64()))
  batch <- arrow::RecordBatch$create(
    nested = arrow::StructArray$create(id = automatic_int64_array(values)),
    items = lists
  )
  slices <- lapply(
    0:3,
    function(i) nanoarrow::as_nanoarrow_array(batch$Slice(i, 1))
  )
  for (method in c("to_tibble", "to_data_frame")) {
    result <- automatic_int64_reader(slices)[[method]]()
    expect_automatic_int64(result$nested$id, values)
    expect_automatic_int64(result$items[[1]], values[1:2])
    expect_null(result$items[[2]])
    expect_automatic_int64(result$items[[3]], character())
    expect_automatic_int64(result$items[[4]], values[3:4])
    empty <- automatic_int64_reader(
      list(),
      nanoarrow::as_nanoarrow_schema(batch$schema)
    )[[method]]()
    expect_equal(nrow(empty), 0L)
    expect_automatic_int64(empty$nested$id, character())
  }
})

test_that("valid INT64_MIN is rejected in scalar, struct, list and map values", {
  minimum <- "-9223372036854775808"
  scalar <- automatic_int64_array(c(minimum, NA_character_, "42"))
  lists <- arrow::Array$create(
    list(minimum, NA_character_, character()),
    type = arrow::list_of(arrow::utf8())
  )$cast(arrow::list_of(arrow::int64()))
  maps <- arrow::Array$create(
    list(data.frame(key = "a", value = minimum)),
    type = arrow::map_of(arrow::utf8(), arrow::utf8())
  )$cast(arrow::map_of(arrow::utf8(), arrow::int64()))
  keys <- arrow::Array$create(
    list(data.frame(key = minimum, value = "a")),
    type = arrow::map_of(arrow::utf8(), arrow::utf8())
  )$cast(arrow::map_of(arrow::int64(), arrow::utf8()))
  for (field in list(
    scalar,
    arrow::StructArray$create(id = scalar),
    lists,
    maps,
    keys
  )) {
    batch <- arrow::RecordBatch$create(value = field)
    for (method in c("to_tibble", "to_data_frame")) {
      state <- new.env(parent = emptyenv())
      state$released <- 0L
      reader <- automatic_int64_reader(
        list(nanoarrow::as_nanoarrow_array(batch)),
        state = state
      )
      withr::with_options(list(arrow.int64_downcast = TRUE), {
        expect_error(
          reader[[method]](),
          "-9223372036854775808",
          fixed = TRUE,
          class = "delta_sharing_unsupported_error"
        )
        expect_identical(getOption("arrow.int64_downcast"), TRUE)
      })
      expect_identical(state$released, 1L)
    }
  }
})

test_that("INT64_MIN outside a selected nested slice does not reject the result", {
  values <- c("-9223372036854775808", "42")
  ints <- automatic_int64_array(values)
  lists <- arrow::Array$create(
    as.list(values),
    type = arrow::list_of(arrow::utf8())
  )$cast(arrow::list_of(arrow::int64()))
  maps <- arrow::Array$create(
    lapply(values, function(value) data.frame(key = "a", value = value)),
    type = arrow::map_of(arrow::utf8(), arrow::utf8())
  )$cast(arrow::map_of(arrow::utf8(), arrow::int64()))
  batch <- arrow::RecordBatch$create(
    id = ints,
    nested = arrow::StructArray$create(id = ints),
    items = lists,
    mapping = maps
  )$Slice(1, 1)
  for (method in c("to_tibble", "to_data_frame")) {
    result <- automatic_int64_reader(list(nanoarrow::as_nanoarrow_array(
      batch
    )))[[method]]()
    expect_automatic_int64(result$id, "42")
    expect_automatic_int64(result$nested$id, "42")
    expect_automatic_int64(result$items[[1]], "42")
    expect_automatic_int64(result$mapping[[1]]$value, "42")
  }
})

test_that("eager R conversion releases readers on success and native read errors", {
  state <- new.env(parent = emptyenv())
  state$released <- 0L
  batch <- arrow::RecordBatch$create(id = automatic_int64_array("42"))
  reader <- automatic_int64_reader(
    list(nanoarrow::as_nanoarrow_array(batch)),
    state = state
  )
  result <- reader$to_tibble()
  expect_identical(state$released, 1L)
  expect_automatic_int64(result$id, "42")
  gc()
  expect_identical(state$released, 1L)

  root <- fs::path(withr::local_tempdir(), "table")
  fs::dir_copy(fixture_table("local-table"), root)
  writeBin(charToRaw("invalid parquet"), fs::path(root, "part-00000.parquet"))
  ErrorReader <- R6::R6Class(
    inherit = SharingReader,
    cloneable = FALSE,
    private = list(open_stream = function(batch_size) {
      nanoarrow::array_stream_set_finalizer(
        native_snapshot_stream(root, batch_size = batch_size),
        function() state$released <- state$released + 1L
      )
    })
  )
  withr::with_options(list(arrow.int64_downcast = TRUE), {
    expect_error(
      ErrorReader$new()$to_tibble(),
      "Delta Kernel data scan failed",
      fixed = TRUE
    )
    expect_identical(getOption("arrow.int64_downcast"), TRUE)
  })
  expect_identical(state$released, 2L)
})

test_that("automatic BIGINT conversion works through native Parquet reads", {
  values <- c(
    "9007199254740992",
    "9007199254740993",
    NA_character_,
    "-9223372036854775807"
  )
  root <- withr::local_tempdir()
  data_path <- fs::path(root, "part.parquet")
  arrow::write_parquet(
    arrow::Table$create(id = automatic_int64_array(values)),
    data_path
  )
  metadata <- fixture_commit_actions("local-table", 0L)[[2]]
  schema <- jsonlite::fromJSON(
    metadata$metaData$schemaString,
    simplifyVector = FALSE
  )
  schema$fields <- Filter(function(field) field$name == "id", schema$fields)
  schema$fields[[1]]$nullable <- TRUE
  metadata$metaData$schemaString <- log_json_line(schema)
  fs::dir_create(fs::path(root, "_delta_log"))
  writeLines(
    ndjson_body(list(
      list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
      metadata,
      list(
        add = list(
          path = "part.parquet",
          partitionValues = as_json_map(NULL),
          size = as.numeric(fs::file_size(data_path)),
          modificationTime = 0,
          dataChange = TRUE
        )
      )
    )),
    fs::path(root, "_delta_log", "00000000000000000000.json")
  )
  NativeReader <- R6::R6Class(
    inherit = SharingReader,
    cloneable = FALSE,
    private = list(open_stream = function(batch_size) {
      native_snapshot_stream(root, columns = "id", batch_size = batch_size)
    })
  )
  for (batch_size in c(1L, 65536L)) {
    for (method in c("to_tibble", "to_data_frame")) {
      expect_automatic_int64(
        NativeReader$new()[[method]](batch_size = batch_size)$id,
        values
      )
    }
  }

  # Read the unrepresentable value from a separate real Parquet table too.
  rejected <- fs::path(withr::local_tempdir(), "table")
  fs::dir_copy(root, rejected)
  data_path <- fs::path(rejected, "part.parquet")
  arrow::write_parquet(
    arrow::Table$create(id = automatic_int64_array("-9223372036854775808")),
    data_path
  )
  commit <- fs::path(rejected, "_delta_log", "00000000000000000000.json")
  actions <- lapply(
    readLines(commit),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
  actions[[3]]$add$size <- as.numeric(fs::file_size(data_path))
  writeLines(ndjson_body(actions), commit)
  root <- rejected
  for (method in c("to_tibble", "to_data_frame")) {
    expect_error(
      NativeReader$new()[[method]](batch_size = 1L),
      "-9223372036854775808",
      fixed = TRUE,
      class = "delta_sharing_unsupported_error"
    )
  }
})

test_that("INT64_MIN in hidden children of null containers is ignored", {
  minimum <- "-9223372036854775808"
  ints <- automatic_int64_array(c(minimum, "42"))
  lists <- arrow::Array$create(
    list(minimum, "42"),
    type = arrow::list_of(arrow::utf8())
  )$cast(arrow::list_of(arrow::int64()))
  maps <- arrow::Array$create(
    list(
      data.frame(key = minimum, value = minimum),
      data.frame(key = "42", value = "42")
    ),
    type = arrow::map_of(arrow::utf8(), arrow::utf8())
  )$cast(arrow::map_of(arrow::int64(), arrow::int64()))
  fields <- list(arrow::StructArray$create(id = ints), lists, maps)
  for (field in fields) {
    array <- nanoarrow::as_nanoarrow_array(field)
    # Mark only the first container null while retaining its INT64_MIN child
    # storage. The second container is valid; no result value is INT64_MIN.
    masked <- nanoarrow::nanoarrow_array_modify(
      array,
      list(
        null_count = 1L,
        buffers = c(list(as.raw(2L)), array$buffers[-1L])
      )
    )
    batch <- arrow::RecordBatch$create(value = arrow::as_arrow_array(masked))
    for (method in c("to_tibble", "to_data_frame")) {
      result <- automatic_int64_reader(
        list(nanoarrow::as_nanoarrow_array(batch))
      )[[method]]()
      if (inherits(field, "StructArray")) {
        expect_automatic_int64(result$value$id, c(NA_character_, "42"))
      } else {
        expect_null(result$value[[1]])
        valid <- result$value[[2]]
        if (inherits(field, "MapArray")) {
          expect_automatic_int64(valid$key, "42")
          expect_automatic_int64(valid$value, "42")
        } else {
          expect_automatic_int64(valid, "42")
        }
      }
    }
  }
})

test_that("R conversion restores options and closes its reader exactly once on failure", {
  imported <- sharing_stream_to_arrow_reader
  batch <- arrow::RecordBatch$create(id = automatic_int64_array("42"))
  for (failure in c("conversion", "native", "interrupt")) {
    state <- new.env(parent = emptyenv())
    state$released <- 0L
    state$closed <- 0L
    condition <- switch(
      failure,
      conversion = simpleError("conversion failed"),
      native = simpleError(native_stream_interrupt_message),
      interrupt = structure(
        list(message = "interrupted"),
        class = c("interrupt", "condition")
      )
    )
    proxy <- function(stream, operation) {
      reader <- imported(stream, operation)
      list(
        read_table = function() {
          if (failure != "conversion") stop(condition)
          reader$read_table()
        },
        Close = function() {
          state$closed <- state$closed + 1L
          reader$Close()
        }
      )
    }
    withr::with_options(list(arrow.int64_downcast = NULL), {
      testthat::with_mocked_bindings(
        {
          reader <- automatic_int64_reader(
            list(nanoarrow::as_nanoarrow_array(batch)),
            state = state
          )
          if (failure == "conversion") {
            testthat::with_mocked_bindings(
              {
                caught <- tryCatch(reader$to_tibble(), error = identity)
                expect_identical(caught, condition)
              },
              as_tibble = function(...) stop(condition),
              .package = "tibble"
            )
          } else {
            expect_error(reader$to_tibble(), class = "delta_sharing_cancelled")
          }
        },
        sharing_stream_to_arrow_reader = proxy,
        .package = "delta.sharing"
      )
      expect_null(getOption("arrow.int64_downcast"))
      expect_identical(state$closed, 1L)
      expect_identical(state$released, 1L)
    })
  }
})
