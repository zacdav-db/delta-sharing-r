test_that("unimplemented R and Kernel operations fail with typed conditions", {
  client <- test_client()
  table <- sharing_table(client, "sales.default.orders")

  expect_error(
    list_shares(client),
    "not available",
    class = "delta_sharing_not_implemented_error"
  )
  expect_error(
    table_version(table),
    "not available",
    class = "delta_sharing_error"
  )
  expect_error(
    read_arrow_stream(sharing_read(table)),
    "not available",
    class = "delta_sharing_native_unavailable_error"
  )
})

test_that("execution interface injects discovery without client mutation", {
  recorder <- new.env(parent = emptyenv())
  interface <- test_execution_interface(recorder)

  delta.sharing:::.with_execution_interface(interface, {
    client <- sharing_client("profile.share")

    expect_identical(client@state, "descriptor")
    expect_identical(list_shares(client)$name, c("sales", "product"))
    expect_identical(list_schemas(client)$name, "default")
    expect_identical(list_schemas(client, "sales")$share, "sales")
    expect_identical(
      list_tables(client, "sales", "default")$name,
      "orders"
    )
    expect_identical(recorder$list_schemas$share, "sales")
    expect_identical(recorder$list_tables$schema, "default")
    expect_error(
      list_tables(client, schema = "default"),
      "without `share`",
      class = "delta_sharing_validation_error"
    )
  })
})

test_that("table metadata operations receive structured identifiers", {
  recorder <- new.env(parent = emptyenv())
  interface <- test_execution_interface(recorder)

  delta.sharing:::.with_execution_interface(interface, {
    table <- sharing_table(
      sharing_client("profile.share"),
      share = "sales.eu",
      schema = "default",
      table = "events.v2"
    )

    expect_identical(table_version(table), 42)
    expect_identical(table_protocol(table)$minReaderVersion, 1L)
    expect_identical(table_metadata(table)$id, "table-id")
    expect_s3_class(table_schema(table), "test_schema")
    expect_identical(
      recorder$table_version$identifier@share,
      "sales.eu"
    )
    expect_identical(
      recorder$table_metadata$identifier@table,
      "events.v2"
    )
    read <- sharing_read(table, columns = "id")
    expect_s3_class(read_schema(read), "test_schema")
    expect_identical(recorder$read_schema, read)
  })
})

test_that("read generics dispatch one immutable specification", {
  recorder <- new.env(parent = emptyenv())
  interface <- test_execution_interface(recorder)

  delta.sharing:::.with_execution_interface(interface, {
    table <- sharing_table(
      sharing_client("profile.share"),
      "sales.default.orders"
    )
    snapshot <- sharing_read(table, columns = "id", limit = 5)

    stream <- read_arrow_stream(snapshot, batch_size = 1024)
    expect_s3_class(stream, "test_arrow_stream")
    expect_identical(recorder$stream_calls, 1L)
    expect_identical(
      recorder$read_arrow_stream$specification,
      snapshot
    )
    expect_identical(
      recorder$read_arrow_stream$options$batch_size,
      1024
    )

    arrow_table <- read_arrow(snapshot)
    expect_s3_class(arrow_table, "test_arrow_table")
    expect_identical(recorder$stream_calls, 2L)
    expect_identical(
      recorder$read_arrow_stream$specification,
      snapshot
    )
    expect_s3_class(
      recorder$arrow_from_stream,
      "test_arrow_stream"
    )

    data <- read_data_frame(snapshot)
    expect_s3_class(data, "data.frame")
    expect_identical(recorder$stream_calls, 3L)
    expect_identical(
      recorder$read_arrow_stream$specification,
      snapshot
    )
    expect_s3_class(
      recorder$data_frame_from_stream,
      "test_arrow_stream"
    )
  })
})

test_that("tables require an explicit read descriptor", {
  table <- test_table()

  expect_error(
    read_arrow_stream(table),
    "Can't find method",
    class = "S7_error_method_not_found"
  )
})

test_that("as.data.frame uses the same data-frame adapter", {
  recorder <- new.env(parent = emptyenv())
  interface <- test_execution_interface(recorder)

  delta.sharing:::.with_execution_interface(interface, {
    snapshot <- sharing_read(sharing_table(
      sharing_client("profile.share"),
      "sales.default.orders"
    ))

    result <- as.data.frame(snapshot)

    expect_s3_class(result, "data.frame")
    expect_identical(
      recorder$read_arrow_stream$specification,
      snapshot
    )
    expect_s3_class(
      recorder$data_frame_from_stream,
      "test_arrow_stream"
    )
  })
})

test_that("change specifications use the same stream path", {
  recorder <- new.env(parent = emptyenv())
  interface <- test_execution_interface(recorder)

  delta.sharing:::.with_execution_interface(interface, {
    changes <- sharing_changes(test_table(), starting_version = 10)

    result <- read_data_frame(changes)

    expect_s3_class(result, "data.frame")
    expect_identical(
      recorder$read_arrow_stream$specification,
      changes
    )
    expect_identical(recorder$stream_calls, 1L)
  })
})

test_that("untyped execution errors are wrapped without leaking messages", {
  secret <- "SUPER-SECRET-TOKEN"
  interface <- delta.sharing:::.new_execution_interface(list(
    list_shares = function(client) {
      stop("request failed with ", secret)
    }
  ))

  delta.sharing:::.with_execution_interface(interface, {
    client <- sharing_client("profile.share")
    condition <- expect_error(
      list_shares(client),
      class = "delta_sharing_protocol_error"
    )

    expect_false(grepl(secret, conditionMessage(condition), fixed = TRUE))
    expect_identical(condition$operation, "list_shares")
  })
})

test_that("diagnostics dispatch on the returned stream", {
  recorder <- new.env(parent = emptyenv())
  interface <- test_execution_interface(recorder)

  delta.sharing:::.with_execution_interface(interface, {
    stream <- read_arrow_stream(sharing_read(test_table()))
    diagnostics <- read_diagnostics(stream)

    expect_identical(diagnostics$batches_emitted, 1L)
    expect_identical(recorder$diagnostics_stream, stream)
  })
})
