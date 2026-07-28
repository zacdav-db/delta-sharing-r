test_client <- function(profile = "test.share") {
  sharing_client(profile)
}

test_table <- function(profile = "test.share") {
  sharing_table(test_client(profile), "sales.default.orders")
}

expect_read_only <- function(object, property, value) {
  expect_error(
    S7::prop(object, property) <- value,
    "read-only property"
  )
}

test_execution_interface <- function(recorder = new.env(parent = emptyenv())) {
  callbacks <- list(
    list_shares = function(client) {
      recorder$list_shares_client <- client
      data.frame(name = c("sales", "product"))
    },
    list_schemas = function(client, share) {
      recorder$list_schemas <- list(client = client, share = share)
      data.frame(
        share = if (is.null(share)) NA_character_ else share,
        name = "default"
      )
    },
    list_tables = function(client, share, schema) {
      recorder$list_tables <- list(
        client = client,
        share = share,
        schema = schema
      )
      data.frame(share = share, schema = schema, name = "orders")
    },
    table_version = function(client, identifier) {
      recorder$table_version <- list(client = client, identifier = identifier)
      42
    },
    table_protocol = function(client, identifier) {
      recorder$table_protocol <- list(client = client, identifier = identifier)
      list(minReaderVersion = 1L)
    },
    table_metadata = function(client, identifier) {
      recorder$table_metadata <- list(client = client, identifier = identifier)
      list(id = "table-id")
    },
    table_schema = function(client, identifier) {
      recorder$table_schema <- list(client = client, identifier = identifier)
      structure(list(fields = "id"), class = "test_schema")
    },
    read_schema = function(specification) {
      recorder$read_schema <- specification
      structure(list(fields = specification@columns), class = "test_schema")
    },
    read_arrow_stream = function(specification, ...) {
      recorder$stream_calls <- if (is.null(recorder$stream_calls)) {
        1L
      } else {
        recorder$stream_calls + 1L
      }
      recorder$read_arrow_stream <- list(
        specification = specification,
        options = list(...)
      )
      structure(list(kind = "stream"), class = "test_arrow_stream")
    },
    arrow_from_stream = function(stream) {
      recorder$arrow_from_stream <- stream
      structure(list(kind = "table"), class = "test_arrow_table")
    },
    data_frame_from_stream = function(stream) {
      recorder$data_frame_from_stream <- stream
      data.frame(value = 1:2)
    },
    read_diagnostics = function(stream) {
      recorder$diagnostics_stream <- stream
      list(batches_emitted = 1L)
    }
  )

  delta.sharing:::.new_execution_interface(callbacks)
}
