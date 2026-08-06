# Parquet-format Query Table response normalization. The server sends parquet
# file metadata plus a parquet-flavored protocol/metadata; we synthesize the
# flat Delta actions the kernel needs. Parquet-format responses carry no
# deletion vectors, so a flat `add` per file suffices.

# Coerce a value to a JSON object (map). An empty or NULL value must serialize
# as `{}`, not `[]`, so the Delta log stays schema-valid.
as_json_map <- function(x) {
  if (is.null(x) || length(x) == 0L) {
    return(structure(list(), names = character()))
  }
  as.list(x)
}

# The kernel needs a minimal valid protocol. Parquet-format shares predate
# reader features; advertise the base reader/writer versions.
parquet_protocol_action <- function(protocol) {
  list(
    minReaderVersion = protocol$minReaderVersion %||% 1L,
    minWriterVersion = protocol$minWriterVersion %||% 2L
  )
}

# Build a Delta metaData action from the parquet-response metadata. schemaString
# and partitionColumns come straight from the sharing metadata.
parquet_metadata_action <- function(metadata, operation) {
  envelope <- metadata
  schema_string <- envelope$schemaString
  if (!is_scalar_character(schema_string)) {
    abort(
      "The parquet response metadata is missing its schema.",
      type = "protocol",
      operation = operation
    )
  }
  list(
    id = envelope$id %||% "00000000-0000-0000-0000-000000000000",
    format = list(provider = "parquet", options = as_json_map(NULL)),
    schemaString = schema_string,
    partitionColumns = as.list(envelope$partitionColumns %||% list()),
    configuration = as_json_map(envelope$configuration)
  )
}

# Synthesize a flat `add` from a parquet file wrapper.
parquet_add_action <- function(file, operation) {
  url <- file$url
  if (!is_scalar_character(url)) {
    abort(
      "A parquet file action is missing its URL.",
      type = "protocol",
      operation = operation
    )
  }
  add <- list(
    path = url,
    partitionValues = as_json_map(file$partitionValues),
    size = as.numeric(file$size %||% 0),
    modificationTime = 0L,
    dataChange = TRUE
  )
  if (!is.null(file$stats)) {
    add$stats <- file$stats
  }
  add
}
