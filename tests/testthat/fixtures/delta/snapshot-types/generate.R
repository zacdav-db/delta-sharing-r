if (!file.exists("DESCRIPTION")) {
  stop("Run this generator from the package root.", call. = FALSE)
}
if (!requireNamespace("arrow", quietly = TRUE)) {
  stop("The fixture generator requires the `arrow` package.", call. = FALSE)
}
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("The fixture generator requires the `jsonlite` package.", call. = FALSE)
}

fixture_dir <- file.path(
  "tests",
  "testthat",
  "fixtures",
  "delta",
  "snapshot-types"
)
log_dir <- file.path(fixture_dir, "_delta_log")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

empty_object <- structure(list(), names = character())
fields <- list(
  list(name = "id", type = "long", nullable = FALSE, metadata = empty_object),
  list(
    name = "flag",
    type = "boolean",
    nullable = TRUE,
    metadata = empty_object
  ),
  list(name = "tiny", type = "byte", nullable = FALSE, metadata = empty_object),
  list(
    name = "small",
    type = "short",
    nullable = FALSE,
    metadata = empty_object
  ),
  list(
    name = "count",
    type = "integer",
    nullable = FALSE,
    metadata = empty_object
  ),
  list(
    name = "ratio",
    type = "float",
    nullable = TRUE,
    metadata = empty_object
  ),
  list(
    name = "measure",
    type = "double",
    nullable = TRUE,
    metadata = empty_object
  ),
  list(
    name = "label",
    type = "string",
    nullable = TRUE,
    metadata = empty_object
  ),
  list(
    name = "payload",
    type = "binary",
    nullable = TRUE,
    metadata = empty_object
  ),
  list(
    name = "event_date",
    type = "date",
    nullable = TRUE,
    metadata = empty_object
  ),
  list(
    name = "event_at",
    type = "timestamp",
    nullable = TRUE,
    metadata = empty_object
  ),
  list(
    name = "tags",
    type = list(
      type = "array",
      elementType = "string",
      containsNull = FALSE
    ),
    nullable = TRUE,
    metadata = empty_object
  ),
  list(
    name = "details",
    type = list(
      type = "struct",
      fields = list(
        list(
          name = "score",
          type = "integer",
          nullable = TRUE,
          metadata = empty_object
        ),
        list(
          name = "note",
          type = "string",
          nullable = TRUE,
          metadata = empty_object
        )
      )
    ),
    nullable = FALSE,
    metadata = empty_object
  )
)

table <- arrow::Table$create(
  id = arrow::Array$create(c(1, 2, 3), type = arrow::int64()),
  flag = arrow::Array$create(
    c(TRUE, FALSE, NA),
    type = arrow::boolean()
  ),
  tiny = arrow::Array$create(
    c(-128L, 0L, 127L),
    type = arrow::int8()
  ),
  small = arrow::Array$create(
    c(-32768L, 0L, 32767L),
    type = arrow::int16()
  ),
  count = arrow::Array$create(c(10L, 20L, 30L), type = arrow::int32()),
  ratio = arrow::Array$create(c(1.25, NA, 3.5), type = arrow::float32()),
  measure = arrow::Array$create(c(1.5, 2.5, NA), type = arrow::float64()),
  label = arrow::Array$create(
    c("alpha", "beta", NA),
    type = arrow::utf8()
  ),
  payload = arrow::Array$create(
    list(charToRaw("A"), as.raw(c(0, 255)), NULL),
    type = arrow::binary()
  ),
  event_date = arrow::Array$create(
    as.Date(c("2025-01-02", "2025-06-07", NA)),
    type = arrow::date32()
  ),
  event_at = arrow::Array$create(
    as.POSIXct(
      c("2025-01-02 03:04:05", "2025-06-07 08:09:10", NA),
      tz = "UTC"
    ),
    type = arrow::timestamp("us", timezone = "UTC")
  ),
  tags = arrow::Array$create(
    list(c("red", "blue"), character(), NULL),
    type = arrow::list_of(arrow::utf8())
  ),
  details = arrow::StructArray$create(
    score = c(7L, 8L, NA),
    note = c("left", NA, "right")
  )
)

data_path <- file.path(fixture_dir, "part-00000.parquet")
arrow::write_parquet(
  table,
  data_path,
  compression = "uncompressed",
  use_dictionary = FALSE,
  write_statistics = FALSE
)

schema_string <- jsonlite::toJSON(
  list(type = "struct", fields = fields),
  auto_unbox = TRUE,
  null = "null",
  digits = NA
)
actions <- list(
  list(protocol = list(minReaderVersion = 1L, minWriterVersion = 2L)),
  list(metaData = list(
    id = "delta-sharing-r-snapshot-conformance-types",
    format = list(provider = "parquet", options = empty_object),
    schemaString = schema_string,
    partitionColumns = list(),
    configuration = empty_object,
    createdTime = 0
  )),
  list(add = list(
    path = basename(data_path),
    partitionValues = empty_object,
    size = unname(file.info(data_path)$size),
    modificationTime = 0,
    dataChange = TRUE
  ))
)
lines <- vapply(
  actions,
  jsonlite::toJSON,
  character(1),
  auto_unbox = TRUE,
  null = "null",
  digits = NA
)
writeLines(
  lines,
  file.path(log_dir, "00000000000000000000.json"),
  useBytes = TRUE
)

cat(
  "Generated snapshot conformance fixture with arrow ",
  as.character(utils::packageVersion("arrow")),
  ".\n",
  sep = ""
)
