if (!file.exists("DESCRIPTION")) {
  stop("Run this generator from the package root.", call. = FALSE)
}
if (!requireNamespace("arrow", quietly = TRUE)) {
  stop("The fixture generator requires the `arrow` package.", call. = FALSE)
}
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("The fixture generator requires the `jsonlite` package.", call. = FALSE)
}
if (!requireNamespace("openssl", quietly = TRUE)) {
  stop("The fixture generator requires the `openssl` package.", call. = FALSE)
}

fixture_dir <- file.path(
  "tests",
  "testthat",
  "fixtures",
  "delta",
  "snapshot-logical-types"
)
log_dir <- file.path(fixture_dir, "_delta_log")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

empty_object <- structure(list(), names = character())
mapping_metadata <- function(id, physical_name) {
  list(
    "delta.columnMapping.id" = id,
    "delta.columnMapping.physicalName" = physical_name
  )
}
mapped_field <- function(name,
                         type,
                         nullable,
                         id,
                         physical_name) {
  list(
    name = name,
    type = type,
    nullable = nullable,
    metadata = mapping_metadata(id, physical_name)
  )
}

event_type <- list(
  type = "struct",
  fields = list(
    mapped_field("code", "string", FALSE, 12L, "phys-code"),
    mapped_field("at", "timestamp", FALSE, 13L, "phys-at")
  )
)
contact_type <- list(
  type = "struct",
  fields = list(
    mapped_field("label", "string", TRUE, 9L, "phys-label"),
    mapped_field("seen", "timestamp_ntz", FALSE, 10L, "phys-seen")
  )
)
profile_type <- list(
  type = "struct",
  fields = list(
    mapped_field("score", "decimal(12,3)", TRUE, 7L, "phys-score"),
    mapped_field("contact", contact_type, FALSE, 8L, "phys-contact"),
    mapped_field(
      "events",
      list(
        type = "array",
        elementType = event_type,
        containsNull = FALSE
      ),
      TRUE,
      11L,
      "phys-events"
    )
  )
)
fields <- list(
  mapped_field("id", "long", FALSE, 1L, "phys-id"),
  mapped_field("amount", "decimal(18,4)", TRUE, 2L, "phys-amount"),
  mapped_field(
    "metrics",
    list(
      type = "map",
      keyType = "string",
      valueType = "decimal(10,2)",
      valueContainsNull = FALSE
    ),
    TRUE,
    3L,
    "phys-metrics"
  ),
  mapped_field(
    "observed_at",
    "timestamp",
    FALSE,
    4L,
    "phys-observed-at"
  ),
  mapped_field(
    "local_at",
    "timestamp_ntz",
    FALSE,
    5L,
    "phys-local-at"
  ),
  mapped_field("profile", profile_type, FALSE, 6L, "phys-profile")
)

observed_at <- as.POSIXct(
  c(
    "2025-01-02 03:04:05",
    "2025-06-07 08:09:10",
    "2025-12-31 23:59:59"
  ),
  tz = "UTC"
) + c(0.123456, 0.654321, 0.999999)
local_at <- as.POSIXct(
  c(
    "2024-03-01 01:02:03",
    "2024-09-15 10:11:12",
    "2025-02-28 20:21:22"
  ),
  tz = "UTC"
) + c(0.000001, 0.100001, 0.200001)

metrics <- arrow::Array$create(
  list(
    data.frame(
      key = c("alpha", "beta"),
      value = c(1.25, -2.50),
      stringsAsFactors = FALSE
    ),
    data.frame(
      key = "gamma",
      value = 999.99,
      stringsAsFactors = FALSE
    ),
    NULL
  ),
  type = arrow::map_of(
    arrow::utf8(),
    arrow::decimal128(10, 2)
  )
)
events <- arrow::Array$create(
  list(
    data.frame(
      `phys-code` = c("open", "close"),
      `phys-at` = observed_at[c(1L, 2L)],
      check.names = FALSE
    ),
    data.frame(
      `phys-code` = "review",
      `phys-at` = observed_at[3L],
      check.names = FALSE
    ),
    NULL
  ),
  type = arrow::list_of(arrow::struct(
    `phys-code` = arrow::utf8(),
    `phys-at` = arrow::timestamp("us", timezone = "UTC")
  ))
)
contact <- arrow::StructArray$create(
  `phys-label` = c("first", NA, "third"),
  `phys-seen` = arrow::Array$create(
    local_at,
    type = arrow::timestamp("us")
  )
)
profile <- arrow::StructArray$create(
  `phys-score` = arrow::Array$create(
    c(7.125, 8.500, NA),
    type = arrow::decimal128(12, 3)
  ),
  `phys-contact` = contact,
  `phys-events` = events
)
table <- arrow::Table$create(
  `phys-id` = arrow::Array$create(c(101, 102, 103), type = arrow::int64()),
  `phys-amount` = arrow::Array$create(
    c(12345.6789, -0.0100, NA),
    type = arrow::decimal128(18, 4)
  ),
  `phys-metrics` = metrics,
  `phys-observed-at` = arrow::Array$create(
    observed_at,
    type = arrow::timestamp("us", timezone = "UTC")
  ),
  `phys-local-at` = arrow::Array$create(
    local_at,
    type = arrow::timestamp("us")
  ),
  `phys-profile` = profile
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
  list(protocol = list(
    minReaderVersion = 3L,
    minWriterVersion = 7L,
    readerFeatures = list("columnMapping", "timestampNtz"),
    writerFeatures = list("columnMapping", "timestampNtz")
  )),
  list(metaData = list(
    id = "delta-sharing-r-snapshot-logical-types",
    format = list(provider = "parquet", options = empty_object),
    schemaString = schema_string,
    partitionColumns = list(),
    configuration = list(
      "delta.columnMapping.mode" = "name",
      "delta.columnMapping.maxColumnId" = "13"
    ),
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
log_path <- file.path(log_dir, "00000000000000000000.json")
writeLines(lines, log_path, useBytes = TRUE)

hash_paths <- c(
  "part-00000.parquet",
  file.path("_delta_log", basename(log_path))
)
hashes <- vapply(
  file.path(fixture_dir, hash_paths),
  function(path) {
    bytes <- readBin(path, what = "raw", n = file.info(path)$size)
    unclass(as.character(openssl::sha256(bytes)))
  },
  character(1)
)
writeLines(
  paste0(unname(hashes), "  ", hash_paths),
  file.path(fixture_dir, "SHA256SUMS"),
  useBytes = TRUE
)

cat(
  "Generated snapshot logical-type fixture with arrow ",
  as.character(utils::packageVersion("arrow")),
  ".\n",
  sep = ""
)
