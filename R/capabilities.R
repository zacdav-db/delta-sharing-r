.snapshot_response_formats <- c("delta", "parquet")
# Every entry must have an end-to-end Delta Sharing wrapper -> R synthetic log
# -> pinned Kernel -> Arrow stream fixture in test-native-feature-conformance.R.
.snapshot_reader_features <- c(
  "columnmapping",
  "timestampntz"
)

.snapshot_capability_header <- function(response_format = "auto") {
  response_format <- .normalize_response_format(response_format)
  formats <- if (identical(response_format, "auto")) {
    .snapshot_response_formats
  } else {
    response_format
  }

  capabilities <- paste0(
    "responseformat=",
    paste(formats, collapse = ",")
  )
  if ("delta" %in% formats) {
    capabilities <- c(
      capabilities,
      paste0(
        "readerfeatures=",
        paste(sort(.snapshot_reader_features), collapse = ",")
      )
    )
  }
  paste(capabilities, collapse = ";")
}

.parse_table_version_header <- function(headers, operation = "table_version") {
  if (!.is_scalar_character(operation)) {
    stop("`operation` must be one non-empty string.", call. = FALSE)
  }
  if (is.null(headers) || is.null(names(headers))) {
    .abort_delta_sharing(
      "The server response is missing the table version.",
      type = "protocol",
      operation = operation
    )
  }

  index <- which(tolower(names(headers)) == "delta-table-version")
  if (length(index) != 1L) {
    .abort_delta_sharing(
      "The server response has an invalid table version.",
      type = "protocol",
      operation = operation
    )
  }

  value <- headers[[index]]
  if (!.is_scalar_character(value) || !grepl("^[0-9]+$", value)) {
    .abort_delta_sharing(
      "The server response has an invalid table version.",
      type = "protocol",
      operation = operation
    )
  }

  version <- suppressWarnings(as.numeric(value))
  if (!is.finite(version) ||
      version < 0 ||
      version != floor(version) ||
      version > 2^53) {
    .abort_delta_sharing(
      "The server response has an invalid table version.",
      type = "protocol",
      operation = operation
    )
  }

  version
}

.format_protocol_timestamp <- function(timestamp) {
  timestamp <- .normalize_timestamp(timestamp, "timestamp", required = TRUE)
  format(timestamp, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
}
