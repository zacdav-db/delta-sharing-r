.validate_materializer_stream <- function(stream, operation) {
  if (!inherits(stream, "nanoarrow_array_stream") ||
      !nanoarrow::nanoarrow_pointer_is_valid(stream)) {
    .abort_delta_sharing(
      "The materializer requires a live Arrow C Stream.",
      type = "validation",
      operation = operation
    )
  }
  stream
}

.release_materializer_stream <- function(stream) {
  if (inherits(stream, "nanoarrow_array_stream")) {
    try(stream$release(), silent = TRUE)
  }
  invisible(NULL)
}

.arrow_package_available <- function() {
  requireNamespace("arrow", quietly = TRUE)
}

.arrow_reader_from_stream <- function(stream) {
  arrow::as_record_batch_reader(stream)
}

.nanoarrow_data_frame_from_stream <- function(stream) {
  prototype <- nanoarrow::infer_nanoarrow_ptype(stream$get_schema())
  if (!inherits(prototype, "data.frame")) {
    .abort_delta_sharing(
      "The Arrow C Stream schema is not a record schema.",
      type = "protocol",
      operation = "read_data_frame"
    )
  }
  nanoarrow::convert_array_stream(stream, to = prototype)
}

.materialize_arrow_stream <- function(
  stream,
  arrow_available = .arrow_package_available,
  reader_factory = .arrow_reader_from_stream
) {
  stream <- .validate_materializer_stream(stream, "read_arrow")
  on.exit(.release_materializer_stream(stream), add = TRUE)
  if (!is.function(arrow_available) || !is.function(reader_factory)) {
    stop("Arrow materializer hooks must be functions.", call. = FALSE)
  }
  if (!isTRUE(arrow_available())) {
    .abort_delta_sharing(
      "The optional package `{arrow}` is required for `read_arrow()`.",
      type = "unsupported",
      operation = "read_arrow",
      feature = "arrow_package"
    )
  }

  reader <- reader_factory(stream)
  if (is.null(reader$read_table) ||
      !is.function(reader$read_table) ||
      is.null(reader$Close) ||
      !is.function(reader$Close)) {
    .abort_delta_sharing(
      "The optional Arrow adapter returned an invalid record-batch reader.",
      type = "protocol",
      operation = "read_arrow"
    )
  }
  on.exit(try(reader$Close(), silent = TRUE), add = TRUE)
  reader$read_table()
}

.materialize_data_frame_stream <- function(
  stream,
  converter = .nanoarrow_data_frame_from_stream
) {
  stream <- .validate_materializer_stream(stream, "read_data_frame")
  on.exit(.release_materializer_stream(stream), add = TRUE)
  if (!is.function(converter)) {
    stop("`converter` must be a function.", call. = FALSE)
  }

  result <- converter(stream)
  if (!inherits(result, "data.frame")) {
    .abort_delta_sharing(
      "The nanoarrow adapter did not return a data frame.",
      type = "protocol",
      operation = "read_data_frame"
    )
  }
  result
}
