# bit64 reserves the minimum signed int64 for NA. Check the Arrow values before
# converting them, while their validity bitmap still distinguishes nulls.
check_arrow_integer64 <- function(array) {
  if (inherits(array, "ChunkedArray")) {
    for (chunk in array$chunks) {
      check_arrow_integer64(chunk)
    }
  } else if (array$type_id() == arrow::Type$INT64) {
    minimum <- arrow::Scalar$create("-9223372036854775808")$cast(arrow::int64())
    matches <- arrow::call_function("equal", array, minimum)
    if (isTRUE(arrow::call_function("any", matches)$as_vector())) {
      abort(
        c(
          "Cannot convert BIGINT -9223372036854775808 to R integer64.",
          "i" = "bit64 reserves that value for NA.",
          "i" = "Use to_arrow() or to_arrow_reader() to retain this value."
        ),
        type = "unsupported",
        operation = "read_arrow_stream",
        feature = "int64_min"
      )
    }
  } else if (inherits(array, "StructArray")) {
    # Flatten applies the struct's offset and parent validity to its children.
    for (child in array$Flatten()) {
      check_arrow_integer64(child)
    }
  } else if (inherits(array, "MapArray")) {
    # Nested key/item views do not retain the parent map's validity bitmap.
    if (array$null_count > 0) {
      array <- array$Filter(arrow::call_function("is_valid", array))
    }
    check_arrow_integer64(array$keys_nested())
    check_arrow_integer64(array$items_nested())
  } else if (
    inherits(array, c("ListArray", "LargeListArray", "FixedSizeListArray"))
  ) {
    # Inspect only entries in the logical slice and in non-null lists.
    check_arrow_integer64(arrow::call_function("list_flatten", array))
  }
  invisible(NULL)
}
