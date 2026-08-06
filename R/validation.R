# Public argument normalization shared across the package. Each helper returns
# a normalized value or raises a typed validation condition.

is_scalar_character <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x) && nzchar(x)
}

normalize_identifier_part <- function(x, name) {
  if (!is_scalar_character(x)) {
    abort(
      "{.arg {name}} must be one non-empty string.",
      type = "validation",
      operation = "table_identifier"
    )
  }
  x
}

# A non-negative whole number (a Delta table version or a row limit/count).
normalize_count <- function(x, name, required = FALSE) {
  if (is.null(x)) {
    if (required) {
      abort("{.arg {name}} is required.", type = "validation")
    }
    return(NULL)
  }
  if (!rlang::is_scalar_integerish(x, finite = TRUE) || x < 0) {
    abort(
      "{.arg {name}} must be one non-negative whole number.",
      type = "validation"
    )
  }
  as.double(x)
}

normalize_version <- function(x, name, required = FALSE) {
  normalize_count(x, name, required = required)
}

normalize_limit <- function(limit) {
  normalize_count(limit, "limit")
}

normalize_timestamp <- function(x, name, required = FALSE) {
  if (is.null(x)) {
    if (required) {
      abort("{.arg {name}} is required.", type = "validation")
    }
    return(NULL)
  }
  if (is_scalar_character(x)) {
    return(x)
  }
  if (!inherits(x, "POSIXct") || length(x) != 1L || !is.finite(as.double(x))) {
    abort(
      "{.arg {name}} must be one non-empty timestamp string or POSIXct value.",
      type = "validation"
    )
  }
  structure(as.double(x), class = c("POSIXct", "POSIXt"), tzone = "UTC")
}

normalize_columns <- function(columns) {
  if (is.null(columns)) {
    return(NULL)
  }
  if (
    !is.character(columns) ||
      !length(columns) ||
      anyNA(columns) ||
      any(!nzchar(columns))
  ) {
    abort(
      "{.arg columns} must be a non-empty character vector of non-empty names.",
      type = "validation"
    )
  }
  if (anyDuplicated(columns)) {
    abort(
      "{.arg columns} must not contain duplicate names.",
      type = "validation"
    )
  }
  columns
}

normalize_predicate <- function(predicate) {
  if (is.null(predicate)) {
    return(NULL)
  }
  if (!is.list(predicate)) {
    abort(
      "{.arg predicate} must be a list describing a structured server hint.",
      type = "validation"
    )
  }
  predicate
}

normalize_response_format <- function(response_format) {
  rlang::arg_match0(response_format, c("auto", "delta", "parquet"))
}

format_timestamp <- function(x) {
  if (is.character(x)) x else format(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}
