.readonly_property <- function(class = S7::class_any, attribute) {
  force(attribute)
  S7::new_property(
    class = class,
    getter = function(self) attr(self, attribute, exact = TRUE)
  )
}

.is_scalar_character <- function(x) {
  is.character(x) &&
    length(x) == 1L &&
    !is.na(x) &&
    nzchar(x)
}

.normalize_identifier_part <- function(x, name) {
  if (!.is_scalar_character(x)) {
    .abort_delta_sharing(
      sprintf("`%s` must be one non-empty string.", name),
      type = "validation",
      operation = "table_identifier"
    )
  }
  x
}

.normalize_version <- function(x, name, required = FALSE) {
  if (is.null(x)) {
    if (required) {
      .abort_delta_sharing(
        sprintf("`%s` is required.", name),
        type = "validation"
      )
    }
    return(NULL)
  }

  if (!is.numeric(x) ||
      length(x) != 1L ||
      is.na(x) ||
      !is.finite(x) ||
      x < 0 ||
      x != floor(x) ||
      x > 2^53) {
    .abort_delta_sharing(
      sprintf(
        "`%s` must be one non-negative whole number no greater than 2^53.",
        name
      ),
      type = "validation"
    )
  }

  as.double(x)
}

.normalize_timestamp <- function(x, name, required = FALSE) {
  if (is.null(x)) {
    if (required) {
      .abort_delta_sharing(
        sprintf("`%s` is required.", name),
        type = "validation"
      )
    }
    return(NULL)
  }

  if (!inherits(x, "POSIXct") ||
      length(x) != 1L ||
      is.na(x) ||
      !is.finite(as.double(x))) {
    .abort_delta_sharing(
      sprintf("`%s` must be one non-missing POSIXct value.", name),
      type = "validation"
    )
  }

  structure(as.double(x), class = c("POSIXct", "POSIXt"), tzone = "UTC")
}

.normalize_columns <- function(columns) {
  if (is.null(columns)) {
    return(NULL)
  }

  if (!is.character(columns) ||
      length(columns) == 0L ||
      anyNA(columns) ||
      any(!nzchar(columns))) {
    .abort_delta_sharing(
      "`columns` must be NULL or a non-empty character vector of non-empty names.",
      type = "validation"
    )
  }

  if (anyDuplicated(columns)) {
    .abort_delta_sharing(
      "`columns` must not contain duplicate names.",
      type = "validation"
    )
  }

  columns
}

.normalize_limit <- function(limit) {
  if (is.null(limit)) {
    return(NULL)
  }

  if (!is.numeric(limit) ||
      length(limit) != 1L ||
      is.na(limit) ||
      !is.finite(limit) ||
      limit < 0 ||
      limit != floor(limit) ||
      limit > 2^53) {
    .abort_delta_sharing(
      "`limit` must be one non-negative whole number no greater than 2^53.",
      type = "validation"
    )
  }

  as.double(limit)
}

.normalize_predicate <- function(predicate) {
  if (is.null(predicate)) {
    return(NULL)
  }

  if (!is.list(predicate)) {
    .abort_delta_sharing(
      "`predicate` must be NULL or a list describing a structured server hint.",
      type = "validation"
    )
  }

  predicate
}

.normalize_response_format <- function(response_format) {
  if (!.is_scalar_character(response_format) ||
      !response_format %in% c("auto", "delta", "parquet")) {
    .abort_delta_sharing(
      "`response_format` must be one of \"auto\", \"delta\", or \"parquet\".",
      type = "validation"
    )
  }

  response_format
}

.format_timestamp <- function(x) {
  format(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

.object_is <- function(x, class) {
  isTRUE(S7::S7_inherits(x, class))
}
