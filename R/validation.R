# Small value helpers shared across package boundaries.

is_scalar_character <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x) && nzchar(x)
}

format_timestamp <- function(x) {
  if (is.character(x)) x else format(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}
