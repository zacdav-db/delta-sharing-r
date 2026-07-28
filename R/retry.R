.retryable_status <- function(status) {
  is.numeric(status) &&
    length(status) == 1L &&
    !is.na(status) &&
    is.finite(status) &&
    status == floor(status) &&
    (status %in% c(408, 429) || (status >= 500 && status <= 599))
}

.parse_retry_after <- function(value, now = Sys.time()) {
  if (is.null(value) || length(value) == 0L) {
    return(NULL)
  }
  if (!.is_scalar_character(value) ||
      !inherits(now, "POSIXct") ||
      length(now) != 1L ||
      is.na(now)) {
    return(NULL)
  }

  value <- trimws(value)
  if (grepl("^[0-9]+$", value)) {
    seconds <- suppressWarnings(as.numeric(value))
    if (is.finite(seconds)) {
      return(seconds)
    }
    return(NULL)
  }

  parsed <- suppressWarnings(as.POSIXct(
    strptime(
      value,
      format = "%a, %d %b %Y %H:%M:%S GMT",
      tz = "GMT"
    )
  ))
  if (is.na(parsed)) {
    return(NULL)
  }

  max(0, as.numeric(difftime(parsed, now, units = "secs")))
}

.retry_delay <- function(attempt,
                         retry_after = NULL,
                         now = Sys.time(),
                         base = 0.1,
                         cap = 30,
                         random = stats::runif) {
  valid_positive <- function(x, allow_zero = FALSE) {
    is.numeric(x) &&
      length(x) == 1L &&
      !is.na(x) &&
      is.finite(x) &&
      if (allow_zero) x >= 0 else x > 0
  }

  if (!valid_positive(attempt) || attempt != floor(attempt)) {
    stop("`attempt` must be one positive whole number.", call. = FALSE)
  }
  if (!valid_positive(base) || !valid_positive(cap)) {
    stop("`base` and `cap` must be positive finite numbers.", call. = FALSE)
  }
  if (!is.function(random)) {
    stop("`random` must be a function.", call. = FALSE)
  }

  server_delay <- .parse_retry_after(retry_after, now = now)
  if (!is.null(server_delay)) {
    return(min(server_delay, cap))
  }

  ceiling <- min(cap, base * (2^(attempt - 1)))
  jitter <- random(1L, min = 0, max = ceiling)
  if (!valid_positive(jitter, allow_zero = TRUE) || jitter > ceiling) {
    stop("`random` returned an invalid retry delay.", call. = FALSE)
  }

  as.numeric(jitter)
}
