.perform_with_retry <- function(request,
                                send,
                                status_of,
                                retry_after_of = function(response) NULL,
                                operation,
                                endpoint_host = NULL,
                                replayable = FALSE,
                                max_attempts = 5L,
                                sleeper = Sys.sleep,
                                clock = Sys.time,
                                random = stats::runif,
                                base_delay = 0.1,
                                delay_cap = 30,
                                return_statuses = integer()) {
  if (!is.function(send) ||
      !is.function(status_of) ||
      !is.function(retry_after_of) ||
      !is.function(sleeper) ||
      !is.function(clock) ||
      !is.function(random)) {
    stop("HTTP control hooks must be functions.", call. = FALSE)
  }
  if (!.is_scalar_character(operation)) {
    stop("`operation` must be one non-empty string.", call. = FALSE)
  }
  if (!is.null(endpoint_host) && !.is_scalar_character(endpoint_host)) {
    stop("`endpoint_host` must be NULL or one non-empty string.", call. = FALSE)
  }
  if (!is.logical(replayable) ||
      length(replayable) != 1L ||
      is.na(replayable)) {
    stop("`replayable` must be TRUE or FALSE.", call. = FALSE)
  }
  if (!is.numeric(max_attempts) ||
      length(max_attempts) != 1L ||
      is.na(max_attempts) ||
      !is.finite(max_attempts) ||
      max_attempts < 1 ||
      max_attempts != floor(max_attempts) ||
      max_attempts > .Machine$integer.max) {
    stop("`max_attempts` must be one positive whole number.", call. = FALSE)
  }
  if (!is.numeric(return_statuses) ||
      anyNA(return_statuses) ||
      any(!is.finite(return_statuses)) ||
      any(return_statuses != floor(return_statuses)) ||
      any(return_statuses < 100 | return_statuses > 599)) {
    stop(
      "`return_statuses` must contain valid whole-number HTTP statuses.",
      call. = FALSE
    )
  }
  return_statuses <- unique(as.integer(return_statuses))

  max_attempts <- as.integer(max_attempts)
  for (attempt in seq_len(max_attempts)) {
    outcome <- tryCatch(
      list(response = send(request), error = NULL),
      error = function(cnd) list(response = NULL, error = cnd)
    )

    if (!is.null(outcome$error)) {
      if (replayable && attempt < max_attempts) {
        delay <- .retry_delay(
          attempt = attempt,
          now = clock(),
          base = base_delay,
          cap = delay_cap,
          random = random
        )
        sleeper(delay)
        next
      }

      .abort_delta_sharing(
        "The Delta Sharing request could not be completed.",
        type = "http",
        operation = operation,
        endpoint_host = endpoint_host,
        retry_count = attempt - 1L
      )
    }

    status <- status_of(outcome$response)
    if (!is.numeric(status) ||
        length(status) != 1L ||
        is.na(status) ||
        !is.finite(status) ||
        status != floor(status) ||
        status < 100 ||
        status > 599) {
      .abort_delta_sharing(
        "The server returned an invalid HTTP status.",
        type = "protocol",
        operation = operation,
        endpoint_host = endpoint_host,
        retry_count = attempt - 1L
      )
    }
    status <- as.integer(status)

    if (status < 400L || status %in% return_statuses) {
      return(outcome$response)
    }

    if (replayable &&
        .retryable_status(status) &&
        attempt < max_attempts) {
      delay <- .retry_delay(
        attempt = attempt,
        retry_after = retry_after_of(outcome$response),
        now = clock(),
        base = base_delay,
        cap = delay_cap,
        random = random
      )
      sleeper(delay)
      next
    }

    .abort_delta_sharing(
      "The Delta Sharing server rejected the request.",
      type = "http",
      operation = operation,
      status = status,
      endpoint_host = endpoint_host,
      retry_count = attempt - 1L
    )
  }

  stop("Unreachable retry state.", call. = FALSE)
}
