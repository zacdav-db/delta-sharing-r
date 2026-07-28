.condition_classes <- list(
  validation = "delta_sharing_validation_error",
  auth = "delta_sharing_auth_error",
  http = "delta_sharing_http_error",
  protocol = "delta_sharing_protocol_error",
  kernel = "delta_sharing_kernel_error",
  unsupported = "delta_sharing_unsupported_error",
  not_implemented = c(
    "delta_sharing_not_implemented_error",
    "delta_sharing_unsupported_error"
  ),
  cancelled = "delta_sharing_cancelled",
  native = "delta_sharing_native_error",
  native_unavailable = c(
    "delta_sharing_native_unavailable_error",
    "delta_sharing_unsupported_error"
  )
)

.safe_condition_fields <- c(
  "operation",
  "status",
  "endpoint_host",
  "retry_count",
  "table",
  "kernel_category",
  "response_format",
  "feature"
)

#' Delta Sharing conditions
#'
#' All errors raised by the public API inherit from
#' `delta_sharing_error`. More specific subclasses identify validation,
#' authentication, HTTP, protocol, kernel, unsupported-feature, cancellation,
#' and native-boundary failures.
#'
#' Conditions may carry only safe diagnostic fields. Credentials, request
#' bodies, signed URLs, and secret material are never attached.
#'
#' @name delta_sharing_conditions
#' @keywords internal
NULL

.new_delta_sharing_condition <- function(message,
                                         type,
                                         ...,
                                         call = NULL) {
  if (!.is_scalar_character(message)) {
    stop("`message` must be one non-empty string.", call. = FALSE)
  }
  if (!.is_scalar_character(type) || is.null(.condition_classes[[type]])) {
    stop("Unknown Delta Sharing condition type.", call. = FALSE)
  }

  details <- list(...)
  if (length(details) > 0L && is.null(names(details))) {
    stop("Condition metadata must be named.", call. = FALSE)
  }
  details <- details[intersect(names(details), .safe_condition_fields)]

  structure(
    c(list(message = message, call = call), details),
    class = c(
      .condition_classes[[type]],
      "delta_sharing_error",
      "error",
      "condition"
    )
  )
}

.abort_delta_sharing <- function(message,
                                 type,
                                 ...,
                                 call = NULL) {
  stop(.new_delta_sharing_condition(
    message = message,
    type = type,
    ...,
    call = call
  ))
}
