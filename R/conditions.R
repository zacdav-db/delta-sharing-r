# Typed conditions for the package, layered on cli/rlang. Every public-facing
# error inherits from `delta_sharing_error`; a `type` selects the specific
# subclass. `cli::cli_abort()` does the formatting and captures the calling
# frame, so messages support cli's inline markup (e.g. `{.arg x}`).

condition_classes <- list(
  validation = "delta_sharing_validation_error",
  auth = "delta_sharing_auth_error",
  http = "delta_sharing_http_error",
  protocol = "delta_sharing_protocol_error",
  kernel = "delta_sharing_kernel_error",
  unsupported = "delta_sharing_unsupported_error",
  cancelled = "delta_sharing_cancelled"
)

#' Delta Sharing conditions
#'
#' All errors raised by the public API inherit from `delta_sharing_error`. More
#' specific subclasses identify validation, authentication, HTTP, protocol,
#' kernel, unsupported-feature, and cancellation failures. Diagnostic fields
#' (such as `operation` or `status`) are attached as condition data; secrets
#' are never included.
#'
#' @name delta_sharing_conditions
#' @keywords internal
NULL

abort <- function(
  message,
  type,
  ...,
  call = rlang::caller_env(),
  .envir = rlang::caller_env()
) {
  classes <- condition_classes[[type]]
  if (is.null(classes)) {
    cli::cli_abort("Unknown Delta Sharing condition type {.val {type}}.")
  }
  cli::cli_abort(
    message,
    class = c(classes, "delta_sharing_error"),
    ...,
    call = call,
    .envir = .envir
  )
}
