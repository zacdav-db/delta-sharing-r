.execution_operations <- c(
  "list_shares",
  "list_schemas",
  "list_tables",
  "table_version",
  "table_protocol",
  "table_metadata",
  "table_schema",
  "read_schema",
  "read_arrow_stream",
  "arrow_from_stream",
  "data_frame_from_stream",
  "read_diagnostics"
)

.execution_state <- new.env(parent = emptyenv())
.execution_state$interface <- NULL

#' R and native execution interface
#'
#' This internal callback interface keeps the first S7 tranche testable without
#' retaining the previous reader. Discovery, profile, protocol, and planning
#' callbacks are placeholders for later R implementations. Only
#' `read_arrow_stream(specification, ...)` represents the future compact Rust
#' and Delta Kernel boundary.
#'
#' Eager `arrow_from_stream(stream)` and `data_frame_from_stream(stream)`
#' adapters receive only that stream, preventing an independent scan path.
#'
#' @param callbacks Uniquely named list of execution callback functions.
#' @return A validated internal execution interface.
#' @keywords internal
.new_execution_interface <- function(callbacks) {
  if (!is.list(callbacks) ||
      is.null(names(callbacks)) ||
      any(!nzchar(names(callbacks))) ||
      anyDuplicated(names(callbacks))) {
    stop("`callbacks` must be a uniquely named list.", call. = FALSE)
  }

  unknown <- setdiff(names(callbacks), .execution_operations)
  if (length(unknown) > 0L) {
    stop(
      "Unknown execution callback: ",
      paste(unknown, collapse = ", "),
      call. = FALSE
    )
  }

  if (any(!vapply(callbacks, is.function, logical(1)))) {
    stop("Every execution callback must be a function.", call. = FALSE)
  }

  structure(callbacks, class = c("delta_sharing_execution_interface", "list"))
}

.set_execution_interface <- function(interface) {
  if (!is.null(interface) &&
      !inherits(interface, "delta_sharing_execution_interface")) {
    stop(
      "`interface` must be created by `.new_execution_interface()`.",
      call. = FALSE
    )
  }

  old <- .execution_state$interface
  .execution_state$interface <- interface
  invisible(old)
}

.with_execution_interface <- function(interface, code) {
  old <- .set_execution_interface(interface)
  on.exit(.set_execution_interface(old), add = TRUE)
  force(code)
}

.execution_callback <- function(operation) {
  interface <- .execution_state$interface
  callback <- if (is.null(interface)) NULL else interface[[operation]]

  if (is.null(callback)) {
    type <- if (identical(operation, "read_arrow_stream")) {
      "native_unavailable"
    } else {
      "not_implemented"
    }
    .abort_delta_sharing(
      sprintf("Execution for `%s()` is not available in this build.", operation),
      type = type,
      operation = operation
    )
  }

  callback
}

.invoke_execution <- function(operation, ...) {
  callback <- .execution_callback(operation)

  tryCatch(
    callback(...),
    delta_sharing_error = function(cnd) stop(cnd),
    error = function(cnd) {
      .abort_delta_sharing(
        sprintf("Execution operation `%s` failed.", operation),
        type = if (identical(operation, "read_arrow_stream")) {
          "native"
        } else {
          "protocol"
        },
        operation = operation
      )
    }
  )
}
