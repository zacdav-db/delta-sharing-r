.onLoad <- function(...) {
  S7::methods_register()
  .set_execution_callbacks(.new_control_execution_callbacks())
}

.onUnload <- function(...) {
  .set_execution_callbacks(NULL)
}

#' @useDynLib delta.sharing, .registration = TRUE, .fixes = "C_"
NULL
