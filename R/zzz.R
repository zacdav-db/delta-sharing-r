.onLoad <- function(...) {
  S7::methods_register()
}

#' @useDynLib delta.sharing, .registration = TRUE, .fixes = "C_"
NULL
