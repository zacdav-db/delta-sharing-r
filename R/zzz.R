.onUnload <- function(libpath) {
  library.dynam.unload("delta.sharing", libpath)
}

#' @useDynLib delta.sharing, .registration = TRUE, .fixes = "C_"
#' @importFrom R6 R6Class
#' @importFrom rlang %||%
NULL
