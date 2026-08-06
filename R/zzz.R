.onUnload <- function(libpath) {
  # Reaping is best-effort here; the shared library must still be unloaded.
  try(native_reap_pending_cleanups(), silent = TRUE)
  library.dynam.unload("delta.sharing", libpath)
}

#' @useDynLib delta.sharing, .registration = TRUE, .fixes = "C_"
#' @importFrom R6 R6Class
#' @importFrom rlang %||%
NULL
