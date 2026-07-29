.onUnload <- function(libpath) {
  active_jobs <- try(native_collect_active(), silent = TRUE)
  if (is.numeric(active_jobs) && active_jobs > 0) {
    warning(
      paste(
        "The delta.sharing native library remains loaded for process safety",
        "because a background read is active or was detached."
      ),
      call. = FALSE
    )
    return(invisible(NULL))
  }
  try(native_reap_pending_cleanups(), silent = TRUE)
  library.dynam.unload("delta.sharing", libpath)
}

#' @useDynLib delta.sharing, .registration = TRUE, .fixes = "C_"
#' @importFrom R6 R6Class
#' @importFrom rlang %||%
NULL
