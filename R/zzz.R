.register_internal_snapshot_s3_methods <- function(namespace) {
  classes <- c(
    "delta_sharing_prepared_snapshot",
    "delta_sharing_snapshot_http_request",
    "delta_sharing_snapshot_log",
    "delta_sharing_snapshot_pull_response",
    "delta_sharing_snapshot_request"
  )
  for (class in classes) {
    registerS3method(
      "print",
      class,
      get(paste0("print.", class), envir = namespace, inherits = FALSE),
      envir = namespace
    )
  }
  invisible(NULL)
}

.onLoad <- function(libname, pkgname) {
  namespace <- asNamespace(pkgname)
  .register_internal_snapshot_s3_methods(namespace)
  S7::methods_register()
  .set_execution_callbacks(.new_control_execution_callbacks())
}

.onUnload <- function(...) {
  try(.native_reap_pending_cleanups(), silent = TRUE)
  .set_execution_callbacks(NULL)
}

#' @useDynLib delta.sharing, .registration = TRUE, .fixes = "C_"
NULL
