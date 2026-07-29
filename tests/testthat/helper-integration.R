# Integration tests run against the public Delta Sharing open datasets endpoint
# (the profile shipped in delta-io/delta-sharing examples). Mirroring the Python
# suite, they are gated behind an opt-in env var so offline/CRAN runs skip them,
# and exercise the real network + Delta Kernel path rather than mocks.

# The public, credential-free open datasets profile.
open_datasets_profile <- function() {
  list(
    shareCredentialsVersion = 1,
    endpoint = "https://sharing.delta.io/delta-sharing/",
    bearerToken = "faaie590d541265bcab1f2de9813274bf233"
  )
}

skip_if_no_integration <- function() {
  testthat::skip_if_not(
    nzchar(Sys.getenv("DELTA_SHARING_RUN_INTEGRATION")),
    "Set DELTA_SHARING_RUN_INTEGRATION=1 to run integration tests."
  )
  reachable <- tryCatch(
    !is.null(curl::nslookup("sharing.delta.io", error = FALSE)),
    error = function(e) FALSE
  )
  testthat::skip_if_not(reachable, "sharing.delta.io is not reachable.")
}

open_datasets_client <- function() {
  sharing_client(open_datasets_profile())
}
