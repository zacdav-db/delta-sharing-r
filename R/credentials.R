#' Print DeltaShareCredentials
#'
#' @param x Object of class `DeltaShareCredentials`
#' @param ... Additional args
#' @export
print.DeltaShareCredentials <- function(x, ...) {
  x <- paste0("(V", x$shareCredentialsVersion, ") [", x$endpoint, "]\n")
  cat(x)
  invisible(x)
}

is.DeltaShareCredentials <- function(x) {
  inherits(x, "DeltaShareCredentials")
}

process_credentials <- function(credentials) {
  # check expiration is valid when present
  if (!is.na(credentials$expirationTime)) {
    credentials$expirationTime <- strptime(
      x = credentials$expirationTime,
      format = "%Y-%m-%dT%H:%M:%S",
      tz = "UTC"
    )
    if (Sys.time() > credentials$expirationTime) {
      stop("Credentials are expired as of ", credentials$expirationTime)
    }
  }
  class(credentials) <- "DeltaShareCredentials"
  credentials
}


#' Get Delta Sharing Credentials from Environment Variables
#'
#' @param version Environment variable name that matches credential value of
#' `shareCredentialsVersion`. Defaults to `"DSHARING_VERSION"`.
#' @param token Environment variable name that matches credential value of
#' `bearerToken`. Defaults to `"DSHARING_TOKEN"`.
#' @param endpoint Environment variable name that matches credential value of
#' `endpoint`. Defaults to `"DSHARING_ENDPOINT"`.
#' @param expiration Environment variable name that matches credential value of
#' `expirationTime`. Defaults to `"DSHARING_EXPTIME"`.
#' @export
sharing_creds_from_env <- function(version = "DSHARING_VERSION",
                                   token = "DSHARING_TOKEN",
                                   endpoint = "DSHARING_ENDPOINT",
                                   expiration = "DSHARING_EXPTIME") {
  # expiration is optional and therefore will fall back to NA
  creds <- list(
    shareCredentialsVersion = Sys.getenv(version),
    bearerToken = Sys.getenv(token),
    endpoint = Sys.getenv(endpoint),
    expirationTime = Sys.getenv(expiration, NA)
  )
  process_credentials(creds)
}