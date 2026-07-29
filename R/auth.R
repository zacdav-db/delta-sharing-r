# Authentication for Delta Sharing requests, built on httr2.
#
# The heavy lifting (token exchange, caching, refresh, JWT signing) is delegated
# to httr2/openssl rather than hand-rolled:
#   - bearer_token: httr2::req_auth_bearer_token()
#   - basic:        httr2::req_auth_basic()
#   - oauth_client_credentials:            httr2::req_oauth_client_credentials()
#   - oauth_jwt_bearer_private_key_jwt:    oauth_client() with JWT-signature
#                                          client auth + client-credentials flow
#
# `sharing_auth_context(profile)` returns an object with an `$authenticate(req)`
# function that applies the correct auth to an httr2 request. httr2's OAuth cache
# lives inside the created oauth_client and is reused across requests.

jwt_assertion_type <- "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

auth_abort <- function(message, operation = "authenticate") {
  abort(message, type = "auth", operation = operation)
}

# Build the httr2 oauth_client used by both OAuth flows. For client-secret the
# client authenticates with a secret in the body/header; for private-key JWT the
# client authenticates by signing a JWT assertion with the RSA key.
oauth_client_for <- function(credentials) {
  if (identical(credentials$kind, "oauth_client_credentials")) {
    httr2::oauth_client(
      id = credentials$client_id,
      token_url = credentials$token_endpoint,
      secret = credentials$client_secret,
      auth = "header",
      name = "delta.sharing"
    )
  } else if (identical(credentials$kind, "oauth_jwt_bearer_private_key_jwt")) {
    key <- load_private_key(credentials$private_key_file)
    claim <- list(
      iss = credentials$issuer,
      sub = credentials$client_id,
      aud = credentials$audience
    )
    httr2::oauth_client(
      id = credentials$client_id,
      token_url = credentials$token_endpoint,
      key = key,
      auth = "jwt_sig",
      auth_params = list(
        claim = claim,
        header = if (is.null(credentials$key_id)) {
          list()
        } else {
          list(kid = credentials$key_id)
        }
      ),
      name = "delta.sharing"
    )
  } else {
    stop("Unknown internal OAuth credential type.", call. = FALSE)
  }
}

load_private_key <- function(path) {
  key <- tryCatch(
    openssl::read_key(path),
    error = function(e) NULL
  )
  if (is.null(key)) {
    auth_abort(
      "The configured private key could not be read.",
      operation = "oauth_jwt_bearer_private_key_jwt"
    )
  }
  key
}

# Construct the authentication context from a parsed profile. This does no
# network I/O; token exchange happens lazily inside httr2 on first request.
sharing_auth_context <- function(profile) {
  credentials <- profile$credentials
  kind <- credentials$kind

  oauth_client <- NULL
  get_oauth_client <- function() {
    if (is.null(oauth_client)) {
      oauth_client <<- oauth_client_for(credentials)
    }
    oauth_client
  }

  authenticate <- switch(
    kind,
    bearer_token = function(req) {
      httr2::req_auth_bearer_token(req, credentials$bearer_token)
    },
    basic = function(req) {
      httr2::req_auth_basic(req, credentials$username, credentials$password)
    },
    oauth_client_credentials = function(req) {
      httr2::req_oauth_client_credentials(
        req,
        client = get_oauth_client(),
        scope = credentials$scope
      )
    },
    oauth_jwt_bearer_private_key_jwt = function(req) {
      httr2::req_oauth_client_credentials(
        req,
        client = get_oauth_client(),
        scope = credentials$scope
      )
    },
    auth_abort("The configured profile authentication type is not supported.")
  )

  structure(
    list(
      kind = kind,
      authenticate = authenticate
    ),
    class = "delta_sharing_auth"
  )
}
