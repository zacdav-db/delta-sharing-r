# Authentication for Delta Sharing requests, built on httr2.
#
# The heavy lifting (token exchange, caching, refresh, JWT signing) is delegated
# to httr2/openssl rather than hand-rolled:
#   - bearer_token: httr2::req_auth_bearer_token()
#   - basic:        httr2::req_auth_basic()
#   - oauth_client_credentials:            httr2::req_oauth_client_credentials()
#   - oauth_jwt_bearer_private_key_jwt:    httr2::req_oauth_bearer_jwt()
#
# `sharing_auth_context(profile)` returns an object with an `$authenticate(req)`
# function that applies the correct auth to an httr2 request. httr2's OAuth cache
# lives inside the created oauth_client and is reused across requests.

# Build the httr2 oauth_client used by both OAuth flows. For client-secret the
# client authenticates with a secret; for private-key JWT the key signs the
# bearer assertion used by the token grant.
oauth_no_client_auth <- function(req, ...) {
  req
}

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
    httr2::oauth_client(
      id = credentials$client_id,
      token_url = credentials$token_endpoint,
      key = load_private_key(credentials$private_key_file),
      auth = oauth_no_client_auth,
      name = "delta.sharing"
    )
  } else {
    stop("Unknown internal OAuth credential type.", call. = FALSE)
  }
}

load_private_key <- function(path) {
  # Keep parser details behind the package's stable authentication condition.
  tryCatch(
    openssl::read_key(path),
    error = function(cnd) {
      abort(
        "The configured private key could not be read.",
        type = "auth",
        operation = "oauth_jwt_bearer_private_key_jwt"
      )
    }
  )
}

# Construct the authentication context from a parsed profile. This does no
# network I/O; token exchange happens lazily inside httr2 on first request.
sharing_auth_context <- function(profile) {
  credentials <- profile$credentials
  kind <- credentials$kind

  state <- new.env(parent = emptyenv())
  state$oauth_client <- NULL
  get_oauth_client <- function() {
    if (is.null(state$oauth_client)) {
      state$oauth_client <- oauth_client_for(credentials)
    }
    state$oauth_client
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
      httr2::req_oauth_bearer_jwt(
        req,
        client = get_oauth_client(),
        claim = httr2::jwt_claim(
          iss = credentials$client_id,
          aud = credentials$issuer,
          exp = Sys.time() + 120,
          scope = credentials$scope,
          resource = credentials$audience
        ),
        signature = jose::jwt_encode_sig,
        signature_params = list(
          size = as.integer(
            substr(credentials$algorithm %||% "RS256", 3, 5)
          ),
          header = if (is.null(credentials$key_id)) {
            list()
          } else {
            list(kid = credentials$key_id)
          }
        )
      )
    },
    abort(
      "The configured profile authentication type is not supported.",
      type = "auth",
      operation = "authenticate"
    )
  )

  list(
    authenticate = authenticate,
    # Response-format negotiation is stable for a table within one client
    # session. Keep this cache beside the shared auth context so every table
    # handle and reader created by the client sees the same result without
    # introducing package-global state.
    response_format_cache = new.env(parent = emptyenv())
  )
}
