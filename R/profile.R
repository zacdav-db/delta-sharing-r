# Delta Sharing profile parsing. Parses profile versions 1 and 2 from a file
# path or an already-parsed list, and returns a plain list. No network request
# or token exchange happens here; credential material stays in the returned
# list and the client holds it privately.
#
# The profile is a config file the user controls. Matching the Python client,
# parsing is structural: decode the object, select a supported version/auth
# shape, and extract the fields that shape requires. Credential content is left
# to httr2, openssl, the token endpoint, or the sharing server when it is used.

# Read a profile file or accept an already-parsed profile list.
read_profile <- function(source) {
  if (is.list(source)) {
    return(source)
  }
  if (!is_scalar_character(source)) {
    abort(
      "{.arg profile} must be a file path or a list.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  json <- read_file(source)
  parsed <- jsonlite::fromJSON(json, simplifyVector = FALSE)
  if (!is.list(parsed) || is.null(names(parsed))) {
    abort(
      "The profile must be one valid JSON object.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  parsed
}

#' Delta Sharing open-datasets profile
#'
#' Fetches the official profile for the Delta Sharing open-datasets server and
#' returns it as a parsed list. The profile is public, but fetching it requires
#' an internet connection.
#'
#' @return A parsed Delta Sharing profile list.
#' @examplesIf interactive()
#' profile <- demo_profile()
#' names(profile)
#' @export
demo_profile <- function() {
  profile_url <- paste0(
    "https://raw.githubusercontent.com/delta-io/delta-sharing/",
    "main/examples/open-datasets.share"
  )
  httr2::request(profile_url) |>
    httr2::req_perform() |>
    httr2::resp_body_json(check_type = FALSE, simplifyVector = FALSE)
}

read_file <- function(path) {
  path <- fs::path_expand(path)
  if (!fs::file_exists(path)) {
    abort(
      "The profile file {.path {path}} does not exist.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  readChar(path, fs::file_size(path), useBytes = TRUE)
}

required_profile_field <- function(profile, name, label = name) {
  if (is.null(names(profile)) || !name %in% names(profile)) {
    abort(
      "Profile field {.field {label}} is required.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  profile[[name]]
}

optional_profile_field <- function(profile, name) {
  profile[[name]]
}

normalize_profile_url <- function(value) {
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    abort(
      "Profile URL fields must be strings.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  if (endsWith(value, "/")) {
    substr(value, 1L, nchar(value) - 1L)
  } else {
    value
  }
}

parse_profile_version <- function(profile) {
  version <- as.integer(
    required_profile_field(profile, "shareCredentialsVersion")
  )

  if (
    length(version) != 1L ||
      is.na(version) ||
      !version %in% c(1L, 2L)
  ) {
    abort(
      "Profile field {.field shareCredentialsVersion} must be 1 or 2.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  version
}

parse_bearer_auth <- function(profile) {
  list(
    kind = "bearer_token",
    bearer_token = required_profile_field(profile, "bearerToken"),
    expiration_time = optional_profile_field(profile, "expirationTime")
  )
}

parse_oauth_client_auth <- function(profile) {
  list(
    kind = "oauth_client_credentials",
    token_endpoint = normalize_profile_url(
      required_profile_field(profile, "tokenEndpoint")
    ),
    client_id = required_profile_field(profile, "clientId"),
    client_secret = required_profile_field(profile, "clientSecret"),
    scope = optional_profile_field(profile, "scope")
  )
}

parse_basic_auth <- function(profile) {
  list(
    kind = "basic",
    username = required_profile_field(profile, "username"),
    password = required_profile_field(profile, "password")
  )
}

parse_private_key_auth <- function(profile) {
  auth <- required_profile_field(profile, "auth")
  if (!is.list(auth)) {
    abort(
      "Profile field {.field auth} must be an object.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  key <- if ("privateKey" %in% names(auth)) auth[["privateKey"]] else list()
  if (!is.list(key)) {
    abort(
      "Profile field {.field auth.privateKey} must be an object.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  list(
    kind = "oauth_jwt_bearer_private_key_jwt",
    token_endpoint = normalize_profile_url(
      required_profile_field(auth, "tokenEndpoint", "auth.tokenEndpoint")
    ),
    client_id = required_profile_field(auth, "clientId", "auth.clientId"),
    issuer = required_profile_field(auth, "issuer", "auth.issuer"),
    audience = required_profile_field(auth, "audience", "auth.audience"),
    scope = optional_profile_field(auth, "scope"),
    private_key_file = optional_profile_field(key, "privateKeyFile"),
    key_id = optional_profile_field(key, "keyId"),
    algorithm = optional_profile_field(key, "algorithm")
  )
}

profile_auth_type <- function(profile) {
  type <- required_profile_field(profile, "type")
  if (!is.character(type) || length(type) != 1L || is.na(type)) {
    abort(
      "Unsupported profile authentication type.",
      type = "unsupported",
      operation = "sharing_profile",
      feature = "profile authentication type"
    )
  }
  type
}

parse_profile_auth <- function(profile, version) {
  # As in Python, version 1 is bearer-only and ignores an optional `type`.
  if (version == 1L) {
    return(parse_bearer_auth(profile))
  }
  type <- profile_auth_type(profile)
  switch(
    type,
    bearer_token = parse_bearer_auth(profile),
    oauth_client_credentials = parse_oauth_client_auth(profile),
    oauth_jwt_bearer_private_key_jwt = parse_private_key_auth(profile),
    basic = parse_basic_auth(profile),
    abort(
      "Unsupported profile authentication type {.val {type}}.",
      type = "unsupported",
      operation = "sharing_profile",
      feature = "profile authentication type"
    )
  )
}

# Parse a profile source into the plain list the client holds internally.
sharing_profile_parse <- function(source) {
  profile <- read_profile(source)
  version <- parse_profile_version(profile)
  credentials <- parse_profile_auth(profile, version)
  list(
    version = version,
    endpoint = normalize_profile_url(
      required_profile_field(profile, "endpoint")
    ),
    auth_type = credentials$kind,
    expiration_time = credentials$expiration_time,
    credentials = credentials
  )
}
