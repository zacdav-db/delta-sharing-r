.profile_max_bytes <- 1024L * 1024L
.profile_versions <- c(1, 2)
.private_key_algorithms <- "RS256"

.profile_credentials_registry <- new.env(hash = TRUE, parent = emptyenv())
.client_context_registry <- new.env(hash = TRUE, parent = emptyenv())

.private_handle_sequence <- local({
  sequence <- 0
  function(prefix) {
    sequence <<- sequence + 1
    paste0(prefix, "-", sequence)
  }
})

.new_private_handle <- function(registry, value, prefix) {
  id <- .private_handle_sequence(prefix)
  nonce <- new.env(parent = emptyenv())
  handle <- new.env(parent = emptyenv())
  handle$id <- id
  handle$nonce <- nonce
  lockEnvironment(handle, bindings = TRUE)

  assign(id, list(value = value, nonce = nonce), envir = registry)
  reg.finalizer(
    handle,
    function(handle) {
      id <- handle$id
      if (exists(id, envir = registry, inherits = FALSE)) {
        record <- get(id, envir = registry, inherits = FALSE)
        if (is.list(record) && identical(record$nonce, handle$nonce)) {
          rm(list = id, envir = registry)
        }
      }
    },
    onexit = TRUE
  )
  handle
}

.private_handle_value <- function(object, attribute, registry, kind) {
  handle <- attr(object, attribute, exact = TRUE)
  record <- if (is.environment(handle) &&
      .is_scalar_character(handle$id) &&
      exists(handle$id, envir = registry, inherits = FALSE)) {
    get(handle$id, envir = registry, inherits = FALSE)
  } else {
    NULL
  }
  if (!is.list(record) ||
      !is.environment(handle$nonce) ||
      !identical(handle$nonce, record$nonce)) {
    .abort_delta_sharing(
      sprintf("The internal %s is no longer available.", kind),
      type = "validation"
    )
  }
  record$value
}

.profile_credentials <- function(profile) {
  if (!.object_is(profile, SharingProfile)) {
    .abort_delta_sharing(
      "`profile` must be a SharingProfile.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  .private_handle_value(
    profile,
    ".credential_handle",
    .profile_credentials_registry,
    "profile credential source"
  )
}

.client_context <- function(client) {
  if (!.object_is(client, SharingClient)) {
    .abort_delta_sharing(
      "`client` must be a SharingClient.",
      type = "validation",
      operation = "sharing_client"
    )
  }
  .private_handle_value(
    client,
    ".context_handle",
    .client_context_registry,
    "client context"
  )
}

.new_client_context <- function(profile) {
  context <- new.env(parent = emptyenv())
  context$endpoint <- profile@endpoint
  context$auth_type <- profile@auth_type
  context$credentials <- .profile_credentials(profile)
  context$state <- "configured"
  context$access_token <- NULL
  context$access_token_issued_at <- NULL
  context$access_token_expires_at <- NULL
  context$access_token_refresh_at <- NULL
  context$access_token_generation <- 0
  context
}

.profile_source_type <- function(source, source_type = NULL) {
  supported <- c("path", "json", "connection", "list")
  if (is.null(source_type)) {
    source_type <- if (inherits(source, "connection")) {
      "connection"
    } else if (is.raw(source)) {
      "json"
    } else if (is.list(source)) {
      "list"
    } else if (.is_scalar_character(source)) {
      if (startsWith(trimws(source), "{")) "json" else "path"
    } else {
      .abort_delta_sharing(
        paste0(
          "`source` must be a profile path, inline JSON, raw vector, ",
          "connection, or list."
        ),
        type = "validation",
        operation = "sharing_profile"
      )
    }
  }

  if (!.is_scalar_character(source_type) ||
      !source_type %in% supported) {
    .abort_delta_sharing(
      paste0(
        "`source_type` must be one of ",
        paste(sprintf("\"%s\"", supported), collapse = ", "),
        "."
      ),
      type = "validation",
      operation = "sharing_profile"
    )
  }

  valid <- switch(
    source_type,
    path = .is_scalar_character(source),
    json = .is_scalar_character(source) || is.raw(source),
    connection = inherits(source, "connection"),
    list = is.list(source)
  )
  if (!valid) {
    .abort_delta_sharing(
      sprintf("`source` is not valid for source type \"%s\".", source_type),
      type = "validation",
      operation = "sharing_profile"
    )
  }
  source_type
}

.profile_source_label <- function(source, source_type) {
  switch(
    source_type,
    path = basename(source),
    json = "inline JSON",
    connection = "connection",
    list = "inline profile"
  )
}

.read_profile_path <- function(path) {
  info <- suppressWarnings(file.info(path))
  if (nrow(info) != 1L ||
      is.na(info$size) ||
      isTRUE(info$isdir) ||
      info$size > .profile_max_bytes) {
    message <- if (!is.na(info$size) && info$size > .profile_max_bytes) {
      "Profile input exceeds the 1 MiB size limit."
    } else {
      "The profile file could not be read."
    }
    .abort_delta_sharing(
      message,
      type = "validation",
      operation = "sharing_profile"
    )
  }

  connection <- tryCatch(
    file(path, open = "rb"),
    error = function(error) NULL
  )
  if (is.null(connection)) {
    .abort_delta_sharing(
      "The profile file could not be read.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  on.exit(close(connection), add = TRUE)
  tryCatch(
    readBin(connection, what = "raw", n = .profile_max_bytes + 1L),
    error = function(error) {
      .abort_delta_sharing(
        "The profile file could not be read.",
        type = "validation",
        operation = "sharing_profile"
      )
    }
  )
}

.read_profile_connection <- function(connection) {
  opened_here <- FALSE
  if (!isOpen(connection)) {
    tryCatch(
      {
        open(connection, open = "rb")
        opened_here <- TRUE
      },
      error = function(error) {
        .abort_delta_sharing(
          "The profile connection could not be read.",
          type = "validation",
          operation = "sharing_profile"
        )
      }
    )
  }
  if (opened_here) {
    on.exit(close(connection), add = TRUE)
  }
  if (!isOpen(connection, rw = "read")) {
    .abort_delta_sharing(
      "The profile connection could not be read.",
      type = "validation",
      operation = "sharing_profile"
    )
  }

  pieces <- list()
  size <- 0L
  repeat {
    chunk <- tryCatch(
      readBin(connection, what = "raw", n = 65536L),
      error = function(error) {
        .abort_delta_sharing(
          paste0(
            "The profile connection must support bounded binary reads."
          ),
          type = "validation",
          operation = "sharing_profile"
        )
      }
    )
    if (length(chunk) == 0L) {
      break
    }
    size <- size + length(chunk)
    if (size > .profile_max_bytes) {
      .abort_delta_sharing(
        "Profile input exceeds the 1 MiB size limit.",
        type = "validation",
        operation = "sharing_profile"
      )
    }
    pieces[[length(pieces) + 1L]] <- chunk
  }
  if (length(pieces) == 0L) {
    return(raw())
  }
  do.call(c, pieces)
}

.profile_json_bytes <- function(source, source_type) {
  bytes <- switch(
    source_type,
    path = .read_profile_path(source),
    json = if (is.raw(source)) source else charToRaw(enc2utf8(source)),
    connection = .read_profile_connection(source)
  )
  if (length(bytes) > .profile_max_bytes) {
    .abort_delta_sharing(
      "Profile input exceeds the 1 MiB size limit.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  bytes
}

.parse_profile_json <- function(bytes) {
  force(bytes)
  parsed <- tryCatch(
    jsonlite::fromJSON(
      rawToChar(bytes),
      simplifyVector = FALSE,
      simplifyDataFrame = FALSE,
      simplifyMatrix = FALSE
    ),
    error = function(error) NULL
  )
  if (!is.list(parsed) || is.null(names(parsed))) {
    .abort_delta_sharing(
      "Profile input must be one valid JSON object.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  parsed
}

.validate_profile_object <- function(profile, field = "profile") {
  if (!is.list(profile) ||
      is.null(names(profile)) ||
      anyNA(names(profile)) ||
      any(!nzchar(names(profile))) ||
      anyDuplicated(names(profile))) {
    .abort_delta_sharing(
      sprintf("`%s` must be a JSON-style object with unique named fields.", field),
      type = "validation",
      operation = "sharing_profile"
    )
  }
  profile
}

.profile_field <- function(profile, name) {
  if (!name %in% names(profile)) {
    return(NULL)
  }
  profile[[name]]
}

.required_profile_text <- function(profile, name, display_name = name) {
  value <- .profile_field(profile, name)
  if (!.is_scalar_character(value) || !nzchar(trimws(value))) {
    .abort_delta_sharing(
      sprintf("Profile field `%s` must be one non-empty string.", display_name),
      type = "validation",
      operation = "sharing_profile"
    )
  }
  value
}

.optional_profile_text <- function(profile, name, display_name = name) {
  value <- .profile_field(profile, name)
  if (is.null(value)) {
    return(NULL)
  }
  if (!.is_scalar_character(value) || !nzchar(trimws(value))) {
    .abort_delta_sharing(
      sprintf("Profile field `%s` must be one non-empty string.", display_name),
      type = "validation",
      operation = "sharing_profile"
    )
  }
  value
}

.normalize_profile_url <- function(value, field) {
  if (!.is_scalar_character(value) ||
      grepl("[[:space:]\\\\?#]", value) ||
      !grepl("^https?://", value, ignore.case = TRUE)) {
    .abort_delta_sharing(
      sprintf("Profile field `%s` must be an absolute HTTP(S) URL.", field),
      type = "validation",
      operation = "sharing_profile"
    )
  }

  authority <- sub("^https?://", "", value, ignore.case = TRUE)
  authority <- strsplit(authority, "/", fixed = TRUE)[[1L]][[1L]]
  if (!nzchar(authority) || grepl("@", authority, fixed = TRUE)) {
    .abort_delta_sharing(
      sprintf(
        "Profile field `%s` must be an absolute HTTP(S) URL without embedded credentials.",
        field
      ),
      type = "validation",
      operation = "sharing_profile"
    )
  }
  sub("/+$", "", value)
}

.normalize_profile_version <- function(profile) {
  version <- .profile_field(profile, "shareCredentialsVersion")
  if (!is.numeric(version) ||
      length(version) != 1L ||
      is.na(version) ||
      !is.finite(version) ||
      version != floor(version)) {
    .abort_delta_sharing(
      "Profile field `shareCredentialsVersion` must be the number 1 or 2.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  if (version > max(.profile_versions)) {
    .abort_delta_sharing(
      paste0(
        "This profile version is newer than supported; ",
        "upgrade delta.sharing before using it."
      ),
      type = "unsupported",
      operation = "sharing_profile",
      feature = "profile version"
    )
  }
  if (!version %in% .profile_versions) {
    .abort_delta_sharing(
      "Profile field `shareCredentialsVersion` must be the number 1 or 2.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  as.double(version)
}

.normalize_profile_expiration <- function(profile) {
  value <- .profile_field(profile, "expirationTime")
  if (is.null(value)) {
    return(NULL)
  }
  if (!.is_scalar_character(value) ||
      !grepl(
        paste0(
          "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}",
          "(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})$"
        ),
        value,
        perl = TRUE
      )) {
    .abort_delta_sharing(
      "Profile field `expirationTime` must be an RFC 3339 timestamp.",
      type = "validation",
      operation = "sharing_profile"
    )
  }

  normalized <- sub("Z$", "+0000", value)
  normalized <- sub("([+-]\\d{2}):(\\d{2})$", "\\1\\2", normalized)
  parsed <- suppressWarnings(as.POSIXct(
    strptime(normalized, "%Y-%m-%dT%H:%M:%OS%z", tz = "UTC"),
    tz = "UTC"
  ))
  if (length(parsed) != 1L || is.na(parsed)) {
    .abort_delta_sharing(
      "Profile field `expirationTime` must be an RFC 3339 timestamp.",
      type = "validation",
      operation = "sharing_profile"
    )
  }
  structure(as.double(parsed), class = c("POSIXct", "POSIXt"), tzone = "UTC")
}

.parse_bearer_auth <- function(profile) {
  list(
    kind = "bearer_token",
    bearer_token = .required_profile_text(profile, "bearerToken"),
    expiration_time = .normalize_profile_expiration(profile)
  )
}

.parse_oauth_client_auth <- function(profile) {
  list(
    kind = "oauth_client_credentials",
    token_endpoint = .normalize_profile_url(
      .required_profile_text(profile, "tokenEndpoint"),
      "tokenEndpoint"
    ),
    client_id = .required_profile_text(profile, "clientId"),
    client_secret = .required_profile_text(profile, "clientSecret"),
    scope = .optional_profile_text(profile, "scope")
  )
}

.parse_basic_auth <- function(profile) {
  list(
    kind = "basic",
    username = .required_profile_text(profile, "username"),
    password = .required_profile_text(profile, "password")
  )
}

.parse_private_key_auth <- function(profile) {
  auth <- .profile_field(profile, "auth")
  auth <- .validate_profile_object(auth, "auth")
  key <- .profile_field(auth, "privateKey")
  key <- .validate_profile_object(key, "auth.privateKey")
  algorithm <- .optional_profile_text(
    key,
    "algorithm",
    "auth.privateKey.algorithm"
  )
  if (is.null(algorithm)) {
    algorithm <- "RS256"
  }
  if (!algorithm %in% .private_key_algorithms) {
    .abort_delta_sharing(
      "Profile field `auth.privateKey.algorithm` is not supported.",
      type = "unsupported",
      operation = "sharing_profile",
      feature = "private-key signing algorithm"
    )
  }

  list(
    kind = "oauth_jwt_bearer_private_key_jwt",
    token_endpoint = .normalize_profile_url(
      .required_profile_text(auth, "tokenEndpoint", "auth.tokenEndpoint"),
      "auth.tokenEndpoint"
    ),
    client_id = .required_profile_text(auth, "clientId", "auth.clientId"),
    issuer = .required_profile_text(auth, "issuer", "auth.issuer"),
    audience = .required_profile_text(auth, "audience", "auth.audience"),
    scope = .optional_profile_text(auth, "scope", "auth.scope"),
    private_key_file = .required_profile_text(
      key,
      "privateKeyFile",
      "auth.privateKey.privateKeyFile"
    ),
    key_id = .optional_profile_text(key, "keyId", "auth.privateKey.keyId"),
    algorithm = algorithm
  )
}

.parse_profile_auth <- function(profile, version) {
  type <- .profile_field(profile, "type")
  if (version == 1) {
    if (!is.null(type) &&
        (!.is_scalar_character(type) || !identical(type, "bearer_token"))) {
      .abort_delta_sharing(
        "Profile version 1 supports bearer authentication only.",
        type = "validation",
        operation = "sharing_profile"
      )
    }
    return(.parse_bearer_auth(profile))
  }

  type <- .required_profile_text(profile, "type")
  switch(
    type,
    bearer_token = .parse_bearer_auth(profile),
    oauth_client_credentials = .parse_oauth_client_auth(profile),
    oauth_jwt_bearer_private_key_jwt = .parse_private_key_auth(profile),
    basic = .parse_basic_auth(profile),
    .abort_delta_sharing(
      "The configured profile authentication type is not supported.",
      type = "unsupported",
      operation = "sharing_profile",
      feature = "profile authentication type"
    )
  )
}

.parse_profile_source <- function(source, source_type = NULL) {
  source_type <- .profile_source_type(source, source_type)
  profile <- if (identical(source_type, "list")) {
    source
  } else {
    .parse_profile_json(.profile_json_bytes(source, source_type))
  }
  profile <- .validate_profile_object(profile)
  version <- .normalize_profile_version(profile)
  endpoint <- .normalize_profile_url(
    .required_profile_text(profile, "endpoint"),
    "endpoint"
  )
  credentials <- .parse_profile_auth(profile, version)

  list(
    source_type = source_type,
    label = .profile_source_label(source, source_type),
    version = version,
    endpoint = endpoint,
    auth_type = credentials$kind,
    expiration_time = credentials$expiration_time,
    credentials = credentials
  )
}
