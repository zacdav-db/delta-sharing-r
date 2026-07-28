.optional_numeric <- S7::new_union(NULL, S7::class_numeric)
.optional_timestamp <- S7::new_union(NULL, S7::class_POSIXct)
.optional_character <- S7::new_union(NULL, S7::class_character)
.optional_list <- S7::new_union(NULL, S7::class_list)

#' A Delta Sharing profile source
#'
#' `SharingProfile` is a small, read-only descriptor for a standard Delta
#' Sharing profile source. The source may be a file path, inline JSON, a
#' connection, or an inline list. Construction parses and validates profile
#' versions 1 and 2 without making a network request. Only safe metadata is
#' exposed as properties; credential material is held behind an opaque
#' package-private handle and is excluded from printed output.
#'
#' Most users create this object indirectly with [sharing_client()].
#'
#' @param source Profile file path, inline JSON string or raw vector,
#'   connection, or inline list.
#' @param source_type Optional explicit source type: `"path"`, `"json"`,
#'   `"connection"`, or `"list"`.
#' @return A read-only `SharingProfile` object.
#' @section Supported profiles:
#' Profile version 1 supports bearer credentials. Profile version 2 supports
#' `bearer_token`, `basic`, `oauth_client_credentials`, and
#' `oauth_jwt_bearer_private_key_jwt` descriptors. OAuth exchange and
#' private-key loading happen in the later authentication layer, not during
#' construction.
#'
#' JSON, file, and connection inputs are limited to 1 MiB. A supplied open
#' connection is consumed from its current position and remains open. A
#' supplied closed connection is opened for reading and closed after parsing.
#'
#' The endpoint and token endpoint must be absolute HTTP(S) URLs without
#' embedded credentials, query strings, or fragments. Bearer expiration times
#' use RFC 3339.
#' @export
SharingProfile <- S7::new_class(
  "SharingProfile",
  package = "delta.sharing",
  properties = list(
    source_type = .readonly_property(S7::class_character, ".source_type"),
    label = .readonly_property(S7::class_character, ".label"),
    version = .readonly_property(S7::class_numeric, ".version"),
    endpoint = .readonly_property(S7::class_character, ".endpoint"),
    auth_type = .readonly_property(S7::class_character, ".auth_type"),
    expiration_time = .readonly_property(
      .optional_timestamp,
      ".expiration_time"
    )
  ),
  constructor = function(source, source_type = NULL) {
    parsed <- .parse_profile_source(source, source_type)
    credential_handle <- .new_private_handle(
      .profile_credentials_registry,
      parsed$credentials,
      "profile"
    )

    S7::new_object(
      S7::S7_object(),
      .source_type = parsed$source_type,
      .label = parsed$label,
      .version = parsed$version,
      .endpoint = parsed$endpoint,
      .auth_type = parsed$auth_type,
      .expiration_time = parsed$expiration_time,
      .credential_handle = credential_handle
    )
  },
  validator = function(self) {
    if (!.is_scalar_character(self@source_type) ||
        !self@source_type %in% c("path", "json", "connection", "list")) {
      "@source_type must identify a supported profile source"
    } else if (!.is_scalar_character(self@label)) {
      "@label must be one non-empty string"
    } else if (!is.numeric(self@version) ||
        length(self@version) != 1L ||
        !self@version %in% .profile_versions) {
      "@version must be 1 or 2"
    } else if (!.is_scalar_character(self@endpoint)) {
      "@endpoint must be one absolute HTTP(S) URL"
    } else if (!.is_scalar_character(self@auth_type)) {
      "@auth_type must identify the configured authentication type"
    }
  }
)

.as_sharing_profile <- function(x) {
  if (.object_is(x, SharingProfile)) {
    x
  } else {
    SharingProfile(x)
  }
}

#' An immutable Delta Sharing client descriptor
#'
#' A `SharingClient` stores a safe profile descriptor. Its mutable
#' authentication context is R-owned, hidden behind a package-private handle,
#' and never exposed as an S7 property. Construction validates configuration
#' but performs no token exchange or network request.
#'
#' Most users call [sharing_client()] rather than this class constructor.
#'
#' @param profile A [SharingProfile].
#' @return A read-only `SharingClient` object.
#' @export
SharingClient <- S7::new_class(
  "SharingClient",
  package = "delta.sharing",
  properties = list(
    profile = .readonly_property(class = SharingProfile, ".profile")
  ),
  constructor = function(profile) {
    profile <- .as_sharing_profile(profile)
    context_handle <- .new_private_handle(
      .client_context_registry,
      .new_client_context(profile),
      "client"
    )
    S7::new_object(
      S7::S7_object(),
      .profile = profile,
      .context_handle = context_handle
    )
  },
  validator = function(self) {
    if (!.object_is(self@profile, SharingProfile)) {
      "@profile must be a SharingProfile"
    }
  }
)

#' A structured Delta Sharing table identifier
#'
#' A structured identifier preserves share, schema, and table names exactly,
#' including names that themselves contain dots.
#'
#' Use [table_identifier()] for the functional constructor.
#'
#' @param share Share name.
#' @param schema Schema name.
#' @param table Table name.
#' @return A read-only `SharingTableIdentifier` object.
#' @export
SharingTableIdentifier <- S7::new_class(
  "SharingTableIdentifier",
  package = "delta.sharing",
  properties = list(
    share = .readonly_property(S7::class_character, ".share"),
    schema = .readonly_property(S7::class_character, ".schema"),
    table = .readonly_property(S7::class_character, ".table")
  ),
  constructor = function(share, schema, table) {
    share <- .normalize_identifier_part(share, "share")
    schema <- .normalize_identifier_part(schema, "schema")
    table <- .normalize_identifier_part(table, "table")
    S7::new_object(
      S7::S7_object(),
      .share = share,
      .schema = schema,
      .table = table
    )
  },
  validator = function(self) {
    problems <- character()
    for (name in c("share", "schema", "table")) {
      if (!.is_scalar_character(S7::prop(self, name))) {
        problems <- c(
          problems,
          sprintf("@%s must be one non-empty string", name)
        )
      }
    }
    problems
  }
)

#' An immutable Delta Sharing table descriptor
#'
#' A `SharingTable` pairs a client with a structured table identifier. It is
#' cheap and reusable; query configuration belongs to [SharingRead] or
#' [SharingChanges] objects.
#'
#' Most users call [sharing_table()] rather than this class constructor.
#'
#' @param client A [SharingClient].
#' @param identifier A [SharingTableIdentifier].
#' @return A read-only `SharingTable` object.
#' @export
SharingTable <- S7::new_class(
  "SharingTable",
  package = "delta.sharing",
  properties = list(
    client = .readonly_property(SharingClient, ".client"),
    identifier = .readonly_property(SharingTableIdentifier, ".identifier")
  ),
  constructor = function(client, identifier) {
    if (!.object_is(client, SharingClient)) {
      .abort_delta_sharing(
        "`client` must be a SharingClient.",
        type = "validation",
        operation = "sharing_table"
      )
    }
    if (!.object_is(identifier, SharingTableIdentifier)) {
      .abort_delta_sharing(
        "`identifier` must be a SharingTableIdentifier.",
        type = "validation",
        operation = "sharing_table"
      )
    }
    S7::new_object(
      S7::S7_object(),
      .client = client,
      .identifier = identifier
    )
  }
)

#' An immutable snapshot read specification
#'
#' A read identifies the latest table state, an explicit version, or a point
#' in time. Projection, limit, and predicate hints are part of the descriptor
#' and never mutate the reusable table handle.
#'
#' Most users call [sharing_read()] rather than this class constructor.
#'
#' @param table A [SharingTable].
#' @param columns Optional character vector of projected columns.
#' @param limit Optional non-negative whole-number row limit. The reader sends
#'   it as a server hint and must also enforce it exactly.
#' @param version Optional non-negative whole-number table version.
#' @param timestamp Optional scalar `POSIXct` timestamp.
#' @param predicate Optional structured server-side predicate hint. Hints are
#'   best effort and are not exact row filters.
#' @param response_format One of `"auto"`, `"delta"`, or `"parquet"`.
#' @return A read-only `SharingRead` object.
#' @export
SharingRead <- S7::new_class(
  "SharingRead",
  package = "delta.sharing",
  properties = list(
    table = .readonly_property(SharingTable, ".table"),
    columns = .readonly_property(.optional_character, ".columns"),
    response_format = .readonly_property(
      S7::class_character,
      ".response_format"
    ),
    version = .readonly_property(.optional_numeric, ".version"),
    timestamp = .readonly_property(.optional_timestamp, ".timestamp"),
    limit = .readonly_property(.optional_numeric, ".limit"),
    predicate = .readonly_property(
      .optional_list,
      ".predicate"
    )
  ),
  constructor = function(table,
                         columns = NULL,
                         limit = NULL,
                         version = NULL,
                         timestamp = NULL,
                         predicate = NULL,
                         response_format = "auto") {
    if (!.object_is(table, SharingTable)) {
      .abort_delta_sharing(
        "`table` must be a SharingTable.",
        type = "validation",
        operation = "sharing_read"
      )
    }
    columns <- .normalize_columns(columns)
    limit <- .normalize_limit(limit)
    version <- .normalize_version(version, "version")
    timestamp <- .normalize_timestamp(timestamp, "timestamp")
    predicate <- .normalize_predicate(predicate)
    response_format <- .normalize_response_format(response_format)

    if (!is.null(version) && !is.null(timestamp)) {
      .abort_delta_sharing(
        "`version` and `timestamp` are mutually exclusive.",
        type = "validation",
        operation = "sharing_read"
      )
    }

    S7::new_object(
      S7::S7_object(),
      .table = table,
      .columns = columns,
      .response_format = response_format,
      .version = version,
      .timestamp = timestamp,
      .limit = limit,
      .predicate = predicate
    )
  },
  validator = function(self) {
    if (!is.null(self@version) && !is.null(self@timestamp)) {
      "@version and @timestamp must not both be set"
    }
  }
)

#' An immutable change data feed read specification
#'
#' A change specification has exactly one starting bound and an optional
#' ending bound of the same kind. Version and timestamp bounds cannot be mixed.
#'
#' Most users call [sharing_changes()] rather than this class constructor.
#'
#' @param table A [SharingTable].
#' @param starting_version,ending_version Optional non-negative whole-number
#'   version bounds.
#' @param starting_timestamp,ending_timestamp Optional scalar `POSIXct`
#'   timestamp bounds.
#' @param columns Optional character vector of projected columns.
#' @param response_format One of `"auto"`, `"delta"`, or `"parquet"`.
#' @return A read-only `SharingChanges` object.
#' @export
SharingChanges <- S7::new_class(
  "SharingChanges",
  package = "delta.sharing",
  properties = list(
    table = .readonly_property(SharingTable, ".table"),
    columns = .readonly_property(.optional_character, ".columns"),
    response_format = .readonly_property(
      S7::class_character,
      ".response_format"
    ),
    starting_version = .readonly_property(
      .optional_numeric,
      ".starting_version"
    ),
    ending_version = .readonly_property(
      .optional_numeric,
      ".ending_version"
    ),
    starting_timestamp = .readonly_property(
      .optional_timestamp,
      ".starting_timestamp"
    ),
    ending_timestamp = .readonly_property(
      .optional_timestamp,
      ".ending_timestamp"
    )
  ),
  constructor = function(table,
                         starting_version = NULL,
                         ending_version = NULL,
                         starting_timestamp = NULL,
                         ending_timestamp = NULL,
                         columns = NULL,
                         response_format = "auto") {
    if (!.object_is(table, SharingTable)) {
      .abort_delta_sharing(
        "`table` must be a SharingTable.",
        type = "validation",
        operation = "sharing_changes"
      )
    }

    starting_version <- .normalize_version(
      starting_version,
      "starting_version"
    )
    ending_version <- .normalize_version(ending_version, "ending_version")
    starting_timestamp <- .normalize_timestamp(
      starting_timestamp,
      "starting_timestamp"
    )
    ending_timestamp <- .normalize_timestamp(
      ending_timestamp,
      "ending_timestamp"
    )
    columns <- .normalize_columns(columns)
    response_format <- .normalize_response_format(response_format)

    has_version <- !is.null(starting_version) || !is.null(ending_version)
    has_timestamp <- !is.null(starting_timestamp) ||
      !is.null(ending_timestamp)

    if (has_version && has_timestamp) {
      .abort_delta_sharing(
        "Version and timestamp bounds cannot be mixed.",
        type = "validation",
        operation = "sharing_changes"
      )
    }
    if (!has_version && !has_timestamp) {
      .abort_delta_sharing(
        "One of `starting_version` or `starting_timestamp` is required.",
        type = "validation",
        operation = "sharing_changes"
      )
    }
    if (has_version && is.null(starting_version)) {
      .abort_delta_sharing(
        "`starting_version` is required for a version range.",
        type = "validation",
        operation = "sharing_changes"
      )
    }
    if (has_timestamp && is.null(starting_timestamp)) {
      .abort_delta_sharing(
        "`starting_timestamp` is required for a timestamp range.",
        type = "validation",
        operation = "sharing_changes"
      )
    }
    if (!is.null(ending_version) && ending_version < starting_version) {
      .abort_delta_sharing(
        "`ending_version` must be greater than or equal to `starting_version`.",
        type = "validation",
        operation = "sharing_changes"
      )
    }
    if (!is.null(ending_timestamp) &&
        ending_timestamp < starting_timestamp) {
      .abort_delta_sharing(
        "`ending_timestamp` must be greater than or equal to `starting_timestamp`.",
        type = "validation",
        operation = "sharing_changes"
      )
    }

    S7::new_object(
      S7::S7_object(),
      .table = table,
      .columns = columns,
      .response_format = response_format,
      .starting_version = starting_version,
      .ending_version = ending_version,
      .starting_timestamp = starting_timestamp,
      .ending_timestamp = ending_timestamp
    )
  },
  validator = function(self) {
    has_version <- !is.null(self@starting_version) ||
      !is.null(self@ending_version)
    has_timestamp <- !is.null(self@starting_timestamp) ||
      !is.null(self@ending_timestamp)

    problems <- character()
    if (has_version == has_timestamp) {
      problems <- c(
        problems,
        "exactly one kind of starting bound must be set"
      )
    }
    if (has_version && is.null(self@starting_version)) {
      problems <- c(problems, "@starting_version must be set")
    }
    if (has_timestamp && is.null(self@starting_timestamp)) {
      problems <- c(problems, "@starting_timestamp must be set")
    }
    if (!is.null(self@ending_version) &&
        self@ending_version < self@starting_version) {
      problems <- c(problems, "@ending_version must not precede its start")
    }
    if (!is.null(self@ending_timestamp) &&
        self@ending_timestamp < self@starting_timestamp) {
      problems <- c(
        problems,
        "@ending_timestamp must not precede its start"
      )
    }
    problems
  }
)
