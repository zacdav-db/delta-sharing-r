# Inspect a live Delta-format manifest without printing URLs, query strings,
# credentials, tokens, action payloads, or filesystem paths.
#
# Usage:
#   Rscript tools/profile_signed_dv.R PROFILE TABLE LIMIT

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 3L) {
  stop(
    "Usage: profile_signed_dv.R PROFILE TABLE LIMIT",
    call. = FALSE
  )
}

profile_path <- fs::path_expand(args[[1L]])
table_name <- args[[2L]]
limit <- suppressWarnings(as.numeric(args[[3L]]))
if (
  !fs::file_exists(profile_path) ||
    !nzchar(table_name) ||
    !rlang::is_scalar_integerish(limit, finite = TRUE) ||
    limit < 0
) {
  stop("The profile, table, or limit is invalid.", call. = FALSE)
}

benchmark_library <- Sys.getenv(
  "DELTA_SHARING_BENCHMARK_LIBRARY",
  unset = NA_character_
)
if (is.na(benchmark_library)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  benchmark_library <- as.character(
    fs::path_real(fs::path_expand(benchmark_library))
  )
  .libPaths(c(benchmark_library, .libPaths()))
  suppressPackageStartupMessages(library(delta.sharing))
  installed_path <- as.character(fs::path_real(find.package("delta.sharing")))
  if (!identical(as.character(fs::path_dir(installed_path)), benchmark_library)) {
    stop("The benchmark did not load delta.sharing from its test library.")
  }
}

is_absolute_url <- function(value) {
  is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    grepl("^https?://", value)
}

url_signature_summary <- function(value) {
  if (!is_absolute_url(value)) {
    return(list(
      absolute_https = FALSE,
      query_parameter_count = 0L,
      has_signature_parameter = FALSE,
      has_expiry_parameter = FALSE
    ))
  }
  parsed <- httr2::url_parse(value)
  query <- if (is.null(parsed$query)) list() else parsed$query
  query_names <- tolower(names(query))
  list(
    absolute_https = identical(tolower(parsed$scheme), "https"),
    query_parameter_count = length(query_names),
    has_signature_parameter = any(query_names %in% c(
      "sig", "signature", "x-amz-signature"
    )),
    has_expiry_parameter = any(query_names %in% c(
      "se", "expires", "x-amz-expires"
    ))
  )
}

normalize_expiry <- function(value) {
  expiry <- suppressWarnings(as.numeric(value))
  if (length(expiry) != 1L || !is.finite(expiry)) {
    return(NA_real_)
  }
  if (expiry > 1e12) expiry / 1000 else expiry
}

profile <- delta.sharing:::sharing_profile_parse(profile_path)
auth <- delta.sharing:::sharing_auth_context(profile)
identifier <- delta.sharing:::sharing_table_identifier(table_name)
spec <- list(
  version = NULL,
  timestamp = NULL,
  columns = NULL,
  limit = limit,
  predicate = NULL
)

page_token <- NULL
files <- list()
repeat {
  request <- delta.sharing:::sharing_request(
    profile,
    auth,
    c(delta.sharing:::table_path(identifier), "query"),
    method = "POST",
    operation = "read"
  )
  request <- httr2::req_headers(
    request,
    `delta-sharing-capabilities` = delta.sharing:::query_capabilities("delta")
  )
  request <- httr2::req_body_json(
    request,
    delta.sharing:::query_body(spec, page_token)
  )
  request <- httr2::req_timeout(request, seconds = 30)
  response <- delta.sharing:::sharing_perform(request)
  actions <- delta.sharing:::parse_ndjson_lines(
    httr2::resp_body_string(response),
    "read"
  )
  files <- c(
    files,
    purrr::map(
      purrr::keep(actions, function(action) !is.null(action$file)),
      "file"
    )
  )
  page_token <- delta.sharing:::find_next_page_token(actions)
  if (is.null(page_token)) {
    break
  }
}

if (length(files) == 0L) {
  stop("The live manifest did not contain file actions.", call. = FALSE)
}

summaries <- purrr::map(files, function(file) {
  add <- file$deltaSingleAction$add
  deletion_vector <- add$deletionVector
  list(
    expiration = normalize_expiry(file$expirationTimestamp),
    data_url = url_signature_summary(add$path),
    deletion_vector_storage = if (is.null(deletion_vector$storageType)) {
      NA_character_
    } else {
      deletion_vector$storageType
    },
    deletion_vector_url = url_signature_summary(
      if (is.null(deletion_vector$pathOrInlineDv)) {
        NA_character_
      } else {
        deletion_vector$pathOrInlineDv
      }
    )
  )
})

expirations <- purrr::map_dbl(summaries, "expiration")
expirations <- expirations[is.finite(expirations)]
data_urls <- purrr::map(summaries, "data_url")
dv_urls <- purrr::map(summaries, "deletion_vector_url")
storage_types <- purrr::map_chr(summaries, "deletion_vector_storage")
storage_types <- storage_types[!is.na(storage_types)]
now <- as.numeric(Sys.time())

result <- list(
  table = table_name,
  limit_hint = limit,
  files = length(files),
  files_with_expiration = length(expirations),
  minimum_expiration_utc = if (length(expirations)) {
    format(
      as.POSIXct(min(expirations), origin = "1970-01-01", tz = "UTC"),
      "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    )
  } else {
    NULL
  },
  minimum_seconds_remaining = if (length(expirations)) {
    min(expirations) - now
  } else {
    NULL
  },
  signed_https_data_urls = sum(purrr::map_lgl(
    data_urls,
    ~ .x$absolute_https && .x$has_signature_parameter
  )),
  data_urls_with_expiry_parameter = sum(purrr::map_lgl(
    data_urls,
    "has_expiry_parameter"
  )),
  deletion_vectors = length(storage_types),
  deletion_vector_storage_types = sort(unique(storage_types)),
  signed_https_deletion_vector_urls = sum(purrr::map_lgl(
    dv_urls,
    ~ .x$absolute_https && .x$has_signature_parameter
  )),
  deletion_vector_urls_with_expiry_parameter = sum(purrr::map_lgl(
    dv_urls,
    "has_expiry_parameter"
  )),
  redaction = list(
    urls_emitted = FALSE,
    query_strings_emitted = FALSE,
    credentials_emitted = FALSE
  )
)

cat(
  jsonlite::toJSON(
    result,
    auto_unbox = TRUE,
    null = "null",
    pretty = TRUE
  ),
  "\n"
)
