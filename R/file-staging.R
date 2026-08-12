# Local data-file staging and the opt-in session cache.
#
# Every read gets a fresh synthetic Delta log and a private `table/data`
# directory. By default, selected files are downloaded directly into that
# read-owned directory. With caching enabled, immutable objects are downloaded
# once into a table-scoped session cache and hard-linked (or copied when links
# are unavailable) into each read directory. This keeps active reads isolated
# from `table$clear_cache()`.

DEFAULT_THREADS <- 4L

download_cache_state <- new.env(parent = emptyenv())
download_cache_state$root <- NULL

hash_cache_value <- function(value) {
  as.character(openssl::sha256(charToRaw(enc2utf8(value))))
}

# Signed query parameters rotate, while Delta data objects are immutable. Use
# the URL without credentials, query, or fragment as the reusable identity.
stable_url_identity <- function(url) {
  parsed <- httr2::url_parse(url)
  parsed$username <- NULL
  parsed$password <- NULL
  parsed$query <- NULL
  parsed$fragment <- NULL
  httr2::url_build(parsed)
}

session_cache_root <- function(create = TRUE) {
  root <- download_cache_state$root
  if (!is.null(root) && fs::dir_exists(root)) {
    return(root)
  }
  if (!create) {
    return(NULL)
  }

  root <- fs::file_temp(pattern = ".delta-sharing-cache-")
  fs::dir_create(root, mode = "u=rwx,go=")
  download_cache_state$root <- fs::path_real(root)
  download_cache_state$root
}

table_download_cache <- function(profile, identifier, create = TRUE) {
  root <- session_cache_root(create)
  if (is.null(root)) {
    return(NULL)
  }
  key <- hash_cache_value(paste(
    stable_url_identity(profile$endpoint),
    identifier$share,
    identifier$schema,
    identifier$table,
    sep = "\n"
  ))
  path <- fs::path(root, key)
  if (create) {
    fs::dir_create(path, mode = "u=rwx,go=")
  }
  path
}

clear_table_download_cache <- function(profile, identifier) {
  path <- table_download_cache(profile, identifier, create = FALSE)
  if (!is.null(path) && fs::dir_exists(path)) {
    fs::dir_delete(path)
  }
  invisible(NULL)
}

clear_session_download_cache <- function() {
  root <- session_cache_root(create = FALSE)
  if (!is.null(root) && fs::dir_exists(root)) {
    fs::dir_delete(root)
  }
  download_cache_state$root <- NULL
  invisible(NULL)
}

new_staging_context <- function(profile, identifier, table_dir, cache) {
  context <- new.env(parent = emptyenv())
  context$data_dir <- fs::path(table_dir, "data")
  context$cache_dir <- if (cache) {
    table_download_cache(profile, identifier)
  } else {
    NULL
  }
  context$cache <- cache
  context$paths <- new.env(parent = emptyenv())
  context$downloaded <- 0L
  context$cache_hits <- 0L
  context
}

delta_file_field <- function(action) {
  purrr::detect(c("add", "remove", "cdc"), function(name) {
    !is.null(action[[name]])
  })
}

staged_asset <- function(kind, url, size = NULL) {
  identity <- stable_url_identity(url)
  key <- hash_cache_value(paste(kind, identity, size %||% "", sep = "\n"))
  list(
    kind = kind,
    url = url,
    size = size,
    key = key,
    name = paste0(key, if (identical(kind, "data")) ".parquet" else ".bin")
  )
}

action_staged_assets <- function(action) {
  field <- delta_file_field(action)
  if (is.null(field)) {
    return(list())
  }
  file <- action[[field]]
  assets <- list(staged_asset("data", file$path, file$size))

  dv <- file$deletionVector
  if (!is.null(dv) && identical(dv$storageType, "p")) {
    assets[[2L]] <- staged_asset(
      "deletion-vector",
      dv$pathOrInlineDv
    )
  } else if (!is.null(dv) && identical(dv$storageType, "u")) {
    abort(
      "A change file used a relative deletion vector that was not signed by the server.",
      type = "unsupported",
      operation = "read",
      feature = "relative_deletion_vector"
    )
  }
  assets
}

staged_asset_is_complete <- function(path, asset) {
  fs::file_exists(path) && (
    is.null(asset$size) || fs::file_size(path) == asset$size
  )
}

local_file_path <- function(url) {
  parsed <- httr2::url_parse(url)
  if (!identical(parsed$scheme, "file")) {
    return(NULL)
  }
  parsed$path
}

local_file_url <- function(path) {
  parsed <- httr2::url_parse("file:///")
  parsed$path <- as.character(fs::path_abs(path))
  httr2::url_build(parsed)
}

download_request <- function(url) {
  httr2::request(url) |>
    httr2::req_user_agent(user_agent()) |>
    httr2::req_retry(
      max_tries = 5,
      is_transient = \(resp) {
        httr2::resp_status(resp) %in% c(429, 500, 502, 503, 504)
      }
    )
}

staging_download_error <- function() {
  cli::cli_abort(
    "A shared data file could not be downloaded.",
    class = c("httr2_error", "delta_sharing_error")
  )
}

# Download into sibling temporary paths, then publish only complete files.
download_staged_assets <- function(assets, targets, threads) {
  if (length(assets) == 0L) {
    return(invisible(NULL))
  }

  purrr::walk(unique(fs::path_dir(targets)), function(path) {
    fs::dir_create(path, mode = "u=rwx,go=")
  })
  temporary <- purrr::map_chr(targets, function(target) {
    fs::file_temp(
      pattern = ".download-",
      tmp_dir = fs::path_dir(target)
    )
  })
  on.exit(
    purrr::walk(temporary[fs::file_exists(temporary)], fs::file_delete),
    add = TRUE
  )

  local <- purrr::map(assets, function(asset) local_file_path(asset$url))
  local_index <- which(!purrr::map_lgl(local, is.null))
  remote_index <- setdiff(seq_along(assets), local_index)

  purrr::walk(local_index, function(index) {
    source <- local[[index]]
    if (!fs::file_exists(source)) {
      staging_download_error()
    }
    fs::file_copy(source, temporary[[index]])
  })

  if (length(remote_index) > 0L) {
    responses <- httr2::req_perform_parallel(
      purrr::map(assets[remote_index], function(asset) {
        download_request(asset$url)
      }),
      paths = temporary[remote_index],
      on_error = "return",
      progress = FALSE,
      max_active = threads
    )
    failed <- length(responses) != length(remote_index) ||
      purrr::some(responses, inherits, "error") ||
      any(!fs::file_exists(temporary[remote_index]))
    if (failed) {
      staging_download_error()
    }
  }

  if (!all(purrr::map2_lgl(temporary, assets, staged_asset_is_complete))) {
    staging_download_error()
  }

  purrr::walk2(temporary, targets, function(source, target) {
    fs::file_chmod(source, "u=rw,go=")
    fs::file_move(source, target)
  })
  invisible(NULL)
}

# Link a cached object into the read-owned directory. Hard links keep an active
# reader valid if its table cache is cleared. Copying is the portable fallback.
materialize_cached_asset <- function(source, target) {
  if (fs::file_exists(target)) {
    return(invisible(target))
  }
  fs::dir_create(fs::path_dir(target), mode = "u=rwx,go=")
  # Hard links are not universally available (for example across filesystems),
  # so a copy is the required portable fallback.
  linked <- tryCatch(
    {
      fs::link_create(source, target, symbolic = FALSE)
      TRUE
    },
    error = function(condition) FALSE
  )
  if (!linked) {
    fs::file_copy(source, target)
  }
  invisible(target)
}

ensure_staged_assets <- function(assets, context, threads) {
  if (length(assets) == 0L) {
    return(invisible(NULL))
  }
  keys <- purrr::map_chr(assets, "key")
  assets <- assets[!duplicated(keys)]
  keys <- purrr::map_chr(assets, "key")

  source_targets <- if (context$cache) {
    fs::path(context$cache_dir, purrr::map_chr(assets, "name"))
  } else {
    fs::path(context$data_dir, purrr::map_chr(assets, "name"))
  }
  valid_size <- purrr::map2_lgl(
    source_targets,
    assets,
    staged_asset_is_complete
  )
  invalid <- fs::file_exists(source_targets) & !valid_size
  purrr::walk(source_targets[invalid], fs::file_delete)
  missing <- !valid_size
  if (context$cache) {
    context$cache_hits <- context$cache_hits + sum(!missing)
  }

  if (any(missing)) {
    download_staged_assets(assets[missing], source_targets[missing], threads)
    context$downloaded <- context$downloaded + sum(missing)
  }

  read_targets <- fs::path(context$data_dir, purrr::map_chr(assets, "name"))
  if (context$cache) {
    purrr::walk2(source_targets, read_targets, materialize_cached_asset)
  }
  purrr::walk2(keys, read_targets, function(key, path) {
    context$paths[[key]] <- local_file_url(path)
  })
  invisible(NULL)
}

rewrite_staged_action <- function(action, context) {
  field <- delta_file_field(action)
  if (is.null(field)) {
    return(action)
  }
  file <- action[[field]]
  data <- staged_asset("data", file$path, file$size)
  file$path <- context$paths[[data$key]]

  dv <- file$deletionVector
  if (!is.null(dv) && identical(dv$storageType, "p")) {
    asset <- staged_asset("deletion-vector", dv$pathOrInlineDv)
    dv$pathOrInlineDv <- context$paths[[asset$key]]
    file$deletionVector <- dv
  }
  action[[field]] <- file
  action
}

stage_delta_actions <- function(actions, context, threads) {
  assets <- purrr::list_flatten(purrr::map(actions, action_staged_assets))
  ensure_staged_assets(assets, context, threads)
  purrr::map(actions, rewrite_staged_action, context = context)
}
