# Session-scoped local staging for shared data files.
#
# A table handle creates a deterministic cache directory under R's temporary
# directory. File and deletion-vector IDs supplied by the sharing server are
# used directly as filenames, so new handles for the same table reuse the same
# immutable objects. Each read creates only a fresh synthetic Delta log.

hash_cache_value <- function(value) {
  unclass(as.character(openssl::sha256(charToRaw(enc2utf8(value)))))
}

session_cache_root <- function() {
  root <- fs::path_temp(".delta-sharing-cache")
  fs::dir_create(root, mode = "u=rwx,go=")
  fs::path_real(root)
}

table_download_cache <- function(profile, identifier) {
  key <- hash_cache_value(paste(
    profile$endpoint,
    identifier$share,
    identifier$schema,
    identifier$table,
    sep = "\n"
  ))
  path <- fs::path(session_cache_root(), key)
  fs::dir_create(path, mode = "u=rwx,go=")
  fs::path_real(path)
}

delta_file_field <- function(action) {
  purrr::detect(c("add", "remove", "cdc"), function(name) {
    !is.null(action[[name]])
  })
}

staged_asset <- function(kind, id, url, size = NULL) {
  if (!is_scalar_character(id)) {
    abort(
      "A shared file action did not include its ID.",
      type = "protocol",
      operation = "read"
    )
  }
  extension <- if (identical(kind, "data")) ".parquet" else ".bin"
  list(
    kind = kind,
    id = id,
    url = url,
    size = size,
    name = paste0(id, extension)
  )
}

file_wrapper_action <- function(file, response_format, operation) {
  synthetic_file_action(file, response_format, operation)
}

file_wrapper_assets <- function(file, response_format, operation) {
  action <- file_wrapper_action(file, response_format, operation)
  field <- delta_file_field(action)
  if (is.null(field)) {
    return(list())
  }
  data <- action[[field]]
  assets <- list(staged_asset(
    "data",
    file$id,
    data$path,
    file$size %||% data$size
  ))

  dv <- data$deletionVector
  if (!is.null(dv) && identical(dv$storageType, "p")) {
    assets[[2L]] <- staged_asset(
      "deletion-vector",
      file$deletionVectorFileId,
      dv$pathOrInlineDv,
      dv$sizeInBytes
    )
  } else if (!is.null(dv) && identical(dv$storageType, "u")) {
    abort(
      "A change file used a relative deletion vector that was not signed by the server.",
      type = "unsupported",
      operation = operation,
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

# Download beside each target so publishing can use a same-filesystem rename.
download_staged_assets <- function(assets, targets, concurrency) {
  if (length(assets) == 0L) {
    return(invisible(NULL))
  }

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
    fs::file_copy(local[[index]], temporary[[index]])
  })

  if (length(remote_index) > 0L) {
    httr2::req_perform_parallel(
      purrr::map(assets[remote_index], function(asset) {
        download_request(asset$url)
      }),
      paths = temporary[remote_index],
      on_error = "stop",
      progress = FALSE,
      max_active = concurrency
    )
  }

  complete <- purrr::map2_lgl(temporary, assets, staged_asset_is_complete)
  if (!all(complete)) {
    abort(
      "A shared data file was incomplete.",
      type = "protocol",
      operation = "read"
    )
  }

  purrr::walk2(temporary, targets, function(source, target) {
    fs::file_chmod(source, "u=rw,go=")
    # This atomically publishes the file without copying its contents again.
    fs::file_move(source, target)
  })
  invisible(NULL)
}

ensure_staged_assets <- function(assets, cache_path, concurrency) {
  fs::dir_create(cache_path, mode = "u=rwx,go=")
  if (length(assets) == 0L) {
    return(list(paths = list(), downloaded = 0L, cache_hits = 0L))
  }

  asset_names <- purrr::map_chr(assets, "name")
  keep <- !duplicated(asset_names)
  assets <- assets[keep]
  asset_names <- asset_names[keep]
  targets <- fs::path(cache_path, asset_names)
  complete <- purrr::map2_lgl(targets, assets, staged_asset_is_complete)
  invalid <- fs::file_exists(targets) & !complete
  purrr::walk(targets[invalid], fs::file_delete)
  missing <- !complete

  download_staged_assets(assets[missing], targets[missing], concurrency)

  list(
    paths = stats::setNames(
      purrr::map(targets, local_file_url),
      asset_names
    ),
    downloaded = sum(missing),
    cache_hits = sum(!missing)
  )
}

rewrite_staged_file <- function(file, response_format, operation, paths) {
  action <- file_wrapper_action(file, response_format, operation)
  field <- delta_file_field(action)
  if (is.null(field)) {
    return(action)
  }

  data_asset <- staged_asset("data", file$id, action[[field]]$path)
  action[[field]]$path <- paths[[data_asset$name]]

  dv <- action[[field]]$deletionVector
  if (!is.null(dv) && identical(dv$storageType, "p")) {
    dv_asset <- staged_asset(
      "deletion-vector",
      file$deletionVectorFileId,
      dv$pathOrInlineDv
    )
    dv$pathOrInlineDv <- paths[[dv_asset$name]]
    action[[field]]$deletionVector <- dv
  }
  action
}

stage_file_wrappers <- function(
  files,
  response_format,
  cache_path,
  concurrency,
  operation
) {
  assets <- purrr::list_flatten(purrr::map(
    files,
    file_wrapper_assets,
    response_format = response_format,
    operation = operation
  ))
  staged <- ensure_staged_assets(assets, cache_path, concurrency)
  list(
    actions = purrr::map(
      files,
      rewrite_staged_file,
      response_format = response_format,
      operation = operation,
      paths = staged$paths
    ),
    downloaded = staged$downloaded,
    cache_hits = staged$cache_hits
  )
}
