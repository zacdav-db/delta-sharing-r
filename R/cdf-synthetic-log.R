.cdf_log_operation <- "prepare_cdf_log"
.cdf_log_max_versions <- 1000000L

.cdf_log_abort <- function(message, type = "protocol", ...) {
  .abort_delta_sharing(
    message,
    type = type,
    operation = .cdf_log_operation,
    ...
  )
}

.cdf_whole_version <- function(value, label) {
  valid <- is.numeric(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    is.finite(value) &&
    value == floor(value) &&
    value >= 0 &&
    value <= 2^53
  if (!valid) {
    .cdf_log_abort(sprintf("%s must be a supported provider version.", label))
  }
  as.double(value)
}

.cdf_version_key <- function(version) {
  sprintf("%.0f", .cdf_whole_version(version, "CDF version"))
}

.cdf_validate_range <- function(start_version, end_version) {
  start_version <- .cdf_whole_version(start_version, "`start_version`")
  end_version <- .cdf_whole_version(end_version, "`end_version`")
  count <- end_version - start_version + 1
  if (end_version < start_version ||
      count > .cdf_log_max_versions) {
    .cdf_log_abort(
      "The resolved CDF range is invalid or exceeds the internal version limit.",
      type = "validation"
    )
  }
  list(
    start = start_version,
    end = end_version,
    count = as.integer(count)
  )
}

.cdf_protocol_version <- function(protocol, label) {
  if (!inherits(protocol, "delta_sharing_protocol") ||
      is.null(protocol$version)) {
    .cdf_log_abort(sprintf("%s must carry its provider version.", label))
  }
  .cdf_whole_version(protocol$version, sprintf("%s version", label))
}

.cdf_metadata_version <- function(metadata, label) {
  if (!inherits(metadata, "delta_sharing_metadata") ||
      is.null(metadata$version)) {
    .cdf_log_abort(sprintf("%s must carry its provider version.", label))
  }
  .cdf_whole_version(metadata$version, sprintf("%s version", label))
}

.cdf_file_state <- function(file) {
  state <- tryCatch(
    .snapshot_file_state(file),
    delta_sharing_error = function(condition) NULL
  )
  if (is.null(state) ||
      is.null(state$version) ||
      is.null(state$timestamp)) {
    .cdf_log_abort(
      "Every CDF file wrapper must carry its provider version and timestamp."
    )
  }
  list(
    id = state$id,
    action_type = state$action_type,
    delta_action = state$delta_action,
    expiration_timestamp = state$expiration_timestamp,
    version = .cdf_whole_version(
      state$version,
      "CDF file provider version"
    ),
    timestamp = .cdf_whole_version(
      state$timestamp,
      "CDF file provider timestamp"
    )
  )
}

.cdf_action_plan <- function(protocol,
                             metadata,
                             historical_protocols,
                             historical_metadata,
                             files,
                             start_version,
                             end_version) {
  range <- .cdf_validate_range(start_version, end_version)
  if (!inherits(protocol, "delta_sharing_protocol") ||
      !identical(protocol$response_format, "delta")) {
    .cdf_log_abort("CDF protocol must use Delta response format.")
  }
  if (!inherits(metadata, "delta_sharing_metadata") ||
      !identical(metadata$response_format, "delta")) {
    .cdf_log_abort("CDF metadata must use Delta response format.")
  }
  if (.cdf_protocol_version(protocol, "Head CDF protocol") != range$start ||
      .cdf_metadata_version(metadata, "Head CDF metadata") != range$start) {
    .cdf_log_abort(
      "Head CDF protocol and metadata must match the resolved start version."
    )
  }
  if (!is.list(historical_protocols) ||
      !is.list(historical_metadata) ||
      !is.list(files)) {
    .cdf_log_abort("CDF actions must be supplied as validated lists.")
  }
  if (length(files) > .snapshot_log_max_files) {
    .cdf_log_abort("CDF response contains too many file actions.")
  }

  commits <- vector("list", range$count)
  timestamps <- rep(NA_real_, range$count)
  seen_protocols <- new.env(parent = emptyenv(), hash = TRUE)
  seen_metadata <- new.env(parent = emptyenv(), hash = TRUE)
  seen_files <- new.env(parent = emptyenv(), hash = TRUE)

  offset_for <- function(version, label) {
    if (version < range$start || version > range$end) {
      .cdf_log_abort(sprintf("%s is outside the resolved CDF range.", label))
    }
    as.integer(version - range$start + 1)
  }
  add_action <- function(version, action) {
    offset <- offset_for(version, "CDF action version")
    commits[[offset]] <<- c(commits[[offset]], list(action))
    invisible(NULL)
  }

  protocol_action <- .validate_snapshot_protocol(protocol)
  metadata_action <- .validate_snapshot_metadata(metadata)
  add_action(range$start, list(protocol = protocol_action))
  add_action(range$start, list(metaData = metadata_action))
  assign(.cdf_version_key(range$start), TRUE, envir = seen_protocols)
  assign(.cdf_version_key(range$start), TRUE, envir = seen_metadata)

  for (candidate in historical_protocols) {
    version <- .cdf_protocol_version(candidate, "Historical CDF protocol")
    key <- .cdf_version_key(version)
    if (exists(key, envir = seen_protocols, inherits = FALSE)) {
      .cdf_log_abort("CDF response repeats a protocol version.")
    }
    assign(key, TRUE, envir = seen_protocols)
    add_action(
      version,
      list(protocol = .validate_snapshot_protocol(candidate))
    )
  }
  for (candidate in historical_metadata) {
    version <- .cdf_metadata_version(candidate, "Historical CDF metadata")
    key <- .cdf_version_key(version)
    if (exists(key, envir = seen_metadata, inherits = FALSE)) {
      .cdf_log_abort("CDF response repeats a metadata version.")
    }
    assign(key, TRUE, envir = seen_metadata)
    add_action(
      version,
      list(metaData = .validate_snapshot_metadata(candidate))
    )
  }

  for (file in files) {
    state <- .cdf_file_state(file)
    offset <- offset_for(state$version, "CDF file version")
    key <- paste(
      .cdf_version_key(state$version),
      state$action_type,
      state$id,
      sep = ":"
    )
    if (exists(key, envir = seen_files, inherits = FALSE)) {
      .cdf_log_abort("CDF response repeats a file action.")
    }
    assign(key, TRUE, envir = seen_files)
    if (!is.na(timestamps[[offset]]) &&
        timestamps[[offset]] != state$timestamp) {
      .cdf_log_abort(
        "CDF file wrappers disagree on a provider commit timestamp."
      )
    }
    timestamps[[offset]] <- state$timestamp
    add_action(state$version, state$delta_action)
  }

  list(
    range = range,
    commits = lapply(commits, function(actions) {
      if (length(actions) == 0L) {
        return(character())
      }
      vapply(actions, .snapshot_json_line, character(1), USE.NAMES = FALSE)
    }),
    timestamps = timestamps,
    file_count = length(files)
  )
}

.cdf_checkpoint_asset <- function() {
  path <- system.file(
    "extdata",
    "cdf-empty-checkpoint.parquet",
    package = "delta.sharing"
  )
  if (!.is_scalar_character(path) ||
      !file.exists(path) ||
      isTRUE(file.info(path)$isdir)) {
    .cdf_log_abort("The bundled CDF checkpoint asset is unavailable.")
  }
  path
}

.cdf_commit_name <- function(version, suffix = ".json") {
  paste0(sprintf("%020.0f", version), suffix)
}

.cdf_set_commit_timestamp <- function(path, milliseconds) {
  timestamp <- structure(
    milliseconds / 1000,
    class = c("POSIXct", "POSIXt"),
    tzone = "UTC"
  )
  updated <- suppressWarnings(Sys.setFileTime(path, timestamp))
  observed <- suppressWarnings(file.info(path)$mtime)
  if (!isTRUE(updated) ||
      length(observed) != 1L ||
      is.na(observed) ||
      abs(as.double(observed) * 1000 - milliseconds) > 0.001) {
    .cdf_log_abort(
      "The provider commit timestamp could not be preserved exactly."
    )
  }
  invisible(path)
}

.cdf_secure_file <- function(path, label) {
  if (!file.exists(path) || isTRUE(file.info(path)$isdir)) {
    .cdf_log_abort(sprintf("%s was not written.", label))
  }
  permissions_set <- suppressWarnings(Sys.chmod(path, mode = "0600"))
  if (.Platform$OS.type != "windows" && !isTRUE(permissions_set)) {
    .cdf_log_abort(sprintf("%s permissions could not be secured.", label))
  }
  invisible(path)
}

.prepare_cdf_log <- function(
  protocol,
  metadata,
  historical_protocols = list(),
  historical_metadata = list(),
  files = list(),
  start_version,
  end_version,
  temp_parent = tempdir(),
  checkpoint_asset = .cdf_checkpoint_asset(),
  write_commit = .write_snapshot_commit
) {
  if (!is.function(write_commit)) {
    .cdf_log_abort("`write_commit` must be a function.", type = "validation")
  }
  if (!.is_scalar_character(checkpoint_asset) ||
      !file.exists(checkpoint_asset) ||
      isTRUE(file.info(checkpoint_asset)$isdir)) {
    .cdf_log_abort("`checkpoint_asset` must be one readable file.")
  }
  plan <- .cdf_action_plan(
    protocol = protocol,
    metadata = metadata,
    historical_protocols = historical_protocols,
    historical_metadata = historical_metadata,
    files = files,
    start_version = start_version,
    end_version = end_version
  )
  parent <- tryCatch(
    .validate_snapshot_temp_parent(temp_parent),
    delta_sharing_error = function(condition) {
      .cdf_log_abort(
        "`temp_parent` must be an existing non-symlink directory.",
        type = "validation"
      )
    }
  )
  root <- tempfile(".delta-sharing-snapshot-", tmpdir = parent)
  if (!dir.create(root, mode = "0700", showWarnings = FALSE)) {
    .cdf_log_abort("CDF temporary root could not be created.")
  }
  published <- FALSE
  stage <- "ownership marker"
  on.exit({
    if (!published) {
      .cleanup_snapshot_root(root)
    }
  }, add = TRUE)

  tryCatch(
    {
      stage <- "ownership marker"
      marker <- file.path(root, .snapshot_log_marker_name)
      .write_snapshot_commit(marker, .snapshot_log_marker_value)
      .cdf_secure_file(marker, "CDF ownership marker")

      staging <- file.path(root, ".staging")
      log_dir <- file.path(staging, "_delta_log")
      stage <- "staging directory"
      if (!dir.create(log_dir, recursive = TRUE, mode = "0700")) {
        .cdf_log_abort("CDF staging directory could not be created.")
      }

      if (plan$range$start > 0) {
        stage <- "checkpoint bootstrap"
        checkpoint_version <- plan$range$start - 1
        checkpoint <- file.path(
          log_dir,
          .cdf_commit_name(checkpoint_version, ".checkpoint.parquet")
        )
        if (!isTRUE(file.copy(
          checkpoint_asset,
          checkpoint,
          overwrite = FALSE,
          copy.mode = FALSE,
          copy.date = FALSE
        ))) {
          .cdf_log_abort("CDF checkpoint bootstrap could not be written.")
        }
        .cdf_secure_file(checkpoint, "CDF checkpoint bootstrap")
        last_checkpoint <- file.path(log_dir, "_last_checkpoint")
        stage <- "checkpoint pointer"
        last_line <- jsonlite::toJSON(
          list(
            version = checkpoint_version,
            size = unname(file.info(checkpoint)$size)
          ),
          auto_unbox = TRUE,
          digits = NA
        )
        write_commit(last_checkpoint, last_line)
        .cdf_secure_file(last_checkpoint, "CDF checkpoint pointer")
      }

      for (offset in seq_len(plan$range$count)) {
        stage <- "versioned commit"
        version <- plan$range$start + offset - 1
        commit <- file.path(log_dir, .cdf_commit_name(version))
        write_commit(commit, plan$commits[[offset]])
        .cdf_secure_file(commit, "CDF commit")
        if (!is.na(plan$timestamps[[offset]])) {
          .cdf_set_commit_timestamp(commit, plan$timestamps[[offset]])
        }
      }

      table_path <- file.path(root, "table")
      stage <- "atomic publication"
      if (!file.rename(staging, table_path)) {
        .cdf_log_abort("CDF log could not be published atomically.")
      }
      published <- TRUE
      stage <- "prepared guard"
      guard <- .new_snapshot_log_guard(
        root,
        table_path,
        plan$file_count
      )
      guard_state <- guard$state
      guard_state$read_kind <- "cdf"
      guard_state$start_version <- plan$range$start
      guard_state$end_version <- plan$range$end
      guard
    },
    delta_sharing_error = function(condition) stop(condition),
    error = function(condition) {
      .cdf_log_abort(sprintf(
        "CDF log preparation failed during %s.",
        stage
      ))
    }
  )
}
