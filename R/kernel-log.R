# Synthetic Delta log construction for the Delta Kernel scan.
#
# R fetches the Delta Sharing Query Table response and writes a local
# `_delta_log/00...0.json` that Delta Kernel reads; the kernel then fetches data
# directly from the pre-signed URLs in the log. There is no second downloader.
#
# Following the Python client, we do not build Delta action structs for the
# Delta-format path: the server already returns fully-formed actions under
# `protocol.deltaProtocol`, `metaData.deltaMetadata`, and `file.deltaSingleAction`,
# so we unwrap and write them verbatim. Parquet-format responses synthesize a
# flat `add` action (that path carries no deletion vectors).

# The native cleanup guard only deletes a directory it can prove it owns, so the
# synthetic log is written into a private layout it validates:
#
#   <root .delta-sharing-snapshot-*>/   (mode 0700)
#   |-- .delta-sharing-r-prepared-log   (ownership marker)
#   `-- table/_delta_log/<commit>.json  (the table location handed to the kernel)
log_root_prefix <- ".delta-sharing-snapshot-"
log_marker_name <- ".delta-sharing-r-prepared-log"
log_marker_value <- "delta-sharing-r:vnext\n"
log_dir_name <- "_delta_log"
log_commit_name <- "00000000000000000000.json"

delete_log_root <- function(root) {
  if (fs::dir_exists(root)) {
    fs::dir_delete(root)
  }
  invisible(NULL)
}

# Encode one action list as a single JSON line.
log_json_line <- function(action) {
  jsonlite::toJSON(
    action,
    auto_unbox = TRUE,
    null = "null",
    digits = NA,
    pretty = FALSE
  )
}

# Encode the protocol and metadata lines that begin a snapshot commit.
synthetic_log_header <- function(
  response_format,
  protocol,
  metadata,
  operation = "read"
) {
  if (identical(response_format, "delta")) {
    c(
      log_json_line(list(protocol = protocol$deltaProtocol %||% protocol)),
      log_json_line(list(metaData = metadata$deltaMetadata %||% metadata))
    )
  } else {
    c(
      log_json_line(list(protocol = parquet_protocol_action(protocol))),
      log_json_line(list(
        metaData = parquet_metadata_action(metadata, operation)
      ))
    )
  }
}

# Turn a parsed Query Table response into the ordered JSON lines of the
# synthetic commit: protocol, metadata, then one line per file action.
synthetic_log_lines <- function(
  response_format,
  protocol,
  metadata,
  files,
  operation = "read"
) {
  file_lines <- purrr::map_chr(
    files,
    function(file) {
      log_json_line(synthetic_file_action(file, response_format, operation))
    }
  )
  c(
    synthetic_log_header(
      response_format,
      protocol,
      metadata,
      operation
    ),
    file_lines
  )
}

# Delta format: the file action already carries a fully-formed single action.
# Parquet format: synthesize a flat `add` from the sharing file fields.
synthetic_file_action <- function(file, response_format, operation) {
  if (identical(response_format, "delta")) {
    file$deltaSingleAction %||% file
  } else {
    list(add = parquet_add_action(file$file %||% file, operation))
  }
}

# Create the private, ownership-marked layout the native cleanup guard
# validates, and return its paths plus a `write()` callback that receives the
# `_delta_log` directory to populate. A writer may return named fields to append
# to the handle. The handle carries an explicit `cleanup()` for the failure
# path, before ownership transfers to Rust.
prepare_log <- function(write) {
  root <- fs::file_temp(pattern = log_root_prefix)
  log_dir <- fs::path(root, "table", log_dir_name)
  fs::dir_create(log_dir, mode = "u=rwx,go=")

  log_complete <- FALSE
  on.exit(
    {
      if (!log_complete) {
        delete_log_root(root)
      }
    },
    add = TRUE
  )

  details <- write(log_dir)
  writeChar(log_marker_value, fs::path(root, log_marker_name), eos = NULL)
  log_complete <- TRUE

  c(
    list(
      root = fs::path_real(root),
      path = fs::path_real(fs::path(root, "table")),
      cleanup = function() delete_log_root(root)
    ),
    details
  )
}

# Snapshot: a single version-0 commit holding protocol, metadata, and adds.
prepare_synthetic_log <- function(lines) {
  prepare_log(function(log_dir) {
    writeLines(lines, fs::path(log_dir, log_commit_name), useBytes = TRUE)
    invisible(NULL)
  })
}

# Write a commit header followed by the bytes in one bounded action stage.
write_staged_commit <- function(commit, header, staged_actions) {
  local({
    output <- file(commit, open = "wb")
    on.exit(close(output), add = TRUE)
    input <- file(staged_actions, open = "rb")
    on.exit(close(input), add = TRUE)

    writeLines(header, output, useBytes = TRUE)
    repeat {
      bytes <- readBin(input, what = "raw", n = 1024 * 1024)
      if (length(bytes) == 0L) {
        break
      }
      writeBin(bytes, output)
    }
  })
  invisible(commit)
}

# Publish a snapshot commit from a bounded action staging file. The staging
# file lives inside the private log root and is removed before native ownership
# validation, leaving exactly the one commit expected by the cleanup guard.
write_staged_snapshot_commit <- function(log_dir, header, staged_actions) {
  commit <- fs::path(log_dir, log_commit_name)
  write_staged_commit(commit, header, staged_actions)
  fs::file_delete(staged_actions)
  invisible(commit)
}

# Change data feed: the kernel's TableChanges reads a real multi-version log,
# so this writes one commit per version across the observed `[start, end]` range
# (including interior versions with no changes). The protocol goes in the first
# commit and each commit's mtime is set to the version timestamp (the kernel
# derives `_commit_timestamp` from it). When the range does not start at 0 a
# fake checkpoint at `{start-1}` lets the kernel begin there without earlier
# commits.
# `by_version` is keyed by as.character(version) ->
# list(timestamp_ms=, actions=list(...)); `protocol` is pre-unwrapped.
prepare_cdf_log <- function(protocol, by_version, start_version, end_version) {
  log <- prepare_log(function(log_dir) {
    if (start_version > 0) {
      write_fake_checkpoint(log_dir, start_version - 1)
    }

    purrr::walk(seq.int(start_version, end_version), function(version) {
      version_data <- by_version[[as.character(version)]]
      if (is.null(version_data)) {
        version_data <- list(actions = list(), timestamp_ms = NULL)
      }
      actions <- version_data$actions

      if (version == start_version) {
        actions <- c(list(list(protocol = protocol)), actions)
      }

      write_cdf_commit(
        log_dir,
        version,
        actions,
        timestamp_ms = version_data$timestamp_ms
      )
    })
    invisible(NULL)
  })

  log$start_version <- start_version
  log$end_version <- end_version
  log
}

cdf_commit_name <- function(version) {
  sprintf("%020.0f.json", version)
}

# Write one CDF commit and preserve the provider's timestamp. Empty `actions`
# deliberately create an empty commit so the synthetic log has no version gaps.
write_cdf_commit <- function(log_dir, version, actions, timestamp_ms = NULL) {
  commit <- fs::path(log_dir, cdf_commit_name(version))
  writeLines(purrr::map_chr(actions, log_json_line), commit, useBytes = TRUE)

  if (length(timestamp_ms) == 1L && is.finite(timestamp_ms)) {
    timestamp <- as.POSIXct(
      timestamp_ms / 1000,
      origin = "1970-01-01",
      tz = "UTC"
    )
    fs::file_touch(commit, modification_time = timestamp)
  }

  invisible(commit)
}

# A minimal valid checkpoint lets the kernel treat `checkpoint_version` as the
# log's starting point, so a CDF range that begins above 0 needs no earlier
# commits. The parquet bytes are a shared fixture (see inst/extdata).
write_fake_checkpoint <- function(log_dir, checkpoint_version) {
  src <- system.file(
    "extdata",
    "fake_checkpoint.parquet",
    package = "delta.sharing"
  )
  bytes <- readBin(src, "raw", fs::file_size(src))
  name <- sprintf("%020.0f.checkpoint.parquet", checkpoint_version)
  writeBin(bytes, fs::path(log_dir, name))
  last <- sprintf(
    '{"version":%.0f,"size":%d}',
    checkpoint_version,
    length(bytes)
  )
  writeChar(last, fs::path(log_dir, "_last_checkpoint"), eos = NULL)
}
