# Compare retained CDF action bucketing with bounded per-version staging.
#
# Run each mode in a fresh process under the platform RSS tool, for example:
#   /usr/bin/time -l Rscript tools/cdf_manifest_benchmark_worker.R \
#     retained 100000 100
#   /usr/bin/time -l Rscript tools/cdf_manifest_benchmark_worker.R \
#     staged 100000 100

args <- commandArgs(trailingOnly = TRUE)
if (
  length(args) != 3L ||
    !args[[1L]] %in% c("retained", "staged")
) {
  stop(
    "Usage: cdf_manifest_benchmark_worker.R MODE ACTIONS VERSIONS",
    call. = FALSE
  )
}
mode <- args[[1L]]
action_count <- suppressWarnings(as.integer(args[[2L]]))
version_count <- suppressWarnings(as.integer(args[[3L]]))
if (
  is.na(action_count) ||
    action_count < 0L ||
    is.na(version_count) ||
    version_count < 1L
) {
  stop(
    "ACTIONS must be non-negative and VERSIONS must be positive.",
    call. = FALSE
  )
}

pkgload::load_all(".", quiet = TRUE)

cdf_file_action <- function(index) {
  version <- (index - 1L) %% version_count
  list(
    file = list(
      version = version,
      timestamp = version * 1000,
      deltaSingleAction = list(
        cdc = list(
          path = sprintf(
            "https://storage.example.test/cdf-%08d.parquet?signature=benchmark",
            index
          ),
          partitionValues = list(day = sprintf("%02d", version %% 31L + 1L)),
          size = 1048576,
          dataChange = FALSE
        )
      )
    )
  )
}

protocol <- list(
  minReaderVersion = 3,
  minWriterVersion = 7,
  readerFeatures = list("deletionVectors", "changeDataFeed")
)
metadata <- list(
  id = "benchmark",
  schemaString = "{\"type\":\"struct\",\"fields\":[]}",
  partitionColumns = list("day"),
  configuration = list(delta.enableChangeDataFeed = "true")
)

append_stage <- function(staging_dir, version, actions) {
  stage <- fs::path(staging_dir, cdf_commit_name(version))
  output <- file(stage, open = "ab")
  on.exit(close(output), add = TRUE)
  writeLines(purrr::map_chr(actions, log_json_line), output, useBytes = TRUE)
}

write_staged_log <- function(log_dir, staging_dir, by_version) {
  purrr::walk(seq.int(0, version_count - 1L), function(version) {
    stage <- fs::path(staging_dir, cdf_commit_name(version))
    commit <- fs::path(log_dir, cdf_commit_name(version))
    header <- if (version == 0) {
      log_json_line(list(protocol = protocol))
    } else {
      character()
    }
    if (fs::file_exists(stage)) {
      write_staged_commit(commit, header, stage)
    } else {
      writeLines(header, commit, useBytes = TRUE)
    }
    fs::file_touch(
      commit,
      modification_time = as.POSIXct(
        by_version[[as.character(version)]]$timestamp_ms / 1000,
        origin = "1970-01-01",
        tz = "UTC"
      )
    )
  })
  fs::dir_delete(staging_dir)
}

started <- proc.time()[["elapsed"]]
if (identical(mode, "retained")) {
  actions <- c(
    list(
      list(protocol = list(deltaProtocol = protocol)),
      list(metaData = list(version = 0, deltaMetadata = metadata))
    ),
    purrr::map(seq_len(action_count), cdf_file_action)
  )
  bucket <- bucket_cdf_actions(actions, 0, version_count - 1L)
  log <- prepare_cdf_log(
    bucket$protocol,
    bucket$by_version,
    bucket$start_version,
    bucket$end_version
  )
} else {
  by_version <- purrr::map(
    rlang::set_names(seq.int(0, version_count - 1L)),
    function(version) {
      list(version = as.numeric(version), timestamp_ms = version * 1000)
    }
  )
  log <- prepare_log(function(log_dir) {
    staging_dir <- fs::path(log_dir, ".cdf-actions")
    fs::dir_create(staging_dir, mode = "u=rwx,go=")
    append_stage(
      staging_dir,
      0,
      list(list(metaData = metadata))
    )

    if (action_count > 0L) {
      starts <- seq.int(1L, action_count, by = 256L)
      purrr::walk(starts, function(start) {
        end <- min(start + 255L, action_count)
        files <- purrr::map(seq.int(start, end), cdf_file_action)
        groups <- split(
          files,
          purrr::map_chr(files, function(action) {
            as.character(action$file$version)
          })
        )
        purrr::walk(groups, function(group) {
          append_stage(
            staging_dir,
            group[[1L]]$file$version,
            purrr::map(group, c("file", "deltaSingleAction"))
          )
        })
      })
    }

    write_staged_log(
      log_dir,
      staging_dir,
      by_version
    )
  })
}
on.exit(log$cleanup(), add = TRUE)
elapsed <- proc.time()[["elapsed"]] - started

commits <- fs::dir_ls(
  fs::path(log$path, "_delta_log"),
  glob = "*.json",
  type = "file"
)
result <- list(
  mode = mode,
  actions = action_count,
  versions = version_count,
  elapsed_seconds = unname(elapsed),
  commit_bytes = sum(as.numeric(fs::file_size(commits))),
  commit_md5 = unname(tools::md5sum(commits))
)
cat(jsonlite::toJSON(result, auto_unbox = TRUE, pretty = TRUE), "\n")
