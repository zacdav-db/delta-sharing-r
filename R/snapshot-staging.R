.snapshot_stage_run_files <- 1024L
.snapshot_stage_merge_fan_in <- 16L

.snapshot_stage_abort <- function(message, type = "protocol") {
  .abort_delta_sharing(
    message,
    type = type,
    operation = .snapshot_log_operation
  )
}

.snapshot_stage_positive_integer <- function(value, name) {
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 1 ||
      value != floor(value) ||
      value > .Machine$integer.max
  ) {
    .snapshot_stage_abort(
      sprintf("`%s` must be one supported positive whole number.", name),
      type = "validation"
    )
  }
  as.integer(value)
}

.snapshot_stage_secure_directory <- function(directory) {
  secured <- suppressWarnings(Sys.chmod(directory, mode = "0700"))
  if (.Platform$OS.type != "windows" && !isTRUE(secured)) {
    .snapshot_stage_abort("Snapshot staging directory could not be secured.")
  }
  invisible(directory)
}

.snapshot_stage_secure_file <- function(path) {
  secured <- suppressWarnings(Sys.chmod(path, mode = "0600"))
  if (.Platform$OS.type != "windows" && !isTRUE(secured)) {
    .snapshot_stage_abort("Snapshot staging file could not be secured.")
  }
  invisible(path)
}

.snapshot_stage_write_lines <- function(path, lines) {
  connection <- file(path, open = "wb")
  on.exit(close(connection), add = TRUE)
  for (line in lines) {
    writeBin(charToRaw(paste0(enc2utf8(line), "\n")), connection)
  }
  flush(connection)
  .snapshot_stage_secure_file(path)
}

.snapshot_stage_state <- function(stage) {
  if (
    !inherits(stage, "delta_sharing_snapshot_stage") ||
      !is.environment(stage) ||
      !is.environment(stage$state)
  ) {
    .snapshot_stage_abort(
      "`stage` must be a private snapshot staging sink.",
      type = "validation"
    )
  }
  stage$state
}

.release_snapshot_stage <- function(stage) {
  state <- .snapshot_stage_state(stage)
  if (!isTRUE(state$released) && !isTRUE(state$transferred)) {
    removed <- .cleanup_snapshot_root(
      state$root,
      state$root_identity
    )
    if (!isTRUE(removed)) {
      .snapshot_stage_abort("Snapshot staging sink could not be released.")
    }
    state$released <- TRUE
  }
  invisible(TRUE)
}

.new_snapshot_stage <- function(
  temp_parent = tempdir(),
  run_files = .snapshot_stage_run_files
) {
  parent <- .validate_snapshot_temp_parent(temp_parent)
  run_files <- .snapshot_stage_positive_integer(run_files, "run_files")
  private_root <- .snapshot_create_private_root(
    parent,
    .snapshot_stage_abort,
    "Snapshot staging root could not be created."
  )
  root <- private_root$root
  root_identity <- private_root$identity
  keep <- FALSE
  on.exit({
    if (!keep) {
      .cleanup_snapshot_root(root, root_identity)
    }
  }, add = TRUE)

  marker <- file.path(root, .snapshot_log_marker_name)
  .write_snapshot_commit(marker, .snapshot_log_marker_value)
  if (!file.exists(marker) || isTRUE(file.info(marker)$isdir)) {
    .snapshot_stage_abort("Snapshot staging marker was not written.")
  }
  .snapshot_stage_secure_file(marker)
  root_identity <- .snapshot_new_root_identity(
    root,
    "construction",
    .snapshot_stage_abort
  )

  runs <- file.path(root, ".runs")
  if (!dir.create(runs, mode = "0700", showWarnings = FALSE)) {
    .snapshot_stage_abort("Snapshot staging runs directory could not be created.")
  }
  .snapshot_stage_secure_directory(runs)

  state <- new.env(parent = emptyenv())
  state$root <- root
  state$root_identity <- root_identity
  state$runs <- runs
  state$run_files <- run_files
  state$run_count <- 0L
  state$file_count <- 0L
  state$total_size <- 0
  state$buffer <- list()
  state$action_runs <- character()
  state$id_runs <- character()
  state$path_runs <- character()
  state$initialized <- FALSE
  state$released <- FALSE
  state$transferred <- FALSE

  stage <- new.env(parent = emptyenv())
  stage$state <- state
  class(stage) <- "delta_sharing_snapshot_stage"
  lockEnvironment(stage, bindings = TRUE)
  reg.finalizer(
    stage,
    function(value) {
      state <- value$state
      if (!isTRUE(state$released) && !isTRUE(state$transferred)) {
        removed <- .cleanup_snapshot_root(
          state$root,
          state$root_identity
        )
        if (isTRUE(removed)) {
          state$released <- TRUE
        }
      }
      invisible(NULL)
    },
    onexit = TRUE
  )
  keep <- TRUE
  stage
}

.initialize_snapshot_stage <- function(stage, protocol, metadata) {
  state <- .snapshot_stage_state(stage)
  if (isTRUE(state$released) || isTRUE(state$transferred)) {
    .snapshot_stage_abort("Snapshot staging sink is no longer active.")
  }
  if (isTRUE(state$initialized)) {
    return(invisible(stage))
  }
  protocol_action <- .validate_snapshot_protocol(protocol)
  metadata_state <- if (identical(metadata$response_format, "parquet")) {
    .parquet_snapshot_metadata_action(metadata)
  } else {
    list(action = .validate_snapshot_metadata(metadata), schema = NULL)
  }
  if (!identical(protocol$response_format, metadata$response_format)) {
    .snapshot_stage_abort("Snapshot response mixes response formats.")
  }
  state$protocol <- protocol
  state$metadata <- metadata
  state$protocol_action <- protocol_action
  state$metadata_state <- metadata_state
  state$response_format <- protocol$response_format
  state$header_lines <- c(
    .snapshot_json_line(list(protocol = protocol_action)),
    .snapshot_json_line(list(metaData = metadata_state$action))
  )
  state$initialized <- TRUE
  invisible(stage)
}

.snapshot_stage_duplicate_abort <- function(response_format) {
  if (identical(response_format, "parquet")) {
    .parquet_response_abort("Parquet response contains duplicate files.")
  }
  .snapshot_stage_abort("Snapshot response contains duplicate file actions.")
}

.snapshot_stage_validate_delta_file <- function(state, file_state) {
  if (!identical(file_state$response_format, "delta")) {
    .snapshot_stage_abort("Snapshot response mixes response formats.")
  }
  deletion_vector <- file_state$delta_action[[file_state$action_type]][[
    "deletionVector"
  ]]
  if (!is.null(deletion_vector)) {
    protocol <- state$protocol_action
    reader_features <- unclass(protocol$readerFeatures)
    writer_features <- unclass(protocol$writerFeatures)
    valid <- protocol$minReaderVersion >= 3 &&
      !is.null(protocol$minWriterVersion) &&
      protocol$minWriterVersion >= 7 &&
      "deletionVectors" %in% reader_features &&
      "deletionVectors" %in% writer_features
    if (!valid) {
      .snapshot_stage_abort(
        "Snapshot deletion vectors are inconsistent with the table protocol."
      )
    }
  }
  invisible(file_state)
}

.snapshot_stage_validate_parquet_file <- function(state, file_state) {
  if (
    !identical(file_state$response_format, "parquet") ||
      !identical(file_state$action_type, "add")
  ) {
    .parquet_response_abort(
      "Parquet response contains an incompatible file action."
    )
  }
  .validate_parquet_partition_values(
    file_state$delta_action$add,
    state$metadata_state$schema
  )
  invisible(file_state)
}

.snapshot_stage_flush <- function(stage) {
  state <- .snapshot_stage_state(stage)
  records <- state$buffer
  if (length(records) == 0L) {
    return(invisible(stage))
  }
  ids <- vapply(records, `[[`, character(1), "id")
  paths <- vapply(records, `[[`, character(1), "path")
  if (anyDuplicated(ids) || anyDuplicated(paths)) {
    .snapshot_stage_duplicate_abort(state$response_format)
  }
  types <- vapply(records, `[[`, character(1), "type")
  order_key <- order(types, ids, method = "radix")
  action_lines <- vapply(records[order_key], function(record) {
    paste(record$type, record$id, record$line, sep = "\t")
  }, character(1), USE.NAMES = FALSE)
  id_lines <- ids[order(ids, method = "radix")]
  path_lines <- paths[order(paths, method = "radix")]

  state$run_count <- state$run_count + 1L
  prefix <- sprintf("%08d", state$run_count)
  action_path <- file.path(state$runs, paste0(prefix, ".actions"))
  id_path <- file.path(state$runs, paste0(prefix, ".ids"))
  path_path <- file.path(state$runs, paste0(prefix, ".paths"))
  .snapshot_stage_write_lines(action_path, action_lines)
  .snapshot_stage_write_lines(id_path, id_lines)
  .snapshot_stage_write_lines(path_path, path_lines)
  state$action_runs <- c(state$action_runs, action_path)
  state$id_runs <- c(state$id_runs, id_path)
  state$path_runs <- c(state$path_runs, path_path)
  state$buffer <- list()
  invisible(stage)
}

.snapshot_stage_add_file <- function(stage, file) {
  state <- .snapshot_stage_state(stage)
  if (!isTRUE(state$initialized)) {
    .snapshot_stage_abort("Snapshot staging sink has not been initialized.")
  }
  if (state$file_count >= .snapshot_log_max_files) {
    .snapshot_stage_abort("Snapshot response contains too many file actions.")
  }
  file_state <- .snapshot_file_state(file)
  if (identical(state$response_format, "parquet")) {
    .snapshot_stage_validate_parquet_file(state, file_state)
  } else {
    .snapshot_stage_validate_delta_file(state, file_state)
  }
  action <- file_state$delta_action[[file_state$action_type]]
  state$buffer[[length(state$buffer) + 1L]] <- list(
    id = file_state$id,
    type = file_state$action_type,
    path = action$path,
    line = .snapshot_json_line(file_state$delta_action)
  )
  state$file_count <- state$file_count + 1L
  if (identical(state$response_format, "parquet")) {
    state$total_size <- state$total_size + action$size
  }
  if (length(state$buffer) >= state$run_files) {
    .snapshot_stage_flush(stage)
  }
  invisible(stage)
}

.snapshot_stage_read_line <- function(connection) {
  value <- readLines(
    connection,
    n = 1L,
    warn = FALSE,
    encoding = "UTF-8"
  )
  if (length(value) == 0L) NULL else value[[1L]]
}

.snapshot_stage_action_fields <- function(line) {
  tabs <- gregexpr("\t", line, fixed = TRUE)[[1L]]
  if (length(tabs) < 2L || identical(tabs[[1L]], -1L)) {
    .snapshot_stage_abort("Snapshot staging action run is corrupt.")
  }
  list(
    type = substr(line, 1L, tabs[[1L]] - 1L),
    id = substr(line, tabs[[1L]] + 1L, tabs[[2L]] - 1L),
    json = substr(line, tabs[[2L]] + 1L, nchar(line, type = "chars"))
  )
}

.snapshot_stage_merge_group <- function(
  inputs,
  output,
  kind = c("action", "index"),
  detect_duplicate = FALSE,
  response_format = "delta"
) {
  kind <- match.arg(kind)
  connections <- lapply(inputs, file, open = "rb")
  output_connection <- file(output, open = "wb")
  on.exit({
    for (connection in connections) {
      try(close(connection), silent = TRUE)
    }
    try(close(output_connection), silent = TRUE)
  }, add = TRUE)
  current <- lapply(connections, .snapshot_stage_read_line)
  previous <- NULL
  repeat {
    active <- which(!vapply(current, is.null, logical(1)))
    if (length(active) == 0L) {
      break
    }
    selected <- if (identical(kind, "action")) {
      fields <- lapply(current[active], .snapshot_stage_action_fields)
      active[order(
        vapply(fields, `[[`, character(1), "type"),
        vapply(fields, `[[`, character(1), "id"),
        method = "radix"
      )[[1L]]]
    } else {
      active[order(
        unlist(current[active], use.names = FALSE),
        method = "radix"
      )[[1L]]]
    }
    line <- current[[selected]]
    if (detect_duplicate && !is.null(previous) && identical(line, previous)) {
      .snapshot_stage_duplicate_abort(response_format)
    }
    writeBin(charToRaw(paste0(enc2utf8(line), "\n")), output_connection)
    previous <- line
    current[selected] <- list(.snapshot_stage_read_line(
      connections[[selected]]
    ))
  }
  flush(output_connection)
  for (connection in connections) {
    close(connection)
  }
  close(output_connection)
  connections <- list()
  output_connection <- NULL
  .snapshot_stage_secure_file(output)
  invisible(output)
}

.snapshot_stage_merge_runs <- function(
  stage,
  files,
  kind = c("action", "index"),
  detect_duplicate = FALSE
) {
  kind <- match.arg(kind)
  state <- .snapshot_stage_state(stage)
  if (length(files) == 0L) {
    return(NULL)
  }
  merge_root <- file.path(state$root, ".merge")
  if (!dir.exists(merge_root)) {
    if (!dir.create(merge_root, mode = "0700", showWarnings = FALSE)) {
      .snapshot_stage_abort("Snapshot merge directory could not be created.")
    }
    .snapshot_stage_secure_directory(merge_root)
  }
  pass <- 0L
  while (length(files) > 1L) {
    pass <- pass + 1L
    groups <- split(
      files,
      ceiling(seq_along(files) / .snapshot_stage_merge_fan_in)
    )
    outputs <- character()
    for (group_index in seq_along(groups)) {
      group <- unname(groups[[group_index]])
      if (length(group) == 1L) {
        outputs <- c(outputs, group)
        next
      }
      output <- file.path(
        merge_root,
        sprintf("%s-%03d-%05d.run", kind, pass, group_index)
      )
      .snapshot_stage_merge_group(
        group,
        output,
        kind = kind,
        detect_duplicate = detect_duplicate,
        response_format = state$response_format
      )
      unlink(group, force = TRUE)
      outputs <- c(outputs, output)
    }
    files <- outputs
  }
  files[[1L]]
}

.new_snapshot_commit_source <- function(header_lines, action_path = NULL) {
  state <- new.env(parent = emptyenv())
  state$headers <- header_lines
  state$header_index <- 1L
  state$action_path <- action_path
  state$connection <- NULL
  state$released <- FALSE
  state$exhausted <- FALSE

  source <- new.env(parent = emptyenv())
  source$next_line <- function() {
    if (isTRUE(state$released)) {
      return(NULL)
    }
    if (state$header_index <= length(state$headers)) {
      line <- state$headers[[state$header_index]]
      state$header_index <- state$header_index + 1L
      return(line)
    }
    if (is.null(state$action_path)) {
      state$exhausted <- TRUE
      return(NULL)
    }
    if (is.null(state$connection)) {
      state$connection <- file(state$action_path, open = "rb")
    }
    line <- .snapshot_stage_read_line(state$connection)
    if (is.null(line)) {
      close(state$connection)
      state$connection <- NULL
      state$action_path <- NULL
      state$exhausted <- TRUE
      return(NULL)
    }
    .snapshot_stage_action_fields(line)$json
  }
  source$release <- function() {
    if (!is.null(state$connection)) {
      try(close(state$connection), silent = TRUE)
      state$connection <- NULL
    }
    state$released <- TRUE
    invisible(TRUE)
  }
  source$is_exhausted <- function() {
    isTRUE(state$exhausted)
  }
  class(source) <- "delta_sharing_snapshot_commit_source"
  lockEnvironment(source, bindings = TRUE)
  reg.finalizer(
    source,
    function(value) {
      try(value$release(), silent = TRUE)
      invisible(NULL)
    },
    onexit = TRUE
  )
  source
}

.snapshot_stage_remove_work <- function(state) {
  for (directory in c(state$runs, file.path(state$root, ".merge"))) {
    if (!.snapshot_temp_root_is_safe(
      state$root,
      state$root_identity
    )) {
      .snapshot_stage_abort("Snapshot staging root identity changed.")
    }
    if (.snapshot_path_exists(directory)) {
      if (
        !dir.exists(directory) ||
          !identical(Sys.readlink(directory), "") ||
          !identical(
            dirname(normalizePath(
              directory,
              winslash = "/",
              mustWork = TRUE
            )),
            state$root
          )
      ) {
        .snapshot_stage_abort("Snapshot staging work directory is invalid.")
      }
      unlink(directory, recursive = TRUE, force = TRUE)
    }
    if (.snapshot_path_exists(directory)) {
      .snapshot_stage_abort("Snapshot staging work could not be removed.")
    }
  }
  invisible(TRUE)
}

.publish_snapshot_stage_impl <- function(
  stage,
  write_commit = .write_snapshot_commit
) {
  state <- .snapshot_stage_state(stage)
  if (!is.function(write_commit)) {
    .snapshot_stage_abort(
      "`write_commit` must be a function.",
      type = "validation"
    )
  }
  if (!isTRUE(state$initialized)) {
    .snapshot_stage_abort("Snapshot staging sink has not been initialized.")
  }
  .snapshot_stage_flush(stage)

  id_index <- .snapshot_stage_merge_runs(
    stage,
    state$id_runs,
    kind = "index",
    detect_duplicate = TRUE
  )
  if (!is.null(id_index)) {
    unlink(id_index, force = TRUE)
  }
  path_index <- .snapshot_stage_merge_runs(
    stage,
    state$path_runs,
    kind = "index",
    detect_duplicate = TRUE
  )
  if (!is.null(path_index)) {
    unlink(path_index, force = TRUE)
  }
  action_run <- .snapshot_stage_merge_runs(
    stage,
    state$action_runs,
    kind = "action",
    detect_duplicate = FALSE
  )

  staging <- file.path(state$root, ".staging")
  log_dir <- file.path(staging, "_delta_log")
  if (!dir.create(log_dir, recursive = TRUE, mode = "0700")) {
    .snapshot_stage_abort("Snapshot staging directory could not be created.")
  }
  .snapshot_stage_secure_directory(staging)
  .snapshot_stage_secure_directory(log_dir)
  commit <- file.path(log_dir, "00000000000000000000.json")
  source <- .new_snapshot_commit_source(state$header_lines, action_run)
  on.exit(try(source$release(), silent = TRUE), add = TRUE)
  write_commit(commit, source)
  consumed <- source$is_exhausted()
  source$release()
  if (!consumed) {
    .snapshot_stage_abort(
      "Snapshot commit writer did not consume the complete staged manifest."
    )
  }
  if (!file.exists(commit) || isTRUE(file.info(commit)$isdir)) {
    .snapshot_stage_abort("Snapshot commit was not written.")
  }
  .snapshot_stage_secure_file(commit)
  .snapshot_stage_remove_work(state)

  table_path <- file.path(state$root, "table")
  if (!file.rename(staging, table_path)) {
    .snapshot_stage_abort("Snapshot log could not be published atomically.")
  }
  root_identity <- .snapshot_publish_root_identity(
    state$root,
    state$root_identity,
    .snapshot_stage_abort
  )
  state$transferred <- TRUE
  .new_snapshot_log_guard(
    state$root,
    table_path,
    state$file_count,
    root_identity = root_identity
  )
}

.publish_snapshot_stage <- function(
  stage,
  write_commit = .write_snapshot_commit
) {
  tryCatch(
    .publish_snapshot_stage_impl(stage, write_commit),
    error = function(condition) {
      if (inherits(condition, "delta_sharing_error")) {
        stop(condition)
      }
      .snapshot_stage_abort("Snapshot log preparation failed.")
    }
  )
}
