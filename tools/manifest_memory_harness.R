mm_abort <- function(message) {
  stop(message, call. = FALSE)
}

mm_scalar_character <- function(value) {
  is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    nzchar(value)
}

mm_parse_cli <- function(args, repo_root) {
  values <- list(
    mode = "quick",
    repetitions = NULL,
    output = file.path(tempdir(), "delta-sharing-r-manifest-memory.json"),
    repo_root = normalizePath(repo_root, winslash = "/", mustWork = TRUE)
  )
  index <- 1L
  while (index <= length(args)) {
    argument <- args[[index]]
    if (!argument %in% c("--mode", "--repetitions", "--output")) {
      mm_abort(sprintf("Unknown manifest-memory argument: %s", argument))
    }
    if (index == length(args)) {
      mm_abort(sprintf("Argument %s requires a value.", argument))
    }
    value <- args[[index + 1L]]
    if (identical(argument, "--mode")) {
      if (!value %in% c("quick", "standard")) {
        mm_abort("`--mode` must be `quick` or `standard`.")
      }
      values$mode <- value
    } else if (identical(argument, "--repetitions")) {
      parsed <- suppressWarnings(as.integer(value))
      if (is.na(parsed) || parsed < 1L || parsed > 10L) {
        mm_abort("`--repetitions` must be an integer from 1 through 10.")
      }
      values$repetitions <- parsed
    } else {
      if (!mm_scalar_character(value)) {
        mm_abort("`--output` must be one non-empty path.")
      }
      values$output <- value
    }
    index <- index + 2L
  }
  values
}

mm_config <- function(mode = "quick", repetitions = NULL) {
  config <- if (identical(mode, "quick")) {
    list(
      mode = mode,
      file_counts = c(100L, 1000L, 10000L),
      repetitions = 1L,
      chunk_files = 256L
    )
  } else if (identical(mode, "standard")) {
    list(
      mode = mode,
      file_counts = c(1000L, 10000L, 100000L),
      repetitions = 3L,
      chunk_files = 256L
    )
  } else {
    mm_abort("Unknown manifest-memory mode.")
  }
  if (!is.null(repetitions)) {
    config$repetitions <- as.integer(repetitions)
  }
  config
}

mm_time_backend <- function(sysname = unname(Sys.info()[["sysname"]])) {
  path <- "/usr/bin/time"
  if (!file.exists(path)) {
    return(list(
      available = FALSE,
      reason = "`/usr/bin/time` is unavailable."
    ))
  }
  if (identical(sysname, "Darwin")) {
    return(list(
      available = TRUE,
      name = "darwin-time-l",
      path = path,
      flags = "-l",
      unit = "bytes"
    ))
  }
  if (identical(sysname, "Linux")) {
    version <- tryCatch(
      suppressWarnings(system2(path, "--version", stdout = TRUE, stderr = TRUE)),
      error = function(condition) character()
    )
    if (length(version) > 0L &&
        any(grepl("GNU time", version, fixed = TRUE))) {
      return(list(
        available = TRUE,
        name = "gnu-time-v",
        path = path,
        flags = "-v",
        unit = "kibibytes"
      ))
    }
    return(list(
      available = FALSE,
      reason = "Linux `/usr/bin/time` is not GNU time."
    ))
  }
  list(
    available = FALSE,
    reason = sprintf("Peak RSS is not implemented for %s.", sysname)
  )
}

mm_parse_peak_rss <- function(lines, backend) {
  if (identical(backend$name, "darwin-time-l")) {
    matching <- grep(
      "^[[:space:]]*[0-9]+[[:space:]]+maximum resident set size[[:space:]]*$",
      lines,
      value = TRUE
    )
    if (length(matching) != 1L) {
      mm_abort("Darwin time output did not contain one peak-RSS value.")
    }
    return(as.double(sub(
      "^[[:space:]]*([0-9]+).*$",
      "\\1",
      matching
    )))
  }
  if (identical(backend$name, "gnu-time-v")) {
    matching <- grep(
      "Maximum resident set size \\(kbytes\\):",
      lines,
      value = TRUE
    )
    if (length(matching) != 1L) {
      mm_abort("GNU time output did not contain one peak-RSS value.")
    }
    return(as.double(sub(
      "^.*:[[:space:]]*([0-9]+).*$",
      "\\1",
      matching
    )) * 1024)
  }
  mm_abort("Unknown peak-RSS backend.")
}

mm_timed_worker <- function(repo_root,
                            backend,
                            files,
                            chunk_files,
                            outcome) {
  worker <- file.path(repo_root, "tools", "manifest_memory_worker.R")
  output <- tempfile("delta-sharing-manifest-worker-", fileext = ".json")
  timing <- tempfile("delta-sharing-manifest-time-", fileext = ".txt")
  stdout <- tempfile("delta-sharing-manifest-worker-", fileext = ".stdout")
  stderr <- tempfile("delta-sharing-manifest-worker-", fileext = ".stderr")
  on.exit(unlink(c(output, timing, stdout, stderr), force = TRUE), add = TRUE)
  arguments <- c(
    backend$flags,
    "-o",
    timing,
    file.path(R.home("bin"), "Rscript"),
    worker,
    "--files",
    as.character(files),
    "--chunk-files",
    as.character(chunk_files),
    "--outcome",
    outcome,
    "--output",
    output
  )
  status <- suppressWarnings(system2(
    backend$path,
    vapply(arguments, shQuote, character(1)),
    stdout = stdout,
    stderr = stderr
  ))
  if (!identical(as.integer(status), 0L)) {
    mm_abort(sprintf(
      "Manifest-memory worker failed (%s): %s",
      status,
      paste(readLines(stderr, warn = FALSE), collapse = "\n")
    ))
  }
  list(
    peak_rss_bytes = mm_parse_peak_rss(
      readLines(timing, warn = FALSE),
      backend
    ),
    worker = jsonlite::read_json(output, simplifyVector = FALSE)
  )
}

mm_command <- function(command, args = character()) {
  output <- tryCatch(
    suppressWarnings(system2(command, args, stdout = TRUE, stderr = TRUE)),
    error = function(condition) character()
  )
  if (length(output) == 0L) NA_character_ else paste(output, collapse = "\n")
}

mm_environment <- function(repo_root, backend) {
  git_status <- mm_command(
    "git",
    c("-C", shQuote(repo_root), "status", "--short")
  )
  list(
    captured_at_utc = format(
      Sys.time(),
      "%Y-%m-%dT%H:%M:%SZ",
      tz = "UTC"
    ),
    git_commit = mm_command(
      "git",
      c("-C", shQuote(repo_root), "rev-parse", "HEAD")
    ),
    git_branch = mm_command(
      "git",
      c("-C", shQuote(repo_root), "branch", "--show-current")
    ),
    git_worktree_dirty = !is.na(git_status) && nzchar(git_status),
    r_version = R.version.string,
    platform = R.version$platform,
    os = unname(Sys.info()[["sysname"]]),
    release = unname(Sys.info()[["release"]]),
    machine = unname(Sys.info()[["machine"]]),
    package_version = as.character(utils::packageVersion("delta.sharing")),
    peak_rss_backend = backend
  )
}

mm_median <- function(samples, field) {
  stats::median(vapply(samples, function(sample) {
    as.double(sample[[field]])
  }, numeric(1)))
}

mm_summarize <- function(baselines, successful) {
  baseline_rss <- mm_median(baselines, "peak_rss_bytes")
  groups <- split(
    successful,
    vapply(successful, function(sample) {
      as.character(sample$worker$files)
    }, character(1))
  )
  summaries <- lapply(groups, function(samples) {
    peak <- mm_median(samples, "peak_rss_bytes")
    worker <- lapply(samples, `[[`, "worker")
    wire <- mm_median(worker, "wire_bytes")
    commit <- mm_median(lapply(worker, `[[`, "result"), "commit_bytes")
    list(
      files = as.integer(worker[[1L]]$files),
      median_elapsed_seconds = mm_median(worker, "elapsed_seconds"),
      median_peak_rss_bytes = peak,
      median_incremental_peak_rss_bytes = max(0, peak - baseline_rss),
      median_wire_bytes = wire,
      median_commit_bytes = commit,
      incremental_rss_to_wire_ratio = if (wire == 0) {
        NA_real_
      } else {
        max(0, peak - baseline_rss) / wire
      }
    )
  })
  summaries[order(vapply(summaries, `[[`, integer(1), "files"))]
}

mm_all_cleanup_passed <- function(samples) {
  all(vapply(samples, function(sample) {
    identical(sample$worker$result$status, "pass") &&
      identical(as.integer(sample$worker$result$roots_after_cleanup), 0L) &&
      identical(as.integer(sample$worker$closes), 1L)
  }, logical(1)))
}

mm_manifest_gates <- function(cleanup_passed) {
  list(
    list(
      id = "temporary-root-lifecycle",
      status = if (cleanup_passed) "pass" else "fail",
      criterion = paste(
        "success/explicit release, injected write failure, and finalization",
        "leave zero roots; every response closes exactly once"
      )
    ),
    list(
      id = "production-action-retention",
      status = "not_evaluable",
      criterion = paste(
        "release workload envelope has no agreed RSS/time threshold;",
        "production retains one bounded encoded action run and bounded",
        "merge cursors rather than a whole normalized manifest"
      )
    ),
    list(
      id = "adr-003-rust-scope-expansion",
      status = "not_met",
      criterion = paste(
        "this R-owned planning workload has no Rust comparator and its",
        "disk-backed staging implementation does not justify Rust expansion"
      )
    )
  )
}

mm_retention_model <- function() {
  list(
    input_transport = "bounded pull chunks",
    normalized_actions =
      "at most one bounded encoded action run buffer retained in R",
    pagination = "file actions staged directly; page file lists not retained",
    validation = paste(
      "bounded action/ID/path runs with shell-free R merges for",
      "global duplicate checks and deterministic ordering"
    ),
    encoding = "final commit streamed from one merged action run",
    hard_action_limit = 1000000L,
    conclusion = paste(
      "R memory is bounded by run size and merge fan-in rather than total",
      "file count; disk staging remains O(file_count)."
    )
  )
}

mm_write_artifact <- function(artifact, path) {
  parent <- dirname(path)
  if (!dir.exists(parent) && !dir.create(parent, recursive = TRUE)) {
    mm_abort("Could not create the artifact directory.")
  }
  temporary <- tempfile(paste0(".", basename(path), "-"), tmpdir = parent)
  on.exit(unlink(temporary, force = TRUE), add = TRUE)
  jsonlite::write_json(
    artifact,
    temporary,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null",
    na = "null",
    digits = NA
  )
  if (!file.rename(temporary, path)) {
    mm_abort("Could not publish the manifest-memory artifact.")
  }
  invisible(normalizePath(path, winslash = "/", mustWork = TRUE))
}

mm_run <- function(repo_root, config, output) {
  backend <- mm_time_backend()
  if (!isTRUE(backend$available)) {
    mm_abort(backend$reason)
  }
  baselines <- lapply(seq_len(config$repetitions), function(iteration) {
    sample <- mm_timed_worker(
      repo_root,
      backend,
      files = 0L,
      chunk_files = config$chunk_files,
      outcome = "explicit_release"
    )
    sample$iteration <- iteration
    sample
  })
  successful <- list()
  index <- 1L
  for (files in config$file_counts) {
    for (iteration in seq_len(config$repetitions)) {
      sample <- mm_timed_worker(
        repo_root,
        backend,
        files = files,
        chunk_files = config$chunk_files,
        outcome = "explicit_release"
      )
      sample$iteration <- iteration
      successful[[index]] <- sample
      index <- index + 1L
    }
  }
  largest <- max(config$file_counts)
  lifecycle <- lapply(c("write_error", "finalizer"), function(outcome) {
    mm_timed_worker(
      repo_root,
      backend,
      files = largest,
      chunk_files = config$chunk_files,
      outcome = outcome
    )
  })
  names(lifecycle) <- c("write_error", "finalizer")
  all_samples <- c(baselines, successful, lifecycle)
  summaries <- mm_summarize(baselines, successful)
  cleanup_passed <- mm_all_cleanup_passed(all_samples)
  artifact <- list(
    schema_version = 1L,
    environment = mm_environment(repo_root, backend),
    configuration = c(
      config,
      list(
        staging_run_files =
          delta.sharing:::.snapshot_stage_run_files,
        merge_fan_in =
          delta.sharing:::.snapshot_stage_merge_fan_in
      )
    ),
    measurements = list(
      baselines = baselines,
      successful_preparations = successful,
      lifecycle = lifecycle
    ),
    summaries = summaries,
    gates = mm_manifest_gates(cleanup_passed),
    retention_model = mm_retention_model(),
    limitations = c(
      "Peak RSS includes R startup and loaded package/native libraries; the artifact reports a fresh-process zero-file baseline.",
      "Generated HTTPS actions are deterministic and representative but do not include wide stats, partitions, deletion vectors, or maximum-size fields.",
      "Local timings are trend evidence, not a cross-platform release baseline.",
      "The standard 100,000-file workload is the default per-page request ceiling, not the one-million-action absolute guardrail.",
      "Disk-backed staging intentionally trades additional local I/O and merge time for bounded R-side manifest memory."
    )
  )
  mm_write_artifact(artifact, output)
  artifact
}
