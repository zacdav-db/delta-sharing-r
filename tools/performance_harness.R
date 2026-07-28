ph_abort <- function(message) {
  stop(message, call. = FALSE)
}

ph_scalar_character <- function(value) {
  is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    nzchar(value)
}

ph_parse_cli <- function(args, repo_root) {
  if (!ph_scalar_character(repo_root)) {
    ph_abort("`repo_root` must be one non-empty path.")
  }
  result <- list(
    mode = "quick",
    output = file.path(tempdir(), "delta-sharing-r-performance.json"),
    repetitions = NULL,
    repo_root = normalizePath(repo_root, winslash = "/", mustWork = TRUE)
  )
  index <- 1L
  while (index <= length(args)) {
    argument <- args[[index]]
    if (identical(argument, "--help")) {
      result$help <- TRUE
      index <- index + 1L
      next
    }
    if (!argument %in% c("--mode", "--output", "--repetitions")) {
      ph_abort(sprintf("Unknown benchmark argument: %s", argument))
    }
    if (index == length(args)) {
      ph_abort(sprintf("Benchmark argument %s requires a value.", argument))
    }
    value <- args[[index + 1L]]
    if (identical(argument, "--mode")) {
      if (!value %in% c("quick", "standard")) {
        ph_abort("`--mode` must be `quick` or `standard`.")
      }
      result$mode <- value
    } else if (identical(argument, "--output")) {
      if (!ph_scalar_character(value)) {
        ph_abort("`--output` must be one non-empty path.")
      }
      result$output <- value
    } else {
      repetitions <- suppressWarnings(as.integer(value))
      if (is.na(repetitions) || repetitions < 1L || repetitions > 100L) {
        ph_abort("`--repetitions` must be an integer from 1 through 100.")
      }
      result$repetitions <- repetitions
    }
    index <- index + 2L
  }
  result
}

ph_config <- function(mode = "quick", repetitions = NULL) {
  if (!mode %in% c("quick", "standard")) {
    ph_abort("Unknown benchmark mode.")
  }
  config <- if (identical(mode, "quick")) {
    list(
      mode = mode,
      repetitions = 2L,
      manifest_file_counts = c(10L, 100L, 1000L),
      ffi_batches = 16L,
      ffi_rows_per_batch = c(1024L, 65536L),
      kernel_repetitions = 5L,
      release_repetitions = 10L,
      backpressure_idle_seconds = 0.05,
      heap_batch_counts = c(8L, 32L),
      heap_rows_per_batch = 4096L
    )
  } else {
    list(
      mode = mode,
      repetitions = 5L,
      manifest_file_counts = c(100L, 1000L, 10000L),
      ffi_batches = 64L,
      ffi_rows_per_batch = c(1024L, 65536L),
      kernel_repetitions = 30L,
      release_repetitions = 50L,
      backpressure_idle_seconds = 0.25,
      heap_batch_counts = c(256L, 2048L, 8192L),
      heap_rows_per_batch = 4096L
    )
  }
  if (!is.null(repetitions)) {
    config$repetitions <- as.integer(repetitions)
    config$kernel_repetitions <- as.integer(repetitions)
    config$release_repetitions <- as.integer(repetitions)
  }
  config
}

ph_elapsed <- function(expression) {
  started <- as.numeric(Sys.time())
  value <- force(expression)
  list(
    value = value,
    elapsed_seconds = as.numeric(Sys.time()) - started
  )
}

ph_rprofmem_bytes <- function(lines) {
  if (length(lines) == 0L) {
    return(numeric())
  }
  matching <- grepl("^[0-9]+[[:space:]]*:", lines)
  if (!any(matching)) {
    return(numeric())
  }
  as.numeric(sub("^([0-9]+)[[:space:]]*:.*$", "\\1", lines[matching]))
}

ph_gc_heap_peak_proxy_bytes <- function(baseline, high_water) {
  if (
    !is.matrix(baseline) ||
      !is.matrix(high_water) ||
      is.null(colnames(baseline)) ||
      is.null(colnames(high_water))
  ) {
    ph_abort("GC heap proxy inputs must be named matrices.")
  }
  mb_column_after <- function(matrix, count_name, fallback) {
    columns <- colnames(matrix)
    count_column <- match(count_name, columns)
    if (
      !is.na(count_column) &&
        count_column < ncol(matrix) &&
        grepl("mb", columns[[count_column + 1L]], ignore.case = TRUE)
    ) {
      return(count_column + 1L)
    }
    mb_columns <- which(grepl("mb", columns, ignore.case = TRUE))
    if (length(mb_columns) == 0L) {
      ph_abort("GC heap proxy input does not expose megabyte columns.")
    }
    fallback(mb_columns)
  }
  baseline_used_mb <- mb_column_after(baseline, "used", min)
  high_water_max_mb <- mb_column_after(high_water, "max used", max)
  max(
    0,
    sum(high_water[, high_water_max_mb], na.rm = TRUE) -
      sum(baseline[, baseline_used_mb], na.rm = TRUE)
  ) * 1024^2
}

ph_profile_r <- function(callback) {
  if (!is.function(callback)) {
    ph_abort("`callback` must be a function.")
  }
  profile_path <- tempfile("delta-sharing-r-rprofmem-", fileext = ".out")
  on.exit(unlink(profile_path, force = TRUE), add = TRUE)
  profile_available <- isTRUE(unname(capabilities("profmem")))
  baseline <- gc(reset = TRUE)
  profiling <- FALSE
  error <- NULL
  value <- NULL
  started <- as.numeric(Sys.time())
  if (profile_available) {
    Rprofmem(profile_path, threshold = 0L)
    profiling <- TRUE
  }
  tryCatch(
    value <- callback(),
    error = function(condition) {
      error <<- condition
    }
  )
  elapsed <- as.numeric(Sys.time()) - started
  if (profiling) {
    Rprofmem(NULL)
    profiling <- FALSE
  }
  high_water <- gc()
  if (!is.null(error)) {
    stop(error)
  }
  allocation_bytes <- if (profile_available) {
    ph_rprofmem_bytes(readLines(profile_path, warn = FALSE))
  } else {
    numeric()
  }
  # gc(reset = TRUE) makes the final max-used MB column a high-water mark from
  # this operation's starting heap. The column position differs across R
  # versions. This is an R-heap proxy only: it excludes native allocations and
  # is deliberately not labelled peak RSS.
  list(
    value = value,
    elapsed_seconds = elapsed,
    r_allocation_bytes = if (profile_available) {
      sum(allocation_bytes)
    } else {
      NA_real_
    },
    r_allocation_count = if (profile_available) {
      length(allocation_bytes)
    } else {
      NA_integer_
    },
    r_heap_peak_proxy_bytes = ph_gc_heap_peak_proxy_bytes(
      baseline,
      high_water
    ),
    rprofmem_available = profile_available
  )
}

ph_command_output <- function(command, args = character()) {
  output <- tryCatch(
    suppressWarnings(system2(command, args, stdout = TRUE, stderr = TRUE)),
    error = function(condition) character()
  )
  status <- attr(output, "status")
  if (length(output) == 0L || (!is.null(status) && status != 0L)) {
    return(NULL)
  }
  paste(output, collapse = "\n")
}

ph_environment <- function(repo_root) {
  diagnostics <- delta.sharing:::.native_diagnostics()
  package_root <- system.file(package = "delta.sharing")
  native_candidates <- list.files(
    file.path(package_root, "libs"),
    pattern = paste0("\\", .Platform$dynlib.ext, "$"),
    recursive = TRUE,
    full.names = TRUE
  )
  native_path <- if (length(native_candidates) == 1L) {
    normalizePath(native_candidates, winslash = "/", mustWork = TRUE)
  } else {
    NULL
  }
  package_version <- function(package) {
    if (!requireNamespace(package, quietly = TRUE)) {
      return(NULL)
    }
    as.character(utils::packageVersion(package))
  }
  sys <- Sys.info()
  list(
    captured_at_utc = format(
      Sys.time(),
      "%Y-%m-%dT%H:%M:%OS6Z",
      tz = "UTC"
    ),
    git_commit = ph_command_output(
      "git",
      c("-C", repo_root, "rev-parse", "HEAD")
    ),
    git_branch = ph_command_output(
      "git",
      c("-C", repo_root, "branch", "--show-current")
    ),
    git_dirty = nzchar(
      ph_command_output(
        "git",
        c("-C", repo_root, "status", "--porcelain")
      ) %||% ""
    ),
    r_version = R.version.string,
    r_platform = R.version$platform,
    os = unname(sys[["sysname"]]),
    os_release = unname(sys[["release"]]),
    machine = unname(sys[["machine"]]),
    cpu_count = unname(parallel::detectCores(logical = TRUE)),
    delta_sharing_version = package_version("delta.sharing"),
    delta_sharing_library = normalizePath(
      package_root,
      winslash = "/",
      mustWork = TRUE
    ),
    native_library_path = native_path,
    native_library_bytes = if (is.null(native_path)) {
      NULL
    } else {
      unname(file.info(native_path)$size)
    },
    native_library_md5 = if (is.null(native_path)) {
      NULL
    } else {
      unname(tools::md5sum(native_path))
    },
    nanoarrow_version = package_version("nanoarrow"),
    arrow_version = package_version("arrow"),
    delta_kernel_version = diagnostics$delta_kernel_version,
    arrow_rs_version = diagnostics$arrow_rs_version,
    rustc = ph_command_output("rustc", "--version"),
    cargo = ph_command_output("cargo", "--version"),
    rprofmem_available = isTRUE(unname(capabilities("profmem")))
  )
}

`%||%` <- function(value, fallback) {
  if (is.null(value)) fallback else value
}

ph_fixture_components <- function(repo_root) {
  path <- file.path(
    repo_root,
    "tests",
    "testthat",
    "fixtures",
    "protocol",
    "snapshot-delta.ndjson"
  )
  bytes <- readBin(path, what = "raw", n = file.info(path)$size)
  decoder <- delta.sharing:::.new_ndjson_decoder(
    "performance harness fixture"
  )
  actions <- c(
    delta.sharing:::.ndjson_decoder_push(decoder, bytes),
    delta.sharing:::.ndjson_decoder_finish(decoder)
  )
  list(
    protocol = actions[[1L]]$value,
    metadata = actions[[2L]]$value,
    files = lapply(actions[3:4], `[[`, "value")
  )
}

ph_scaled_snapshot_files <- function(components, count) {
  states <- lapply(components$files, delta.sharing:::.snapshot_file_state)
  add_state <- states[[which(vapply(
    states,
    function(state) identical(state$action_type, "add"),
    logical(1)
  ))[[1L]]]]
  lapply(seq_len(count), function(index) {
    add <- add_state$delta_action$add
    add$path <- sprintf(
      "https://benchmark.invalid/part-%08d.parquet?fixture=deterministic",
      index
    )
    # The manifest benchmark measures ordinary add actions; duplicating the
    # fixture's deletion-vector URL would make the synthetic input unrealistic.
    add$deletionVector <- NULL
    delta.sharing:::.new_private_snapshot_file(
      id = sprintf("benchmark-file-%08d", index),
      action_type = "add",
      delta_action = list(add = add),
      expiration_timestamp = 4102444800000
    )
  })
}

ph_measure_manifest_once <- function(components, files) {
  guard <- NULL
  measured <- ph_profile_r(function() {
    delta.sharing:::.prepare_snapshot_log(
      components$protocol,
      components$metadata,
      files
    )
  })
  guard <- measured$value
  on.exit({
    if (!is.null(guard) && !isTRUE(guard$state$released)) {
      delta.sharing:::.release_snapshot_log(guard)
    }
  }, add = TRUE)
  commit <- file.path(
    delta.sharing:::.snapshot_log_path(guard),
    "_delta_log",
    "00000000000000000000.json"
  )
  commit_bytes <- unname(file.info(commit)$size)
  cleanup <- ph_elapsed(delta.sharing:::.release_snapshot_log(guard))
  measured$value <- NULL
  c(
    measured,
    list(
      commit_bytes = commit_bytes,
      cleanup_seconds = cleanup$elapsed_seconds
    )
  )
}

ph_benchmark_manifests <- function(repo_root, config) {
  components <- ph_fixture_components(repo_root)
  staging_results <- list()
  manifest_results <- list()
  result_index <- 1L
  for (file_count in config$manifest_file_counts) {
    for (iteration in seq_len(config$repetitions)) {
      staged <- ph_profile_r(function() {
        ph_scaled_snapshot_files(components, file_count)
      })
      files <- staged$value
      staging_results[[result_index]] <- list(
        file_count = as.integer(file_count),
        iteration = as.integer(iteration),
        staging_seconds = staged$elapsed_seconds,
        r_allocation_bytes = staged$r_allocation_bytes,
        r_allocation_count = staged$r_allocation_count,
        r_heap_peak_proxy_bytes = staged$r_heap_peak_proxy_bytes,
        object_size_bytes = as.numeric(utils::object.size(files)),
        serialized_size_proxy_bytes = length(serialize(
          files,
          connection = NULL,
          xdr = FALSE
        ))
      )
      measurement <- ph_measure_manifest_once(components, files)
      manifest_results[[result_index]] <- list(
        file_count = as.integer(file_count),
        iteration = as.integer(iteration),
        prepare_seconds = measurement$elapsed_seconds,
        cleanup_seconds = measurement$cleanup_seconds,
        r_allocation_bytes = measurement$r_allocation_bytes,
        r_allocation_count = measurement$r_allocation_count,
        r_heap_peak_proxy_bytes = measurement$r_heap_peak_proxy_bytes,
        commit_bytes = measurement$commit_bytes
      )
      staged$value <- NULL
      rm(files)
      result_index <- result_index + 1L
    }
  }
  list(
    action_staging = staging_results,
    manifest = manifest_results
  )
}

ph_pull_native_stream <- function(batch_count, rows_per_batch) {
  diagnostics_before <- delta.sharing:::.native_diagnostics()
  creation <- ph_elapsed(
    delta.sharing:::.native_test_stream(batch_count, rows_per_batch)
  )
  stream <- creation$value
  on.exit(try(stream$release(), silent = TRUE), add = TRUE)
  first <- ph_elapsed(stream$get_next())
  rows <- if (is.null(first$value)) 0L else first$value$length
  batches <- if (is.null(first$value)) 0L else 1L
  steady <- ph_elapsed(repeat {
    batch <- stream$get_next()
    if (is.null(batch)) {
      break
    }
    rows <- rows + batch$length
    batches <- batches + 1L
  })
  release <- ph_elapsed(stream$release())
  diagnostics_after <- delta.sharing:::.native_diagnostics()
  expected_rows <- as.double(batch_count) * as.double(rows_per_batch)
  list(
    batch_count = as.integer(batch_count),
    rows_per_batch = as.integer(rows_per_batch),
    rows = as.double(rows),
    batches = as.integer(batches),
    expected_rows = expected_rows,
    creation_seconds = creation$elapsed_seconds,
    first_batch_seconds = first$elapsed_seconds,
    steady_seconds = steady$elapsed_seconds,
    release_seconds = release$elapsed_seconds,
    steady_rows_per_second = if (steady$elapsed_seconds > 0) {
      (rows - rows_per_batch) / steady$elapsed_seconds
    } else {
      Inf
    },
    steady_batches_per_second = if (steady$elapsed_seconds > 0) {
      max(0, batches - 1L) / steady$elapsed_seconds
    } else {
      Inf
    },
    active_streams_delta = as.double(
      diagnostics_after$active_streams - diagnostics_before$active_streams
    ),
    emitted_batches_delta = as.double(
      diagnostics_after$emitted_batches -
        diagnostics_before$emitted_batches
    )
  )
}

ph_benchmark_ffi <- function(config) {
  results <- list()
  result_index <- 1L
  for (rows_per_batch in config$ffi_rows_per_batch) {
    # The first run warms native code and allocator paths; it is intentionally
    # not included in the reported samples.
    invisible(ph_pull_native_stream(2L, rows_per_batch))
    for (iteration in seq_len(config$repetitions)) {
      result <- ph_pull_native_stream(config$ffi_batches, rows_per_batch)
      result$iteration <- as.integer(iteration)
      results[[result_index]] <- result
      result_index <- result_index + 1L
    }
  }
  results
}

ph_native_delta_fixture <- function(repo_root) {
  normalizePath(
    file.path(
      repo_root,
      "tests",
      "testthat",
      "fixtures",
      "delta",
      "local-table"
    ),
    winslash = "/",
    mustWork = TRUE
  )
}

ph_pull_kernel_stream <- function(path, limit, batch_size = 2L) {
  diagnostics_before <- delta.sharing:::.native_diagnostics()
  started <- as.numeric(Sys.time())
  stream <- delta.sharing:::.native_snapshot_stream(
    path,
    limit = limit,
    batch_size = batch_size
  )
  on.exit(try(stream$release(), silent = TRUE), add = TRUE)
  first_started <- as.numeric(Sys.time())
  first <- stream$get_next()
  first_batch_seconds <- as.numeric(Sys.time()) - first_started
  time_to_first_batch_seconds <- as.numeric(Sys.time()) - started
  rows <- if (is.null(first)) 0L else first$length
  batches <- if (is.null(first)) 0L else 1L
  repeat {
    batch <- stream$get_next()
    if (is.null(batch)) {
      break
    }
    rows <- rows + batch$length
    batches <- batches + 1L
  }
  total_seconds <- as.numeric(Sys.time()) - started
  release <- ph_elapsed(stream$release())
  diagnostics_after <- delta.sharing:::.native_diagnostics()
  list(
    limit = if (is.null(limit)) NULL else as.double(limit),
    batch_size = as.integer(batch_size),
    rows = as.double(rows),
    batches = as.integer(batches),
    time_to_first_batch_seconds = time_to_first_batch_seconds,
    first_batch_pull_seconds = first_batch_seconds,
    total_seconds = total_seconds,
    rows_per_second = if (total_seconds > 0) rows / total_seconds else Inf,
    release_seconds = release$elapsed_seconds,
    active_streams_delta = as.double(
      diagnostics_after$active_streams - diagnostics_before$active_streams
    ),
    emitted_batches_delta = as.double(
      diagnostics_after$emitted_batches -
        diagnostics_before$emitted_batches
    )
  )
}

ph_benchmark_kernel <- function(repo_root, config) {
  path <- ph_native_delta_fixture(repo_root)
  results <- list()
  result_index <- 1L
  invisible(ph_pull_kernel_stream(path, 1, batch_size = 2L))
  for (iteration in seq_len(config$kernel_repetitions)) {
    for (limit_name in c("one", "all")) {
      limit <- if (identical(limit_name, "one")) 1 else NULL
      result <- ph_pull_kernel_stream(path, limit, batch_size = 2L)
      result$case <- limit_name
      result$iteration <- as.integer(iteration)
      results[[result_index]] <- result
      result_index <- result_index + 1L
    }
  }
  results
}

ph_release_once <- function() {
  diagnostics_before <- delta.sharing:::.native_diagnostics()
  stream <- delta.sharing:::.native_test_stream(
    batches = 10000L,
    rows_per_batch = 4096L
  )
  first <- stream$get_next()
  release <- ph_elapsed(stream$release())
  diagnostics_after <- delta.sharing:::.native_diagnostics()
  list(
    first_batch_rows = first$length,
    release_seconds = release$elapsed_seconds,
    active_streams_delta = as.double(
      diagnostics_after$active_streams - diagnostics_before$active_streams
    ),
    cancelled_streams_delta = as.double(
      diagnostics_after$cancelled_streams -
        diagnostics_before$cancelled_streams
    ),
    emitted_batches_delta = as.double(
      diagnostics_after$emitted_batches -
        diagnostics_before$emitted_batches
    )
  )
}

ph_benchmark_release <- function(config) {
  lapply(seq_len(config$release_repetitions), function(iteration) {
    result <- ph_release_once()
    result$iteration <- as.integer(iteration)
    result
  })
}

ph_backpressure_once <- function(config) {
  diagnostics_before <- delta.sharing:::.native_diagnostics()
  profiled <- ph_profile_r(function() {
    delta.sharing:::.native_test_stream(
      batches = 10000L,
      rows_per_batch = 65536L
    )
  })
  stream <- profiled$value
  on.exit(try(stream$release(), silent = TRUE), add = TRUE)
  diagnostics_created <- delta.sharing:::.native_diagnostics()
  Sys.sleep(config$backpressure_idle_seconds)
  diagnostics_idle <- delta.sharing:::.native_diagnostics()
  pull <- ph_elapsed(stream$get_next())
  diagnostics_pulled <- delta.sharing:::.native_diagnostics()
  release <- ph_elapsed(stream$release())
  diagnostics_released <- delta.sharing:::.native_diagnostics()
  list(
    idle_seconds = config$backpressure_idle_seconds,
    constructor_seconds = profiled$elapsed_seconds,
    constructor_r_allocation_bytes = profiled$r_allocation_bytes,
    constructor_r_heap_peak_proxy_bytes =
      profiled$r_heap_peak_proxy_bytes,
    first_pull_seconds = pull$elapsed_seconds,
    first_batch_rows = pull$value$length,
    release_seconds = release$elapsed_seconds,
    active_after_create_delta = as.double(
      diagnostics_created$active_streams -
        diagnostics_before$active_streams
    ),
    emitted_while_idle = as.double(
      diagnostics_idle$emitted_batches -
        diagnostics_created$emitted_batches
    ),
    emitted_after_one_pull = as.double(
      diagnostics_pulled$emitted_batches -
        diagnostics_idle$emitted_batches
    ),
    active_after_release_delta = as.double(
      diagnostics_released$active_streams -
        diagnostics_before$active_streams
    )
  )
}

ph_consume_for_heap_proxy <- function(batch_count, rows_per_batch) {
  ph_profile_r(function() {
    stream <- delta.sharing:::.native_test_stream(
      batches = batch_count,
      rows_per_batch = rows_per_batch
    )
    on.exit(try(stream$release(), silent = TRUE), add = TRUE)
    rows <- 0
    repeat {
      batch <- stream$get_next()
      if (is.null(batch)) {
        break
      }
      rows <- rows + batch$length
    }
    rows
  })
}

ph_benchmark_backpressure <- function(config) {
  idle <- ph_backpressure_once(config)
  heap <- lapply(config$heap_batch_counts, function(batch_count) {
    result <- ph_consume_for_heap_proxy(
      batch_count,
      config$heap_rows_per_batch
    )
    list(
      batch_count = as.integer(batch_count),
      rows_per_batch = as.integer(config$heap_rows_per_batch),
      rows = as.double(result$value),
      elapsed_seconds = result$elapsed_seconds,
      r_allocation_bytes = result$r_allocation_bytes,
      r_allocation_count = result$r_allocation_count,
      r_heap_peak_proxy_bytes = result$r_heap_peak_proxy_bytes
    )
  })
  list(idle = idle, heap_scaling = heap)
}

ph_gate <- function(id, requirement, status, evidence, threshold, reason = NULL) {
  list(
    id = id,
    requirement = requirement,
    status = status,
    threshold = threshold,
    evidence = evidence,
    reason = reason
  )
}

ph_evaluate_gates <- function(measurements) {
  exact_one <- Filter(
    function(item) identical(item$case, "one"),
    measurements$kernel
  )
  exact_limit_ok <- length(exact_one) > 0L &&
    all(vapply(exact_one, function(item) {
      identical(item$rows, 1) &&
        identical(item$batches, 1L) &&
        identical(item$active_streams_delta, 0)
    }, logical(1)))
  release_ok <- length(measurements$release) > 0L &&
    all(vapply(measurements$release, function(item) {
      identical(item$active_streams_delta, 0) &&
        identical(item$cancelled_streams_delta, 1) &&
        identical(item$emitted_batches_delta, 1)
    }, logical(1)))
  idle <- measurements$backpressure$idle
  backpressure_ok <-
    identical(idle$active_after_create_delta, 1) &&
    identical(idle$emitted_while_idle, 0) &&
    identical(idle$emitted_after_one_pull, 1) &&
    identical(idle$active_after_release_delta, 0)

  list(
    ph_gate(
      "exact-limit-correctness",
      "Exact limit stops after the requested row count.",
      if (exact_limit_ok) "pass" else "fail",
      list(samples = exact_one),
      "rows = 1, batches = 1, active-stream delta = 0"
    ),
    ph_gate(
      "explicit-release-lifecycle",
      "Explicit release cancels and drops every live native stream.",
      if (release_ok) "pass" else "fail",
      list(samples = measurements$release),
      "active-stream delta = 0; cancelled delta = 1; emitted delta = 1"
    ),
    ph_gate(
      "NFR-PERF-05-demand-driven-boundary",
      "The Arrow C Stream boundary emits only when R pulls.",
      if (backpressure_ok) "pass" else "fail",
      list(sample = idle),
      "0 batches while idle; exactly 1 after one pull; release returns active count"
    ),
    ph_gate(
      "NFR-PERF-02-ffi-overhead",
      "R/FFI overhead for 64K+ batches is below the Rust-only scan.",
      "not_evaluable",
      list(absolute_ffi_samples = measurements$ffi),
      "< 2% overhead versus the same Rust-only scan",
      "No same-fixture Rust-only benchmark executable exists yet."
    ),
    ph_gate(
      "NFR-PERF-03-throughput",
      "End-to-end R Arrow stream throughput approaches Rust-only Kernel.",
      "not_evaluable",
      list(r_kernel_samples = measurements$kernel),
      ">= 90% of the Rust-only Delta Kernel baseline",
      "The deterministic local table is available, but the required Rust-only comparator is not."
    ),
    ph_gate(
      "NFR-PERF-04-peak-rss",
      "Streaming memory is bounded by in-flight batches and fixed overhead.",
      "not_evaluable",
      list(
        demand_driven = idle,
        r_heap_proxy_samples = measurements$backpressure$heap_scaling
      ),
      "bounded peak RSS as total rows grow",
      "This harness records R allocations and an R-heap high-water proxy, not process peak RSS or native allocator memory."
    ),
    ph_gate(
      "ADR-003-rust-scope-exception",
      "A representative end-to-end result justifies expanding Rust scope.",
      "not_evaluable",
      list(),
      ">= 25% wall-time improvement or >= 50% peak-RSS reduction",
      "No Rust-scope expansion is proposed and no alternative implementation is benchmarked."
    )
  )
}

ph_summary_stats <- function(values) {
  values <- as.numeric(values)
  values <- values[is.finite(values)]
  if (length(values) == 0L) {
    return(list(n = 0L))
  }
  list(
    n = length(values),
    min = min(values),
    median = stats::median(values),
    p95 = unname(stats::quantile(values, 0.95, names = FALSE, type = 8)),
    max = max(values)
  )
}

ph_group_summary <- function(samples, key, metrics) {
  keys <- unique(vapply(samples, `[[`, numeric(1), key))
  lapply(sort(keys), function(value) {
    group <- Filter(
      function(sample) identical(as.numeric(sample[[key]]), value),
      samples
    )
    summaries <- lapply(metrics, function(metric) {
      ph_summary_stats(vapply(group, `[[`, numeric(1), metric))
    })
    names(summaries) <- metrics
    c(stats::setNames(list(value), key), summaries)
  })
}

ph_summarize <- function(measurements) {
  kernel_cases <- unique(vapply(
    measurements$kernel,
    `[[`,
    character(1),
    "case"
  ))
  list(
    action_staging_by_file_count = ph_group_summary(
      measurements$action_staging,
      "file_count",
      c(
        "staging_seconds",
        "r_allocation_bytes",
        "r_heap_peak_proxy_bytes",
        "object_size_bytes",
        "serialized_size_proxy_bytes"
      )
    ),
    manifest_by_file_count = ph_group_summary(
      measurements$manifest,
      "file_count",
      c(
        "prepare_seconds",
        "cleanup_seconds",
        "r_allocation_bytes",
        "r_heap_peak_proxy_bytes"
      )
    ),
    ffi_by_rows_per_batch = ph_group_summary(
      measurements$ffi,
      "rows_per_batch",
      c(
        "first_batch_seconds",
        "steady_rows_per_second",
        "steady_batches_per_second",
        "release_seconds"
      )
    ),
    kernel_by_case = lapply(sort(kernel_cases), function(case) {
      group <- Filter(
        function(sample) identical(sample$case, case),
        measurements$kernel
      )
      list(
        case = case,
        time_to_first_batch_seconds = ph_summary_stats(vapply(
          group,
          `[[`,
          numeric(1),
          "time_to_first_batch_seconds"
        )),
        total_seconds = ph_summary_stats(vapply(
          group,
          `[[`,
          numeric(1),
          "total_seconds"
        )),
        rows_per_second = ph_summary_stats(vapply(
          group,
          `[[`,
          numeric(1),
          "rows_per_second"
        ))
      )
    }),
    explicit_release_seconds = ph_summary_stats(vapply(
      measurements$release,
      `[[`,
      numeric(1),
      "release_seconds"
    )),
    backpressure = measurements$backpressure
  )
}

ph_validate_artifact <- function(artifact) {
  required <- c(
    "schema_version",
    "environment",
    "configuration",
    "measurements",
    "summaries",
    "gates",
    "metric_classes",
    "limitations"
  )
  if (!is.list(artifact) || length(setdiff(required, names(artifact))) > 0L) {
    ph_abort("Performance artifact is missing required top-level fields.")
  }
  if (!identical(artifact$schema_version, 1L)) {
    ph_abort("Performance artifact has an unsupported schema version.")
  }
  measurement_names <- c(
    "action_staging",
    "manifest",
    "ffi",
    "kernel",
    "release",
    "backpressure"
  )
  if (
    !is.list(artifact$measurements) ||
      length(setdiff(measurement_names, names(artifact$measurements))) > 0L
  ) {
    ph_abort("Performance artifact is missing measurement groups.")
  }
  if (!is.list(artifact$gates) || length(artifact$gates) == 0L) {
    ph_abort("Performance artifact must include evaluated gates.")
  }
  gate_ids <- vapply(artifact$gates, function(gate) {
    if (
      !is.list(gate) ||
        !ph_scalar_character(gate$id) ||
        !ph_scalar_character(gate$status) ||
        !gate$status %in% c("pass", "fail", "not_evaluable")
    ) {
      ph_abort("Performance artifact contains an invalid gate.")
    }
    gate$id
  }, character(1))
  if (anyDuplicated(gate_ids)) {
    ph_abort("Performance artifact contains duplicate gate identifiers.")
  }
  if (
    !is.list(artifact$metric_classes) ||
      !identical(
        sort(names(artifact$metric_classes)),
        c("controlled", "trend")
      ) ||
      !all(vapply(artifact$metric_classes, is.character, logical(1)))
  ) {
    ph_abort("Performance artifact must separate controlled and trend metrics.")
  }
  if (
    !is.character(artifact$limitations) ||
      length(artifact$limitations) == 0L ||
      anyNA(artifact$limitations)
  ) {
    ph_abort("Performance artifact must record limitations.")
  }
  invisible(artifact)
}

ph_write_artifact <- function(artifact, path) {
  ph_validate_artifact(artifact)
  parent <- dirname(path)
  if (!dir.exists(parent) && !dir.create(parent, recursive = TRUE)) {
    ph_abort("Could not create the benchmark output directory.")
  }
  temporary <- tempfile(
    paste0(".", basename(path), "-"),
    tmpdir = parent
  )
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
  parsed <- jsonlite::read_json(temporary, simplifyVector = FALSE)
  parsed$schema_version <- as.integer(parsed$schema_version)
  parsed$limitations <- unlist(parsed$limitations, use.names = FALSE)
  parsed$metric_classes <- lapply(
    parsed$metric_classes,
    unlist,
    use.names = FALSE
  )
  ph_validate_artifact(parsed)
  if (!file.rename(temporary, path)) {
    ph_abort("Could not atomically publish the benchmark artifact.")
  }
  invisible(normalizePath(path, winslash = "/", mustWork = TRUE))
}

ph_read_artifact <- function(path) {
  artifact <- jsonlite::read_json(path, simplifyVector = FALSE)
  artifact$schema_version <- as.integer(artifact$schema_version)
  artifact$limitations <- unlist(artifact$limitations, use.names = FALSE)
  artifact$metric_classes <- lapply(
    artifact$metric_classes,
    unlist,
    use.names = FALSE
  )
  ph_validate_artifact(artifact)
  artifact
}

ph_run <- function(repo_root, config, output) {
  manifest_measurements <- ph_benchmark_manifests(repo_root, config)
  measurements <- list(
    action_staging = manifest_measurements$action_staging,
    manifest = manifest_measurements$manifest,
    ffi = ph_benchmark_ffi(config),
    kernel = ph_benchmark_kernel(repo_root, config),
    release = ph_benchmark_release(config),
    backpressure = ph_benchmark_backpressure(config)
  )
  artifact <- list(
    schema_version = 1L,
    environment = ph_environment(repo_root),
    configuration = config,
    measurements = measurements,
    summaries = ph_summarize(measurements),
    gates = ph_evaluate_gates(measurements),
    metric_classes = list(
      controlled = c(
        "exact-limit-correctness",
        "explicit-release-lifecycle",
        "NFR-PERF-05-demand-driven-boundary"
      ),
      trend = c(
        "validated action staging, retained-size proxies, and R allocation scaling",
        "synthetic-log preparation and R allocation scaling",
        "synthetic Arrow C Stream first-batch and steady-state rates",
        "local Kernel first-batch and end-to-end rates",
        "exact-limit early-stop latency",
        "explicit release latency",
        "R-heap high-water proxy scaling",
        "NFR-PERF-02, NFR-PERF-03, NFR-PERF-04, and ADR 003 comparators"
      )
    ),
    limitations = c(
      "Rprofmem cumulative allocations are allocation pressure, not retained memory or peak RSS.",
      "The gc high-water delta is an R-heap proxy; it excludes Rust, Arrow, Delta Kernel, memory maps, and object-store buffers.",
      "Scalable action staging starts from one already decoded fixture action and does not measure repeated wire NDJSON decoding.",
      "The seven-row local Delta fixture validates the path and lifecycle but is too small for a release throughput claim.",
      "Synthetic native streams isolate C Stream pull/lifecycle cost but do not represent Parquet decode, object storage, or a Rust-only baseline.",
      "Scheduler, thermal, filesystem-cache, and background-load noise remain in local trend timings.",
      "Cloud/object-store, 1 GB/10 GB, wide/nested, deletion-vector, CDF, and peak-RSS baselines remain release work."
    )
  )
  ph_write_artifact(artifact, output)
  artifact
}
