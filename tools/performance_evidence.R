pe_abort <- function(message) {
  stop(message, call. = FALSE)
}

pe_scalar_character <- function(value) {
  is.character(value) &&
    length(value) == 1L &&
    !is.na(value) &&
    nzchar(value)
}

pe_parse_cli <- function(args, repo_root) {
  result <- list(
    base = NULL,
    output = file.path(
      tempdir(),
      "delta-sharing-r-performance-evidence.json"
    ),
    mode = "quick",
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
    if (!argument %in% c("--base", "--output", "--mode")) {
      pe_abort(sprintf("Unknown evidence argument: %s", argument))
    }
    if (index == length(args)) {
      pe_abort(sprintf("Evidence argument %s requires a value.", argument))
    }
    value <- args[[index + 1L]]
    if (identical(argument, "--base")) {
      result$base <- value
    } else if (identical(argument, "--output")) {
      result$output <- value
    } else {
      if (!value %in% c("quick", "standard")) {
        pe_abort("`--mode` must be `quick` or `standard`.")
      }
      result$mode <- value
    }
    index <- index + 2L
  }
  if (!isTRUE(result$help) && !pe_scalar_character(result$base)) {
    pe_abort("`--base` is required.")
  }
  result
}

pe_config <- function(mode) {
  if (identical(mode, "quick")) {
    return(list(
      mode = mode,
      rust_warmups = 1L,
      kernel_repetitions = 5L,
      kernel_table_rows = 1048576L,
      kernel_batch_size = 65536L,
      kernel_row_group_size = 65536L,
      rss_repetitions = 1L,
      rss_batch_counts = c(8L, 64L, 512L),
      rss_rows_per_batch = 4096L,
      rss_kernel_row_counts = c(65536L, 1048576L, 4194304L),
      rss_growth_tolerance_bytes = 32 * 1024^2,
      rss_growth_tolerance_fraction = 0.20
    ))
  }
  if (identical(mode, "standard")) {
    return(list(
      mode = mode,
      rust_warmups = 3L,
      kernel_repetitions = 15L,
      kernel_table_rows = 8388608L,
      kernel_batch_size = 65536L,
      kernel_row_group_size = 65536L,
      rss_repetitions = 3L,
      rss_batch_counts = c(256L, 2048L, 8192L),
      rss_rows_per_batch = 4096L,
      rss_kernel_row_counts = c(65536L, 4194304L, 16777216L),
      rss_growth_tolerance_bytes = 32 * 1024^2,
      rss_growth_tolerance_fraction = 0.20
    ))
  }
  pe_abort("Unknown evidence mode.")
}

pe_command <- function(command, args, stdout = TRUE, stderr = TRUE) {
  output <- tryCatch(
    suppressWarnings(system2(command, args, stdout = stdout, stderr = stderr)),
    error = identity
  )
  if (inherits(output, "error")) {
    pe_abort(conditionMessage(output))
  }
  status <- attr(output, "status") %||% 0L
  if (!identical(as.integer(status), 0L)) {
    details <- if (is.character(output)) paste(output, collapse = "\n") else ""
    pe_abort(sprintf(
      "Command failed (%s): %s",
      status,
      details
    ))
  }
  output
}

pe_git_value <- function(repo_root, args) {
  output <- pe_command("git", c("-C", repo_root, args))
  paste(output, collapse = "\n")
}

pe_fixture_path <- function(repo_root) {
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

pe_source_identity <- function(repo_root) {
  relative <- c(
    "src/rust/Cargo.toml",
    "src/rust/Cargo.lock",
    "src/rust/src/kernel/adapter.rs",
    "src/rust/examples/kernel_scan_comparator.rs",
    "src/rust/examples/generate_kernel_benchmark_table.rs",
    "tests/testthat/fixtures/delta/local-table/_delta_log/00000000000000000000.json",
    "tests/testthat/fixtures/delta/local-table/part-00000.parquet",
    "tests/testthat/fixtures/delta/local-table/part-00001.parquet"
  )
  paths <- file.path(repo_root, relative)
  if (!all(file.exists(paths))) {
    pe_abort("Performance evidence source identity is incomplete.")
  }
  as.list(stats::setNames(unname(tools::md5sum(paths)), relative))
}

pe_kernel_samples <- function(base) {
  samples <- Filter(
    function(sample) identical(sample$case, "all"),
    base$measurements$kernel
  )
  if (length(samples) == 0L) {
    pe_abort("Base artifact has no unlimited local Kernel samples.")
  }
  batch_sizes <- unique(vapply(
    samples,
    function(sample) as.integer(sample$batch_size),
    integer(1)
  ))
  row_counts <- unique(vapply(
    samples,
    function(sample) as.double(sample$rows),
    numeric(1)
  ))
  if (length(batch_sizes) != 1L || length(row_counts) != 1L) {
    pe_abort("Base Kernel samples do not share one comparable workload.")
  }
  list(
    samples = samples,
    batch_size = batch_sizes[[1L]],
    rows = row_counts[[1L]]
  )
}

pe_rust_binary_path <- function(repo_root, name) {
  suffix <- if (identical(.Platform$OS.type, "windows")) ".exe" else ""
  file.path(
    repo_root,
    "src",
    "rust",
    "target",
    "release",
    "examples",
    paste0(name, suffix)
  )
}

pe_build_rust_tools <- function(repo_root) {
  manifest <- file.path(repo_root, "src", "rust", "Cargo.toml")
  pe_command(
    "cargo",
    c(
      "build",
      "--release",
      "--manifest-path",
      manifest,
      "--example",
      "kernel_scan_comparator",
      "--example",
      "generate_kernel_benchmark_table"
    )
  )
  comparator <- pe_rust_binary_path(
    repo_root,
    "kernel_scan_comparator"
  )
  generator <- pe_rust_binary_path(
    repo_root,
    "generate_kernel_benchmark_table"
  )
  if (!file.exists(comparator) || !file.exists(generator)) {
    pe_abort("Cargo did not produce both performance executables.")
  }
  list(
    comparator = normalizePath(
      comparator,
      winslash = "/",
      mustWork = TRUE
    ),
    comparator_md5 = unname(tools::md5sum(comparator)),
    generator = normalizePath(
      generator,
      winslash = "/",
      mustWork = TRUE
    ),
    generator_md5 = unname(tools::md5sum(generator))
  )
}

pe_invoke_rust_comparator <- function(
  binary,
  table,
  batch_size,
  repetitions,
  warmups,
  expected_rows
) {
  output <- tempfile("delta-sharing-r-rust-comparator-", fileext = ".json")
  on.exit(unlink(output, force = TRUE), add = TRUE)
  pe_command(
    binary,
    c(
      "--table",
      table,
      "--batch-size",
      as.character(batch_size),
      "--repetitions",
      as.character(repetitions),
      "--warmups",
      as.character(warmups),
      "--expected-rows",
      as.character(expected_rows),
      "--output",
      output
    )
  )
  result <- jsonlite::read_json(output, simplifyVector = FALSE)
  result$schema_version <- as.integer(result$schema_version)
  if (!identical(result$schema_version, 1L) ||
      !identical(result$implementation, "package-kernel-adapter-direct") ||
      !identical(as.integer(result$batch_size), as.integer(batch_size)) ||
      length(result$samples) != repetitions ||
      any(vapply(result$samples, function(sample) {
        !identical(as.double(sample$rows), as.double(expected_rows))
      }, logical(1)))) {
    pe_abort("Rust comparator output does not match the requested workload.")
  }
  result
}

pe_generate_table <- function(tools, path, rows, row_group_size) {
  pe_command(
    tools$generator,
    c(
      "--output",
      path,
      "--rows",
      as.character(rows),
      "--row-group-size",
      as.character(row_group_size)
    )
  )
  parquet <- file.path(path, "part-00000.parquet")
  log <- file.path(
    path,
    "_delta_log",
    "00000000000000000000.json"
  )
  if (!file.exists(parquet) || !file.exists(log)) {
    pe_abort("Benchmark table generator produced an incomplete Delta table.")
  }
  list(
    kind = "generated-temporary-local-table",
    rows = as.double(rows),
    row_group_size = as.integer(row_group_size),
    parquet_bytes = unname(file.info(parquet)$size),
    parquet_md5 = unname(tools::md5sum(parquet)),
    log_md5 = unname(tools::md5sum(log))
  )
}

pe_pull_r_kernel <- function(path, batch_size) {
  diagnostics_before <- delta.sharing:::.native_diagnostics()
  started <- as.numeric(Sys.time())
  stream <- delta.sharing:::.native_snapshot_stream(
    path,
    limit = NULL,
    batch_size = batch_size
  )
  on.exit(try(stream$release(), silent = TRUE), add = TRUE)
  first_started <- as.numeric(Sys.time())
  first <- stream$get_next()
  first_batch_pull_seconds <- as.numeric(Sys.time()) - first_started
  time_to_first_batch_seconds <- as.numeric(Sys.time()) - started
  rows <- if (is.null(first)) 0 else first$length
  batches <- if (is.null(first)) 0L else 1L
  maximum_batch_rows <- if (is.null(first)) 0L else first$length
  repeat {
    batch <- stream$get_next()
    if (is.null(batch)) {
      break
    }
    rows <- rows + batch$length
    batches <- batches + 1L
    maximum_batch_rows <- max(maximum_batch_rows, batch$length)
  }
  total_seconds <- as.numeric(Sys.time()) - started
  stream$release()
  diagnostics_after <- delta.sharing:::.native_diagnostics()
  if (!identical(
    as.double(
      diagnostics_after$active_streams -
        diagnostics_before$active_streams
    ),
    0
  )) {
    pe_abort("R comparator leaked an active native stream.")
  }
  list(
    rows = as.double(rows),
    batches = batches,
    maximum_batch_rows = maximum_batch_rows,
    batch_size = as.integer(batch_size),
    first_batch_pull_seconds = first_batch_pull_seconds,
    time_to_first_batch_seconds = time_to_first_batch_seconds,
    total_seconds = total_seconds,
    rows_per_second = rows / total_seconds
  )
}

pe_benchmark_comparable_kernel <- function(repo_root, tools, config) {
  parent <- tempfile("delta-sharing-r-generated-kernel-")
  table <- file.path(parent, "table")
  dir.create(parent)
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  fixture <- pe_generate_table(
    tools,
    table,
    config$kernel_table_rows,
    config$kernel_row_group_size
  )

  invisible(pe_pull_r_kernel(table, config$kernel_batch_size))
  invisible(pe_invoke_rust_comparator(
    tools$comparator,
    table,
    config$kernel_batch_size,
    1L,
    config$rust_warmups,
    config$kernel_table_rows
  ))

  r_samples <- vector("list", config$kernel_repetitions)
  rust_samples <- vector("list", config$kernel_repetitions)
  for (iteration in seq_len(config$kernel_repetitions)) {
    rust_once <- function() {
      pe_invoke_rust_comparator(
        tools$comparator,
        table,
        config$kernel_batch_size,
        1L,
        0L,
        config$kernel_table_rows
      )$samples[[1L]]
    }
    if (iteration %% 2L == 1L) {
      r_samples[[iteration]] <- pe_pull_r_kernel(
        table,
        config$kernel_batch_size
      )
      rust_samples[[iteration]] <- rust_once()
    } else {
      rust_samples[[iteration]] <- rust_once()
      r_samples[[iteration]] <- pe_pull_r_kernel(
        table,
        config$kernel_batch_size
      )
    }
    r_samples[[iteration]]$iteration <- iteration
    rust_samples[[iteration]]$iteration <- iteration
  }
  if (any(vapply(r_samples, function(sample) {
    !identical(sample$rows, as.double(config$kernel_table_rows))
  }, logical(1)))) {
    pe_abort("R comparator returned an unexpected generated-table row count.")
  }
  list(
    fixture = fixture,
    order = "odd iterations R then Rust; even iterations Rust then R",
    r_samples = r_samples,
    rust_samples = rust_samples
  )
}

pe_time_backend <- function(sysname = unname(Sys.info()[["sysname"]])) {
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
      reason = "Linux `/usr/bin/time` is not GNU time; maximum RSS units are not portable."
    ))
  }
  list(
    available = FALSE,
    reason = sprintf(
      "No accurate peak-RSS backend is implemented for %s.",
      sysname %||% "this platform"
    )
  )
}

pe_parse_peak_rss <- function(lines, backend) {
  if (identical(backend$name, "darwin-time-l")) {
    matching <- grep(
      "^[[:space:]]*[0-9]+[[:space:]]+maximum resident set size[[:space:]]*$",
      lines,
      value = TRUE
    )
    if (length(matching) != 1L) {
      pe_abort("Darwin time output did not contain one peak-RSS value.")
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
      pe_abort("GNU time output did not contain one peak-RSS value.")
    }
    kib <- as.double(sub("^.*:[[:space:]]*([0-9]+).*$", "\\1", matching))
    return(kib * 1024)
  }
  pe_abort("Unknown peak-RSS backend.")
}

pe_timed_worker <- function(repo_root, backend, worker_args) {
  worker <- file.path(repo_root, "tools", "performance_peak_rss_worker.R")
  worker_output <- tempfile("delta-sharing-r-rss-worker-", fileext = ".json")
  time_output <- tempfile("delta-sharing-r-time-", fileext = ".txt")
  stdout <- tempfile("delta-sharing-r-rss-worker-", fileext = ".stdout")
  stderr <- tempfile("delta-sharing-r-rss-worker-", fileext = ".stderr")
  on.exit(unlink(
    c(worker_output, time_output, stdout, stderr),
    force = TRUE
  ), add = TRUE)
  rscript <- file.path(R.home("bin"), "Rscript")
  arguments <- c(
    backend$flags,
    "-o",
    time_output,
    rscript,
    worker,
    worker_args,
    "--output",
    worker_output
  )
  status <- suppressWarnings(system2(
    backend$path,
    vapply(arguments, shQuote, character(1)),
    stdout = stdout,
    stderr = stderr
  ))
  if (!identical(as.integer(status), 0L)) {
    pe_abort(sprintf(
      "Peak-RSS worker failed (%s): %s",
      status,
      paste(readLines(stderr, warn = FALSE), collapse = "\n")
    ))
  }
  worker_result <- jsonlite::read_json(worker_output, simplifyVector = FALSE)
  list(
    peak_rss_bytes = pe_parse_peak_rss(
      readLines(time_output, warn = FALSE),
      backend
    ),
    worker = worker_result
  )
}

pe_benchmark_peak_rss <- function(repo_root, rust_tools, config) {
  backend <- pe_time_backend()
  if (!isTRUE(backend$available)) {
    return(list(
      backend = backend,
      baseline = list(),
      synthetic_scaling = list(),
      kernel_scaling = list()
    ))
  }
  generated_parents <- character()
  on.exit(
    unlink(generated_parents, recursive = TRUE, force = TRUE),
    add = TRUE
  )
  baseline <- lapply(seq_len(config$rss_repetitions), function(iteration) {
    result <- pe_timed_worker(
      repo_root,
      backend,
      c("--workload", "baseline")
    )
    result$iteration <- iteration
    result
  })
  synthetic <- list()
  index <- 1L
  for (batch_count in config$rss_batch_counts) {
    for (iteration in seq_len(config$rss_repetitions)) {
      result <- pe_timed_worker(
        repo_root,
        backend,
        c(
          "--workload",
          "synthetic",
          "--batches",
          as.character(batch_count),
          "--rows-per-batch",
          as.character(config$rss_rows_per_batch)
        )
      )
      result$batch_count <- batch_count
      result$rows_per_batch <- config$rss_rows_per_batch
      result$iteration <- iteration
      synthetic[[index]] <- result
      index <- index + 1L
    }
  }
  kernel <- list()
  index <- 1L
  for (table_rows in config$rss_kernel_row_counts) {
    parent <- tempfile("delta-sharing-r-rss-kernel-")
    generated_parents <- c(generated_parents, parent)
    table <- file.path(parent, "table")
    dir.create(parent)
    fixture <- tryCatch(
      pe_generate_table(
        rust_tools,
        table,
        table_rows,
        config$kernel_row_group_size
      ),
      error = function(condition) {
        unlink(parent, recursive = TRUE, force = TRUE)
        stop(condition)
      }
    )
    for (iteration in seq_len(config$rss_repetitions)) {
      result <- pe_timed_worker(
        repo_root,
        backend,
        c(
          "--workload",
          "kernel",
          "--table",
          table,
          "--batch-size",
          as.character(config$kernel_batch_size),
          "--expected-rows",
          as.character(table_rows)
        )
      )
      result$table_rows <- as.double(table_rows)
      result$parquet_bytes <- fixture$parquet_bytes
      result$iteration <- iteration
      result$worker$parameters$table <- "generated-temporary-local-table"
      kernel[[index]] <- result
      index <- index + 1L
    }
    unlink(parent, recursive = TRUE, force = TRUE)
  }
  list(
    backend = backend,
    baseline = baseline,
    synthetic_scaling = synthetic,
    kernel_scaling = kernel
  )
}

pe_median <- function(values) {
  stats::median(as.double(values))
}

pe_comparisons <- function(comparable) {
  r_total <- vapply(
    comparable$r_samples,
    function(sample) as.double(sample$total_seconds),
    numeric(1)
  )
  r_throughput <- vapply(
    comparable$r_samples,
    function(sample) as.double(sample$rows_per_second),
    numeric(1)
  )
  rust_total <- vapply(
    comparable$rust_samples,
    function(sample) as.double(sample$total_seconds),
    numeric(1)
  )
  rust_throughput <- vapply(
    comparable$rust_samples,
    function(sample) as.double(sample$rows_per_second),
    numeric(1)
  )
  r_maximum_batch_rows <- max(vapply(
    comparable$r_samples,
    function(sample) as.double(sample$maximum_batch_rows),
    numeric(1)
  ))
  rust_maximum_batch_rows <- max(vapply(
    comparable$rust_samples,
    function(sample) as.double(sample$maximum_batch_rows),
    numeric(1)
  ))
  list(
    workload_rows = comparable$fixture$rows,
    parquet_bytes = comparable$fixture$parquet_bytes,
    configured_batch_size =
      as.integer(comparable$r_samples[[1L]]$batch_size),
    r_observed_maximum_batch_rows = r_maximum_batch_rows,
    rust_observed_maximum_batch_rows = rust_maximum_batch_rows,
    sample_order = comparable$order,
    r_total_seconds = ph_summary_stats(r_total),
    rust_total_seconds = ph_summary_stats(rust_total),
    r_rows_per_second = ph_summary_stats(r_throughput),
    rust_rows_per_second = ph_summary_stats(rust_throughput),
    median_total_time_overhead_fraction =
      pe_median(r_total) / pe_median(rust_total) - 1,
    median_throughput_ratio =
      pe_median(r_throughput) / pe_median(rust_throughput)
  )
}

pe_rss_scaling <- function(rss, config) {
  if (!isTRUE(rss$backend$available)) {
    return(list(
      evaluable = FALSE,
      pass = FALSE,
      reason = rss$backend$reason
    ))
  }
  if (!identical(config$mode, "standard")) {
    return(list(
      evaluable = FALSE,
      pass = FALSE,
      reason = paste(
        "Quick mode is an instrumentation smoke test; standard mode is",
        "required for the controlled real-Kernel peak-RSS gate."
      )
    ))
  }
  medians <- lapply(config$rss_kernel_row_counts, function(table_rows) {
    samples <- Filter(
      function(sample) identical(
        as.double(sample$table_rows),
        as.double(table_rows)
      ),
      rss$kernel_scaling
    )
    list(
      table_rows = as.double(table_rows),
      parquet_bytes = as.double(samples[[1L]]$parquet_bytes),
      median_peak_rss_bytes = pe_median(vapply(
        samples,
        `[[`,
        numeric(1),
        "peak_rss_bytes"
      ))
    )
  })
  values <- vapply(
    medians,
    `[[`,
    numeric(1),
    "median_peak_rss_bytes"
  )
  observed_growth <- max(values) - min(values)
  tolerance <- max(
    config$rss_growth_tolerance_bytes,
    min(values) * config$rss_growth_tolerance_fraction
  )
  list(
    evaluable = TRUE,
    pass = observed_growth <= tolerance,
    medians = medians,
    observed_growth_bytes = observed_growth,
    tolerance_bytes = tolerance,
    rule = paste(
      "Maximum minus minimum median process peak RSS must be <=",
      "max(32 MiB, 20% of the smallest median) across isolated",
      "real Delta Kernel scan subprocesses."
    )
  )
}

pe_gate <- function(id, requirement, status, evidence, threshold, reason = NULL) {
  list(
    id = id,
    requirement = requirement,
    status = status,
    threshold = threshold,
    evidence = evidence,
    reason = reason
  )
}

pe_evaluate_gates <- function(comparisons, rss_scaling, provenance, config) {
  rss_status <- if (!isTRUE(rss_scaling$evaluable)) {
    "not_evaluable"
  } else if (isTRUE(rss_scaling$pass)) {
    "pass"
  } else {
    "fail"
  }
  throughput_status <- if (!identical(config$mode, "standard")) {
    "not_evaluable"
  } else if (comparisons$median_throughput_ratio >= 0.90) {
    "pass"
  } else {
    "fail"
  }
  ffi_evaluable <-
    comparisons$r_observed_maximum_batch_rows >= 65536 &&
      comparisons$rust_observed_maximum_batch_rows >= 65536
  ffi_status <- if (!ffi_evaluable) {
    "not_evaluable"
  } else if (comparisons$median_total_time_overhead_fraction < 0.02) {
    "pass"
  } else {
    "fail"
  }
  ffi_reason <- if (!ffi_evaluable) {
    paste(
      "The 64K+ batch precondition was not met. Observed maximums were",
      sprintf(
        "%d R rows and %d Rust rows per batch.",
        as.integer(comparisons$r_observed_maximum_batch_rows),
        as.integer(comparisons$rust_observed_maximum_batch_rows)
      )
    )
  } else {
    NULL
  }
  list(
    pe_gate(
      "comparator-provenance",
      "R and Rust samples resolve to one clean checkout and exact fixture.",
      if (isTRUE(provenance$pass)) "pass" else "fail",
      provenance,
      "base artifact commit = current commit; checkout clean"
    ),
    pe_gate(
      "kernel-process-rss-scaling",
      "Kernel scan peak RSS remains approximately flat as table rows increase.",
      rss_status,
      rss_scaling,
      paste(
        "<= max(32 MiB, 20% of smallest median) growth;",
        "separate subprocess for every sample"
      ),
      if (!isTRUE(rss_scaling$evaluable)) rss_scaling$reason else NULL
    ),
    pe_gate(
      "NFR-PERF-02-ffi-overhead",
      "R/FFI overhead for 64K+ batches is below the Rust-only scan.",
      ffi_status,
      comparisons,
      "< 2% overhead versus the same Rust-only 64K+ scan",
      ffi_reason
    ),
    pe_gate(
      "NFR-PERF-03-throughput",
      "R Arrow stream throughput reaches the Rust-only Kernel baseline.",
      throughput_status,
      comparisons,
      ">= 90% of Rust-only Delta Kernel on the same machine and fixture",
      if (!identical(config$mode, "standard")) {
        "Quick mode is an instrumentation smoke test; standard mode is required."
      } else {
        NULL
      }
    ),
    pe_gate(
      "NFR-PERF-04-peak-rss",
      "Streaming RSS is bounded by in-flight batches plus fixed overhead.",
      rss_status,
      rss_scaling,
      "bounded process peak RSS as real Kernel table size grows",
      if (!isTRUE(rss_scaling$evaluable)) rss_scaling$reason else NULL
    ),
    pe_gate(
      "ADR-003-rust-scope-exception",
      "A representative end-to-end result justifies expanding Rust scope.",
      "not_evaluable",
      list(
        comparator = comparisons,
        nfr_perf_02 = "not_evaluable",
        nfr_perf_03 = throughput_status,
        nfr_perf_04 = rss_status
      ),
      ">= 25% wall-time improvement or >= 50% peak-RSS reduction",
      paste(
        "This benchmark executable calls the already-shipped Kernel adapter;",
        "it is not an alternative implementation. The separately documented",
        "disposable coalescing prototype is not part of this durable artifact",
        "and does not authorize moving any R-owned behavior into Rust."
      )
    )
  )
}

pe_validate_artifact <- function(artifact) {
  required <- c(
    "schema_version",
    "environment",
    "base_artifact",
    "configuration",
    "source_identity",
    "measurements",
    "comparisons",
    "gates",
    "limitations"
  )
  if (!is.list(artifact) ||
      length(setdiff(required, names(artifact))) > 0L ||
      !identical(artifact$schema_version, 1L)) {
    pe_abort("Performance evidence artifact is invalid.")
  }
  if (!is.list(artifact$gates) || length(artifact$gates) == 0L) {
    pe_abort("Performance evidence artifact must contain gates.")
  }
  ids <- vapply(artifact$gates, function(gate) {
    if (!is.list(gate) ||
        !pe_scalar_character(gate$id) ||
        !gate$status %in% c("pass", "fail", "not_evaluable")) {
      pe_abort("Performance evidence artifact contains an invalid gate.")
    }
    gate$id
  }, character(1))
  if (anyDuplicated(ids)) {
    pe_abort("Performance evidence artifact contains duplicate gate IDs.")
  }
  invisible(artifact)
}

pe_write_artifact <- function(artifact, path) {
  pe_validate_artifact(artifact)
  parent <- dirname(path)
  if (!dir.exists(parent) && !dir.create(parent, recursive = TRUE)) {
    pe_abort("Could not create evidence output directory.")
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
  parsed <- jsonlite::read_json(temporary, simplifyVector = FALSE)
  parsed$schema_version <- as.integer(parsed$schema_version)
  pe_validate_artifact(parsed)
  if (!file.rename(temporary, path)) {
    pe_abort("Could not publish performance evidence artifact.")
  }
  invisible(normalizePath(path, winslash = "/", mustWork = TRUE))
}

pe_run <- function(repo_root, base_path, config, output) {
  base <- ph_read_artifact(base_path)
  base_kernel <- pe_kernel_samples(base)
  current_commit <- pe_git_value(repo_root, c("rev-parse", "HEAD"))
  current_dirty <- nzchar(pe_git_value(repo_root, c("status", "--porcelain")))
  provenance <- list(
    pass = identical(base$environment$git_commit, current_commit) &&
      !isTRUE(base$environment$git_dirty) &&
      !current_dirty,
    base_commit = base$environment$git_commit,
    current_commit = current_commit,
    base_dirty = base$environment$git_dirty,
    current_dirty = current_dirty
  )
  rust_tools <- pe_build_rust_tools(repo_root)
  comparable <- pe_benchmark_comparable_kernel(
    repo_root,
    rust_tools,
    config
  )
  rss <- pe_benchmark_peak_rss(repo_root, rust_tools, config)
  comparisons <- pe_comparisons(comparable)
  rss_scaling <- pe_rss_scaling(rss, config)
  artifact <- list(
    schema_version = 1L,
    environment = list(
      captured_at_utc = format(
        Sys.time(),
        "%Y-%m-%dT%H:%M:%OS6Z",
        tz = "UTC"
      ),
      git_commit = current_commit,
      git_branch = pe_git_value(
        repo_root,
        c("branch", "--show-current")
      ),
      git_dirty = current_dirty,
      r_version = R.version.string,
      r_platform = R.version$platform,
      rustc = ph_command_output("rustc", "--version"),
      cargo = ph_command_output("cargo", "--version"),
      peak_rss_backend = rss$backend
    ),
    base_artifact = list(
      path = normalizePath(base_path, winslash = "/", mustWork = TRUE),
      md5 = unname(tools::md5sum(base_path)),
      environment = base$environment,
      configuration = base$configuration
    ),
    configuration = config,
    source_identity = pe_source_identity(repo_root),
    measurements = list(
      base_seven_row_r_kernel = base_kernel$samples,
      generated_kernel_comparison = comparable,
      rust_tools = rust_tools,
      process_peak_rss = rss
    ),
    comparisons = comparisons,
    gates = pe_evaluate_gates(
      comparisons,
      rss_scaling,
      provenance,
      config
    ),
    limitations = c(
      "The checked-in seven-row Delta fixture remains correctness evidence; performance comparisons use a deterministic generated table.",
      "R and Rust samples alternate order but still run sequentially; scheduler, thermal, and filesystem-cache noise remain.",
      "Darwin `/usr/bin/time -l` reports bytes; GNU time `-v` reports KiB and is converted to bytes.",
      "Peak RSS includes R startup, loaded package/native libraries, Delta Kernel, Arrow, allocators, and the workload; each sample uses a fresh subprocess.",
      "Delta Kernel 0.26 can emit 64K Parquet input batches; the evidence gate remains not evaluable whenever either the R or direct Rust comparator observes a smaller maximum batch.",
      "Generated tables cover a deterministic four-column local Parquet scan; cloud/object-store, wide/nested, deletion-vector, and CDF workloads remain release evidence.",
      "Windows and non-GNU Linux time backends are explicitly unavailable rather than assigned guessed units.",
      "No alternative Rust implementation is benchmarked, so this evidence cannot authorize ADR 003 scope expansion."
    )
  )
  pe_write_artifact(artifact, output)
  artifact
}
