sanitizer_library <- Sys.getenv(
  "DELTA_SHARING_SANITIZER_LIBRARY",
  unset = NA_character_
)
fixture <- Sys.getenv("DELTA_SHARING_SANITIZER_FIXTURE", unset = NA_character_)
iterations_text <- Sys.getenv(
  "DELTA_SHARING_SANITIZER_ITERATIONS",
  unset = "16"
)

if (is.na(sanitizer_library) || !fs::dir_exists(sanitizer_library)) {
  stop("DELTA_SHARING_SANITIZER_LIBRARY must name the installed test library.")
}
if (is.na(fixture) || !fs::dir_exists(fixture)) {
  stop("DELTA_SHARING_SANITIZER_FIXTURE must name the local Delta fixture.")
}
iterations <- suppressWarnings(as.integer(iterations_text))
if (is.na(iterations) || iterations < 1L || iterations > 1000L) {
  stop("DELTA_SHARING_SANITIZER_ITERATIONS must be between 1 and 1000.")
}

sanitizer_library <- as.character(fs::path_real(sanitizer_library))
fixture <- as.character(fs::path_real(fixture))
.libPaths(c(sanitizer_library, .libPaths()))
suppressPackageStartupMessages(library(delta.sharing))

installed_path <- as.character(fs::path_real(find.package("delta.sharing")))
if (!identical(as.character(fs::path_dir(installed_path)), sanitizer_library)) {
  stop("The sanitizer gate did not load delta.sharing from its test library.")
}

assert_true <- function(value, message) {
  if (!isTRUE(value)) {
    stop(message, call. = FALSE)
  }
}

assert_identical <- function(value, expected, message) {
  if (!identical(value, expected)) {
    stop(
      paste0(
        message,
        "\nExpected: ",
        paste(capture.output(str(expected)), collapse = " "),
        "\nActual: ",
        paste(capture.output(str(value)), collapse = " ")
      ),
      call. = FALSE
    )
  }
}

native_test_stream <- delta.sharing:::native_test_stream
native_snapshot_stream <- delta.sharing:::native_snapshot_stream
materialize_data_frame <- delta.sharing:::sharing_stream_to_data_frame
native_reap_pending_cleanups <-
  delta.sharing:::native_reap_pending_cleanups

invisible(native_reap_pending_cleanups())
invisible(gc())
assert_identical(
  native_reap_pending_cleanups(),
  0,
  "The installed sanitizer process must start without pending cleanups."
)

assert_released <- function(label) {
  invisible(gc())
  pending <- native_reap_pending_cleanups()
  invisible(gc())
  assert_identical(
    pending,
    0,
    paste0(label, " left a prepared-log cleanup pending.")
  )
}

for (index in seq_len(iterations)) {
  explicit <- native_test_stream(batches = 4L, rows_per_batch = 8L)
  assert_identical(
    explicit$get_next()$length,
    8L,
    "The synthetic stream returned an unexpected batch."
  )
  explicit$release()
  assert_released(paste0("synthetic explicit release iteration ", index))

  exhausted <- native_test_stream(batches = 4L, rows_per_batch = 8L)
  data <- materialize_data_frame(exhausted)
  assert_identical(
    nrow(data),
    32L,
    "nanoarrow did not materialize every synthetic row."
  )
  exhausted$release()
  assert_true(
    !nanoarrow::nanoarrow_pointer_is_valid(exhausted),
    "Explicit release left an exhausted synthetic stream valid."
  )
  assert_released(paste0("synthetic exhaustion iteration ", index))

  failed <- native_test_stream(
    batches = 4L,
    rows_per_batch = 8L,
    error_after = 1L
  )
  invisible(failed$get_next())
  condition <- tryCatch(failed$get_next(), error = identity)
  assert_true(
    inherits(condition, "delta_sharing_kernel_error"),
    "A native pull failure did not cross the installed typed-error boundary."
  )
  assert_true(
    !nanoarrow::nanoarrow_pointer_is_valid(failed),
    "A failed synthetic pull left its stream valid."
  )
  assert_released(paste0("synthetic failure iteration ", index))

  local({
    abandoned <- native_test_stream(batches = 4L, rows_per_batch = 8L)
    invisible(abandoned$get_next())
  })
  assert_released(paste0("synthetic finalizer iteration ", index))

  snapshot <- native_snapshot_stream(
    fixture,
    columns = c("id", "active"),
    batch_size = 1L
  )
  assert_identical(
    snapshot$get_next()$length,
    1L,
    "Delta Kernel did not return the requested first snapshot batch."
  )
  snapshot$release()
  assert_released(paste0("Kernel early release iteration ", index))

  snapshot <- native_snapshot_stream(
    fixture,
    columns = c("id", "active"),
    batch_size = 2L
  )
  data <- materialize_data_frame(snapshot)
  assert_identical(
    nrow(data),
    7L,
    "Delta Kernel snapshot exhaustion returned an unexpected row count."
  )
  assert_identical(
    names(data),
    c("id", "active"),
    "Delta Kernel snapshot exhaustion returned an unexpected schema."
  )
  snapshot$release()
  assert_released(paste0("Kernel exhaustion iteration ", index))
}

panic_stream <- native_test_stream(batches = 2L, panic_after = 0L)
panic_condition <- tryCatch(panic_stream$get_next(), error = identity)
assert_true(
  inherits(panic_condition, "delta_sharing_kernel_error"),
  "A contained Rust panic did not cross the installed typed-error boundary."
)
assert_true(
  !nanoarrow::nanoarrow_pointer_is_valid(panic_stream),
  "A contained Rust panic left its stream valid."
)
assert_released("contained Rust panic")

cat(
  sprintf(
    paste0(
      "Installed sanitizer lifecycle gate passed: %d iterations, ",
      "including explicit release, exhaustion, error, finalizer, and cleanup.\n"
    ),
    iterations
  )
)
