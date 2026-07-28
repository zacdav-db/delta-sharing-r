security_snapshot_components <- function() {
  path <- test_path("fixtures", "protocol", "snapshot-delta.ndjson")
  bytes <- readBin(path, what = "raw", n = file.info(path)$size)
  decoder <- delta.sharing:::.new_ndjson_decoder("temp-root fixture")
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

security_parent <- function(prefix) {
  parent <- tempfile(prefix)
  dir.create(parent, mode = "0700")
  suppressWarnings(Sys.chmod(parent, mode = "0700"))
  normalizePath(parent, winslash = "/", mustWork = TRUE)
}

test_that("Unix temp parents require safe ownership and write modes", {
  skip_on_os("windows")
  parent <- security_parent("snapshot-parent-modes-")
  on.exit({
    Sys.chmod(parent, mode = "0700")
    unlink(parent, recursive = TRUE, force = TRUE)
  }, add = TRUE)

  expect_identical(
    delta.sharing:::.validate_snapshot_temp_parent(parent),
    parent
  )

  expect_true(isTRUE(Sys.chmod(
    parent,
    mode = "0770",
    use_umask = FALSE
  )))
  expect_error(
    delta.sharing:::.validate_snapshot_temp_parent(parent),
    "group/world write access",
    class = "delta_sharing_validation_error"
  )

  expect_true(isTRUE(Sys.chmod(
    parent,
    mode = "1777",
    use_umask = FALSE
  )))
  expect_identical(
    delta.sharing:::.validate_snapshot_temp_parent(parent),
    parent
  )

  session_uid <- unname(file.info(tempdir(), extra_cols = TRUE)$uid[[1L]])
  expect_false(delta.sharing:::.snapshot_temp_parent_mode_is_safe(
    session_uid + 1,
    session_uid,
    strtoi("1777", base = 8L)
  ))
  expect_true(delta.sharing:::.snapshot_temp_parent_mode_is_safe(
    0,
    session_uid,
    strtoi("1777", base = 8L)
  ))

  expect_true(isTRUE(Sys.chmod(parent, mode = "0700")))
  link <- tempfile("snapshot-parent-link-")
  on.exit(unlink(link, force = TRUE), add = TRUE)
  expect_true(file.symlink(parent, link))
  expect_error(
    delta.sharing:::.validate_snapshot_temp_parent(link),
    "non-symlink",
    class = "delta_sharing_validation_error"
  )
})

test_that("default private tempdir validation remains available", {
  expect_identical(
    delta.sharing:::.validate_snapshot_temp_parent(tempdir()),
    normalizePath(tempdir(), winslash = "/", mustWork = TRUE)
  )
})

test_that("published replacement roots and symlinks are never traversed", {
  skip_on_os("windows")
  components <- security_snapshot_components()
  parent <- security_parent("snapshot-replacement-parent-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    components$files,
    temp_parent = parent
  )
  root <- guard$state$root
  original <- paste0(root, "-original")
  expect_true(file.rename(root, original))
  expect_true(dir.create(root, mode = "0700"))
  expect_true(isTRUE(Sys.chmod(root, mode = "0700")))
  marker <- file.path(
    root,
    delta.sharing:::.snapshot_log_marker_name
  )
  delta.sharing:::.write_snapshot_commit(
    marker,
    delta.sharing:::.snapshot_log_marker_value
  )
  Sys.chmod(marker, mode = "0600")
  sentinel <- file.path(root, "replacement-must-remain")
  writeLines("not package-owned", sentinel)

  expect_false(delta.sharing:::.snapshot_temp_root_is_safe(
    root,
    guard$state$root_identity
  ))
  expect_error(
    delta.sharing:::.release_snapshot_log(guard),
    "could not be released",
    class = "delta_sharing_protocol_error"
  )
  expect_true(file.exists(sentinel))
  guard_state <- guard$state
  guard_state$released <- TRUE
  unlink(root, recursive = TRUE, force = TRUE)
  unlink(original, recursive = TRUE, force = TRUE)

  linked_guard <- delta.sharing:::.prepare_snapshot_log(
    components$protocol,
    components$metadata,
    components$files,
    temp_parent = parent
  )
  linked_root <- linked_guard$state$root
  linked_original <- paste0(linked_root, "-original")
  target <- file.path(parent, "unowned-target")
  expect_true(file.rename(linked_root, linked_original))
  expect_true(dir.create(target, mode = "0700"))
  target_sentinel <- file.path(target, "target-must-remain")
  writeLines("not package-owned", target_sentinel)
  expect_true(file.symlink(target, linked_root))

  expect_error(
    delta.sharing:::.release_snapshot_log(linked_guard),
    "could not be released",
    class = "delta_sharing_protocol_error"
  )
  expect_true(file.exists(target_sentinel))
  linked_guard_state <- linked_guard$state
  linked_guard_state$released <- TRUE
  unlink(linked_root, force = TRUE)
  unlink(linked_original, recursive = TRUE, force = TRUE)
})

test_that("staging replacement is refused by release and finalization", {
  skip_on_os("windows")
  parent <- security_parent("snapshot-stage-replacement-parent-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  abandoned <- local({
    stage <- delta.sharing:::.new_snapshot_stage(parent, run_files = 1L)
    state <- delta.sharing:::.snapshot_stage_state(stage)
    root <- state$root
    original <- paste0(root, "-original")
    expect_true(file.rename(root, original))
    expect_true(dir.create(root, mode = "0700"))
    Sys.chmod(root, mode = "0700")
    marker <- file.path(
      root,
      delta.sharing:::.snapshot_log_marker_name
    )
    delta.sharing:::.write_snapshot_commit(
      marker,
      delta.sharing:::.snapshot_log_marker_value
    )
    Sys.chmod(marker, mode = "0600")
    sentinel <- file.path(root, "replacement-must-remain")
    writeLines("not package-owned", sentinel)
    list(root = root, original = original, sentinel = sentinel)
  })

  gc()
  expect_true(file.exists(abandoned$sentinel))
  unlink(abandoned$root, recursive = TRUE, force = TRUE)
  unlink(abandoned$original, recursive = TRUE, force = TRUE)
})

test_that("published identity recording failure retains cleanup authority", {
  parent <- security_parent("snapshot-identity-transition-parent-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)
  private <- delta.sharing:::.snapshot_create_private_root(
    parent,
    delta.sharing:::.snapshot_log_abort,
    "injected root creation failure"
  )
  marker <- file.path(
    private$root,
    delta.sharing:::.snapshot_log_marker_name
  )
  delta.sharing:::.write_snapshot_commit(
    marker,
    delta.sharing:::.snapshot_log_marker_value
  )
  Sys.chmod(marker, mode = "0600")
  identity <- delta.sharing:::.snapshot_new_root_identity(
    private$root,
    "construction",
    delta.sharing:::.snapshot_log_abort
  )

  expect_error(
    delta.sharing:::.snapshot_publish_root_identity(
      private$root,
      identity,
      delta.sharing:::.snapshot_log_abort,
      record_identity = function(root, phase, abort) {
        stop("injected published identity failure")
      }
    ),
    "injected published identity failure"
  )
  expect_true(delta.sharing:::.snapshot_temp_root_is_safe(
    private$root,
    identity
  ))
  expect_true(delta.sharing:::.cleanup_snapshot_root(
    private$root,
    identity
  ))
  expect_false(file.exists(private$root))
})

test_that("Windows validation stays bounded pending hosted ACL proof", {
  skip_if(.Platform$OS.type != "windows")
  parent <- security_parent("snapshot-windows-parent-")
  on.exit(unlink(parent, recursive = TRUE, force = TRUE), add = TRUE)

  expect_identical(
    delta.sharing:::.validate_snapshot_temp_parent(parent),
    parent
  )
  file_parent <- tempfile("snapshot-windows-file-")
  writeLines("not a directory", file_parent)
  on.exit(unlink(file_parent, force = TRUE), add = TRUE)
  expect_error(
    delta.sharing:::.validate_snapshot_temp_parent(file_parent),
    class = "delta_sharing_validation_error"
  )
})
