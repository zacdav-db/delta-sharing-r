# Run from the package root after installing delta.sharing. No downloads.
# Compare the former named-list lookup with the indexed lookup, including
# index construction in the measured time. Outputs must match exactly.
rewrite <- getFromNamespace("rewrite_staged_file", "delta.sharing")
asset <- getFromNamespace("staged_asset", "delta.sharing")
sizes <- c(5000L, 10000L, 20000L)
results <- lapply(sizes, function(n) {
  ids <- sprintf("file-%08d", seq_len(n))
  files <- lapply(ids, function(id) {
    list(id = id, deltaSingleAction = list(add = list(path = "unused")))
  })
  paths <- setNames(
    as.list(paste0("file:///staged/", ids)),
    vapply(ids, function(id) asset("data", id, "unused")$name, character(1))
  )
  rewrite_all <- function(indexed) {
    lookup <- if (indexed) {
      list2env(paths, hash = TRUE, parent = emptyenv())
    } else {
      paths
    }
    lapply(
      files,
      rewrite,
      response_format = "delta",
      operation = "read",
      paths = lookup
    )
  }
  stopifnot(identical(rewrite_all(FALSE), rewrite_all(TRUE)))
  elapsed <- function(indexed) {
    median(replicate(3L, system.time(rewrite_all(indexed))[["elapsed"]]))
  }
  data.frame(
    files = n,
    list_seconds = elapsed(FALSE),
    index_seconds = elapsed(TRUE)
  )
})
print(do.call(rbind, results), row.names = FALSE)
