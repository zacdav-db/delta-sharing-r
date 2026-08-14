# Run from the repository root:
# Rscript bench/snapshot.R /path/to/profile.share share.schema.table
#
# Each trial measures the same snapshot with an empty table file cache and then
# with its downloaded files cached. Profiles, shared data, and results remain
# outside the repository.

args <- commandArgs(trailingOnly = TRUE)
profile <- args[[1L]]
table_name <- args[[2L]]

devtools::load_all(quiet = TRUE)

table <- sharing_client(profile)$table(table_name)

time_snapshot <- function(limit, empty_cache) {
  if (empty_cache && fs::dir_exists(table$cache_path)) {
    fs::dir_delete(table$cache_path)
  }

  gc()
  result <- NULL
  elapsed <- system.time({
    result <- table$snapshot(limit = limit)$to_tibble()
  })[["elapsed"]]

  list(
    elapsed = unname(elapsed),
    rows = nrow(result),
    size_mib = as.numeric(object.size(result)) / 1024^2
  )
}

benchmark_limit <- function(limit) {
  trials <- purrr::map(seq_len(3L), function(trial) {
    message(
      "Rows: ",
      format(limit, big.mark = ",", scientific = FALSE),
      "; trial ",
      trial,
      " of 3"
    )
    list(
      cold = time_snapshot(limit, empty_cache = TRUE),
      cached = time_snapshot(limit, empty_cache = FALSE)
    )
  })

  cold <- purrr::map_dbl(trials, c("cold", "elapsed"))
  cached <- purrr::map_dbl(trials, c("cached", "elapsed"))

  tibble::tibble(
    rows = trials[[1L]]$cold$rows,
    size_mib = trials[[1L]]$cold$size_mib,
    cold_median = median(cold),
    cold_min = min(cold),
    cold_max = max(cold),
    cached_median = median(cached),
    cached_min = min(cached),
    cached_max = max(cached)
  )
}

results <- c(10^4, 10^6, 10^7) |>
  purrr::map(benchmark_limit) |>
  purrr::list_rbind()

print(results, n = Inf, width = Inf)
