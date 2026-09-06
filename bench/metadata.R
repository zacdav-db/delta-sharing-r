# Rscript bench/metadata.R [file_count] [URL_padding_bytes]
# Synthetic metadata only: no file downloads or native scans.
args <- commandArgs(trailingOnly = TRUE)
n <- if (length(args) >= 1L) as.integer(args[[1]]) else 20000L
padding <- if (length(args) >= 2L) as.integer(args[[2]]) else 1000L
stopifnot(!is.na(n), n > 0L, !is.na(padding), padding >= 0L)
parse <- getFromNamespace("parse_ndjson_lines", "delta.sharing")
suffix <- paste(rep("x", padding), collapse = "")
lines <- sprintf(
  '{"file":{"id":"%08d","url":"https://example.invalid/%08d?padding=%s","size":1}}',
  seq_len(n),
  seq_len(n),
  suffix
)
body <- paste(lines, collapse = "\n")
rm(lines)
invisible(gc())
profile <- tempfile()
Rprofmem(profile)
elapsed <- system.time(result <- parse(body, "benchmark"))[["elapsed"]]
Rprofmem(NULL)
allocations <- suppressWarnings(as.numeric(sub(" .*", "", readLines(profile))))
unlink(profile)
stopifnot(length(result) == n)
print(
  data.frame(
    files = n,
    body_MiB = nchar(body, type = "bytes") / 1024^2,
    elapsed_seconds = elapsed,
    allocated_MiB = sum(allocations, na.rm = TRUE) / 1024^2,
    result_MiB = as.numeric(object.size(result)) / 1024^2
  ),
  row.names = FALSE
)
