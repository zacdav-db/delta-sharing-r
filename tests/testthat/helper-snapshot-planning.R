planned_snapshot_bytes <- function(name) {
  path <- test_path("fixtures", "protocol", name)
  readBin(path, what = "raw", n = file.info(path)$size)
}

planned_snapshot_headers <- function(
  version = "42",
  content_type = "application/x-ndjson; charset=utf-8",
  file_id_hash = "delta",
  capabilities =
    "responseformat=delta;includeendstreamaction=true"
) {
  headers <- c(
    "Content-Type" = content_type,
    "Delta-Table-Version" = version,
    fileidhash = file_id_hash
  )
  if (!is.null(capabilities)) {
    headers <- c(
      headers,
      "delta-sharing-capabilities" = capabilities
    )
  }
  headers
}

planned_pull_response <- function(bytes,
                                  headers = planned_snapshot_headers(),
                                  status = 200L,
                                  chunk_bytes = 17L,
                                  recorder = new.env(parent = emptyenv())) {
  stopifnot(is.raw(bytes))
  offset <- 1L
  recorder$pulls <- 0L
  recorder$closes <- 0L

  delta.sharing:::.new_snapshot_pull_response(
    status = status,
    headers = headers,
    pull = function() {
      recorder$pulls <- recorder$pulls + 1L
      if (offset > length(bytes)) {
        return(NULL)
      }
      end <- min(length(bytes), offset + chunk_bytes - 1L)
      chunk <- bytes[seq.int(offset, end)]
      offset <<- end + 1L
      chunk
    },
    close = function() {
      recorder$closes <- recorder$closes + 1L
      invisible(NULL)
    }
  )
}

planned_condition_text <- function(condition) {
  paste(
    conditionMessage(condition),
    capture.output(str(condition)),
    collapse = "\n"
  )
}
