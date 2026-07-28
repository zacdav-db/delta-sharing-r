.pagination_page_limit <- 10000L

.normalize_page_token <- function(token) {
  if (is.null(token) || identical(token, "")) {
    return(NULL)
  }

  if (!.is_scalar_character(token)) {
    .abort_delta_sharing(
      "The server returned an invalid pagination token.",
      type = "protocol",
      operation = "paginate"
    )
  }

  token
}

.collect_pages <- function(fetch_page,
                           item_field = "items",
                           token_field = "nextPageToken",
                           max_pages = .pagination_page_limit) {
  if (!is.function(fetch_page)) {
    stop("`fetch_page` must be a function.", call. = FALSE)
  }
  if (!.is_scalar_character(item_field) ||
      !.is_scalar_character(token_field)) {
    stop("Pagination field names must be non-empty strings.", call. = FALSE)
  }
  if (!is.numeric(max_pages) ||
      length(max_pages) != 1L ||
      is.na(max_pages) ||
      !is.finite(max_pages) ||
      max_pages < 1 ||
      max_pages != floor(max_pages) ||
      max_pages > .Machine$integer.max) {
    stop("`max_pages` must be one positive whole number.", call. = FALSE)
  }

  token <- NULL
  seen_tokens <- new.env(parent = emptyenv(), hash = TRUE)
  pages <- vector("list", min(as.integer(max_pages), 64L))
  page_count <- 0L

  repeat {
    if (page_count >= max_pages) {
      .abort_delta_sharing(
        "Pagination exceeded the internal page limit.",
        type = "protocol",
        operation = "paginate"
      )
    }

    page <- fetch_page(token)
    if (!is.list(page) || (length(page) > 0L && is.null(names(page)))) {
      .abort_delta_sharing(
        "The server returned an invalid discovery page.",
        type = "protocol",
        operation = "paginate"
      )
    }

    items <- page[[item_field]]
    if (is.null(items)) {
      items <- list()
    }
    if (!is.list(items)) {
      .abort_delta_sharing(
        "The server returned invalid discovery items.",
        type = "protocol",
        operation = "paginate"
      )
    }

    page_count <- page_count + 1L
    if (page_count > length(pages)) {
      length(pages) <- min(length(pages) * 2L, as.integer(max_pages))
    }
    pages[[page_count]] <- items

    token <- .normalize_page_token(page[[token_field]])
    if (is.null(token)) {
      break
    }
    if (exists(token, envir = seen_tokens, inherits = FALSE)) {
      .abort_delta_sharing(
        "The server repeated a pagination token.",
        type = "protocol",
        operation = "paginate"
      )
    }
    assign(token, TRUE, envir = seen_tokens)
  }

  unlist(pages[seq_len(page_count)], recursive = FALSE, use.names = FALSE)
}
