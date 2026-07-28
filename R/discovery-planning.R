.discovery_operations <- c(
  shares = "list_shares",
  schemas = "list_schemas",
  tables = "list_tables",
  all_tables = "list_tables"
)

.discovery_identifier <- function(value, name, operation) {
  if (!.is_scalar_character(value) ||
      grepl("[[:cntrl:]]", value)) {
    .abort_delta_sharing(
      sprintf("`%s` must be one non-empty name without control characters.", name),
      type = "validation",
      operation = operation
    )
  }
  value
}

.encode_discovery_segment <- function(value, name, operation) {
  value <- .discovery_identifier(value, name, operation)
  utils::URLencode(
    enc2utf8(value),
    reserved = TRUE,
    repeated = TRUE
  )
}

.discovery_route_segments <- function(resource,
                                      share = NULL,
                                      schema = NULL) {
  if (!.is_scalar_character(resource) ||
      !resource %in% names(.discovery_operations)) {
    stop("Unknown discovery resource.", call. = FALSE)
  }
  operation <- unname(.discovery_operations[[resource]])

  if (identical(resource, "shares")) {
    if (!is.null(share) || !is.null(schema)) {
      stop("Share and schema filters do not apply to the shares route.", call. = FALSE)
    }
    return("shares")
  }

  if (is.null(share)) {
    .abort_delta_sharing(
      "`share` is required to construct this discovery route.",
      type = "validation",
      operation = operation
    )
  }
  share <- .discovery_identifier(share, "share", operation)

  if (identical(resource, "schemas")) {
    if (!is.null(schema)) {
      stop("A schema filter does not apply to the schemas route.", call. = FALSE)
    }
    return(c("shares", share, "schemas"))
  }

  if (identical(resource, "all_tables")) {
    if (!is.null(schema)) {
      stop("A schema filter does not apply to the all-tables route.", call. = FALSE)
    }
    return(c("shares", share, "all-tables"))
  }

  if (is.null(schema)) {
    .abort_delta_sharing(
      "`schema` is required to construct the tables route.",
      type = "validation",
      operation = operation
    )
  }
  schema <- .discovery_identifier(schema, "schema", operation)
  c("shares", share, "schemas", schema, "tables")
}

.discovery_route <- function(resource, share = NULL, schema = NULL) {
  segments <- .discovery_route_segments(
    resource,
    share = share,
    schema = schema
  )
  operation <- unname(.discovery_operations[[resource]])
  encoded <- vapply(seq_along(segments), function(index) {
    name <- if (identical(segments[[index]], share)) {
      "share"
    } else if (identical(segments[[index]], schema)) {
      "schema"
    } else {
      "route"
    }
    .encode_discovery_segment(segments[[index]], name, operation)
  }, character(1))
  paste0("/", paste(encoded, collapse = "/"))
}

.empty_discovery_routes <- function() {
  data.frame(
    operation = character(),
    share = character(),
    schema = character(),
    path_segments = I(vector("list", 0L)),
    stringsAsFactors = FALSE
  )
}

.new_discovery_route_record <- function(operation,
                                        path_segments,
                                        share = NA_character_,
                                        schema = NA_character_) {
  data.frame(
    operation = operation,
    share = share,
    schema = schema,
    path_segments = I(list(path_segments)),
    stringsAsFactors = FALSE
  )
}

.validate_discovered_shares <- function(shares, operation) {
  if (!is.data.frame(shares) ||
      !"name" %in% names(shares) ||
      !is.character(shares$name) ||
      anyNA(shares$name) ||
      any(!nzchar(shares$name)) ||
      anyDuplicated(shares$name) ||
      any(vapply(shares$name, function(value) {
        grepl("[[:cntrl:]]", value)
      }, logical(1)))) {
    .abort_delta_sharing(
      "Share discovery did not return valid names for fan-out.",
      type = "protocol",
      operation = operation
    )
  }
  shares
}

.plan_schema_routes <- function(share = NULL, shares = NULL) {
  if (!is.null(share)) {
    share <- .discovery_identifier(share, "share", "list_schemas")
    return(.new_discovery_route_record(
      operation = "list_schemas",
      share = share,
      path_segments = .discovery_route_segments("schemas", share = share)
    ))
  }

  shares <- .validate_discovered_shares(shares, "list_schemas")
  if (nrow(shares) == 0L) {
    return(.empty_discovery_routes())
  }
  do.call(
    rbind,
    lapply(shares$name, function(name) {
      .new_discovery_route_record(
        operation = "list_schemas",
        share = name,
        path_segments = .discovery_route_segments("schemas", share = name)
      )
    })
  )
}

.plan_table_routes <- function(share = NULL,
                               schema = NULL,
                               shares = NULL) {
  if (!is.null(schema) && is.null(share)) {
    .abort_delta_sharing(
      "`schema` cannot be supplied without `share`.",
      type = "validation",
      operation = "list_tables"
    )
  }

  if (!is.null(share)) {
    share <- .discovery_identifier(share, "share", "list_tables")
    if (is.null(schema)) {
      return(.new_discovery_route_record(
        operation = "list_tables",
        share = share,
        path_segments = .discovery_route_segments(
          "all_tables",
          share = share
        )
      ))
    }
    schema <- .discovery_identifier(schema, "schema", "list_tables")
    return(.new_discovery_route_record(
      operation = "list_tables",
      share = share,
      schema = schema,
      path_segments = .discovery_route_segments(
        "tables",
        share = share,
        schema = schema
      )
    ))
  }

  shares <- .validate_discovered_shares(shares, "list_tables")
  if (nrow(shares) == 0L) {
    return(.empty_discovery_routes())
  }
  do.call(
    rbind,
    lapply(shares$name, function(name) {
      .new_discovery_route_record(
        operation = "list_tables",
        share = name,
        path_segments = .discovery_route_segments(
          "all_tables",
          share = name
        )
      )
    })
  )
}

.discovery_record_object <- function(record, operation) {
  if (!is.list(record) ||
      is.null(names(record)) ||
      anyNA(names(record)) ||
      any(!nzchar(names(record))) ||
      anyDuplicated(names(record))) {
    .abort_delta_sharing(
      "The server returned an invalid discovery record.",
      type = "protocol",
      operation = operation
    )
  }
  record
}

.discovery_record_text <- function(record,
                                   name,
                                   operation,
                                   required = FALSE) {
  if (!name %in% names(record) || is.null(record[[name]])) {
    if (required) {
      .abort_delta_sharing(
        "The server returned an incomplete discovery record.",
        type = "protocol",
        operation = operation
      )
    }
    return(NA_character_)
  }
  value <- record[[name]]
  if (!is.character(value) ||
      length(value) != 1L ||
      is.na(value) ||
      (required && !nzchar(value)) ||
      grepl("[[:cntrl:]]", value)) {
    .abort_delta_sharing(
      "The server returned an invalid discovery record.",
      type = "protocol",
      operation = operation
    )
  }
  value
}

.discovery_record_text_array <- function(record, name, operation) {
  if (!name %in% names(record) || is.null(record[[name]])) {
    return(character())
  }
  value <- record[[name]]
  valid <- is.list(value) &&
    is.null(names(value)) &&
    all(vapply(value, function(element) {
      is.character(element) &&
        length(element) == 1L &&
        !is.na(element) &&
        !grepl("[[:cntrl:]]", element)
    }, logical(1)))
  if (!valid) {
    .abort_delta_sharing(
      "The server returned an invalid discovery record.",
      type = "protocol",
      operation = operation
    )
  }
  if (length(value) == 0L) character() else unlist(value, use.names = FALSE)
}

.empty_share_records <- function() {
  data.frame(
    name = character(),
    id = character(),
    display_name = character(),
    comment = character(),
    stringsAsFactors = FALSE
  )
}

.empty_schema_records <- function() {
  data.frame(
    share = character(),
    name = character(),
    stringsAsFactors = FALSE
  )
}

.empty_table_records <- function() {
  data.frame(
    share = character(),
    schema = character(),
    name = character(),
    share_id = character(),
    id = character(),
    access_modes = I(vector("list", 0L)),
    stringsAsFactors = FALSE
  )
}

.normalize_share_records <- function(records) {
  if (!is.list(records)) {
    .abort_delta_sharing(
      "The server returned invalid share records.",
      type = "protocol",
      operation = "list_shares"
    )
  }
  if (length(records) == 0L) {
    return(.empty_share_records())
  }

  rows <- lapply(records, function(record) {
    record <- .discovery_record_object(record, "list_shares")
    data.frame(
      name = .discovery_record_text(
        record,
        "name",
        "list_shares",
        required = TRUE
      ),
      id = .discovery_record_text(record, "id", "list_shares"),
      display_name = .discovery_record_text(
        record,
        "displayName",
        "list_shares"
      ),
      comment = .discovery_record_text(record, "comment", "list_shares"),
      stringsAsFactors = FALSE
    )
  })
  result <- do.call(rbind, rows)
  rownames(result) <- NULL
  result
}

.normalize_schema_records <- function(records, expected_share = NULL) {
  if (!is.list(records)) {
    .abort_delta_sharing(
      "The server returned invalid schema records.",
      type = "protocol",
      operation = "list_schemas"
    )
  }
  if (!is.null(expected_share)) {
    expected_share <- .discovery_identifier(
      expected_share,
      "share",
      "list_schemas"
    )
  }
  if (length(records) == 0L) {
    return(.empty_schema_records())
  }

  rows <- lapply(records, function(record) {
    record <- .discovery_record_object(record, "list_schemas")
    share <- .discovery_record_text(
      record,
      "share",
      "list_schemas",
      required = TRUE
    )
    if (!is.null(expected_share) && !identical(share, expected_share)) {
      .abort_delta_sharing(
        "The server returned a schema outside the requested share.",
        type = "protocol",
        operation = "list_schemas"
      )
    }
    data.frame(
      share = share,
      name = .discovery_record_text(
        record,
        "name",
        "list_schemas",
        required = TRUE
      ),
      stringsAsFactors = FALSE
    )
  })
  result <- do.call(rbind, rows)
  rownames(result) <- NULL
  result
}

.normalize_table_records <- function(records,
                                     expected_share = NULL,
                                     expected_schema = NULL) {
  if (!is.list(records)) {
    .abort_delta_sharing(
      "The server returned invalid table records.",
      type = "protocol",
      operation = "list_tables"
    )
  }
  if (!is.null(expected_share)) {
    expected_share <- .discovery_identifier(
      expected_share,
      "share",
      "list_tables"
    )
  }
  if (!is.null(expected_schema)) {
    expected_schema <- .discovery_identifier(
      expected_schema,
      "schema",
      "list_tables"
    )
  }
  if (length(records) == 0L) {
    return(.empty_table_records())
  }

  rows <- lapply(records, function(record) {
    record <- .discovery_record_object(record, "list_tables")
    share <- .discovery_record_text(
      record,
      "share",
      "list_tables",
      required = TRUE
    )
    schema <- .discovery_record_text(
      record,
      "schema",
      "list_tables",
      required = TRUE
    )
    if ((!is.null(expected_share) && !identical(share, expected_share)) ||
        (!is.null(expected_schema) && !identical(schema, expected_schema))) {
      .abort_delta_sharing(
        "The server returned a table outside the requested scope.",
        type = "protocol",
        operation = "list_tables"
      )
    }

    modes <- .discovery_record_text_array(
      record,
      "accessModes",
      "list_tables"
    )
    data.frame(
      share = share,
      schema = schema,
      name = .discovery_record_text(
        record,
        "name",
        "list_tables",
        required = TRUE
      ),
      share_id = .discovery_record_text(record, "shareId", "list_tables"),
      id = .discovery_record_text(record, "id", "list_tables"),
      access_modes = I(list(modes)),
      stringsAsFactors = FALSE
    )
  })
  result <- do.call(rbind, rows)
  rownames(result) <- NULL
  result
}

.safe_discovery_page <- function(fetch_page,
                                 path_segments,
                                 page_token,
                                 operation) {
  tryCatch(
    fetch_page(
      path_segments = path_segments,
      page_token = page_token
    ),
    error = function(condition) {
      if (inherits(condition, "delta_sharing_error")) {
        stop(condition)
      }
      .abort_delta_sharing(
        "The discovery page could not be obtained.",
        type = "protocol",
        operation = operation
      )
    }
  )
}

.collect_discovery_route <- function(fetch_page,
                                     route,
                                     normalizer) {
  if (!is.function(fetch_page) || !is.function(normalizer)) {
    stop("Discovery collection hooks must be functions.", call. = FALSE)
  }
  if (!is.data.frame(route) ||
      nrow(route) != 1L ||
      !all(c(
        "operation",
        "share",
        "schema",
        "path_segments"
      ) %in% names(route))) {
    stop("`route` must be one discovery route record.", call. = FALSE)
  }

  records <- .collect_pages(function(token) {
    .safe_discovery_page(
      fetch_page = fetch_page,
      path_segments = route$path_segments[[1L]],
      page_token = token,
      operation = route$operation[[1L]]
    )
  })
  normalizer(
    records,
    expected_share = if (is.na(route$share[[1L]])) {
      NULL
    } else {
      route$share[[1L]]
    },
    expected_schema = if (is.na(route$schema[[1L]])) {
      NULL
    } else {
      route$schema[[1L]]
    }
  )
}

.bind_discovery_frames <- function(frames, empty) {
  if (length(frames) == 0L) {
    return(empty())
  }
  result <- do.call(rbind, frames)
  rownames(result) <- NULL
  result
}

.collect_share_records <- function(fetch_page) {
  route <- .new_discovery_route_record(
    operation = "list_shares",
    path_segments = .discovery_route_segments("shares")
  )
  .collect_discovery_route(
    fetch_page,
    route,
    normalizer = function(records,
                          expected_share = NULL,
                          expected_schema = NULL) {
      .normalize_share_records(records)
    }
  )
}

.collect_schema_records <- function(fetch_page, share = NULL) {
  shares <- if (is.null(share)) {
    .collect_share_records(fetch_page)
  } else {
    NULL
  }
  routes <- .plan_schema_routes(share = share, shares = shares)
  frames <- lapply(seq_len(nrow(routes)), function(index) {
    .collect_discovery_route(
      fetch_page,
      routes[index, , drop = FALSE],
      normalizer = function(records,
                            expected_share = NULL,
                            expected_schema = NULL) {
        .normalize_schema_records(records, expected_share = expected_share)
      }
    )
  })
  .bind_discovery_frames(frames, .empty_schema_records)
}

.collect_table_records <- function(fetch_page,
                                   share = NULL,
                                   schema = NULL) {
  shares <- if (is.null(share)) {
    .collect_share_records(fetch_page)
  } else {
    NULL
  }
  routes <- .plan_table_routes(
    share = share,
    schema = schema,
    shares = shares
  )
  frames <- lapply(seq_len(nrow(routes)), function(index) {
    .collect_discovery_route(
      fetch_page,
      routes[index, , drop = FALSE],
      normalizer = .normalize_table_records
    )
  })
  .bind_discovery_frames(frames, .empty_table_records)
}
