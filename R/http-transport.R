.http_buffer_limits <- c(
  discovery = 8 * 1024^2,
  metadata = 16 * 1024^2
)
.http_read_chunk_bytes <- 65536L
.http_control_operations <- c(
  "list_shares",
  "list_schemas",
  "list_tables",
  "table_version",
  "table_protocol",
  "table_metadata",
  "table_schema",
  "read_schema"
)
.http_methods <- c("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE")
.http_reserved_headers <- c(
  "authorization",
  "connection",
  "content-length",
  "cookie",
  "host",
  "proxy-authorization",
  "transfer-encoding"
)

.http_validation_abort <- function(message) {
  .abort_delta_sharing(
    message,
    type = "validation",
    operation = "http_request"
  )
}

.normalize_http_method <- function(method) {
  if (!.is_scalar_character(method)) {
    .http_validation_abort("The HTTP method is invalid.")
  }
  method <- toupper(method)
  if (!method %in% .http_methods) {
    .http_validation_abort("The HTTP method is not supported.")
  }
  method
}

.normalize_http_path <- function(path) {
  if (!is.character(path) ||
      length(path) == 0L ||
      anyNA(path) ||
      any(!nzchar(path)) ||
      any(path %in% c(".", "..")) ||
      any(grepl("[[:cntrl:]]", path))) {
    .http_validation_abort(
      "The relative HTTP path must contain safe non-empty segments."
    )
  }

  encoded <- vapply(
    enc2utf8(path),
    utils::URLencode,
    character(1),
    reserved = TRUE,
    repeated = TRUE,
    USE.NAMES = FALSE
  )
  if (any(!nzchar(encoded))) {
    .http_validation_abort(
      "The relative HTTP path must contain safe non-empty segments."
    )
  }
  encoded
}

.validate_named_http_fields <- function(fields,
                                        kind,
                                        allow_vectors = FALSE) {
  if (is.null(fields)) {
    return(list())
  }
  if (is.list(fields) && length(fields) == 0L) {
    return(list())
  }
  if (!is.list(fields) ||
      is.null(names(fields)) ||
      anyNA(names(fields)) ||
      any(!nzchar(names(fields))) ||
      any(startsWith(names(fields), ".")) ||
      anyDuplicated(names(fields))) {
    .http_validation_abort(
      sprintf("HTTP %s fields must be a uniquely named list.", kind)
    )
  }

  valid_value <- function(value) {
    if (is.null(value)) {
      return(TRUE)
    }
    if (!is.atomic(value) ||
        is.raw(value) ||
        is.factor(value) ||
        length(value) == 0L ||
        (!allow_vectors && length(value) != 1L) ||
        anyNA(value)) {
      return(FALSE)
    }
    if (is.numeric(value) && any(!is.finite(value))) {
      return(FALSE)
    }
    is.character(value) || is.numeric(value) || is.logical(value)
  }
  if (any(!vapply(fields, valid_value, logical(1)))) {
    .http_validation_abort(
      sprintf("HTTP %s contains an invalid value.", kind)
    )
  }
  fields
}

.normalize_http_headers <- function(headers) {
  headers <- .validate_named_http_fields(headers, "headers")
  if (length(headers) == 0L) {
    return(character())
  }
  if (any(!grepl("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$", names(headers))) ||
      any(tolower(names(headers)) %in% .http_reserved_headers)) {
    .http_validation_abort("HTTP headers contain a reserved or invalid name.")
  }

  values <- vapply(headers, as.character, character(1))
  if (any(grepl("[\r\n]", values))) {
    .http_validation_abort("HTTP headers contain an invalid value.")
  }
  values
}

.normalize_http_json <- function(json) {
  if (is.null(json)) {
    return(NULL)
  }
  if (!is.list(json) ||
      is.null(names(json)) ||
      anyNA(names(json)) ||
      any(!nzchar(names(json))) ||
      anyDuplicated(names(json))) {
    .http_validation_abort(
      "The JSON request body must be one uniquely named object."
    )
  }
  json
}

.normalize_http_buffer_limit <- function(response_kind,
                                         max_response_bytes = NULL) {
  if (!.is_scalar_character(response_kind) ||
      !response_kind %in% names(.http_buffer_limits)) {
    .http_validation_abort(
      "Buffered HTTP responses are limited to discovery and metadata."
    )
  }
  configured <- unname(.http_buffer_limits[[response_kind]])
  if (is.null(max_response_bytes)) {
    return(configured)
  }
  if (!is.numeric(max_response_bytes) ||
      length(max_response_bytes) != 1L ||
      is.na(max_response_bytes) ||
      !is.finite(max_response_bytes) ||
      max_response_bytes < 1 ||
      max_response_bytes != floor(max_response_bytes) ||
      max_response_bytes > configured) {
    .http_validation_abort(
      "The buffered HTTP response limit is invalid."
    )
  }
  as.double(max_response_bytes)
}

.new_client_http_request <- function(client,
                                     method,
                                     path,
                                     query = NULL,
                                     headers = NULL,
                                     form = NULL,
                                     json = NULL,
                                     response_kind,
                                     max_response_bytes = NULL) {
  context <- .client_context(client)
  method <- .normalize_http_method(method)
  path <- .normalize_http_path(path)
  query <- .validate_named_http_fields(
    query,
    "query",
    allow_vectors = TRUE
  )
  headers <- .normalize_http_headers(headers)
  form <- .validate_named_http_fields(form, "form")
  json <- .normalize_http_json(json)
  if (!is.null(form) && length(form) == 0L) {
    form <- NULL
  }
  if (!is.null(form) && !is.null(json)) {
    .http_validation_abort(
      "Supply at most one of a form body or JSON body."
    )
  }
  if (method %in% c("GET", "HEAD") &&
      (!is.null(form) || !is.null(json))) {
    .http_validation_abort("GET and HEAD requests cannot contain a body.")
  }

  body_type <- if (!is.null(form)) {
    "form"
  } else if (!is.null(json)) {
    "json"
  } else {
    "none"
  }
  body <- if (!is.null(form)) form else json
  endpoint <- sub("/+$", "", context$endpoint)

  list(
    method = method,
    url = paste(c(endpoint, path), collapse = "/"),
    query = query,
    headers = headers,
    body_type = body_type,
    body = body,
    max_response_bytes = .normalize_http_buffer_limit(
      response_kind,
      max_response_bytes
    )
  )
}

.apply_http_authorization <- function(request, authorization) {
  if (length(request$headers) > 0L) {
    request$headers <- request$headers[
      tolower(names(request$headers)) != "authorization"
    ]
  }
  request$headers <- c(request$headers, authorization$headers)
  request
}

.normalize_http_transport <- function(transport) {
  hooks <- c("send", "status", "headers", "body")
  values <- lapply(hooks, function(name) {
    if (is.list(transport)) {
      transport[[name, exact = TRUE]]
    } else {
      NULL
    }
  })
  names(values) <- hooks
  retry_after <- if (is.list(transport)) {
    transport[["retry_after", exact = TRUE]]
  } else {
    NULL
  }
  if (!is.list(transport) ||
      is.null(names(transport)) ||
      any(!vapply(values, is.function, logical(1))) ||
      (!is.null(retry_after) && !is.function(retry_after))) {
    stop(
      paste0(
        "`transport` must provide `send`, `status`, `headers`, and `body` ",
        "functions plus an optional `retry_after` function."
      ),
      call. = FALSE
    )
  }
  if (is.null(retry_after)) {
    retry_after <- function(response) {
      .http_header(values$headers(response), "retry-after")
    }
  }
  c(values, list(retry_after = retry_after))
}

.http_header <- function(headers, name) {
  if (is.null(headers) || is.null(names(headers))) {
    return(NULL)
  }
  index <- which(tolower(names(headers)) == tolower(name))
  if (length(index) != 1L) {
    return(NULL)
  }
  value <- headers[[index]]
  if (!.is_scalar_character(value) || grepl("[\r\n]", value)) {
    return(NULL)
  }
  value
}

.as_bounded_raw_body <- function(body, max_bytes) {
  if (is.character(body) && .is_scalar_character(body)) {
    body <- charToRaw(enc2utf8(body))
  }
  if (!is.raw(body) || length(body) > max_bytes) {
    stop("The buffered HTTP response is invalid or too large.", call. = FALSE)
  }
  body
}

.new_http_response <- function(status, headers = list(), body = raw()) {
  list(status = status, headers = headers, body = body)
}

.normalize_fake_http_response <- function(response, request) {
  if (!is.list(response)) {
    stop("The fake HTTP transport returned an invalid response.", call. = FALSE)
  }
  status <- response[["status"]]
  headers <- response[["headers"]]
  if (is.null(headers)) {
    headers <- list()
  }
  body <- response[["body"]]
  if (is.null(body) || (is.numeric(status) && status >= 400)) {
    body <- raw()
  } else if (is.list(body)) {
    body <- charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE, null = "null"))
  }
  if (is.numeric(status) &&
      length(status) == 1L &&
      !is.na(status) &&
      is.finite(status) &&
      status >= 100 &&
      status <= 399) {
    body <- .as_bounded_raw_body(body, request$max_response_bytes)
  } else {
    body <- raw()
  }
  .new_http_response(status, headers, body)
}

.fake_http_transport <- function(handler) {
  if (!is.function(handler)) {
    stop("`handler` must be a function.", call. = FALSE)
  }
  list(
    send = function(request) {
      .normalize_fake_http_response(handler(request), request)
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    body = function(response) response$body,
    retry_after = function(response) {
      .http_header(response$headers, "retry-after")
    }
  )
}

.encode_http_component <- function(value) {
  utils::URLencode(
    enc2utf8(as.character(value)),
    reserved = TRUE,
    repeated = TRUE
  )
}

.http_query_string <- function(query) {
  if (length(query) == 0L) {
    return("")
  }
  fields <- unlist(lapply(seq_along(query), function(index) {
    value <- query[[index]]
    if (is.null(value)) {
      return(character())
    }
    name <- .encode_http_component(names(query)[[index]])
    paste0(name, "=", vapply(
      value,
      .encode_http_component,
      character(1),
      USE.NAMES = FALSE
    ))
  }), use.names = FALSE)
  if (length(fields) == 0L) "" else paste(fields, collapse = "&")
}

.httr2_prepare_request <- function(request, timeout_seconds) {
  query <- .http_query_string(request$query)
  url <- if (nzchar(query)) {
    paste0(request$url, "?", query)
  } else {
    request$url
  }
  prepared <- httr2::request(url)
  prepared <- httr2::req_method(prepared, request$method)
  prepared <- httr2::req_timeout(prepared, timeout_seconds)
  prepared <- httr2::req_error(
    prepared,
    is_error = function(response) FALSE
  )
  if (length(request$headers) > 0L) {
    prepared <- do.call(
      httr2::req_headers_redacted,
      c(list(prepared), as.list(request$headers))
    )
  }
  if (identical(request$body_type, "form")) {
    prepared <- do.call(
      httr2::req_body_form,
      c(list(prepared), request$body, list(.multi = "explode"))
    )
  } else if (identical(request$body_type, "json")) {
    prepared <- httr2::req_body_json(
      prepared,
      request$body,
      auto_unbox = TRUE,
      null = "null"
    )
  }
  prepared
}

.read_httr2_response_body <- function(response, max_bytes) {
  if (is.raw(response$body)) {
    return(.as_bounded_raw_body(response$body, max_bytes))
  }
  body <- response$body
  if (!is.environment(body) ||
      !is.function(body$read) ||
      !is.function(body$is_complete) ||
      !is.function(body$close)) {
    stop("The HTTP transport returned an invalid body stream.", call. = FALSE)
  }
  on.exit(body$close(), add = TRUE)

  pieces <- list()
  size <- 0
  while (!isTRUE(body$is_complete())) {
    read_size <- min(.http_read_chunk_bytes, max_bytes - size + 1)
    chunk <- body$read(read_size)
    if (!is.raw(chunk)) {
      stop("The HTTP transport returned an invalid body chunk.", call. = FALSE)
    }
    if (length(chunk) == 0L) {
      break
    }
    size <- size + length(chunk)
    if (size > max_bytes) {
      stop("The buffered HTTP response is too large.", call. = FALSE)
    }
    pieces[[length(pieces) + 1L]] <- chunk
  }
  if (!isTRUE(body$is_complete())) {
    stop("The HTTP response body ended unexpectedly.", call. = FALSE)
  }
  if (length(pieces) == 0L) {
    return(raw())
  }
  do.call(c, pieces)
}

.httr2_send_request <- function(request, timeout_seconds) {
  prepared <- .httr2_prepare_request(request, timeout_seconds)
  response <- httr2::req_perform_connection(prepared)
  status <- httr2::resp_status(response)
  headers <- httr2::resp_headers(response)

  body <- if (status >= 400L || identical(request$method, "HEAD")) {
    if (!is.raw(response$body) &&
        is.environment(response$body) &&
        is.function(response$body$close)) {
      response$body$close()
    }
    raw()
  } else {
    content_length <- .http_header(headers, "content-length")
    if (!is.null(content_length) && grepl("^[0-9]+$", content_length)) {
      length_value <- suppressWarnings(as.numeric(content_length))
      if (is.finite(length_value) &&
          length_value > request$max_response_bytes) {
        if (!is.raw(response$body) &&
            is.environment(response$body) &&
            is.function(response$body$close)) {
          response$body$close()
        }
        stop("The buffered HTTP response is too large.", call. = FALSE)
      }
    }
    .read_httr2_response_body(response, request$max_response_bytes)
  }

  .new_http_response(status, headers, body)
}

.httr2_http_transport <- function(timeout_seconds = 60) {
  if (!is.numeric(timeout_seconds) ||
      length(timeout_seconds) != 1L ||
      is.na(timeout_seconds) ||
      !is.finite(timeout_seconds) ||
      timeout_seconds <= 0) {
    stop("`timeout_seconds` must be one positive number.", call. = FALSE)
  }

  list(
    send = function(request) {
      .httr2_send_request(request, timeout_seconds)
    },
    status = function(response) response$status,
    headers = function(response) response$headers,
    body = function(response) response$body,
    retry_after = function(response) {
      .http_header(response$headers, "retry-after")
    }
  )
}

.safe_transport_status <- function(transport, response) {
  tryCatch(
    transport$status(response),
    error = function(error) NA_integer_
  )
}

.safe_transport_retry_after <- function(transport, response) {
  tryCatch(
    transport$retry_after(response),
    error = function(error) NULL
  )
}

.perform_http_round <- function(request,
                                transport,
                                operation,
                                endpoint_host,
                                replayable,
                                max_attempts,
                                sleeper,
                                clock,
                                random,
                                return_unauthorized) {
  .perform_with_retry(
    request = request,
    send = transport$send,
    status_of = function(response) {
      .safe_transport_status(transport, response)
    },
    retry_after_of = function(response) {
      .safe_transport_retry_after(transport, response)
    },
    operation = operation,
    endpoint_host = endpoint_host,
    replayable = replayable,
    max_attempts = max_attempts,
    sleeper = sleeper,
    clock = clock,
    random = random,
    return_statuses = if (return_unauthorized) 401L else integer()
  )
}

.abort_http_unauthorized <- function(operation, endpoint_host) {
  .abort_delta_sharing(
    "The Delta Sharing server rejected the request.",
    type = "http",
    operation = operation,
    status = 401L,
    endpoint_host = endpoint_host,
    retry_count = 0L
  )
}

.collect_http_response <- function(response, transport, max_bytes) {
  status <- .safe_transport_status(transport, response)
  headers <- tryCatch(
    transport$headers(response),
    error = function(error) NULL
  )
  body <- tryCatch(
    transport$body(response),
    error = function(error) NULL
  )
  if (is.null(headers) || is.null(body)) {
    .abort_delta_sharing(
      "The HTTP transport returned an invalid response.",
      type = "protocol",
      operation = "http_response"
    )
  }
  body <- tryCatch(
    .as_bounded_raw_body(body, max_bytes),
    error = function(error) NULL
  )
  if (is.null(body)) {
    .abort_delta_sharing(
      "The buffered HTTP response is invalid or too large.",
      type = "protocol",
      operation = "http_response"
    )
  }
  .new_http_response(as.integer(status), headers, body)
}

.perform_authenticated_http <- function(client,
                                        method,
                                        path,
                                        query = NULL,
                                        headers = NULL,
                                        form = NULL,
                                        json = NULL,
                                        operation,
                                        response_kind,
                                        replayable = FALSE,
                                        transport = .httr2_http_transport(),
                                        clock = Sys.time,
                                        sleeper = Sys.sleep,
                                        random = stats::runif,
                                        max_attempts = 5L,
                                        max_response_bytes = NULL) {
  if (!.is_scalar_character(operation) ||
      !operation %in% .http_control_operations) {
    .http_validation_abort("The buffered HTTP operation is invalid.")
  }
  if (!is.logical(replayable) ||
      length(replayable) != 1L ||
      is.na(replayable)) {
    .http_validation_abort("`replayable` must be TRUE or FALSE.")
  }
  transport <- .normalize_http_transport(transport)
  request <- .new_client_http_request(
    client = client,
    method = method,
    path = path,
    query = query,
    headers = headers,
    form = form,
    json = json,
    response_kind = response_kind,
    max_response_bytes = max_response_bytes
  )
  endpoint_host <- .auth_endpoint_host(.client_context(client)$endpoint)
  authorization <- .client_authorization(
    client,
    transport = transport,
    clock = clock,
    sleeper = sleeper,
    random = random,
    max_attempts = max_attempts
  )
  request <- .apply_http_authorization(request, authorization)

  response <- .perform_http_round(
    request = request,
    transport = transport,
    operation = operation,
    endpoint_host = endpoint_host,
    replayable = replayable,
    max_attempts = max_attempts,
    sleeper = sleeper,
    clock = clock,
    random = random,
    return_unauthorized = TRUE
  )
  status <- .safe_transport_status(transport, response)
  if (identical(as.integer(status), 401L)) {
    can_refresh <- replayable &&
      identical(authorization$auth_type, "oauth_client_credentials") &&
      !is.null(authorization$cache_generation) &&
      isTRUE(.invalidate_client_auth(
        client,
        authorization$cache_generation
      ))
    if (!can_refresh) {
      .abort_http_unauthorized(operation, endpoint_host)
    }

    authorization <- .client_authorization(
      client,
      transport = transport,
      clock = clock,
      sleeper = sleeper,
      random = random,
      max_attempts = max_attempts
    )
    request <- .apply_http_authorization(
      request,
      authorization
    )
    response <- .perform_http_round(
      request = request,
      transport = transport,
      operation = operation,
      endpoint_host = endpoint_host,
      replayable = replayable,
      max_attempts = max_attempts,
      sleeper = sleeper,
      clock = clock,
      random = random,
      return_unauthorized = FALSE
    )
  }

  .collect_http_response(
    response,
    transport,
    request$max_response_bytes
  )
}
