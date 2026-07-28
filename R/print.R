.identifier_text <- function(identifier) {
  paste(
    identifier@share,
    identifier@schema,
    identifier@table,
    sep = " / "
  )
}

.print_header <- function(class, detail = NULL) {
  if (is.null(detail)) {
    cat("<", class, ">\n", sep = "")
  } else {
    cat("<", class, " ", detail, ">\n", sep = "")
  }
}

S7::method(print, SharingProfile) <- function(x, ...) {
  .print_header("SharingProfile")
  cat(" source: ", x@source_type, "\n", sep = "")
  cat(" label: ", x@label, "\n", sep = "")
  cat(" version: ", format(x@version, scientific = FALSE), "\n", sep = "")
  cat(" endpoint: ", x@endpoint, "\n", sep = "")
  cat(" auth: ", x@auth_type, "\n", sep = "")
  invisible(x)
}

S7::method(print, SharingClient) <- function(x, ...) {
  context <- .client_context(x)
  .print_header("SharingClient")
  cat(" profile: ", x@profile@label, "\n", sep = "")
  cat(" endpoint: ", x@profile@endpoint, "\n", sep = "")
  cat(" auth: ", x@profile@auth_type, "\n", sep = "")
  cat(" state: ", context$state, "\n", sep = "")
  invisible(x)
}

S7::method(print, SharingTableIdentifier) <- function(x, ...) {
  .print_header("SharingTableIdentifier", .identifier_text(x))
  invisible(x)
}

S7::method(print, SharingTable) <- function(x, ...) {
  .print_header("SharingTable", .identifier_text(x@identifier))
  invisible(x)
}

S7::method(print, SharingRead) <- function(x, ...) {
  .print_header("SharingRead", .identifier_text(x@table@identifier))
  as_of <- if (!is.null(x@version)) {
    paste0("version ", format(x@version, scientific = FALSE))
  } else if (!is.null(x@timestamp)) {
    .format_timestamp(x@timestamp)
  } else {
    "latest"
  }
  columns <- if (is.null(x@columns)) {
    "all"
  } else {
    paste(x@columns, collapse = ", ")
  }
  limit <- if (is.null(x@limit)) {
    "none"
  } else {
    format(x@limit, scientific = FALSE)
  }

  cat(" as of: ", as_of, "\n", sep = "")
  cat(" columns: ", columns, "\n", sep = "")
  cat(" limit: ", limit, "\n", sep = "")
  cat(" response format: ", x@response_format, "\n", sep = "")
  invisible(x)
}

S7::method(print, SharingChanges) <- function(x, ...) {
  .print_header("SharingChanges", .identifier_text(x@table@identifier))
  if (!is.null(x@starting_version)) {
    start <- paste0(
      "version ",
      format(x@starting_version, scientific = FALSE)
    )
    end <- if (is.null(x@ending_version)) {
      "latest"
    } else {
      paste0("version ", format(x@ending_version, scientific = FALSE))
    }
  } else {
    start <- .format_timestamp(x@starting_timestamp)
    end <- if (is.null(x@ending_timestamp)) {
      "latest"
    } else {
      .format_timestamp(x@ending_timestamp)
    }
  }

  columns <- if (is.null(x@columns)) {
    "all"
  } else {
    paste(x@columns, collapse = ", ")
  }

  cat(" range: ", start, " -> ", end, "\n", sep = "")
  cat(" columns: ", columns, "\n", sep = "")
  cat(" response format: ", x@response_format, "\n", sep = "")
  invisible(x)
}

S7::method(print, SharingReadDiagnostics) <- function(x, ...) {
  .print_header("SharingReadDiagnostics", x@read_kind)
  if (identical(x@read_kind, "snapshot")) {
    cat(
      " version: ",
      format(x@table_version, scientific = FALSE),
      "\n",
      sep = ""
    )
  } else {
    end <- if (is.null(x@ending_version)) {
      "latest"
    } else {
      format(x@ending_version, scientific = FALSE)
    }
    cat(
      " versions: ",
      format(x@starting_version, scientific = FALSE),
      " -> ",
      end,
      "\n",
      sep = ""
    )
  }
  columns <- if (is.null(x@columns)) {
    "all"
  } else {
    paste(x@columns, collapse = ", ")
  }
  limit <- if (is.null(x@limit)) {
    "none"
  } else {
    format(x@limit, scientific = FALSE)
  }
  cat(" response format: ", x@response_format, "\n", sep = "")
  cat(" pages: ", format(x@page_count, scientific = FALSE), "\n", sep = "")
  cat(" files: ", format(x@file_count, scientific = FALSE), "\n", sep = "")
  cat(" columns: ", columns, "\n", sep = "")
  cat(" limit: ", limit, "\n", sep = "")
  cat(
    " batch size: ",
    format(x@batch_size, scientific = FALSE),
    "\n",
    sep = ""
  )
  invisible(x)
}
