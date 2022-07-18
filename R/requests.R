#' Request specific share endpoint
#'
#' @param creds Object of class `DeltaShareCredentials`.
#' @param method HTTP method to use.
#' @param endpoint URL to query.
#' @param body List to use as request body.
#' @param params Named list to be used as query parameters
#'
#' @return httr2 request object
req_share <- function(creds, method, endpoint, body = NULL, params = NULL) {

  req <- httr2::request(base_url = creds$endpoint) %>%
    httr2::req_auth_bearer_token(creds$bearerToken) %>%
    httr2::req_user_agent(string = "r-delta-sharing") %>%
    httr2::req_url_path_append(endpoint) %>%
    httr2::req_method(method)

  if (!is.null(body) && method != "HEAD") {
    body <- base::Filter(length, body)
    req <- req %>%
      httr2::req_body_json(body)
  }

  if (!is.null(params)) {
    params$.req <- req
    req <- do.call(httr2::req_url_query, params)
  }

  req

}

#' Request Error Propagation
#'
#' @param resp httr2 response object.
#' @return Errors.
req_error_body <- function(resp) {
  json <- resp %>% httr2::resp_body_json(check_type = FALSE)
  # if there is "message":
  if ("message" %in% names(json)) {
    paste(json$errorCode, json$message, sep = ": ")
  } else if (length(json) == 1) {
    json[[1]]
  } else {
    paste(json, collapse = " ")
  }
}


#' Make Request
#'
#' @param req Request.
#' @param ... Additional parameters.
#' @return Responses list
make_req <- function(req, ...) {

  responses <- list()

  repeat({

    resp <- req %>%
      httr2::req_retry(max_tries = 2) %>%
      httr2::req_error(body = req_error_body) %>%
      httr2::req_perform()

    if (req$method != "HEAD") {
      resp <- httr2::resp_body_json(resp, ...)
    } else if (req$method == "HEAD") {
      resp <- unclass(httr2::resp_headers(resp))
    }

    responses <- append(responses, list(resp))

    # break if next page token is null, otherwise add token to request
    if (is.null(resp$nextPageToken) || resp$nextPageToken == "") {
      break
    } else {
      req <- httr2::req_url_query(req, pageToken = resp$nextPageToken)
    }

  })

  # when multiple responses it is assumed that we only need `items` from
  # every responses after first
  if (length(responses) > 1) {
    responses[[1]]$items <- purrr::flatten(purrr::map(responses, "items"))
  }

  responses[[1]]

}
