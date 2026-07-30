# Shared helpers for tests that mock the Delta Sharing REST API with httr2.

test_profile <- function() {
  sharing_profile_parse(list(
    shareCredentialsVersion = 2,
    type = "bearer_token",
    endpoint = "https://sharing.example.test/api",
    bearerToken = "tok"
  ))
}

test_client <- function() {
  SharingClient$new(list(
    shareCredentialsVersion = 2,
    type = "bearer_token",
    endpoint = "https://sharing.example.test/api",
    bearerToken = "tok"
  ))
}

fixture_table <- function(name) {
  path <- test_path("fixtures", "delta", name)
  as.character(fs::path_real(path))
}

# Build an NDJSON body from a list of action lists.
ndjson_body <- function(actions) {
  lines <- purrr::map_chr(
    actions,
    function(a) {
      as.character(jsonlite::toJSON(a, auto_unbox = TRUE, null = "null"))
    }
  )
  paste(lines, collapse = "\n")
}

delta_metadata_response <- function(
  version = "42",
  capabilities = "responseformat=delta"
) {
  body <- ndjson_body(list(
    list(
      protocol = list(
        deltaProtocol = list(
          minReaderVersion = 3L,
          minWriterVersion = 7L,
          readerFeatures = list("columnMapping")
        )
      )
    ),
    list(
      metaData = list(
        size = 3000000000,
        numFiles = 5,
        deltaMetadata = list(
          id = "t1",
          name = "orders",
          schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
          partitionColumns = list(),
          createdTime = 1720000000000
        )
      )
    )
  ))
  httr2::response(
    200,
    headers = list(
      `delta-table-version` = version,
      `delta-sharing-capabilities` = capabilities,
      `content-type` = "application/x-ndjson"
    ),
    body = charToRaw(body)
  )
}

fixture_commit_actions <- function(table, version) {
  commit <- fs::path(
    fixture_table(table),
    "_delta_log",
    sprintf("%020d.json", version)
  )
  purrr::map(
    readLines(commit, warn = FALSE),
    jsonlite::fromJSON,
    simplifyVector = FALSE
  )
}

local_snapshot_actions <- function() {
  root <- fixture_table("local-table")
  actions <- fixture_commit_actions("local-table", 0L)

  purrr::map(actions, function(action) {
    if (!is.null(action$protocol)) {
      return(list(protocol = list(deltaProtocol = action$protocol)))
    }
    if (!is.null(action$metaData)) {
      return(list(metaData = list(deltaMetadata = action$metaData)))
    }
    add <- action$add
    add$path <- paste0("file://", fs::path(root, add$path))
    list(file = list(deltaSingleAction = list(add = add)))
  })
}

local_cdf_actions <- function() {
  root <- fixture_table("cdf")
  purrr::map2(
    c(1L, 2L),
    c(1000, 2000),
    function(version, timestamp) {
      purrr::map(fixture_commit_actions("cdf", version), function(action) {
        if (!is.null(action$protocol)) {
          return(list(protocol = list(deltaProtocol = action$protocol)))
        }
        if (!is.null(action$metaData)) {
          return(list(metaData = list(
            version = version,
            deltaMetadata = action$metaData
          )))
        }
        kind <- purrr::detect(c("add", "remove", "cdc"), ~ !is.null(action[[.x]]))
        file_action <- action[[kind]]
        file_action$path <- paste0("file://", fs::path(root, file_action$path))
        list(file = list(
          version = version,
          timestamp = timestamp,
          deltaSingleAction = rlang::set_names(list(file_action), kind)
        ))
      })
    }
  ) |>
    purrr::list_flatten()
}
