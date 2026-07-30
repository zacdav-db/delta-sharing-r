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
