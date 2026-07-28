fixture_path <- function(...) {
  testthat::test_path("fixtures", ...)
}

fixture_json <- function(...) {
  jsonlite::read_json(fixture_path(...), simplifyVector = FALSE)
}

fixture_json_response <- function(..., status_code = 200L) {
  httr2::response_json(
    status_code = status_code,
    url = "https://sharing.example.test/api",
    body = fixture_json(...)
  )
}

fixture_credentials <- function() {
  delta.sharing:::process_credentials(
    fixture_json("profiles", "bearer-v1.json")
  )
}
