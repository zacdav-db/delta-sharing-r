# profile and client printing never expose credentials

    Code
      cat(profile_output, sep = "\n")
    Output
      <SharingProfile>
       source: list
       label: inline profile
       version: 1
       endpoint: https://sharing.example.test
       auth: bearer_token

---

    Code
      cat(client_output, sep = "\n")
    Output
      <SharingClient>
       profile: inline profile
       endpoint: https://sharing.example.test
       auth: bearer_token
       state: configured

# identifier and table printing is unambiguous

    Code
      print(identifier)
    Output
      <SharingTableIdentifier sales.eu / default / events.v2>

---

    Code
      print(table)
    Output
      <SharingTable sales.eu / default / events.v2>

# snapshot printing is safe for every time-travel mode

    Code
      print(sharing_read(table))
    Output
      <SharingRead sales / default / orders>
       as of: latest
       columns: all
       limit: none
       response format: auto

---

    Code
      print(sharing_read(table, version = 42, columns = c("id", "Amount"), limit = 100,
      response_format = "delta"))
    Output
      <SharingRead sales / default / orders>
       as of: version 42
       columns: id, Amount
       limit: 100
       response format: delta

---

    Code
      print(sharing_read(table, timestamp = as.POSIXct("2026-07-01", tz = "UTC")))
    Output
      <SharingRead sales / default / orders>
       as of: 2026-07-01T00:00:00Z
       columns: all
       limit: none
       response format: auto

# CDF printing handles open and closed ranges

    Code
      print(sharing_changes(table, starting_version = 10))
    Output
      <SharingChanges sales / default / orders>
       range: version 10 -> latest
       columns: all
       response format: auto

---

    Code
      print(sharing_changes(table, starting_timestamp = as.POSIXct("2026-07-01", tz = "UTC"),
      ending_timestamp = as.POSIXct("2026-07-02", tz = "UTC"), columns = c("id",
        "_change_type"), response_format = "delta"))
    Output
      <SharingChanges sales / default / orders>
       range: 2026-07-01T00:00:00Z -> 2026-07-02T00:00:00Z
       columns: id, _change_type
       response format: delta

