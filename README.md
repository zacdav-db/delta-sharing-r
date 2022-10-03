# R Delta Sharing Connector

``` r
# connect to client
library(delta.sharing)
client <- delta.sharing::SharingClient$new("~/Desktop/config.share")

# see what data is accessible
client$list_shares()
client$list_all_schemas()
client$list_schemas(share = "deltasharingr")
client$list_tables(share = "deltasharingr", schema = "simple")
client$list_tables_in_share(share = "deltasharingr")

# table class
ds_tbl <- client$table(share = "deltasharingr", schema = "simple", table = "all_types")

# (optional) specify a limit (best effort to enforce)
schema_tbl$set_limit(limit = 1000)
schema_tbl$limit

# (optional) where to download files (before arrow kicks in)
schema_tbl$set_download_path("~/Desktop/share-download/")

# load data in as arrow::Dataset 
ds_tbl_arrow <- ds_tbl$load_as_arrow()
# if schema mapping is casuing problems, infer the schema
# ds_tbl_arrow <- ds_tbl$load_as_arrow(infer_schema = TRUE)

# do standard {dplyr} things if you like that
ds_tbl_arrow %>%
  select(1, 2) %>%
  mutate(x = column1 + column2) %>%
  collect()

# just want a tibble? (alias for collect on arrow)
ds_tbl_tibble <- ds_tbl$load_as_tibble()
# ds_tbl_tibble <- ds_tbl$load_as_tibble(infer_schema = TRUE)
```
