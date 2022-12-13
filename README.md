# R Delta Sharing Connector

``` r
# connect to client
library(delta.sharing)
client <- sharing_client("~/Desktop/config.share")

# see what data is accessible
client$list_shares()
client$list_all_schemas()
client$list_schemas(share = "deltasharingr")
client$list_tables(share = "deltasharingr", schema = "simple")
client$list_tables_in_share(share = "deltasharingr")
client$list_all_tables()

# table class
ds_tbl <- client$table(share = "deltasharingr", schema = "simple", table = "all_types")

# (optional) specify a limit (best effort to enforce)
ds_tbl$set_limit(limit = 1000)
ds_tbl$limit

# (optional) where to download files (before arrow kicks in)
ds_tbl$set_download_path("~/Desktop/share-download/")

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

# For CDF, specify a starting version (or timestamp) before loading
ds_tbl_cdf <- client$table(share = "deltasharingr", schema = "simple", table = "cdf_no_partition")
current_version = ds_tbl_cdf$current_version$version
ds_tbl_cdf$set_cdf_options(starting_version = current_version - 10L)
ds_tbl_cdf$starting_version

ds_tbl_cdf_arrow <- ds_tbl_cdf$load_as_arrow(changes = TRUE)
ds_tbl_cdf_tibble <- ds_tbl_cdf$load_as_tibble(changes = TRUE)
```
