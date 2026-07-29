# Credentialed CDF coverage uses the synthetic E2 fixture share. It is separate
# from the public integration suite because the public endpoint has no CDF
# table. Opt in with both variables; the profile remains outside the package.

test_that("CDF reads versioned changes through Delta Kernel", {
  testthat::skip_if_not(
    nzchar(Sys.getenv("DELTA_SHARING_RUN_CDF_INTEGRATION")),
    "Set DELTA_SHARING_RUN_CDF_INTEGRATION=1 to run the CDF integration test."
  )
  profile <- fs::path_expand(Sys.getenv("DELTA_SHARING_TEST_PROFILE"))
  testthat::skip_if_not(
    nzchar(profile) && fs::file_exists(profile),
    "Set DELTA_SHARING_TEST_PROFILE to a Delta Sharing profile."
  )

  table_reader <- sharing_client(profile)$table(
    "delta_sharing_r_vnext_share.delta_sharing_r_vnext.cdf_dv_interop"
  )
  changes <- table_reader$changes(
    starting_version = 1,
    ending_version = 4
  )$to_data_frame()

  expect_equal(nrow(changes), 3500L)
  expect_setequal(unique(changes$`_commit_version`), c(1, 2, 3))
  expect_equal(
    unname(base::table(changes$`_change_type`)),
    unname(base::table(factor(
      c(
        rep("delete", 500),
        rep("insert", 1000),
        rep("update_postimage", 1000),
        rep("update_preimage", 1000)
      )
    )))
  )

  timestamp_changes <- table_reader$changes(
    starting_timestamp = "2026-07-29T07:35:42Z",
    ending_timestamp = "2026-07-29T07:35:42Z"
  )$to_data_frame()

  expect_equal(nrow(timestamp_changes), 500L)
  expect_equal(unique(timestamp_changes$`_commit_version`), 2)
  expect_equal(
    unname(base::table(timestamp_changes$`_change_type`)),
    unname(base::table(factor(rep("delete", 500))))
  )

  latest_changes <- table_reader$changes(starting_version = 4)$to_data_frame()
  expect_equal(nrow(latest_changes), 0L)
  expect_true(all(
    c(
      "_change_type",
      "_commit_version",
      "_commit_timestamp"
    ) %in%
      names(latest_changes)
  ))
})
