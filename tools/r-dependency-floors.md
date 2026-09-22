# R dependency API floors

The declared versions follow APIs already used by the package, rather than
the newest available versions:

| Dependency | Floor | Required API |
| --- | --- | --- |
| cli | 3.0.0 | `cli_abort()` with structured conditions |
| httr2 | 1.2.0 | Custom parallel-download progress formats and mocked parallel responses; retries and `max_active` already existed in 1.1.1 |
| purrr | 1.0.0 | `list_c()` and `list_flatten()` |
| rlang | 1.2.0 | Exported `check_number_whole()` used by table construction |
| testthat | 3.1.7 | `with_mocked_bindings()` and `local_mocked_bindings()` |

Sources: [cli](https://cli.r-lib.org/news/index.html),
[httr2](https://httr2.r-lib.org/news/index.html),
[purrr](https://purrr.tidyverse.org/news/index.html),
[rlang](https://rlang.r-lib.org/news/index.html), and
[testthat](https://testthat.r-lib.org/news/index.html) release notes.

httr2 1.1.1 only enables parallel progress for `TRUE`, silently ignoring the
format list passed by `download_staged_assets()`. httr2 1.2.0 routes it through
the shared progress-bar implementation, so this is a runtime requirement as
well as a testing requirement.

These are direct API requirements, not a lockfile: dependencies can themselves
require newer versions. In particular, purrr and current testing tools require
cli newer than 3.0.0. Test the other declared minimums together in an isolated
library, and check our cli condition wrapper separately at 3.0.0. Disable user
startup profiles so they cannot preload newer dependencies into the test process.

The nanoarrow 0.8.0 floor is unchanged. Arrow remains a required dependency
following the integer64 conversion work. The installed dependency inventory
must be regenerated whenever DESCRIPTION changes; see `tools/README.md`.
