# R dependency API floors

The declared versions follow APIs already used by the package, rather than
the newest available versions:

| Dependency | Floor | Required API |
| --- | --- | --- |
| cli | 3.0.0 | `cli_abort()` with structured conditions |
| httr2 | 1.2.0 | Parallel retries and `max_active` require 1.1.1; the existing parallel/iterative mocked-response tests require 1.2.0 |
| purrr | 1.0.0 | `list_c()` and `list_flatten()` |
| rlang | 1.2.0 | Exported `check_number_whole()` used by table construction |
| testthat | 3.1.7 | `with_mocked_bindings()` and `local_mocked_bindings()` |

Sources: [cli](https://cli.r-lib.org/news/index.html),
[httr2](https://httr2.r-lib.org/news/index.html),
[purrr](https://purrr.tidyverse.org/news/index.html),
[rlang](https://rlang.r-lib.org/news/index.html), and
[testthat](https://testthat.r-lib.org/news/index.html) release notes.

The nanoarrow 0.8.0 floor is unchanged. The installed dependency inventory
must be regenerated whenever DESCRIPTION changes; see `tools/README.md`.
