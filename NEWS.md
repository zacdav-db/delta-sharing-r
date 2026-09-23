# delta.sharing 0.2.0

This release is a clean redesign of `delta.sharing`; it does not provide
compatibility aliases or migration shims for earlier package versions.

- Remove private synthetic logs when log writing or native stream creation
  fails, without removing cached data files or replacing the original error.
  Successful logs retain their session lifetime for lazy readers.
- Indexed staged file paths so rewriting large manifests no longer repeatedly
  scans the full list of downloaded assets.
- Retain verified completed downloads when another asset fails, so retrying a
  read reuses those files. Failed responses and incomplete files are discarded.
- Reject invalid batch sizes and invalid or duplicate projection names before
  read requests. Zero-row snapshots resolve their schema without downloading
  data files, even when the server returns files despite the zero limit hint.
- Declared minimum versions for the cli, httr2, purrr, rlang, and testthat APIs
  already used by the package and its tests.
- Added an R6 client, table, snapshot, and change data feed interface for
  discovery, metadata inspection, and reads.
- Added Delta Kernel-backed snapshot and change data feed reads with
  projection, limits, predicate hints, version bounds, and timestamp bounds.
- Fixed partition-only and metadata-only CDF projections for inferred insert
  and delete changes, including tables whose first columns are partitions.
- Added direct Arrow stream, Arrow reader, Arrow table, tibble, and data-frame
  materializers, including DuckDB interoperability.
- Eager Arrow materialization now closes its imported reader on success or
  error instead of waiting for garbage collection. Returned table buffers
  remain valid after reader cleanup.
- BIGINT columns automatically use `bit64::integer64` in `to_tibble()` and
  `to_data_frame()`, including empty and nested results. Arrow is a required
  dependency. Valid -9223372036854775808 values raise a clear error instead of
  becoming missing; Arrow materializers retain that value.
- Added support for bearer, basic, OAuth client-credentials, and private-key
  JWT profiles.
- Added deletion-vector, column-mapping, partitioned, nested, and logical-type
  read support through Delta Kernel.
- Added bounded parallel staging with interactive download progress and a
  table-scoped cache under R's session temporary directory.
