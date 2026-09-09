# delta.sharing (development version)

- BIGINT columns now automatically use `bit64::integer64` in `to_tibble()`
  and `to_data_frame()`, including empty and nested results. Arrow is now a
  required dependency. Valid -9223372036854775808 values raise a clear error
  instead of becoming missing; Arrow materializers retain that value.

# delta.sharing 0.2.0

This release is a clean redesign of `delta.sharing`; it does not provide
compatibility aliases or migration shims for earlier package versions.

- Added an R6 client, table, snapshot, and change data feed interface for
  discovery, metadata inspection, and reads.
- Added Delta Kernel-backed snapshot and change data feed reads with
  projection, limits, predicate hints, version bounds, and timestamp bounds.
- Added direct Arrow stream, Arrow reader, Arrow table, tibble, and data-frame
  materializers, including DuckDB interoperability.
- Added support for bearer, basic, OAuth client-credentials, and private-key
  JWT profiles.
- Added deletion-vector, column-mapping, partitioned, nested, and logical-type
  read support through Delta Kernel.
- Added bounded parallel staging with interactive download progress and a
  table-scoped cache under R's session temporary directory.
