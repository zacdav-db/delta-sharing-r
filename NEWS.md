# delta.sharing (development version)

- Reject invalid batch sizes and invalid or duplicate projection names before
  read requests. Zero-row snapshots now resolve their schema without staging
  data files, even when the server returns files despite the zero limit hint.

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
