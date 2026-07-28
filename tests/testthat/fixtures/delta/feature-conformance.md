# Delta Kernel feature-conformance fixtures

These three minimal Delta tables exercise the exact snapshot features advertised
in the Delta Sharing capabilities header:

- `feature-column-mapping` stores physical Parquet names that differ from all
  logical names, including the partition column;
- `feature-column-mapping-id` stores Parquet field IDs and exercises the `id`
  mapping mode with a mapped partition column;
- `feature-deletion-vectors` has an inline deletion vector created by Delta
  Kernel 0.22's `StreamingDeletionVectorWriter`, deleting physical row indexes
  1 and 3;
- `feature-timestamp-ntz` stores an Arrow `timestamp[us]` without a timezone and
  a log schema using `timestamp_ntz`.

The Parquet payloads were written with Arrow 22.0.0. The inline Z85 payload is
the writer's portable 36-byte bitmap body without its persisted-file framing.
Every test decodes a Delta Sharing response wrapper, constructs the package's
private synthetic log, and reads through the production Delta Kernel 0.22 to
Arrow C Stream boundary. Direct Parquet or direct local-Delta-table reads are
not conformance evidence.
