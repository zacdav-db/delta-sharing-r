# Parquet response to Kernel mapping proof

Status: design proof accepted for implementation; no production code in this
slice
Integration base: `2d9d54443db2db139cdda078f955747281441bd6`
Upstream audit revision:
`delta-io/delta-sharing@4b790695e45bc66a7531f0ddd264725718ee2fcc`

## Verdict

Snapshot responses in the Delta Sharing Parquet response format can be
normalized by R into the same private, version-zero synthetic Delta log already
consumed by the package's Delta Kernel snapshot boundary. No new Rust API and
no standalone R or Rust Parquet reader are required.

The official protocol supplies exactly the inputs needed by an ordinary Delta
`protocol`/`metaData`/`add` snapshot: an ordered Spark JSON schema, partition
column names, signed file URLs, serialized partition values, sizes, and
optional file statistics. The fixed synthetic Delta protocol is reader version
1 and writer version 2. It describes the private log, not the Sharing protocol
or provider table.

This conclusion is limited to snapshot `file` actions. Parquet-format CDF
`add`, `cdf`, and `remove` actions have versioned semantics and remain owned by
the separate Phase 5 CDF lane.

## Reference-client evidence

The [official protocol][parquet-actions] requires one `protocol`, `metaData`, or
`file` value per wrapper. Its [File definition][parquet-file] requires
`url`, `id`, `partitionValues`, and `size`, and defines optional `stats`,
`version`, `timestamp`, and `expirationTimestamp`. Its
[partition serialization][partition-values] defines strings for every
primitive partition type and the empty string as null.

The upstream clients establish the semantic obligations, but their standalone
Parquet readers are not the architecture for this package:

- Python reads each signed URL, injects a typed value for a partition column
  missing from the physical file, and restores the top-level field order from
  `schemaString` ([reader lines 151-214][python-reader] and
  [454-499][python-partitions]).
- Scala builds partition rows by casting action values to the partition schema,
  assigns the signed URL to the file status, and uses Spark's Parquet format
  ([file index lines 65-107][scala-index]).
- Both clients model a snapshot `file` independently from CDF action types
  ([Scala model lines 164-191][scala-model]).

The package can obtain those semantics from Delta Kernel instead. The committed
test-only fixture proves that the existing native boundary:

1. reads a Parquet response-shaped file mapped to an `add` action;
2. materializes a partition column absent from the physical Parquet file;
3. casts that value according to the metadata schema;
4. respects requested projection order and an exact limit across one-row
   batches; and
5. releases the existing Arrow/Kernel stream state after materialization.

The existing native loopback test separately proves that the pinned Kernel
default engine opens an absolute action URL with its signed query unchanged.
Existing prepared-log tests prove private-log transfer and cleanup. The spike
therefore adds no production seam or Rust responsibility.

## Wire-to-private-log mapping

| Parquet response input | Private Delta commit output | Rule |
|---|---|---|
| `protocol.minReaderVersion` | `protocol.minReaderVersion = 1`, `minWriterVersion = 2` | Require supported Sharing reader version `1`; never copy it as a Delta protocol version. |
| `metaData.id` | `metaData.id` | Copy after scalar/string validation. |
| `name`, `description` | matching optional metadata fields | Copy safe text only. |
| `format.provider` | `format.provider = "parquet"` | Require `parquet`; emit empty options unless reviewed format options are supported. |
| `schemaString` | `metaData.schemaString` | Preserve bytes after recursive Spark-schema validation; array order is logical field order. |
| `partitionColumns` | `metaData.partitionColumns` | Preserve order; require unique names present in the top-level schema and primitive partition types. |
| `configuration` | normally omitted/empty | It is not a Delta feature declaration. Reject reader-sensitive feature or column-mapping settings; retain benign values only in private R diagnostics if needed. |
| `location`, `auxiliaryLocations`, `accessModes` | none | Never write storage locations or access policy into the log. |
| metadata `version`, `size`, `numFiles` | none | Retain as safe R planning/diagnostic state; they do not define Delta actions. |
| `file.url` | `add.path` | Copy the validated absolute HTTPS URL exactly, including encoded path and query. |
| `file.id` | private R file identity | Use for duplicate detection and deterministic action ordering; do not add a non-Delta field. |
| `file.partitionValues` | `add.partitionValues` | Preserve string values exactly, including empty string/null encoding. Require keys to equal the declared partition columns. Kernel reconstructs typed logical columns. |
| `file.size` | `add.size` | Copy a non-negative whole number within the R JSON-safe range. |
| `file.stats` | `add.stats` | Copy exact UTF-8 JSON only after bounded object validation; omission remains omission. |
| `file.version`, `timestamp` | private R state only | They identify the provider table version, not Delta file modification time. |
| none | `add.modificationTime = 0` | Use a documented sentinel; never mislabel the wire table timestamp as file modification time. |
| none | `add.dataChange = true` | Conventional read-only snapshot value; it does not alter snapshot rows. |
| `expirationTimestamp` | private R expiry state only | Feed the existing minimum-expiry check and diagnostics; never write it to the Delta action. |

Actions remain canonically ordered by type and file ID, matching the current
synthetic-log contract. A table scan without an ordering operation does not
promise row order, so parity tests compare schema and rows independently of
manifest order; exact limits must still return exactly the requested count.

## Safe validation and ownership

R must reject before publishing a log or beginning file I/O:

- a wrapper that does not contain exactly one protocol action, one metadata
  action, or one snapshot `file` action in its expected position;
- Sharing `minReaderVersion` above the implemented value;
- non-Parquet formats, malformed or oversized schemas/stats/actions, duplicate
  field names, duplicate file IDs or URLs, and non-HTTPS file URLs;
- schema names that collide under Delta's case-insensitive resolution;
- partition columns absent from the schema, complex/binary partition types,
  missing/extra partition-value keys, or strings invalid for the declared
  primitive type (the empty string remains the protocol null);
- physical-reader features that cannot be expressed by the fixed synthetic
  protocol, including column mapping metadata/configuration; and
- expired URLs, inconsistent protocol/metadata/table version across pages, or
  a response-format change during pagination.

Unknown fields may be ignored only after the supported Sharing reader version
is established, as the protocol permits. They must not be copied into the
private Delta commit. Signed URLs, locations, raw response text, refresh
tokens, and statistics never enter condition messages or printable objects.

R owns response negotiation, NDJSON parsing, validation, the mapping above,
atomic mode-0700/mode-0600 preparation, expiry, and diagnostics. Rust continues
to receive only the prepared local table path, cleanup capability, projection,
limit, and batch size. Kernel owns snapshot interpretation and Arrow batches;
the existing stream owns Kernel-coupled resources and cleanup.

## Implementation order and phase gate

1. Extend the R file-action normalizer with a disjoint Parquet snapshot branch;
   do not accept top-level CDF `add`/`cdf`/`remove`.
2. Add a response-format branch in synthetic-log validation that produces the
   fixed Delta protocol and the mapped metadata/add actions above.
3. Enable `responseformat=parquet` negotiation in the R snapshot planner and
   reuse pagination, expiry, prepared-log, native invocation, materializers,
   and diagnostics unchanged.
4. Add fixture matrices for empty snapshots, all primitive partition types and
   null, nested schemas, case collisions, malformed stats, duplicate files,
   expiry, pagination, projection, exact limit, early release, and redaction.
5. Add parity fixtures in both response formats. Compare logical schema,
   values, types, row count, and lifecycle; do not require unspecified row
   order.

G6 is not complete until the parity matrix passes package tests and supported
platform CI, HTTPS signed-URL behavior is evidenced beyond loopback, and
source-package checks pass with `{arrow}` absent and present. Performance
validation should measure manifest normalization time and peak R memory plus
first-batch/throughput/RSS against the Delta-format path. Any proposal to move
normalization into Rust remains subject to ADR 003's separate 25% wall-clock
or 50% peak-memory eligibility gate and maintainer approval.

## Kernel glue decision and blockers

Minimum new Kernel glue for snapshot Parquet responses: **none**.

The current adapter already accepts the complete invocation required for this
mapping and proves projection, exact limits, signed action URLs, Arrow export,
and lifecycle. If a future fixture identifies a pinned-Kernel type or partition
semantic defect, the first response is a supported-case restriction or Kernel
upgrade. A new FFI operation is justified only if the missing behavior is
intrinsically a Kernel scan option or Kernel-coupled lifecycle need.

Remaining evidence gaps, not architecture blockers:

- HTTPS signed URLs on every target platform (the local proof is `file://` and
  the existing remote proof is loopback HTTP);
- conformance for the complete primitive/nested schema and partition matrix;
- an explicit supported/rejected matrix for reader-sensitive table
  configuration and field metadata; and
- large-manifest time and peak-memory measurements before G6.

[parquet-actions]: https://github.com/delta-io/delta-sharing/blob/4b790695e45bc66a7531f0ddd264725718ee2fcc/PROTOCOL.md#api-response-actions-in-parquet-format
[parquet-file]: https://github.com/delta-io/delta-sharing/blob/4b790695e45bc66a7531f0ddd264725718ee2fcc/PROTOCOL.md#file
[partition-values]: https://github.com/delta-io/delta-sharing/blob/4b790695e45bc66a7531f0ddd264725718ee2fcc/PROTOCOL.md#partition-value-serialization
[python-reader]: https://github.com/delta-io/delta-sharing/blob/4b790695e45bc66a7531f0ddd264725718ee2fcc/python/delta_sharing/reader.py#L151-L214
[python-partitions]: https://github.com/delta-io/delta-sharing/blob/4b790695e45bc66a7531f0ddd264725718ee2fcc/python/delta_sharing/reader.py#L454-L499
[scala-index]: https://github.com/delta-io/delta-sharing/blob/4b790695e45bc66a7531f0ddd264725718ee2fcc/client/src/main/scala/io/delta/sharing/spark/RemoteDeltaFileIndex.scala#L65-L107
[scala-model]: https://github.com/delta-io/delta-sharing/blob/4b790695e45bc66a7531f0ddd264725718ee2fcc/client/src/main/scala/io/delta/sharing/client/model.scala#L164-L191
