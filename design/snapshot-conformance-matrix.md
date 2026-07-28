# Snapshot conformance matrix

This packet records production-path snapshot evidence. Each matrix test starts
with `SharingRead`, decodes a Delta Sharing response in R, creates the private
R-owned synthetic log, scans it with Delta Kernel, and consumes the resulting
Arrow C Stream. Direct Parquet reads are not counted as conformance evidence.

## Proven representative behavior

| Area | Evidence |
| --- | --- |
| Empty snapshot | An empty file-action set retains the complete Arrow schema, emits no batch, preserves read diagnostics, and removes the synthetic log on exhaustion. |
| Primitive Arrow values | The package-owned fixture covers boolean, signed 8/16/32/64-bit integers, 32/64-bit floating point, UTF-8, binary, date32, and UTC microsecond timestamp values, including representative nulls. |
| Nested Arrow values | The fixture covers a nullable list of non-null UTF-8 elements and a struct with nullable integer and UTF-8 children. |
| Mapped partitions | A name-mapped snapshot restores two logical partition columns, one string and one integer, from physical partition keys while projecting logical data-column names. |
| Time travel | An explicit version is sent in the request and is required to match the response version. Explicit version 6 and latest version 7 materialize distinct server-selected file sets, and diagnostics retain the resolved version. |
| Malformed input | Truncated NDJSON fails as a typed protocol error before native construction. A structurally valid schema with an invalid Delta logical type reaches Kernel and fails as a typed native-preparation error. The transport closes once and no synthetic-log root remains in either case. |
| Lifecycle | Exhaustion, normal eager materialization, and a deterministic adapter failure after the first real Kernel batch all release the Arrow stream and synthetic log. Diagnostics remain readable after release, with native active-stream and pending-cleanup counts balanced. |

The executable matrix is
`tests/testthat/test-snapshot-conformance-matrix.R`. Its type fixture and
reproducible generator live in
`tests/testthat/fixtures/delta/snapshot-types/`.

## Deliberately open

This fixture is representative rather than exhaustive. The companion
`snapshot-logical-type-conformance.md` packet proves decimals, maps, UTC and
timezone-free timestamps, and deeper nested name-mapped combinations through
the same production path. It also records Delta Kernel 0.22.0's typed,
redacted rejection of unsupported Delta interval metadata. Existing focused
fixtures separately cover column mapping by ID and deletion-vector filtering.

The matrix also does not replace target-platform evidence for provider-signed
HTTPS object storage, Windows cancellation, sanitizers, or hosted
cross-platform package checks. The opt-in absolute-DV proof covers a
query-required trusted-HTTPS redirect, but not genuine provider signature or
expiry semantics. Absolute-path deletion-vector (`storageType = "p"`) access
therefore remains outside the advertised reader-feature set.
