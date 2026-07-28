# R control-plane execution wiring contract

Status: implemented Phase 2 control plane, Phase 3 snapshot stream, Phase 4
eager materializers and diagnostics, and Phase 5 explicit-version CDF path
Branch owners: `codex/r-materializers-vnext`,
`codex/read-diagnostics-vnext`, and `codex/cdf-planning-vnext`

This contract connects the immutable S7 client/table descriptors to the
R-owned authenticated HTTP, pagination, JSON/NDJSON, and safe-projection
layers. Snapshot reads add one R planner-to-Kernel callback; there is no
parallel reader implementation.

## Callback installation and lifecycle

`.new_control_execution_callbacks()` constructs callbacks for:

- `list_shares`, `list_schemas`, and `list_tables`;
- `table_version`, `table_protocol`, `table_metadata`, and `table_schema`; and
- `read_arrow_stream` for `SharingRead` and `SharingChanges`;
- `data_frame_from_stream` for eager base data frames; and
- `arrow_from_stream` for eager Arrow tables when `{arrow}` is installed.

Package load installs this set through `.set_execution_callbacks()`. The
callback interface and captured transport hooks remain package-private R
objects. Package unload clears the interface before the namespace is released.
The factory accepts private transport, clock, sleeper, random, retry, and
metadata-chunk controls so tests execute hermetically without mutating public
descriptors or client properties.

The snapshot callback captures a separate pull-only transport. It prepares
Query Table pages through `.prepare_snapshot_http_read()`, then transfers the
private synthetic-log guard exactly once to `.native_snapshot_stream()`.
Projection and the exact (unclamped) limit come only from the prepared compact
invocation. R cleanup stays armed through native construction and the prepared
state is marked released only after native code confirms cleanup ownership.
The returned Arrow C Stream owns the temporary root through exhaustion,
explicit release, or finalization.

The CDF callback uses a separate GET planner and the same pull-only transport.
R requires explicit inclusive starting and ending versions for executable CDF,
requests historical metadata and protocol actions, validates every provider
version and timestamp, and publishes true-version commit files plus the
start-minus-one checkpoint bootstrap. The native callback receives only the
prepared guard, exact inclusive versions, projection, and batch size. It calls
Kernel `TableChanges` and transfers the same cleanup ownership as snapshots.
Timestamp-bound and open-ended descriptors remain valid immutable
specifications but raise typed unsupported conditions before HTTP until their
resolved end version can be proven exactly.

## Eager materializer boundary

`read_arrow()` and `read_data_frame()` first obtain exactly one stream through
the public `read_arrow_stream()` execution callback. Their materializer
callbacks receive only that stream; they cannot plan a request, open HTTP, or
start another Kernel scan.

When `{arrow}` is installed, `read_arrow()` transfers the Arrow C Stream into
an Arrow record-batch reader and eagerly reads an Arrow table. This direct C
Stream import preserves Arrow field order and nested, temporal, and decimal
types without IPC or an intermediate full-table conversion to R vectors. When
`{arrow}` is unavailable, the callback is absent and the existing public
preflight raises a typed `read_arrow`/`arrow_package` unsupported condition
before snapshot HTTP or Kernel work begins.

`read_data_frame()` infers the record prototype from the stream schema and uses
`nanoarrow::convert_array_stream()` across zero, one, or many batches. This is
the one intentional full-table R-vector conversion. It is eager and therefore
requires the complete result to fit in R memory; large reads should consume
`read_arrow_stream()` directly.

Both adapters release the source stream deterministically after success and on
dependency, import, conversion, or mid-stream failure. After Arrow accepts the
C Stream, the record-batch reader owns it and is closed deterministically after
the table has been materialized.

## Stream-local diagnostics

Successful snapshot and explicit-version CDF stream construction attaches an immutable
`SharingReadDiagnostics` containing only safe R-owned planning and selection
facts. `read_diagnostics()` reads that value directly from the stream before
or after consumption and release; it is not an execution callback and does not
own any stream resource.

The attachment excludes credentials, URLs, paths, tokens, predicate values,
protocol actions, temporary locations, and native ownership. No lifecycle
state is inferred from process-global native counters. See
`r-read-diagnostics-contract.md` for the complete field, redaction, and
concurrency-isolation contract.

## Discovery execution

Planners pass validated raw segment vectors to the HTTP transport. The
transport performs exactly one URL-encoding step, preserving raw provider
identifiers containing slashes, percent signs, or Unicode as one segment. Page
tokens are separate query values.

Each JSON page is bounded by the discovery transport cap, parsed without
automatic data-frame simplification, and validated before the existing
pagination and safe-record projections consume it. Omitted share filters reuse
the complete share listing and preserve provider order during fan-out.

## Table execution

Table version uses the current `GET .../version` route. Protocol, metadata, and
logical schema use `GET .../metadata` with the deterministic capability header.
The bounded metadata response is presented to the incremental NDJSON decoder
as fixed-size raw chunks. Protocol, metadata, and schema callbacks return only
the existing safe projections; private storage locations remain confined to
the internal parsed response and are not retained by the public callback
result.

All control-plane GETs are explicitly replayable for shared retry control and
the single generation-matched OAuth 401 refresh rule.

## Failure and scope boundary

HTTP conditions retain their typed status, safe endpoint host, operation, and
retry count across planner injection wrappers. Untyped handler/parser failures
are converted to fixed protocol conditions. URLs, paths, page tokens, request
bodies, authorization values, server bodies, and provider record contents are
never attached to conditions.

`batch_size` is bounded before planning. Every non-NULL `concurrency` is
explicitly rejected until that option reaches the compact native invocation;
it is never silently ignored. CDF rejects Parquet responses, absent or
conflicting provider versions/timestamps, invalid or out-of-range actions, and
timestamp/open-ended execution without a fallback reader. Versions without
actions are represented by intentional empty commits inside the proven bounds.

The callback set still excludes `read_schema`. Diagnostics are intentionally
stream-local rather than process-global execution callbacks.
