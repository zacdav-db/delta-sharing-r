# R control-plane execution wiring contract

Status: implemented Phase 2 control plane and initial Phase 3 snapshot stream
Branch owner: `codex/public-read-wiring-vnext`

This contract connects the immutable S7 client/table descriptors to the
R-owned authenticated HTTP, pagination, JSON/NDJSON, and safe-projection
layers. Snapshot reads add one R planner-to-Kernel callback; there is no
parallel reader implementation.

## Callback installation and lifecycle

`.new_control_execution_callbacks()` constructs callbacks for:

- `list_shares`, `list_schemas`, and `list_tables`;
- `table_version`, `table_protocol`, `table_metadata`, and `table_schema`; and
- `read_arrow_stream` for `SharingRead`.

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

`SharingChanges` is rejected with a typed CDF-unsupported condition before
I/O. `batch_size` is bounded before planning. Every non-NULL `concurrency` is
explicitly rejected until that option reaches the compact native invocation;
it is never silently ignored.

The callback set still excludes `read_schema`, CDF planning, eager Arrow/data
frame adapters, and public diagnostics.
