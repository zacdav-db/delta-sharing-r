# R control-plane execution wiring contract

Status: implemented Phase 2 discovery and metadata execution
Branch owner: `codex/r-execution-wiring-vnext`

This contract connects the immutable S7 client/table descriptors to the
R-owned authenticated HTTP, pagination, JSON/NDJSON, and safe-projection
layers. It installs no row-read, snapshot, CDF, Arrow, native, or Rust
callback.

## Callback installation and lifecycle

`.new_control_execution_callbacks()` constructs callbacks for:

- `list_shares`, `list_schemas`, and `list_tables`;
- `table_version`, `table_protocol`, `table_metadata`, and `table_schema`.

Package load installs this set through `.set_execution_callbacks()`. The
callback interface and captured transport hooks remain package-private R
objects. Package unload clears the interface before the namespace is released.
The factory accepts private transport, clock, sleeper, random, retry, and
metadata-chunk controls so tests execute hermetically without mutating public
descriptors or client properties.

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

This callback set deliberately excludes `read_schema`, snapshot/CDF request
bodies, row NDJSON, signed-file downloads, synthetic logs, Kernel invocation,
Arrow adapters, and diagnostics.
