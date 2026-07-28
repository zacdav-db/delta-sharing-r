# R-owned read diagnostics contract

Status: implemented Phase 4 snapshot and explicit-version CDF diagnostics
Branch owners: `codex/read-diagnostics-vnext` and
`codex/cdf-planning-vnext`

## Public surface

`read_diagnostics(stream)` returns an immutable `SharingReadDiagnostics` for a
stream created by `read_arrow_stream()`. Diagnostics are attached when stream
construction succeeds and remain readable before the first pull, after normal
exhaustion, and after explicit `stream$release()`.

The descriptor contains only safe, immutable facts known by R:

- read kind and selected response format;
- resolved snapshot table version, or inclusive CDF version bounds;
- response page and selected file counts;
- projected column names, exact limit, batch size, and selected concurrency;
- booleans/numeric summaries of predicate and limit hints; and
- the minimum signed-file URL expiration and the time-to-expiry observed when
  planning completed.

The time-to-expiry value is a planning-time observation, not a live countdown.
Snapshot and explicit-version CDF diagnostics are attached through their
separate R planners without coupling their execution semantics.

## Ownership and lifecycle

The nanoarrow stream carries one package-private attribute whose value is the
safe `SharingReadDiagnostics` object. It carries no planner, client,
credential context, protocol action, synthetic-log guard, native handle, or
cleanup capability. Reading diagnostics therefore cannot extend or mutate the
stream lifetime.

Diagnostics are read directly from the stream. They are not resolved through
the process-global execution callback interface, so restoring or replacing
that interface after stream creation cannot mix diagnostics between streams.
Concurrent streams have independent descriptor values.

No active/released flag is reported. The standard nanoarrow stream surface
does not expose a reliable lifecycle query, and the native counters available
today are process-global. Attributing those counters to one stream would be
incorrect when reads overlap. Adding a per-stream native diagnostics API is
outside this implementation and is not justified by the current requirements.

## Redaction boundary

Neither the attached private attribute nor the returned descriptor may
contain:

- authorization values, profile credentials, or auth state;
- endpoint, request, table, data, or deletion-vector URLs;
- filesystem paths, query strings, or private temporary locations;
- page tokens, refresh tokens, or signed URL query parameters;
- predicate expressions or values;
- raw protocol actions, response bodies, or server error text; or
- mutable execution ownership or process-global native counters.

The public printer exposes only read kind, version range, response format,
counts, projection, limit, and batch size. Tests inspect the descriptor,
printer output, and stream attributes for known fixture secrets before and
after exhaustion and release.

## Integration

The shared attachment point is `.execute_snapshot_arrow_stream()` in
`R/execution-control.R`. Snapshot diagnostics use the resolved table version;
CDF diagnostics use the planner's validated inclusive provider versions.
Both attach the same stream-local attribute after native cleanup ownership is
accepted, without adding a Rust or native diagnostics API.
