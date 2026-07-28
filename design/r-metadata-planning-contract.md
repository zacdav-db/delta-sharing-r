# R table metadata planning contract

Status: implemented Phase 2 planning foundation
Branch owner: `codex/r-metadata-planning-vnext`

This slice defines pure-R planning and response handling for table version and
table metadata requests. It uses injected fetch callbacks for tests and does
not select an HTTP library, add authorization, or wire public execution
callbacks.

## Request descriptors

All share, schema, and table names are encoded independently as URL path
segments. Descriptors contain only a method, endpoint-relative path, query
parameters, non-secret capability headers, and an operation name.

Latest table-version planning uses a `HEAD` descriptor for the table route and
parses `Delta-Table-Version` through the shared header parser. Metadata uses a
`GET` descriptor ending in `/metadata`.

Metadata requests include the deterministic snapshot capability allowlist.
Latest metadata has no time-travel query parameter. Version and timestamp
requests use one `version` or `timestamp` parameter and reject mixed modes
before calling the injected fetch function.

## Response handling

Fetch callbacks return a named response list with headers and an NDJSON chunk
source. The chunk source can be raw/character chunks already supplied by a
test transport or a pull function returning one chunk at a time and `NULL` at
end-of-stream.

Incremental sources are consumed through the shared bounded NDJSON decoder.
Line size and chunk-count ceilings fail with typed, fixed protocol errors.
Neither callback error text nor response content is copied into conditions.

## Safe projections

The internal response retains `location` and `auxiliaryLocations` in a locked
private environment for later directory-access planning. Public metadata
projections do not carry that environment and exclude both fields.

The protocol projection contains only response format, reader/writer protocol
versions, and feature names. The metadata projection contains stable logical
metadata and safe statistics. The schema projection parses and minimally
validates the Delta struct-schema JSON without requiring Arrow.

No function in this slice is exported, installed as an execution callback, or
connected to authentication or a concrete HTTP transport.
