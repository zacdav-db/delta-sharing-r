# R table metadata planning contract

Status: implemented and connected to R HTTP execution
Branch owner: `codex/r-metadata-planning-vnext`

This contract defines pure-R planning and response handling for table version
and table metadata requests. Production callbacks use authenticated httr2
execution; lower-level tests retain injected fetch callbacks.

## Request descriptors

Descriptors contain only a method, validated raw endpoint-relative path
segments, query parameters, non-secret capability headers, and an operation
name. Encoding occurs once in the HTTP transport. The deterministic
`.table_route()` helper produces an encoded route only for internal assertions
and is not a request-plan input.

Latest table-version planning uses the protocol's current `GET` descriptor
ending in `/version` and parses `Delta-Table-Version` through the shared header
parser. The deprecated `HEAD` route is not used. Metadata uses a `GET`
descriptor ending in `/metadata`.

Metadata requests include the deterministic snapshot capability allowlist.
Latest metadata has no time-travel query parameter. Version and timestamp
requests use one `version` or `timestamp` parameter and reject mixed modes
before calling the injected fetch function.

## Response handling

Fetch callbacks return a named response list with headers and an NDJSON chunk
source. The chunk source can be raw/character chunks already supplied by a
test transport or a pull function returning one chunk at a time and `NULL` at
end-of-stream.

The bounded control-plane response is exposed to the parser as fixed-size raw
chunks and consumed through the shared incremental NDJSON decoder. Line size
and chunk-count ceilings fail with typed, fixed protocol errors. Neither
callback error text nor response content is copied into conditions.

## Safe projections

The internal response retains `location` and `auxiliaryLocations` in a locked
private environment for later directory-access planning. Public metadata
projections do not carry that environment and exclude both fields.

The protocol projection contains only response format, reader/writer protocol
versions, and feature names. The metadata projection contains stable logical
metadata and safe statistics. The schema projection parses and minimally
validates the Delta struct-schema JSON without requiring Arrow.

No metadata callback reads rows, buffers snapshot/CDF bodies, or invokes the
native layer.
