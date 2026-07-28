# R authenticated HTTP transport contract

Status: implemented internal control-plane transport slice
Branch owner: `codex/r-http-transport-vnext`

This contract defines the R-owned authenticated HTTP boundary. Discovery and
table-metadata callbacks now use it through the separate execution-wiring
contract. The transport itself does not interpret payloads and contains no
snapshot, CDF, native, or Rust path.

## Safe request model

`.perform_authenticated_http()` accepts a validated `SharingClient` and builds
an internal request from:

- an allowlisted HTTP method;
- a vector of relative path segments;
- a separately encoded named query;
- safe caller headers that cannot override Authorization, Host, Cookie,
  Content-Length, or connection control;
- at most one named form or JSON object.

Path segments are URL-encoded independently, so raw identifiers containing
spaces, slashes, backslashes, percent signs, query markers, fragments, or
Unicode cannot change the endpoint host, path hierarchy, query, or fragment.
Exact dot-traversal segments and control characters are rejected. Query, form,
JSON, header, and operation validation conditions use fixed messages and never
echo supplied values.

Validated query fields are percent-encoded and appended only after the path is
complete. This avoids reparsing an encoded `%2F` path segment as a hierarchy
separator when httr2 prepares a request.

Authentication is applied only through `.client_authorization()`. A caller
cannot supply its own Authorization header. The production httr2 request marks
all headers redacted; errors from request preparation, transport, response
hooks, and buffering are converted without retaining request URLs, paths,
queries, bodies, or tokens.

## Transport adapters

The private adapter contract consists of:

- `send(request)`;
- `status(response)`;
- `headers(response)`;
- `body(response)`;
- optional `retry_after(response)`.

`.httr2_http_transport()` is the production implementation. It disables
httr2's HTTP-status error conversion, marks request headers redacted, and
leaves retry decisions to `.perform_with_retry()`.

`.fake_http_transport()` executes the same internal request model against an
injected handler. Tests use it for deterministic request capture, transport
failure, retry, bounds, and 401 behavior; it is not exported.

## Bounded control-plane responses

Only `response_kind = "discovery"` and `"metadata"` are accepted:

- discovery: at most 8 MiB;
- metadata: at most 16 MiB.

Internal callers may choose a lower limit but cannot raise these caps. The
httr2 adapter uses a connection response and reads at most 64 KiB per pull,
stopping at the configured limit. It rejects an oversized Content-Length
before reading and still enforces the limit while reading. Error bodies are
discarded without buffering. The fake adapter enforces the same final bound.

The transport does not expose a snapshot/CDF response kind and must not be
used to buffer table-query NDJSON or data files. A later snapshot/CDF request
path requires incremental protocol consumption rather than this control-plane
collector.

## Retry and OAuth 401 replay

Ordinary transport/status retry uses `.perform_with_retry()` and the shared
Retry-After policy. Callers must explicitly mark a request replayable.

HTTP 401 is returned to the auth orchestrator only for the first round. Exactly
one replay is permitted when all of these are true:

1. the caller explicitly marked the request replayable;
2. the request used cached OAuth client credentials;
3. generation-matched invalidation succeeds.

The context then performs one fresh token exchange, replaces the Authorization
header, and repeats the request. A second 401 fails normally. Basic, profile
bearer, non-replayable OAuth, and stale-generation requests never use this
replay path. Error bodies and identity-provider content are not attached to
conditions.
