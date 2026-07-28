# R snapshot request and preparation contract

Status: implemented for Delta- and Parquet-format snapshots
Scope: snapshot planning and R-owned response normalization
Architecture owner: R

## Boundary

R owns the complete `SharingRead` to prepared-snapshot transition:

1. validate the immutable descriptor;
2. plan the Delta Sharing Query Table request from raw table-name segments;
3. acquire credentials and open an authenticated response stream;
4. decode NDJSON incrementally;
5. follow and validate snapshot pages;
6. atomically prepare the synthetic Delta log; and
7. return a compact Kernel invocation plus safe diagnostics.

This slice does not scan rows or implement CDF. For a Parquet-format response,
R validates the Sharing actions and maps them to a private version-zero Delta
snapshot before handoff. Rust receives only the private prepared table URI, read kind,
synthetic version, projection, and exact limit. Rust does not receive profiles,
credentials, request bodies, response headers, page tokens, refresh tokens, or
Delta Sharing protocol actions.

## Request

The route is:

```text
POST /shares/{share}/schemas/{schema}/tables/{table}/query
```

The planner keeps share, schema, and table names as raw independent segments;
the HTTP seam encodes each segment exactly once.

The JSON body uses structured `jsonPredicateHints`, one of `version` or
`timestamp`, and the current provider fields
`maxFiles`, `pageToken`, and `includeRefreshToken`. The first latest-snapshot
request sends `includeRefreshToken=true`; time-travel requests and subsequent
page clones omit it. This follows the current official Scala
`QueryTableRequest` and `getFiles` implementation where the prose protocol
lags the provider wire model. Projection is not a Query Table body field. It
is a Kernel scan option. A validated limit through the protocol's signed
32-bit ceiling is sent unchanged as the best-effort `limitHint`. A larger
supported limit is omitted from the request rather than clamped, because
clamping could make the server return too few files. The exact limit (through
the descriptor's 2^53 ceiling) is always retained for the Arrow/Kernel
boundary and diagnostics report only the hint actually sent. The deprecated SQL
`predicateHints` field is not generated.

A returned refresh token is accepted and retained privately for a later
refresh implementation.

The capabilities header is built only from the pinned allowlist:

```text
responseformat=delta,parquet;
readerfeatures=columnmapping,deletionvectors,timestampntz;
includeendstreamaction=true
```

`auto` advertises both supported formats. Explicit `delta` or `parquet`
advertises exactly that single format. The response capability and normalized
protocol/metadata/file actions must all agree on the selected format.

The protocol-default fallback is asymmetric. A server that ignores an explicit
Delta request may return Parquet; the planner accepts that response now that
the R Parquet normalizer is implemented, and diagnostics report the actual
selected format. A server may not return Delta after an explicit Parquet
request because the client did not advertise Delta reader-feature support.

The separate `fileidhash: delta` request header is sent and its response echo
is required.

The Query Table operation is read-only even though its wire method is POST.
Following the current official Python and Scala clients, connection-open,
429, and 5xx failures use the existing bounded R retry policy before response
body consumption. Every received retryable response is closed before replay.
Once a successful response body has begun streaming, pull or decoder failures
are terminal and are never replayed. A definitive OAuth 401 response can be
closed and replayed once after refreshing either supported client-credentials
or private-key JWT OAuth credential; OAuth token-endpoint retries remain
governed by the existing authentication policy.

## Pull response contract

Snapshot bodies never use the buffered discovery/metadata transport. The
injectable response contract contains only:

- scalar HTTP status;
- response headers;
- `pull()`, which returns one raw/character chunk or `NULL` at EOF; and
- idempotent `close()`.

The production httr2 seam uses `req_perform_connection()` and pulls bounded
chunks from the connection body. It never concatenates the snapshot response.
The response is closed on success, protocol failure, transport read failure,
streamed server error, cancellation, header failure, and pagination failure.

Current bounds are:

- 8 MiB per NDJSON line/action;
- 1,000,000 pulled chunks per page;
- 10,000 pages per read;
- 100,000 requested files per page; and
- 1,000,000 decoded file actions per snapshot.

The first action on every page must be protocol and the second metadata.
Protocol, metadata, response format, table version, and file-ID hash must be
consistent across pages. A terminal action must be last. It is required when
the response capability says `includeendstreamaction=true`; otherwise it is
optional. Any returned next-page token is followed with cycle and page-ceiling
protection. URL expiry is checked before another page and before publication.
The minimum is computed across private per-file `expirationTimestamp` values
and any terminal `minUrlExpirationTimestamp`. It is checked both immediately
before synthetic-log publication and again after the atomic write, while
failure cleanup is still armed, so publication time cannot return a newly
expired log.

Parquet-format pages additionally require Sharing reader version 1, Parquet
metadata without format options or reader-sensitive configuration, a
recursively valid Spark struct schema without case-insensitive name collisions
or column-mapping metadata, and unique HTTPS file IDs and URLs. Partition keys
must exactly match declared top-level primitive partition columns and their
serialized values must be valid for those types; the empty string retains the
Delta Sharing Parquet protocol's wire null encoding. It is not a generic Delta
null convention. Optional action versions, total file count, and total size are
checked against response headers and the completed manifest.

The normative terminal wire form is the published top-level object. The
current `endStreamAction` wrapper emitted by some server implementations is
also normalized as protocol tolerance, not as package-version compatibility.

## Memory gate

Response bytes are streamed, but the first implementation retains validated
private file-action objects until the atomic synthetic commit is ready. This is
bounded at 1,000,000 actions and deliberately remains in R.

Before the Phase 3 gate closes, benchmark representative manifests at increasing
file counts (including the largest supported fixture/workload), record peak R
memory and preparation time, and compare retained decoded-action memory with
the input manifest and final commit sizes. If retained actions are a material
peak-memory bottleneck or violate the agreed workload envelope, replace the
list with a validated, permission-restricted R staging action sink. This
optimization does not justify moving protocol parsing or log construction to
Rust.

## Lifetime and redaction

The prepared object privately owns the synthetic-log guard and URI. Its
internal invocation accessor is the handoff to the native scan lane. The
native stream integration must retain that owner until scan release. Explicit
release and the R finalizer are idempotent and remove the entire private
temporary root.

Public diagnostics contain only selected response format, table version, page
and file counts, minimum URL expiry/time remaining, and booleans/numeric server
hint summaries. Conditions and diagnostics never include:

- authorization or profile credentials;
- signed table, data, or deletion-vector URLs;
- request URLs, query parameters, or predicate JSON;
- page or refresh tokens; or
- streamed server error text.

## Protocol references

- [Delta Sharing Query Table](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#read-data-from-a-table)
- [Current Scala QueryTableRequest and pagination implementation](https://github.com/delta-io/delta-sharing/blob/main/client/src/main/scala/io/delta/sharing/client/DeltaSharingClient.scala)
- [Current Python Query Table client](https://github.com/delta-io/delta-sharing/blob/main/python/delta_sharing/rest_client.py)
- [Delta Sharing capabilities](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#delta-sharing-capabilities)
- [File ID Hash header](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#file-id-hash-header)
- [EndStreamAction](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#endstreamaction)
