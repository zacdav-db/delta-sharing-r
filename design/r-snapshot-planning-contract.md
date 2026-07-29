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
clamping could make the server return too few files. The exact limit is always
retained for the Arrow/Kernel boundary and diagnostics report only the hint
actually sent. The deprecated SQL `predicateHints` field is not generated.

A returned refresh token is accepted and retained privately for a later
refresh implementation.

The capabilities header is built only from the pinned allowlist:

```text
responseformat=delta,parquet;
readerfeatures=columnmapping,timestampntz;
includeendstreamaction=true
```

The reader-feature allowlist is backed by committed end-to-end fixtures, not
header-string tests. Each fixture enters as Delta Sharing
`protocol`/`metaData`/`file` NDJSON, passes the production R normalizers and
private synthetic-log writer, and is scanned by Delta Kernel 0.22 through the
production Arrow C Stream:

- `columnmapping` proves both name and ID modes restore physical Parquet fields
  to logical names, including mapped partition columns and logical projection;
- `timestampntz` proves the table feature and `timestamp_ntz` schema produce
  Arrow `timestamp[us]` without a timezone while injecting a partition value.

The tests substitute only the already-normalized presigned data-file URL with a
local fixture URI so they remain hermetic. Feature protocol, metadata,
and partition values are unchanged. The same tests assert normal
exhaustion/materialization and early-release lifecycle balance. The allowlist
must be reduced if one of these fixtures is removed or stops passing against a
pinned Kernel update.

`readerfeatures` does not claim support in the Parquet-response mapper. The
[Delta Sharing protocol][sharing-capabilities] defines it as useful only when
the selected response is `delta`. Therefore an `auto` metadata request may
send `responseformat=delta,parquet;readerfeatures=...`: the server considers
the features only if it selects Delta and ignores them if it selects Parquet.
An explicit Parquet request sends only `responseformat=parquet`. The Parquet
normalizer continues to reject Delta physical-reader metadata/configuration
rather than silently reinterpret it.

Deletion vectors are deliberately not advertised yet. A committed fixture
proves that the exact inline (`i`) portable bitmap accepted by the R normalizer
survives synthetic-log encoding and removes selected physical rows through
Kernel. The opt-in absolute-DV proof additionally demonstrates that Kernel
preserves a query required to reach an immutable bitmap over trusted HTTPS and
applies that bitmap through the same production path. A fixed, intentionally
invalid signature-key marker selects Kernel's per-object HTTP branch; neither
that marker nor GitHub's `raw=1` query is a provider signature. The
`deletionvectors` capability may be restored only after a genuine
provider-signed absolute (`p`) URL has equivalent
production-path and hosted cross-platform evidence.

`auto` advertises `delta,parquet`; if the server selects Delta, the retained
reader-feature allowlist applies, while a selected Parquet response uses the
separate R Parquet-action normalizer and the same Kernel stream without
inheriting those feature claims. Explicit `delta` advertises its reader
features. Explicit `parquet` advertises only `responseformat=parquet`.
Metadata and Query Table use the same response-specific construction.

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

[sharing-capabilities]: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#readerfeatures

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

Response bytes and file actions are processed incrementally. Production keeps
at most one 1,024-record encoded run buffer in R, writes
permission-restricted action, ID, and path runs, and performs shell-free
bounded 16-way merges. Separate ID/path merges enforce global duplicate
rejection; the action merge preserves deterministic type/ID order. The final
commit is streamed from the merged action run, and run work is removed before
atomic publication. The one-million-action hard limit remains.

The production-path benchmark records both the original material-list baseline
and the staged implementation. On the Darwin arm64 100,000-file workload,
staging reduced peak memory above the zero-file baseline from 450.109 MiB to
153.000 MiB (66.0%) while increasing median preparation time from 45.227 to
76.753 seconds (69.7%). This is an explicit memory-for-time trade-off, and no
release RSS/time envelope is agreed. Lifecycle probes for success, write
failure, and finalization close the response exactly once and leave no
temporary roots. This R-owned optimization does not justify moving protocol
parsing or log construction to Rust.

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
