# R snapshot synthetic-log contract

Status: implemented and wired to the compact native snapshot scan; full
cross-platform conformance pending
Branch owner: `codex/r-synthetic-log-vnext`

## Scope

This contract covers the R-owned preparation and lifetime of the minimal local
Delta log used for one Delta- or Parquet-format snapshot response. It starts after the
response protocol, metadata, and file wrappers have been incrementally decoded
and normalized. It does not perform HTTP, URL refresh, public S7 dispatch,
Kernel invocation or CDF preparation.

## Authoritative mapping

The current [Delta Sharing protocol][sharing-protocol] specifies that a
Delta-format response wraps:

- `protocol.deltaProtocol`, the original Delta protocol action;
- `metaData.deltaMetadata`, the original Delta metadata action; and
- `file.deltaSingleAction`, one original Delta file action whose data-file
  `path` has already been replaced by a presigned URL.

The upstream Python Kernel reader implements that mapping in
`DeltaSharingReader.__write_temp_delta_log_snapshot()`: it writes those three
inner values, in that order, into version zero of a temporary Delta log. The R
implementation mirrors this mapping, but validates and normalizes first,
publishes atomically, restricts permissions, and couples cleanup to an explicit
lifetime guard.

One local commit is written:

```text
<private root>/.delta-sharing-r-prepared-log
<private root>/table/_delta_log/00000000000000000000.json
```

The commit contains protocol, metadata, and deterministically ordered
`add`/`remove` actions. Sharing-only wrapper fields (`id`, versions,
timestamps, expiry, table size, locations, and access modes) are not copied
into the Delta actions. Table `location` and `auxiliaryLocations` are never
written.

The native invocation receives `.snapshot_log_path(guard)` and the exact
private root only when its cleanup capability is transferred. Ordinary local
table scans receive no cleanup root. `.snapshot_log_uri(guard)` remains the
encoded absolute `file:///...` representation for diagnostics and contracts,
but the internal handoff uses the canonical local path so native code can
prove containment without parsing or owning R semantics.

## Validation and intentionally closed cases

Preparation accepts Delta and Parquet snapshot response formats. Delta protocol
`minReaderVersion` and `minWriterVersion` are required. Metadata must describe
Parquet files and contain a minimally valid Delta struct schema.

For a Parquet response, Sharing reader version 1 maps to the private fixed
Delta protocol reader 1/writer 2. Metadata schema and partition-column order
are preserved, but Sharing-only configuration and table-level values are not
copied. Each `file.url` becomes an `add.path`; partition values, size, and
validated stats are preserved; `modificationTime=0` and `dataChange=true` are
fixed sentinels. File version, timestamp, expiry, and ID remain private R
planning state. Only snapshot `file` actions are accepted; Parquet CDF is
rejected before request execution when explicitly selected.

The file-wrapper allowlist follows the current Delta Sharing wrapper. The
single action must be exactly one `add` or `remove`, using the current
reference-server action fields and an absolute HTTPS path. File IDs and
action paths must not be duplicated. Stats must be a JSON object. File count
and encoded action size are bounded.

Deletion vectors require reader version 3, writer version 7, and
`deletionVectors` in both feature lists. These storage forms are accepted:

- `i`: inline deletion-vector bytes;
- `p`: an absolute HTTPS URL in `pathOrInlineDv`.

Storage type `u` is deliberately rejected. The reference Delta Sharing server
resolves an on-disk relative DV against the provider table, presigns it, and
returns it as storage type `p`. Treating an unresolved `u` path as relative to
the synthetic local table would be incorrect. Unknown wrapper, file-action,
and deletion-vector fields fail closed until the mapping is reviewed.
`cdc` actions are rejected by this snapshot-only contract.

## Atomicity, privacy, and lifetime

All actions are validated and encoded before filesystem publication. R creates
a mode-0700 private root, writes a package ownership marker and commit with
mode 0600, closes the files, and renames the staging table into place on the
same filesystem. A failure at any point recursively removes the exact
generated root and raises a fixed, typed condition that does not copy an input
URL, location, JSON body, or underlying error text.

Presigned data and DV URLs exist only in locked private action state and the
private commit file. The guard's print method reports only active/released
state and action count. Before native handoff, explicit release is idempotent
and the guard finalizer is a fallback. After successful handoff, the guard is
marked released and the Arrow stream owns a native cleanup token. The token
accepts only the canonical generated `root/table`, the exact marker/table/log
shape, private root permissions, and plain non-symlink files. Explicit stream
release, normal exhaustion, an imported Arrow reader's release, or garbage
collection then removes the root.

Native cleanup is staged and non-recursive. A transient filesystem failure is
retried and then retained as a capability-checked process-local pending
cleanup. Later native calls and `.onUnload` retry after stable-identity and
exact-stage revalidation. A changed or replaced root is abandoned rather than
deleted.

## Remaining integration proof

The JSON mapping is specified upstream. The following runtime behavior remains
a G3 integration gate:

- the pinned Delta Kernel default engine reads genuine provider-signed HTTPS
  data and DV URLs on macOS, Linux, and Windows without changing their encoded
  query;
- provider-specific object-store error paths redact those URLs;
- expiry and refresh are coordinated before a stream outlives its URLs;
- deletion-vector application against a genuine provider-signed URL remains
  unproven.

The committed feature-conformance fixture now proves that a
Delta Sharing-wrapped inline (`i`) deletion vector passes the production R
normalizer and synthetic-log writer unchanged, and that Kernel removes its
selected rows before Arrow handoff. The hermetic test substitutes only the
absolute data-file URL after normalization.

The opt-in absolute-DV test now proves a separate local slice: an absolute
(`p`) descriptor whose `raw=1` query is required to obtain an immutable,
hash-pinned bitmap over trusted HTTPS passes through R unchanged, is fetched
by Kernel, filters the exact rows, redacts fixed failures, and cleans up. The
test directly proves that omitting or changing the query key returns HTML, so
successful filtering demonstrates Kernel query propagation. GitHub's query is
not signed. A fixed, intentionally invalid signature-key marker only selects
Kernel's per-object HTTP branch; it does not claim provider-signature or
expiry semantics.

The current native tests prove the Kernel 0.22 default engine's presigned-URL
branch against a loopback HTTP server, including the signed query, and prove
redaction of a downstream reqwest failure. This is useful implementation
evidence but is not TLS/HTTPS or cross-platform proof.

[sharing-protocol]: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#api-response-actions-in-delta-format
