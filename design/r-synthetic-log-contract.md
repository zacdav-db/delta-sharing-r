# R snapshot synthetic-log contract

Status: implemented foundation; Kernel conformance proof pending
Branch owner: `codex/r-synthetic-log-vnext`

## Scope

This contract covers the R-owned preparation and lifetime of the minimal local
Delta log used for one Delta-format snapshot response. It starts after the
response protocol, metadata, and file wrappers have been incrementally decoded
and normalized. It does not perform HTTP, URL refresh, public S7 dispatch,
Kernel invocation, CDF preparation, or Parquet-response normalization.

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
<private root>/table/_delta_log/00000000000000000000.json
```

The commit contains protocol, metadata, and deterministically ordered
`add`/`remove` actions. Sharing-only wrapper fields (`id`, versions,
timestamps, expiry, table size, locations, and access modes) are not copied
into the Delta actions. Table `location` and `auxiliaryLocations` are never
written.

The later native invocation must retain the guard and use
`.snapshot_log_uri(guard)`, which returns an encoded absolute `file:///...`
table URI. `.snapshot_log_path(guard)` exists for local file inspection only.

## Validation and intentionally closed cases

Preparation accepts Delta response format only. Delta protocol
`minReaderVersion` and `minWriterVersion` are required. Metadata must describe
Parquet files and contain a minimally valid Delta struct schema.

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
a mode-0700 private root, writes a mode-0600 commit below a staging directory,
closes the file, and renames the staging table into place on the same
filesystem. A failure at any point recursively removes the exact generated
root and raises a fixed, typed condition that does not copy an input URL,
location, JSON body, or underlying error text.

Presigned data and DV URLs exist only in locked private action state and the
private commit file. The guard's print method reports only active/released
state and action count. Explicit release is idempotent; the guard finalizer is
a fallback. The native Arrow stream must eventually retain this guard until
stream release so Kernel cannot observe a removed log.

## Remaining integration proof

The JSON mapping is specified upstream. The following runtime behavior is not
proven by this R-only slice and remains a G3 integration gate:

- the pinned Delta Kernel default engine reads presigned HTTPS data and DV
  URLs on macOS, Linux, and Windows without changing their encoded query;
- object-store error paths redact those URLs;
- expiry and refresh are coordinated before a stream outlives its URLs;
- Kernel applies remove and deletion-vector semantics to the generated commit;
- early Arrow-stream release and garbage collection release the retained
  temporary root exactly once.

[sharing-protocol]: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#api-response-actions-in-delta-format
