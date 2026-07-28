# R CDF version-mapping proof

Status: Phase 5 implementation gate
Branch owner: `codex/cdf-planning-vnext`

This proof fixes the only accepted mapping from a Delta Sharing change-data
response to Delta Kernel 0.22. It is a clean vNext design. It does not preserve
or emulate any earlier package API.

## Decision

Proceed with a versioned synthetic Delta log and Kernel `TableChanges`.

Provider commit versions are never rebased. A provider action at version `v`
is written to `_delta_log/%020d.json` using `v` itself, and Kernel is invoked
with the inclusive provider range `start_version..=end_version`. Provider
timestamps are retained as commit-file modification times in milliseconds.
When the Delta protocol supplies `inCommitTimestamp`, Kernel uses that value
instead.

No offset map, R-side Parquet reader, or R-side CDF row synthesis is permitted.

## Source evidence

The audit used these exact upstream sources:

- [Delta Sharing CDF protocol][sharing-cdf], including inclusive version and
  timestamp bounds and the required `_change_type`, `_commit_version`, and
  `_commit_timestamp` output columns.
- [Delta Sharing Delta-format actions][sharing-actions], in which each CDF file
  wrapper carries its source `version`, `timestamp`, and `deltaSingleAction`.
- [Delta change-data files][delta-cdf], including the rule that `cdc` actions
  take precedence over add/remove inference for the same commit.
- `delta-io/delta-sharing` commit
  `4b790695e45bc66a7531f0ddd264725718ee2fcc`. Its Python Kernel reader groups
  actions by their provider version, writes true-version commit filenames,
  bootstraps a bounded log with an empty checkpoint at `start_version - 1`,
  and calls `TableChanges::try_new()` with the true inclusive range. Its Scala
  reader independently groups CDF actions by the same provider versions and
  timestamps.
- `delta_kernel` crate `0.22.0`, pinned by this package. Its
  `LogSegment::for_table_changes()` requires the first commit filename to equal
  `start_version` and all following versions to be contiguous.
  `TableChanges::try_new()` builds snapshots at the exact start and end
  versions. Its replay code derives `_commit_version` from the commit filename
  and `_commit_timestamp` from the commit file's last-modified time, except
  when the table's protocol selects `inCommitTimestamp`.

[sharing-cdf]: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#read-change-data-feed-from-a-table
[sharing-actions]: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#api-response-actions-in-delta-format
[delta-cdf]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-data-files

## Exact mapping

| Sharing wire record | Private synthetic-log representation | Kernel meaning |
|---|---|---|
| Head `protocol` | Bootstrap protocol action in the first requested commit | Reader protocol for the bounded log |
| Historical `protocol` with version `v` | Protocol action in commit `v` | Protocol active from provider version `v` |
| Head `metaData` | Bootstrap metadata action in the first requested commit | Schema and CDF table configuration at the range start |
| Historical `metaData` with version `v` | Metadata action in commit `v` | Metadata active from provider version `v` |
| File wrapper with version `v`, timestamp `t`, and `add` | Normalized `add` action in commit `v`; commit mtime `t` | Insert inference when the commit has no `cdc` action |
| File wrapper with version `v`, timestamp `t`, and `remove` | Normalized `remove` action in commit `v`; commit mtime `t` | Delete inference when the commit has no `cdc` action |
| File wrapper with version `v`, timestamp `t`, and `cdc` | Normalized `cdc` action in commit `v`; commit mtime `t` | Explicit change rows and their source `_change_type` |

For `start_version > 0`, an empty checkpoint is written at
`start_version - 1` with a matching `_last_checkpoint`. This gives Kernel a
valid pre-range snapshot without inventing or renumbering provider commits.
Every commit from the resolved start through resolved end is present, including
an empty JSON file when the provider returned no action for that version.

The first requested commit is allowed to contain bootstrap protocol and
metadata alongside its real provider actions. These actions establish the
state that was already active at the range boundary; they do not change the
commit version used for CDF output.

## R and Rust ownership

R owns:

- bound validation and request planning;
- authentication, HTTP retries, pagination, and incremental NDJSON parsing;
- validation of required versions and timestamps;
- protocol, metadata, and file-action normalization;
- grouping actions into the exact provider commit versions;
- expiry checks and deterministic private-log cleanup.

Rust owns only:

- `TableChanges::try_new()` and scan construction;
- exact Arrow projection and batch streaming;
- the existing Arrow C Stream and cleanup-guard lifecycle.

## Rejection rules

Preparation fails with a typed condition before native execution when:

- the response is not Delta format;
- a CDF file wrapper omits its version or timestamp;
- one provider version has conflicting file timestamps;
- the resolved provider range cannot be proven;
- historical protocol or metadata cannot be placed at its provider version;
- the resolved start/end are invalid or exceed the bounded range limit;
- an action, feature, projection, or storage reference is unsupported.

Secrets, URLs, paths, tokens, provider payloads, and bearer values are excluded
from conditions and diagnostics.

## Executable gate

Passed locally on macOS arm64. A Kernel 0.22 test constructs provider versions
1–2 with only an empty checkpoint at version 0, true commit filenames, and
distinct millisecond mtimes. Arrow output retains versions 1 and 2, their exact
timestamps, and the source `delete`/`insert` change types. Separate regressions
cover a zero-start range without a checkpoint, an empty bounded range, and the
inclusive 1–1 versus 1–2 upper bound.

Kernel 0.22 panics when a scan projection contains only the three CDF metadata
columns because its physical read schema is empty. The native adapter contains
this edge by adding one hidden data column to the Kernel scan and projecting it
away from each Arrow batch. The public schema and batches remain exactly the
requested metadata-only projection.

The adapter has a direct pinned `url` dependency because the public
`TableChanges::try_new()` API requires `url::Url`; Kernel's convenience
`try_parse_uri` export is gated behind its private `internal-api` feature,
which this package deliberately does not enable.

Adding the registered CDF stream entry point extends the package's native ABI,
so native diagnostics report ABI version 3. The `.Call` registration remains
fixed-symbol-only and package unload continues to reap pending prepared-log
cleanups before the namespace is released.

This local proof does not close the cross-platform G5 gate. macOS, Linux, and
Windows package builds and exact-mtime lifecycle runs remain required on the
integration branch.
