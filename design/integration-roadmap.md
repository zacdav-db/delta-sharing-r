# Delta Sharing R vNext integration roadmap

Status: active branch checklist
Integration branch: `codex/delta-kernel-s7-overhaul`
Current recorded head: `92c4068`
Last integration update: 2026-07-30

This is the branch-local execution plan for the vNext overhaul. The integration
branch remains the long-lived delivery line until every completion gate below
is satisfied. Work must not be switched to, merged into, pushed to, or opened
against `main` without separate maintainer authorization.

## Governing maintainer decisions

- vNext is a clean break. No prior R package API or behavior is supported.
- **R6 is the public object system.** ADR 004 supersedes the earlier S7
  decision in ADR 001.
- The public model is
  `client -> table -> snapshot()/changes() -> materializer`.
- `SharingClient`, `SharingTable`, `SharingSnapshot`, and `SharingChanges` are
  the canonical public objects. Query configuration is fixed when the reader is
  constructed; configuring a read does not mutate its table or client.
- The old package and the abandoned S7 rewrite are design research only. They
  are not compatibility contracts or test baselines.
- There are no aliases, transition wrappers, deprecation layers, migration
  warnings, or prior-version parity tests.
- Profiles, authentication, HTTP, retry, protocol parsing, pagination,
  planning, synthetic logs, diagnostics, progress semantics, and public
  adapters are implemented in R.
- Rust is limited to Delta Kernel invocation and the minimum Arrow/native
  lifecycle glue required to expose and safely collect Kernel output.
- Kernel-coupled scan state, cancellation, native worker state, and Arrow
  buffers may be owned by Rust while a stream or progress collection is active.
- Expanding Rust beyond that boundary requires the representative benchmark,
  ADR, and maintainer approval described in ADR 003.
- Delta-format and protocol Parquet-format reads both use Delta Kernel through
  the same narrow bridge.
- Arrow C Stream is the materialization boundary. No IPC round trip or separate
  R Parquet reader is introduced.
- The native dependency baseline is Delta Kernel 0.26 with Arrow 58.3 and Rust
  1.88 MSRV.

Support for Delta Sharing protocol/profile versions and the protocol's Parquet
response format is product functionality. It does not imply compatibility with
an earlier R package release.

## Current implementation

### Public R6 surface

```r
client <- sharing_client("~/Desktop/config.share")
orders <- client$table("sales.default.orders")

orders$snapshot(
  columns = c("status", "amount"),
  limit = 1000
)$to_data_frame()

orders$changes(
  starting_version = 10,
  ending_version = 12
)$to_arrow()
```

The same reader also provides:

- `to_arrow_stream()` for the lazy nanoarrow stream;
- `to_arrow_reader()` for an Arrow `RecordBatchReader`;
- `to_arrow()` for eager Arrow materialization;
- `to_data_frame()` for eager R materialization.

### Ownership boundary

R owns:

- profile parsing and authentication;
- requests, retries, pagination, and response parsing;
- table discovery and metadata;
- snapshot and CDF planning;
- protocol action normalization and synthetic-log creation;
- public validation, conditions, progress semantics, and materializers.

Rust owns only:

- Delta Kernel snapshot/CDF construction and scanning;
- the Arrow C Stream adapter, exact batch/row limits, and prepared-log cleanup;
- panic/error containment at the C ABI;
- the native eager-progress worker and its cancellation/library-lifetime guard.

The progress worker accepts an already-created Arrow C stream. It does not
perform HTTP, parse protocol actions, choose formats, or duplicate snapshot/CDF
planning.

## Integration rules

1. All work starts from a known commit on
   `codex/delta-kernel-s7-overhaul`; never from `main`.
2. Handoffs contain coherent commits and report commit SHA, scope, checks,
   known gaps, and required application order.
3. Runtime changes include focused tests. Documentation-only governance commits
   do not need runtime tests.
4. Public R6 method names, query semantics, condition classes, compact native
   invocation fields, FFI symbols, Arrow ownership, and cleanup contracts are
   integration-sensitive boundaries.
5. Cross-boundary changes require paired R/native tests and a review against
   ADR 002 and ADR 003.
6. No specialist silently changes another lane's public contract. Contract
   changes return to the integration owner first.
7. No merge from `main`, compatibility-only code, or unrelated drive-by cleanup
   is accepted.
8. Failed performance experiments remain outside production history unless
   their evidence is deliberately recorded.
9. A phase is complete only when its evidence passes on the integration branch,
   not merely in a specialist worktree.
10. Old S7-era coverage, package-check, or platform evidence does not by itself
    close a gate after the lean R6 rewrite or the new progress worker.

## Current evidence

At `92c4068`:

- The ordinary R suite passes with seven explicitly gated integration skips:
  six public endpoint tests and one credentialed CDF test.
- The locked, offline Rust suite passes all 38 tests.
- `cargo fmt --check` and strict Clippy pass.
- A clean source installation and the installed-package R suite pass locally on
  macOS arm64.
- Live snapshot reads cover small, empty, partitioned, mapped, nested,
  complex-type, 250-million-row, and deletion-vector tables.
- Credentialed CDF versions 1 through 4 return the expected 3,500 rows across
  insert, delete, update-preimage, and update-postimage changes. Timestamp-
  bounded deletion and an open-ended empty result are also live-proven.
- Snapshot progress continues animating while the next Kernel batch is blocked.
  It shows a percentage only when all file row statistics are trustworthy,
  subtracts deletion-vector cardinality, and respects the exact limit.
- CDF progress is deliberately indeterminate because action statistics do not
  reliably equal emitted change rows.

### Performance evidence

Commit `1850bcc` upgraded Kernel 0.22/Arrow 57 to Kernel 0.26/Arrow 58.3 and
configured Kernel source batches from the public batch size, bounded to
1,000–65,536 rows. On the controlled 8,388,608-row local table:

| Path | Earlier median | Current evidence | Change |
|---|---:|---:|---:|
| Direct Arrow | 0.3645 s | 0.1425 s | 60.9% faster |
| Progress-enabled Arrow | 0.7790 s | 0.2025 s | 74.0% faster |

The current direct comparison for the progress-worker experiment was
0.1750 seconds. The source stream changed from 8,448 approximately 1,000-row
batches to 128 batches of 65,536 rows. Live direct/progress samples were within
normal network variance and showed no meaningful regression.

This evidence supports the current narrow native boundary. It does not justify
moving auth, HTTP, protocol, planning, or synthetic-log work into Rust.

## Active phase order

### Phase A — reconcile project and package records

- [x] Record ADR 004 as the governing R6 object-system decision.
- [x] Remove active-roadmap claims that S7 is the public API.
- [x] Record Kernel 0.26, Arrow 58.3, and the progress worker.
- [x] Attribute package copyright to Zac Davies rather than Databricks.
- [ ] Confirm the CDF native preparation error may safely include its underlying
  Kernel detail without exposing a temporary path.

Exit gate A: active documentation, package metadata, and implementation describe
the same public surface and ownership boundary.

### Phase B — progress and native lifecycle hardening

- [x] Unit-test progress status while a source batch is blocked.
- [x] Unit-test successful handoff, repeated finish, cancellation observation,
  detached-worker library pinning, and panic containment.
- [x] Translate owner-thread R interruption to `delta_sharing_cancelled`.
- [x] Leave lazy and `progress = FALSE` paths on the direct stream.
- [ ] Interrupt a credentialed live read while it is genuinely blocked on
  object storage.
- [x] Prove cleanup, garbage collection, package unload/reload, and reinstall
  after normal completion.
- [x] Prove detached-worker library pinning and safe installed-package reuse in
  local subprocess gates.
- [ ] Run the detached blocked-worker policy under hosted sanitizer/leak
  tooling.
- [ ] Pass owner-thread interrupt and foreign-thread no-R-API checks on macOS,
  Linux, and Windows.

Exit gate B: normal, error, interrupt, finalizer, unload, and detached-worker
paths are leak-free and safe on every target.

### Phase C — snapshot manifest performance in R

- [x] Identify the current repeated
  `actions <- c(actions, page_actions)` retention/copying boundary.
- [x] Add reproducible fresh-process manifest workloads at 1,000 and 100,000
  realistic file actions.
- [ ] Measure HTTP-body parsing through first Kernel batch: wall time, first-
  batch latency, peak RSS, retained R objects, temporary bytes, and cleanup.
- [x] Implement incremental snapshot action staging entirely in R.
- [x] Retain only protocol/metadata, pagination state, diagnostics, and bounded
  write buffers.
- [x] Preserve response validation, exact limits, permissions, handoff,
  redaction, and cleanup failures.
- [x] Compare the prototype with the committed baseline and keep it only when
  the result is materially better without unacceptable ordinary-read cost.

Exit gate C: large snapshot manifests no longer require retention of the full
nested action graph, and representative performance evidence is recorded.
That core gate is met; HTTP-to-first-batch and final-candidate remote evidence
remain Phase E work.

### Phase D — bounded CDF staging in R

- [x] Begin only after the snapshot staging shape is accepted.
- [x] Add a reproducible 100,000-action/100-version workload and use the
  existing sparse-version, metadata-evolution, and live CDF fixtures.
- [x] Prototype bounded version-aware spooling without assuming provider ordering
  that the protocol does not guarantee.
- [x] Preserve every synthetic commit required by Kernel, including empty
  interior commits and the checkpoint bootstrap for starts above zero.
- [x] Prove all four change types, schema evolution, timestamps, open-ended
  ranges, cleanup failures, and exact effective bounds.
- [x] Measure wall time and peak RSS against the committed baseline.

Decision D: the prototype was byte-identical and passed credentialed live CDF,
but 24.9% lower maximum RSS came with 63.2% slower transformation at 100,000
actions. It cleared neither ADR 003 threshold and was not integrated.
Production remains the concise R-owned retained path.

### Phase E — representative connector benchmarks

- [x] Compare the R package and current Python connector on the same share and
  source versions.
- [x] Include small snapshot, many-file snapshot, bounded 250-million-row
  nested/deletion-vector snapshot, large CDF, and empty results.
- [x] Compare eager data-frame output and exact result shapes.
- [ ] Extend the connector matrix to Arrow reader/stream and DuckDB paths where
  both connectors expose a comparable form.
- [ ] Record control-plane time, time to first batch, peak RSS,
  emitted batches, rows, projection, limit, and progress on/off.
- [x] Record total wall time, rows, columns, bounds, connector versions, and the
  exact same-profile commands.
- [x] Use controlled local fixtures for regression gates; treat remote samples
  as directional because server, cache, network, and object-store variance are
  material.
- [ ] Reassess format/metadata caching only after the manifest work.

Exit gate E: performance decisions are based on reproducible end-to-end
evidence rather than isolated microbenchmarks.

### Phase F — package and platform completion

- [x] Recalculate whole-tree R coverage for the lean R6 implementation:
  current evidence is 70.46%.
- [ ] Raise current R6 line coverage to at least 90% with reviewed exclusions
  and keep the gate enforced.
- [ ] Recalculate Rust coverage after the Kernel upgrade and progress worker and
  enforce at least 85% line coverage with direct lifecycle coverage.
- [ ] Pass `R CMD check` with zero errors, warnings, and unexplained notes on
  minimum, release, and development R.
- [x] Pass the current production implementation archive locally on macOS
  arm64 with zero
  errors, zero warnings, and one expected new-submission/development note.
- [ ] Pass source builds/tests on macOS arm64/x86_64, Linux x86_64/arm64, and
  Windows x86_64.
- [ ] Pass Rust 1.88 MSRV, stable, locked/offline vendor, advisory, source, and
  dependency-license gates at the final candidate.
- [ ] Prove release binaries do not require end users to install Rust.
- [ ] Run installed-package ASan/LSan or platform-equivalent lifecycle gates.
- [ ] Execute optional `{arrow}` and `{duckdb}` integration paths.
- [ ] Prove a genuine provider-signed deletion-vector URL, including signature
  and expiry behavior, on hosted targets.
- [ ] Build/install the exact final source archive and target binaries in clean
  libraries.
- [ ] Recheck README, reference documentation, examples, and the handover
  against the exact candidate.
- [ ] Rotate the exposed Desktop bearer token and remove it from branch history
  before any push or external handoff.

Exit gate F: all definition-of-done evidence is current for the lean R6 tree and
the exact artifacts intended for release.

## Completion gates

### Correctness and package quality

- Supported snapshot and CDF fixtures agree across lazy stream, Arrow, and data
  frame outputs.
- Public constructor and validation branches are covered.
- Every native ownership boundary has focused lifecycle tests.
- Generated documentation and namespace files are current.
- Installation, load, unload, reinstall, and optional-package behavior pass in
  clean libraries.
- Flaky reruns do not count as a passing gate.

### Portability

- macOS arm64/x86_64, Linux x86_64/arm64, and Windows x86_64 are evidenced by
  final-candidate builds and tests.
- Minimum R, current R, R-devel, Rust MSRV, and Rust stable are evidenced.
- Release users do not need a Rust toolchain.

### Performance and lifecycle

- R Arrow-stream throughput remains at least 90% of the comparable Rust-only
  Kernel scan on controlled fixtures.
- FFI overhead remains below 2% for batches of at least 64K rows.
- Direct streaming RSS is bounded by in-flight batches plus fixed engine
  overhead, not total table size.
- Backpressure, early limit, progress polling, explicit release, R interrupt,
  garbage collection, mid-stream error, and process unload are measured.
- No credential, signed URL, buffer, thread, or temporary-directory leak is
  accepted.
- Moving any client/protocol responsibility from R to Rust additionally
  requires at least a 25% representative end-to-end wall-clock improvement or
  50% peak-memory reduction, plus an approved ADR.

### Documentation and release readiness

- Every exported function and public R6 class has executable examples where
  practical.
- Architecture and ownership documents match the implementation.
- Supported protocol/profile/auth/Kernel features and limitations are explicit.
- The README teaches only the canonical vNext R6 interface.
- The package contains no legacy aliases, shims, deprecations, S7 API
  documentation, or prior-version behavior tests.

## Integration ledger

The early ledger is retained as history. Its S7 artifacts and old evidence are
not the current public implementation or completion proof.

| Period | Commit(s) | Status | Meaning |
|---|---|---|---|
| Initial governance | `b5734a4` onward | Historical foundation | Established clean-break, R-first, Kernel/Arrow, test, CI, and portability goals. The original S7 choice was later superseded. |
| S7 implementation | `fe0d522`, `936f5cf`, later phase commits | Superseded | Built the first vNext implementation and much of the lifecycle/CI evidence. ADR 004 and `e56404c` removed this public surface. |
| Portability/security scaffolding | `2494378`, `4d89ca9`, `cfdad9e`, `e61a221`, `d4347b9`, `e97adfd` | Infrastructure retained; proof must be rerun | Offline vendoring, package artifacts, hosted matrices, Windows interrupt, and sanitizer jobs remain useful, but old passes do not close current-head gates. |
| Lean R6 rewrite | `e56404c` | Current architecture integrated | Replaced the S7 implementation with the compact R6 client/table/read surface and R-owned protocol stack; ADR 004 governs. |
| Live helper correction | `f047384` | Integrated | Uses the Desktop profile explicitly for the large live progress demonstration. |
| Kernel/Arrow upgrade | `1850bcc` | Integrated and locally proven | Delta Kernel 0.26, Arrow 58.3, larger Kernel source batches, offline vendor/license updates, and controlled performance evidence. |
| Live eager progress | `92c4068` | Integrated and locally/live proven | Native Arrow collection worker, continuous spinner, trustworthy snapshot percentages, indeterminate CDF progress, cancellation and unload guards, and focused R/native tests. |

Only after every open gate is evidenced on this integration branch is the
overhaul eligible for a separately authorized release or main-line integration.
