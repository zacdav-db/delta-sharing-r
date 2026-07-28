# Delta Sharing R vNext integration roadmap

Status: active branch checklist
Integration branch: `codex/delta-kernel-s7-overhaul`
Date: 2026-07-28

This is the branch-local execution plan for the vNext overhaul. The integration
branch is the long-lived delivery line until every completion gate in this
document is satisfied. Work must not be switched to, merged into, or opened
against `main` during the overhaul.

## Fixed maintainer decisions

- vNext is a clean break. No prior package API or behavior is supported.
- S7 is the public object system. The implementation will not carry an S3
  fallback.
- Public objects are immutable, value-like descriptors.
- Mutable client, scan, cancellation, and lifecycle state is owned by Rust.
- Delta-format reads are implemented with Delta Kernel.
- Arrow C Stream is the primary materialization boundary.
- The public model is `client -> table -> read -> materializer`.
- Naming follows the client/table/read terminology described in
  `s7-interface-naming-matrix.md`.
- The existing R6 package is design research only. It is not an API contract,
  a behavioral oracle, or a test baseline.
- There will be no aliases, transition wrappers, deprecation layers, migration
  warnings, or prior-version parity tests.

Support for Delta Sharing protocol/profile versions and the protocol's Parquet
response format is product functionality. It does not imply compatibility with
an earlier R package release.

## Repository audit at roadmap creation

- The package is an R6 client and mutable table reader.
- Protocol requests, NDJSON parsing, schema conversion, downloads, and
  materialization currently happen in R.
- Reads download referenced Parquet files before opening an Arrow Dataset.
- The package has no committed automated tests or CI.
- `DESCRIPTION` has unresolved metadata, an undeclared R6 dependency, and an
  eager `{arrow}` dependency.
- `NAMESPACE` and generated help describe only the current R6 implementation.
- The design packet selects the correct Rust/Kernel/Arrow architecture but
  originally contained prior-version transition requirements that are now
  explicitly removed.
- Three specialist worktrees exist from the same design commit:
  `codex/testing-ci-foundation`, `codex/s7-public-api-r-layer`, and
  `codex/rust-kernel-arrow-stream-foundation`.

## Ownership lanes

| Lane | Branch | Initial scope | Out of scope for the lane |
|---|---|---|---|
| Integration | `codex/delta-kernel-s7-overhaul` | Design authority, sequencing, cross-lane review, conflict resolution, final gates | Reimplementing specialist work before handoff |
| Test and CI | `codex/testing-ci-foundation` | Package metadata, test harness, coverage tooling, CI matrices, package-check automation | Public S7 API design and Rust reader implementation |
| S7 R API | `codex/s7-public-api-r-layer` | S7 descriptors, constructors, validation, generics, R conditions, R-level tests/docs | Rust bridge, Kernel adapter, CI ownership |
| Rust/Kernel/Arrow | `codex/rust-kernel-arrow-stream-foundation` | Rust package skeleton, FFI ownership, Arrow C Stream, Kernel isolation, native lifecycle tests | Public API naming and CI ownership |

Later phases may get narrower specialist lanes. Every new lane must have a
written file boundary and an integration dependency before work begins.

## Integration rules

1. All specialist work starts from a known commit on
   `codex/delta-kernel-s7-overhaul`; never from `main`.
2. A specialist hands off coherent commits, not an uncommitted worktree diff.
   Generated files belong in the same commit as their source when practical.
3. Each handoff includes:
   - commit SHA(s), in application order;
   - scope and files changed;
   - checks run and their results;
   - required follow-up or cross-lane dependency;
   - known failures, platform gaps, and performance/lifecycle evidence.
4. The integration owner reviews the diff and checks it against this roadmap
   before applying it. Commits are cherry-picked in dependency order.
5. The default initial order is test/metadata foundation, S7 API foundation,
   then Rust/Arrow foundation. A later handoff must be refreshed from the
   integration head if an earlier landing changes overlapping package files.
6. Specialists do not resolve overlap by silently changing another lane's
   public contract. Contract changes return to the integration owner and the
   interface matrix is updated first.
7. Public names, S7 class layouts, Rust FFI symbols, error payloads, and
   generated registration files are integration-sensitive boundaries.
   Cross-boundary changes require paired R and Rust tests.
8. No merge commit from `main`, no compatibility-only code, and no drive-by
   cleanup is accepted.
9. Every landed production-code commit includes focused tests. A commit may
   add only governance/design documentation without a runtime test.
10. After each landing, the integration owner runs the strongest locally
    available gate and records any platform-only evidence still required.
11. A phase is complete only when its exit gate is evidenced on the
    integration branch. A specialist branch passing alone is insufficient.
12. Failed or partial experiments remain outside the integration history
    unless the experiment itself is an agreed, documented proof artifact.

## Handoff template

```text
Lane:
Integration base:
Commits:
Scope:
Files/boundaries changed:
Checks:
Required evidence not available locally:
Known gaps:
Recommended application order:
```

## Phase order and checklist

### Phase 0 — decisions and integration control

- [x] Confirm S7.
- [x] Confirm the clean vNext break.
- [x] Confirm Rust-owned mutable state.
- [x] Confirm Delta Kernel and Arrow-native reader architecture.
- [x] Establish the canonical interface/naming matrix.
- [x] Establish lane ownership and commit handoff rules.
- [x] Update all design artifacts so none promise prior-version behavior.
- [x] Land this roadmap on the integration branch.

Exit gate G0: the committed design packet is internally consistent, public
names are recorded, and no compatibility work remains in any implementation
phase.

### Phase 1 — independent foundations

#### Test, package, and CI foundation

- [ ] Repair package metadata and adopt the intended license.
- [ ] Declare minimum R and system requirements.
- [ ] Add `testthat` edition 3 infrastructure and deterministic fixtures.
- [ ] Add R coverage measurement and an initial enforced floor.
- [ ] Add `R CMD check`, lint/format, documentation, and source-package jobs.
- [ ] Add the macOS, Linux, Windows, R-version, and optional-dependency matrix.
- [ ] Define how Rust MSRV/stable, audit, sanitizer, and coverage jobs attach.

#### S7 API foundation

- [ ] Implement the classes and canonical constructors in the interface matrix.
- [ ] Prove construction, validation, printing, copying, serialization
  rejection, generic dispatch, and package load/unload.
- [ ] Keep descriptors independent of live scan state.
- [ ] Implement structured condition classes and secret-safe formatting.
- [ ] Generate and check documentation and namespace registration.

#### Rust/Arrow foundation

- [ ] Add the native package skeleton, pinned Rust toolchain policy, and lockfile.
- [ ] Isolate Delta Kernel concrete APIs behind one internal adapter.
- [ ] Prove zero-, one-, and multi-batch Arrow C Streams.
- [ ] Prove early release, garbage-collection release, cancellation, errors
  before/after a batch, and panic containment.
- [ ] Prove `{arrow}` import through the C Stream without IPC.
- [ ] Prove temporary resources and buffers are not leaked.

Integration order: land the test/package foundation, refresh and land the S7
foundation, then refresh and land the Rust/Arrow foundation. If the Rust proof
needs a minimal R callable before the full S7 layer lands, keep it internal and
replace it at integration.

Exit gate G1: package checks pass locally, the public S7 descriptor spike is
accepted, and Arrow stream lifecycle proof passes on macOS, Linux, and Windows.

### Phase 2 — profile, client, discovery, and metadata

- [ ] Parse all supported profile sources and versions in Rust.
- [ ] Implement bearer, OAuth client credentials, JWT assertion, and basic auth.
- [ ] Implement expiry checks and single-flight refresh.
- [ ] Implement pooled HTTP, retry/backoff, pagination, and cancellation.
- [ ] Connect `SharingClient` and `SharingTable` descriptors to Rust handles.
- [ ] Implement `list_shares()`, `list_schemas()`, and `list_tables()`.
- [ ] Implement table version, protocol, metadata, and schema calls.
- [ ] Add protocol fixtures, typed errors, and secret-redaction tests.

Exit gate G2: discovery and metadata are complete, paginated, typed, redacted,
and tested without row reading.

### Phase 3 — Delta Kernel snapshot stream

- [ ] Validate `SharingRead` in R and Rust.
- [ ] Negotiate capabilities and response format from a tested allowlist.
- [ ] Parse response headers and NDJSON incrementally.
- [ ] Build and guard an atomic synthetic Delta log.
- [ ] Execute projection and scan semantics through Delta Kernel.
- [ ] Enforce exact limits across batch boundaries.
- [ ] Return a lazy, bounded, single-consumer Arrow C Stream.
- [ ] Cover empty tables, partitions, nested types, timestamps, column mapping,
  deletion vectors, time travel, malformed input, and mid-stream failure.
- [ ] Record diagnostic counts without credentials or signed URLs.

Exit gate G3: snapshot correctness/conformance fixtures pass on every
materialization-neutral stream case, lifecycle cases pass, and performance
meets the stream thresholds in the design plan.

### Phase 4 — materializers, consumers, and diagnostics

- [ ] Implement `read_arrow()` as an optional eager `{arrow}` adapter.
- [ ] Implement `read_data_frame()` and `as.data.frame()`.
- [ ] Prove every adapter consumes the same Arrow stream path.
- [ ] Add DuckDB registration/composition coverage.
- [ ] Expose stable, redacted read diagnostics.
- [ ] Document eager-memory cost and explicit stream release.

Exit gate G4: Arrow, data-frame, and downstream-consumer fixture results and
schemas agree, with no IPC or full-table R-vector conversion in the stream path.

### Phase 5 — Change Data Feed

- [ ] Implement a separate immutable `SharingChanges` descriptor and planner.
- [ ] Validate homogeneous version or timestamp bounds before I/O.
- [ ] Build the versioned synthetic log required by the Kernel CDF API.
- [ ] Expose only pinned-kernel CDF capabilities.
- [ ] Cover insert/update/delete metadata and unsupported schema ranges.
- [ ] Reuse the materializer and lifecycle interfaces without sharing planners.

Exit gate G5: supported CDF fixtures pass across stream, Arrow, and data-frame
outputs on all target platforms; unsupported cases fail before materialization.

### Phase 6 — protocol Parquet response format

- [ ] Implement the Parquet action path in Rust.
- [ ] Stream signed objects directly; do not restore package-managed downloads.
- [ ] Reconstruct partition values, logical field order, and Arrow types.
- [ ] Apply projection, exact limit, cancellation, and diagnostics consistently.
- [ ] Prove parity with the Delta-format path where both are valid.

Exit gate G6: supported Delta Sharing servers that select the Parquet response
format use the same public descriptors and materializers without the old R
reader architecture.

### Phase 7 — completion hardening

- [ ] Close all correctness and security review findings.
- [ ] Complete R and Rust dependency/license notices and source-build policy.
- [ ] Meet the coverage gates below with reviewed exclusions.
- [ ] Pass package checks on minimum, release, and development R.
- [ ] Pass macOS arm64/x86_64, Linux x86_64/arm64, and Windows x86_64 builds.
- [ ] Pass Rust MSRV and stable builds.
- [ ] Pass native lifecycle, sanitizer/valgrind-equivalent, and leak checks.
- [ ] Meet throughput, FFI overhead, RSS, backpressure, first-batch, and
  cancellation performance gates.
- [ ] Finish reference documentation, README, architecture notes, examples, and
  vignettes for the vNext API.
- [ ] Build and install source and binary artifacts in clean environments.
- [ ] Remove superseded R6 implementation, help, and dependencies.
- [ ] Confirm there are no aliases, shims, deprecations, or prior-version tests.

Exit gate G7: every definition-of-done item is evidenced on the integration
branch. Only then is the overhaul eligible for a separately authorized release
or main-line integration.

## Completion gates

### Correctness and tests

- All required protocol and Kernel fixtures pass for every supported output.
- Public constructor and validation branches are covered.
- Every unsafe/native ownership boundary has a focused lifecycle test.
- R line coverage is at least 90%; Rust line coverage is at least 85%.
  Security, redaction, cancellation, and release modules require direct tests
  regardless of aggregate coverage.
- Flaky-test reruns do not count as a passing gate.

### Package quality

- `R CMD check` has zero errors, warnings, and unexplained notes.
- Generated documentation and namespace files are current.
- The package installs, loads, unloads, and reinstalls in a clean library.
- The package works with optional `{arrow}` absent and present.

### Portability

- macOS arm64/x86_64, Linux x86_64/arm64, and Windows x86_64 are evidenced by
  clean source builds and tests.
- Minimum R, current R, R-devel, Rust MSRV, and Rust stable are evidenced.
- Release binaries do not require end users to install Rust.

### Performance and lifecycle

- R Arrow-stream throughput is at least 90% of the same Rust-only Kernel scan
  on the controlled fixtures.
- FFI overhead is below 2% for batches of at least 64K rows.
- Streaming RSS is bounded by configured in-flight batches plus fixed engine
  overhead, not total table size.
- Backpressure, early limit, explicit release, R interrupt, garbage collection,
  mid-stream error, and process-unload behavior are measured and pass.
- No credential, signed URL, buffer, thread, or temporary-directory leak is
  accepted.

### Documentation and release readiness

- Every exported function and S7 class has executable examples where practical.
- Architecture and ownership contracts match the implementation.
- Supported protocol/profile/auth/Kernel features and known limitations are
  explicit.
- The README teaches only the canonical vNext interface.
- The package contains no documentation for removed R6 names or behavior.

## Integration ledger

| Phase | Lane/commit | Integration status | Evidence |
|---|---|---|---|
| 0 | Integration governance | Complete | This roadmap and interface matrix |
| 1 | Test/package/CI foundation | Pending handoff | — |
| 1 | S7 API foundation | Pending handoff | — |
| 1 | Rust/Arrow foundation | Pending handoff | — |
| 2–7 | Feature and hardening lanes | Not started | — |
