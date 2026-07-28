# Delta Sharing R vNext integration roadmap

Status: active branch checklist
Integration branch: `codex/delta-kernel-s7-overhaul`
Date: 2026-07-28
Last integration update: 2026-07-29

This is the branch-local execution plan for the vNext overhaul. The integration
branch is the long-lived delivery line until every completion gate in this
document is satisfied. Work must not be switched to, merged into, or opened
against `main` during the overhaul.

## Fixed maintainer decisions

- vNext is a clean break. No prior package API or behavior is supported.
- S7 is the public object system. The implementation will not carry an S3
  fallback.
- Public objects are immutable, value-like descriptors.
- Profiles, authentication, HTTP, protocol parsing, planning, synthetic-log
  preparation, diagnostics, and adapters are implemented in R.
- Rust is limited to Delta Kernel invocation and the minimum Arrow/lifecycle
  glue required to expose Kernel output safely.
- Kernel scan state, Kernel-coupled cancellation/resources, and Arrow buffers
  are owned by Rust while a stream is active.
- Expanding Rust beyond that boundary requires the benchmark and ADR gate in
  `adr-003-rust-scope.md`.
- Delta-format reads use Delta Kernel through the narrow Rust bridge.
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
- The design packet originally assigned most client/protocol work to Rust and
  contained prior-version transition requirements. Both decisions are
  superseded by the current R-first, clean-break architecture.
- Three specialist worktrees exist from the same design commit:
  `codex/testing-ci-foundation`, `codex/s7-public-api-r-layer`, and
  `codex/rust-kernel-arrow-stream-foundation`.

## Ownership lanes

| Lane | Branch | Initial scope | Out of scope for the lane |
|---|---|---|---|
| Integration | `codex/delta-kernel-s7-overhaul` | Design authority, sequencing, cross-lane review, conflict resolution, final gates | Reimplementing specialist work before handoff |
| Test and CI | `codex/testing-ci-foundation` | Package metadata, test harness, coverage tooling, CI matrices, package-check automation | Public S7/API implementation and Kernel bridge |
| S7 and R implementation | `codex/s7-public-api-r-layer` | S7 descriptors, profiles/auth, HTTP, protocol, planning, synthetic logs, discovery, R conditions, adapters, R tests/docs | Delta Kernel internals, Arrow C Stream implementation, CI ownership |
| Kernel/Arrow bridge | `codex/rust-kernel-arrow-stream-foundation` | Minimal Rust package skeleton, Kernel adapter, Arrow C Stream, Kernel-coupled lifecycle tests | Auth, HTTP, discovery, protocol parsing, retry, synthetic-log semantics, public API, standalone Parquet reader, CI ownership |

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
7. Public names, S7 class layouts, the compact Kernel invocation, Rust FFI
   symbols, error payloads, and generated registration files are
   integration-sensitive boundaries. Cross-boundary changes require paired R
   and Rust tests.
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
13. An R responsibility moves into Rust only after the controlled performance
    evidence, separate ADR, and maintainer approval required by ADR 003.

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
- [x] Confirm the R-first implementation and minimal Rust/Kernel boundary.
- [x] Establish the benchmark and ADR gate for any additional Rust.
- [x] Confirm Delta Kernel and Arrow-native reader architecture.
- [x] Establish the canonical interface/naming matrix.
- [x] Establish lane ownership and commit handoff rules.
- [x] Update all design artifacts so none promise prior-version behavior.
- [x] Land this roadmap on the integration branch.

Exit gate G0: the committed design packet is internally consistent, public
names are recorded, the Rust scope is limited by ADR 003, and no compatibility
work remains in any implementation phase.

### Phase 1 — independent foundations

#### Test, package, and CI foundation

- [x] Repair package metadata and adopt the intended license.
- [x] Declare the minimum R version; native system requirements remain coupled
  to the Kernel/Arrow landing.
- [x] Add `testthat` edition 3 infrastructure and deterministic fixtures.
- [x] Add R coverage measurement and an initial enforced 80% floor.
- [x] Add `R CMD check`, documentation, and source-package jobs; lint/format
  validation remains a hosted-CI evidence item.
- [x] Add the macOS, Linux, Windows, and R-version matrix.
- [x] Add a pinned Rust advisory, license, and source-policy job.
- [ ] Add Rust sanitizer jobs; MSRV, stable, platform, and coverage jobs are
  already conditional on the native crate landing.

#### S7 API foundation

- [x] Implement the classes and canonical constructors in the interface matrix.
- [x] Prove construction, validation, printing, copying, secret-free inert
  serialization, generic dispatch, and package load/unload.
- [x] Keep descriptors independent of live scan state.
- [x] Implement structured condition classes and secret-safe formatting.
- [x] Generate and check documentation and namespace registration.

#### Minimal Kernel/Arrow foundation

- [x] Add the native package skeleton, pinned Rust toolchain policy, and lockfile.
- [x] Isolate Delta Kernel concrete APIs behind one internal adapter.
- [ ] Define one compact R-to-Kernel invocation contract.
- [x] Prove zero-, one-, and multi-batch Arrow C Streams.
- [x] Prove early release, garbage-collection release, cancellation, errors
  before/after a batch, and panic containment.
- [x] Prove `{arrow}` import through the C Stream without IPC.
- [ ] Prove temporary resources and buffers are not leaked.
- [x] Verify that the native crate contains no client, auth, HTTP, protocol,
  retry, discovery, synthetic-log, or standalone Parquet implementation.

Integration order: land the test/package foundation, refresh and land the S7
foundation, then refresh and land the minimal Kernel/Arrow foundation. If the
native proof needs a minimal R callable before the full S7 layer lands, keep it
internal and replace it at integration.

Exit gate G1: package checks pass locally, the public S7 descriptor spike is
accepted, and Arrow stream lifecycle proof passes on macOS, Linux, and Windows.

### Phase 2 — profile, client, discovery, and metadata

- [x] Parse all supported profile sources and versions in R.
- [x] Implement bearer, OAuth client credentials, JWT assertion, and basic auth
  in R.
- [x] Implement expiry checks and single-flight refresh in R.
- [x] Implement authenticated HTTP, retry/backoff, pagination, and cancellation
  in R.
- [x] Connect `SharingClient` and `SharingTable` to R-owned client state.
- [x] Implement `list_shares()`, `list_schemas()`, and `list_tables()`.
- [x] Implement table version, protocol, metadata, and schema calls.
- [x] Add protocol fixtures, typed errors, and secret-redaction tests.

Exit gate G2: discovery and metadata are complete, paginated, typed, redacted,
and tested without row reading.

### Phase 3 — Delta Kernel snapshot stream

- [x] Validate `SharingRead` in R before native invocation.
- [x] Negotiate capabilities and response format in R from a tested allowlist.
- [x] Parse response headers and NDJSON incrementally in R.
- [x] Build an atomic synthetic Delta log in R.
- [x] Pass only a compact validated scan invocation and prepared log to Rust.
- [ ] Record decoded-action preparation time and peak R memory on
  representative large manifests. If retaining bounded validated actions is
  material, replace the list with a permission-restricted R staging sink
  before G3; this is not a Rust-scope exception.
- [x] Execute projection and scan semantics through Delta Kernel.
- [x] Enforce exact limits across batch boundaries.
- [x] Return a lazy, bounded, single-consumer Arrow C Stream.
- [ ] Cover empty tables, partitions, nested types, timestamps, column mapping,
  deletion vectors, time travel, malformed input, and mid-stream failure.
- [x] Assemble redacted public diagnostics from R-owned planning and selection
  facts without misattributing process-global native counters.

Exit gate G3: snapshot correctness/conformance fixtures pass on every
materialization-neutral stream case, lifecycle cases pass, and performance
meets the stream thresholds in the design plan.

### Phase 4 — materializers, consumers, and diagnostics

- [x] Implement `read_arrow()` as an optional eager `{arrow}` adapter.
- [x] Implement `read_data_frame()` and `as.data.frame()`.
- [x] Prove every adapter consumes the same Arrow stream path.
- [ ] Add DuckDB registration/composition coverage.
- [x] Expose stable, redacted read diagnostics.
- [x] Document eager-memory cost and explicit stream release.

Exit gate G4: Arrow, data-frame, and downstream-consumer fixture results and
schemas agree, with no IPC or full-table R-vector conversion in the stream path.

### Phase 5 — Change Data Feed

- [x] Implement a separate immutable `SharingChanges` descriptor and planner.
- [x] Validate homogeneous version or timestamp bounds before I/O.
- [x] Build the versioned synthetic log required by the Kernel CDF API in R.
- [x] Invoke the narrow Rust bridge only for the Kernel CDF scan.
- [x] Expose only pinned-kernel CDF capabilities. Execution currently requires
  explicit inclusive version bounds; timestamp and open-ended descriptors fail
  with typed unsupported conditions before HTTP.
- [ ] Cover insert/update/delete metadata and unsupported schema ranges.
- [x] Reuse the materializer and lifecycle interfaces without sharing planners.

Exit gate G5: supported CDF fixtures pass across stream, Arrow, and data-frame
outputs on all target platforms; unsupported cases fail before materialization.

### Phase 6 — protocol Parquet response format

- [ ] Parse and normalize Parquet response actions in R.
- [ ] Represent those actions as an R-prepared Kernel-readable synthetic log.
- [ ] Let Delta Kernel stream the referenced objects through the same narrow
  Rust bridge; do not add a standalone Rust Parquet reader.
- [ ] Reconstruct partition values, logical field order, and Arrow types through
  R planning plus Kernel semantics.
- [ ] Apply projection, exact limit, cancellation, and diagnostics consistently.
- [ ] Prove parity with the Delta-format path where both are valid.

Exit gate G6: supported Delta Sharing servers that select the Parquet response
format use the same public descriptors and materializers without the old R
reader architecture.

### Phase 7 — completion hardening

- [ ] Close all correctness and security review findings.
- [ ] Complete R and Rust dependency/license notices and source-build policy.
- [ ] Meet the coverage gates below with reviewed exclusions.
- [x] Enforce and pass the 90% whole-tree R line-coverage gate.
- [x] Enforce and pass the 85% whole-tree Rust line-coverage gate.
- [ ] Pass package checks on minimum, release, and development R.
- [ ] Pass macOS arm64/x86_64, Linux x86_64/arm64, and Windows x86_64 builds.
- [ ] Pass Rust MSRV and stable builds.
- [ ] Pass native lifecycle, sanitizer/valgrind-equivalent, and leak checks.
- [ ] Prove owner-thread R interrupts cancel snapshot, CDF, and synthetic
  streams exactly once on every target; foreign-thread consumers must never
  call the R API.
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
- Moving any client/protocol responsibility from R to Rust additionally
  requires at least a 25% representative end-to-end wall-clock improvement or
  50% peak-memory reduction, plus a separately approved ADR.

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
| 0 | Integration governance | Complete | Roadmap, interface matrix, and ADR 003 |
| 1 | Test/package/CI foundation (`b5734a4`, `aa89e47`, `d4f7836`) | Integrated | `testthat`, initial 80% coverage floor, R/platform matrices, package checks |
| 1 | S7 and R foundation (`fe0d522`, `936f5cf`) | Integrated | Immutable clean-break descriptors, dispatch, validation, documentation, lifecycle guards |
| 1 | Minimal Kernel/Arrow foundation (`2279ee6`) | Local foundation integrated | Registered C shim, pinned Kernel, Arrow C Stream, 15 Rust tests, installed R lifecycle proof; real scan and platform proof remain open |
| 2 | R profiles/auth/control plane (`7379493` through `aa0f8a6`) | Foundations integrated | Profile v1/v2 parsing, bearer/basic/OAuth client auth, retry, pagination, bounded authenticated HTTP |
| 2 | Discovery/metadata planning (`94c4b8f`, `776bd16`, `01cef8b`) | Planning integrated | Safe route planning, incremental NDJSON, metadata projections, current `GET .../version` contract |
| 2 | Public discovery/metadata execution (`e149544`) | Integrated | Raw-segment transport alignment, authenticated callbacks, pagination, bounded parsing, unload reset |
| 3 | R snapshot synthetic log (`497a4f9`) | Integrated | Atomic 0700/0600 preparation, private signed-URL state, deterministic release/finalizer, Kernel URI |
| 3 | R snapshot request/planning (`0b1a709`) | Integrated | Pull-only Query Table transport, bounded incremental NDJSON, pagination consistency, expiry enforcement, and prepared-log invocation |
| 3 | Kernel snapshot execution (`cc71c58`, `941fd46`) | Integrated publicly | Real Kernel Snapshot/Scan, projection, exact limits, bounded Arrow batches, public stream dispatch, and prepared-log lifecycle; platform proof remains open |
| 4 | Eager materializers (`a42e189`) | Integrated | Arrow and data-frame outputs consume one lazy Arrow stream without IPC or a second scan |
| 5 | Explicit-version CDF (`4913f19`) | Integrated | Separate R planner/log, exact inclusive provider versions, Kernel `TableChanges`, shared materializers, typed pre-I/O rejection for timestamp/open-ended ranges |
| 6–7 | Parquet normalization and remaining hardening | Active or open | The R-owned Parquet-to-Kernel mapping is proven; implementation and remaining completion gates stay active |
| 7 | R coverage hardening (`d1598b8`, `4913f19`) | Integrated-tree gate passing | Exact combined snapshot/CDF coverage is 91.34%; tooling and CI enforce the final 90% R gate |
| 7 | Rust coverage evidence | Integrated-tree gate passing | Exact snapshot/CDF Rust line coverage is 85.76% with 36 tests passing; CI enforces the 85% gate |
| 7 | Rust dependency policy | Integrated-tree gate passing | Pinned `cargo-deny` passes advisory, dependency-rule, license, and source checks with four reviewed transitive advisory exceptions |

Current integration evidence: the R planning and native execution handoffs each
pass built-source package checks on macOS arm64, and the integrated native tree
passes strict clippy plus 36 Rust tests. The combined R and explicit-version
CDF tree measures 91.34% R line coverage. A manual `rustc`/LLVM instrumented
coverage run over the locked workspace measures 1,764 of 2,057 lines (85.76%).
Both coverage gates require a final rerun after the remaining feature work.
Cross-platform, final offline packaging, binary-size, diagnostics, Parquet
implementation, and performance/lifecycle gates remain open.
