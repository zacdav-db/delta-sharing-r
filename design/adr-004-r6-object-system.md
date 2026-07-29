# ADR 004: R6 object system for the redesign

Status: accepted
Decision owner: package maintainer
Decision date: 2026-07-29
Supersedes: ADR 001 (S7 object system)

## Context

ADR 001 chose S7 for high-level descriptors and functional generics. In
practice the S7 implementation grew into a large, heavily abstracted surface:
immutable descriptors backed by private-handle registries and finalizers, a
parallel set of functional generics, a callback-based execution interface
between the public API and the work, read-only property scaffolding, and a
standalone diagnostics object hierarchy. The R source reached ~12,800 lines
across 30 files with 26 exported symbols.

The redesign goal is a small, clean user surface that leans on the Delta Kernel
(via a narrow Rust bridge) for the row hot path and on well-known R packages
(`httr2`, `openssl`, `jsonlite`, `nanoarrow`) for everything else, with no
backwards-compatibility layer.

The intended API is a staged object model — `client -> table ->
snapshot/changes -> materializer` — mirroring the Python delta-sharing object
model (delta-io/delta-sharing #862/#949) but expressed idiomatically for R.
Query options live on the `snapshot()` / `changes()` step; materializers hang
off the returned object. This is a reference-semantics, method-on-object shape.

The pre-overhaul package (`main`) used R6 for exactly this shape and was
straightforward to read.

## Decision

Use **R6** for the public object model: `SharingClient`, `SharingTable`, and the
snapshot/changes reader objects are R6 classes with methods. Retain S3 methods
only for established external generics (`print()`, `as.data.frame()`).

- Query configuration is expressed as arguments to `snapshot()` / `changes()`,
  which return reader objects carrying those options. Tables and clients are
  cheap and reusable; configuring a read does not mutate them.
- Mutable authentication state (tokens, OAuth caches, private keys) lives in R6
  private fields, not in a separate registry/finalizer mechanism.
- The exported functions and documented methods — not internal fields — are the
  public contract. There is no requirement to preserve the S7 surface or any
  prior release surface. No aliases, deprecations, or migration shims.

## Consequences

- Reverses ADR 001. S7 classes, functional generics, `.readonly_property`
  scaffolding, the execution-interface callback registry, the private-handle
  registries, and the diagnostics hierarchy are removed. `S7` leaves
  `DESCRIPTION` Imports; `R6` is added.
- The API reads like idiomatic reference-semantics R and matches the Python
  staged model conceptually:

  ```r
  client <- sharing_client("~/config.share")
  orders <- client$table("sales.default.orders")
  orders$snapshot(version = 42, limit = 1000)$to_data_frame()
  orders$changes(starting_version = 120, ending_version = 125)$to_arrow()
  ```

- R6 methods are documented with roxygen `@description` blocks on the class so
  the pkgdown reference renders method signatures (R6 methods are not
  auto-listed as separate topics the way S7 generics are).

## Scope note (unaffected ADRs)

This ADR changes only the object system. The Rust/Kernel/Arrow boundary
(ADR 002) and the R-first / minimal-Rust scope (ADR 003) are unchanged: the
synthetic-log transform stays in R (streamed to disk, per the redesign plan),
Rust remains limited to the kernel scan and Arrow stream lifecycle, and no new
FFI operation is introduced by the object-system change.
