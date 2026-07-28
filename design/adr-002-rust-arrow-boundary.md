# ADR 002: Rust, Delta Kernel, and Arrow boundary

Status: Rust/Kernel/Arrow contract accepted; binding proof pending
Decision owner: package maintainer  
Decision date: 2026-07-28

## Context

The package must execute Delta Kernel correctly, remain highly performant, and
compose with R Arrow consumers. Converting every batch to R vectors, linking
against the Arrow R C++ ABI, or writing Arrow IPC between Rust and R would add
copies and version coupling.

Delta Kernel currently has a pre-1.0 Rust API and selects a specific Arrow Rust
version. The R `{arrow}` package can have a different release cadence.

## Accepted contract and proposed binding

- Integrate Rust package code with `extendr`; use `rextendr` only as a
  development/scaffolding tool.
- Build a static Rust library into the R package shared object.
- Pin Delta Kernel and its default engine behind a single internal adapter.
- Convert kernel `EngineData` to a Rust `RecordBatchReader`.
- Export that reader as an `FFI_ArrowArrayStream`.
- Move the stream into a nanoarrow-allocated R external pointer.
- Import into `{arrow}` or other engines only through the Arrow C Stream ABI.

Rust-owned mutable state, Delta Kernel isolation, and the Arrow C Stream
boundary are accepted. The specialist foundation must still prove that
extendr/rextendr satisfies packaging and lifecycle requirements before the
binding choice is treated as complete.

## Why extendr

extendr has current external-pointer support, generated registered wrappers, R
error handling, and established R-package scaffolding. It is a better default
than maintaining a broad handwritten R C API. Savvy is credible and should be
kept in mind, but switching binding frameworks would not change the Arrow ABI
decision and does not currently offer enough benefit to offset ecosystem risk.

The Arrow stream move/population function should remain deliberately narrow and
may use a small C-compatible entry point rather than trying to make extendr
understand Arrow objects.

## Ownership contract

```text
nanoarrow external pointer
  owns FFI_ArrowArrayStream
    owns Rust RecordBatchReader
      owns kernel scan + engine/client references
      owns temporary synthetic log guard
      owns cancellation token + safe diagnostics
```

The stream release callback drops this ownership chain. Each batch/array release
callback independently keeps its referenced buffers alive.

## Rejected alternatives

### Convert batches to R vectors in extendr

Rejected because it copies data, loses or complicates nested Arrow types, calls
the R allocator in the hot path, and prevents direct Arrow consumers.

### Arrow IPC in memory or on disk

Rejected because serialization is unnecessary inside one process and adds CPU,
buffering, and latency.

### Link directly to the Arrow R C++ ABI

Rejected because it couples two independently versioned native Arrow stacks.

### Reimplement Delta reader features

Rejected because deletion vectors, column mapping, schema transforms, and
future reader features must follow Delta Kernel semantics.

### Custom Delta Kernel engine in the first release

Rejected initially because the default engine already provides Arrow/Tokio and
object-store behavior. The temporary synthetic log is protocol-aligned and much
lower risk. Reconsider only if profiling shows log materialization is material
or a required signed-URL behavior cannot be expressed safely.

## Required proof before implementation

The spike must demonstrate, on macOS, Linux, and Windows:

1. an empty stream with schema;
2. one and multiple record batches;
3. nested, decimal, and timestamp fields;
4. early release before the first/last batch;
5. Rust error before and after one batch;
6. garbage-collection release;
7. `{arrow}` import without IPC;
8. no leaked buffers or temporary directories;
9. panic containment at the native boundary.
