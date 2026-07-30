# ADR 002: Rust, Delta Kernel, and Arrow boundary

Status: Rust/Kernel/Arrow and slim C binding accepted; platform proof pending
Decision owner: package maintainer
Decision date: 2026-07-28

## Context

The package uses R for its client and protocol implementation but must execute
Delta Kernel correctly and compose with R Arrow consumers. Converting every
batch to R vectors, linking against the Arrow R C++ ABI, or writing Arrow IPC
between Rust and R would add copies and version coupling.

Delta Kernel currently has a pre-1.0 Rust API and selects a specific Arrow Rust
version. The R `{arrow}` package can have a different release cadence.

## Accepted contract and binding

- Use a narrow registered C `.Call` shim for R argument validation and routine
  registration.
- Expose pure Rust `extern "C"` status functions that never call the R API.
- Build a static Rust library into the R package shared object.
- Pin Delta Kernel and its default engine behind a single internal adapter.
- Convert kernel `EngineData` to a Rust `RecordBatchReader`.
- Export that reader as an `FFI_ArrowArrayStream`.
- Move the stream into a nanoarrow-allocated R external pointer.
- Import into `{arrow}` or other engines only through the Arrow C Stream ABI.

Rust-owned mutable state, Delta Kernel isolation, and the Arrow C Stream
boundary are accepted only for Kernel execution. R owns the client, HTTP,
protocol, and planning responsibilities described in `adr-003-rust-scope.md`.
The binding foundation must still prove packaging and lifecycle requirements
on every supported platform before Phase 1 is complete.

The native library exports Kernel invocation and Arrow stream operations only.
It must not grow authentication, discovery, Delta Sharing HTTP, NDJSON parsing,
retry policy, or a separate Parquet reader.

## Why the registered C shim

The package needs only a few control-plane calls at the native boundary. A
small registered C shim validates R values, extracts the nanoarrow stream
pointer, and converts a completed Rust status into an R result. Rust owns no R
objects and calls no R API. This keeps long jumps out of Rust frames and avoids
adding a general R binding framework plus its transitive source surface.

This is not a broad handwritten R API. New C/Rust entry points remain subject
to ADR 003 and must be necessary for Kernel invocation, Arrow transport, or
Kernel-coupled lifecycle.

## Ownership contract

```text
nanoarrow external pointer
  owns FFI_ArrowArrayStream
    owns Rust RecordBatchReader
      owns kernel scan + engine references
      may own one capability-checked prepared-log cleanup token
      owns Kernel cancellation + native metrics
```

The stream release callback drops this ownership chain. Each batch/array release
callback independently keeps its referenced buffers alive. Rust never retains
the R guard itself. After successful handoff, R transfers only the exact private
root cleanup capability; native code verifies its marker, canonical
`root/table` relation, exact directory shape, permissions, and absence of
symlinks before it can remove any expected path. It records filesystem
identity at handoff and repeats the complete no-follow validation immediately
before removal; replacement or mutation fails closed. On exhaustion, error,
panic, and explicit release, the Kernel reader is dropped before the cleanup
token.

Removal is staged as individual file and empty-directory operations; native
code never recursively removes a post-handoff tree. A transient failure is
retried three times, then the root, stable identity, and current stage enter a
process-local pending queue. Every later native entry point and `.onUnload`
run the bounded reaper after the same stage-specific validation. A mutated or
replaced root is abandoned fail-closed. A same-UID process can still race one
path operation after validation, but the non-recursive operations can remove
only the expected file/symlink itself or an already-empty directory; they
cannot follow or recursively delete injected content.

## R interrupt boundary

Every package-created Arrow C Stream has one small C wrapper outside the
arrow-rs stream. The wrapper records the native thread that completed the
registered `.Call` construction. Immediately before each `get_next` delegation
on that exact thread, it calls `R_CheckUserInterrupt()` inside
`R_ToplevelExec()`. `R_ToplevelExec()` converts R's non-local interrupt jump
into a normal return to the wrapper, where the inner Arrow stream is released
before an `EINTR` status and the fixed message
`delta-sharing stream interrupted` cross the C Stream ABI. R maps only that
fixed marker to `delta_sharing_cancelled`; provider URLs, paths, query text, and
native diagnostics are never incorporated.

R-native consumers such as nanoarrow or Arrow may observe the process interrupt
inside their own allocation/conversion code before the next C Stream callback.
The package adapters therefore also catch R's `interrupt` condition, release
the same outer stream, and raise the identical typed cancellation. This is a
backstop for R-owned control flow, not an R callback from a native worker. Both
the sentinel-error path and the R-condition path release the outer stream
before raising, while the guarded inner release prevents a second native
cancellation.

The wrapper owns a copy of the original stream callbacks and private data.
Interruption, explicit release, imported-consumer release, and finalization all
funnel through one guarded inner-release operation. Arrays returned before an
interrupt keep their own Arrow array release callbacks and buffers; cancelling
the stream does not invalidate them. The outer wrapper remains alive long
enough for a consumer to call `get_last_error`, then its normal release frees
the wrapper without releasing the already-cancelled inner stream again.

An imported Arrow or DuckDB consumer may invoke C Stream callbacks from a
worker thread. A thread-identity check occurs before the interrupt poll, and a
non-owning thread delegates directly without calling any R API. Such a consumer
must use its own interruption mechanism and release the imported stream; that
release still performs the same exact-once native cancellation and prepared-log
cleanup. This intentionally avoids pretending that R can safely poll
interrupts from an arbitrary downstream worker.

The real-interrupt subprocess gate is platform-neutral. Its child constructs
and pulls the stream on the main R thread; the parent uses `processx` to send
SIGINT on Unix or CTRL+BREAK on Windows. Failure to deliver the platform event,
failure to return a typed cancellation, a second cancellation, a live stream
pointer, or a retained prepared root all fail the test. Windows uses canonical
`file:///C:/...` fixture URIs. This makes hosted Windows execution a real gate,
not a skip, while local Unix execution remains evidence only for the platform
on which it ran.

## Rejected alternatives

### Convert batches to R vectors in a binding framework

Rejected because it copies data, loses or complicates nested Arrow types, calls
the R allocator in the hot path, and prevents direct Arrow consumers.

### General-purpose Rust/R binding framework

Rejected for this narrow boundary because the package requires only registered
control calls and Arrow C Stream pointer transfer. Adding a broader framework
would increase dependency, vendoring, and review surface without changing the
data path.

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
