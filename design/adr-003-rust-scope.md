# ADR 003: R-first implementation and minimal Rust scope

Status: accepted
Decision owner: package maintainer
Decision date: 2026-07-28

## Context

Delta Kernel is a Rust library, and its Arrow batches need a safe native
boundary into R. That does not require the Delta Sharing client, protocol, and
package behavior to be implemented in Rust.

The earlier architecture assigned authentication, HTTP, retry, discovery,
protocol parsing, synthetic-log construction, diagnostics, and both response
formats to Rust. That boundary would make most of the package a Rust client with
an R interface, increasing native code, packaging burden, review cost, and the
number of lifecycle-sensitive components.

## Decision

Use R for the package and protocol implementation. Use Rust only to leverage
Delta Kernel and expose its Arrow output safely.

R owns:

- S7 descriptors, constructors, validation, and generic dispatch;
- profile parsing, credential handling, and token refresh;
- authenticated HTTP, retry/backoff, pagination, and protocol negotiation;
- Delta Sharing response-header and NDJSON parsing;
- snapshot and CDF request planning;
- synthetic Delta-log content and atomic preparation;
- Parquet-response action normalization into a Kernel-readable log;
- public conditions, redaction, diagnostics, and documentation;
- Arrow/data-frame adapters and downstream composition.

Rust owns only:

- the narrow Delta Kernel adapter;
- construction and execution of Kernel snapshot/CDF scans;
- conversion of Kernel output into a Rust `RecordBatchReader`;
- Arrow C Stream export;
- native panic containment;
- cancellation, buffers, and resources whose lifetime must exactly match the
  active Kernel stream;
- retention and cleanup of an R-prepared temporary log when that cleanup must
  be coupled to native stream release.

Rust must not independently implement profiles, authentication, discovery,
Delta Sharing HTTP requests, retry policy, pagination, NDJSON parsing,
synthetic-log semantics, public diagnostics, or a separate Parquet reader.

## Boundary contract

R passes a compact, validated Kernel invocation to Rust. It contains only the
prepared table/log location, read kind, version/range, projection, exact limit,
batch/concurrency settings, and other Kernel scan options.

Rust returns a `nanoarrow_array_stream` through the Arrow C Stream ABI. Errors
cross the boundary as typed safe payloads that R maps to public conditions.
Rust never calls R from a worker thread and never owns the package's
control-plane policy.

The FFI should remain small enough to review as a complete contract. Adding a
new FFI operation requires showing that it is necessary for Kernel invocation,
Arrow transport, or Kernel-coupled lifecycle.

## Performance exception

R remains the default even when a Rust implementation appears cleaner or is
faster in an isolated microbenchmark. Moving an R responsibility into Rust
requires all of the following:

1. the optimized R implementation is profiled on an agreed representative
   end-to-end workload;
2. the stage is a material user-visible bottleneck rather than network noise;
3. a prototype demonstrates at least a 25% end-to-end wall-clock improvement
   or a 50% peak-memory reduction;
4. correctness, portability, lifecycle, and maintenance costs are measured;
5. a separate ADR is approved by the maintainer before integration.

These thresholds define the requested "dramatic performance impact" exception.
They are an eligibility gate, not automatic approval.

## Consequences

- The R implementation remains understandable and modifiable to R package
  maintainers.
- Rust packaging and unsafe/lifecycle review are concentrated around Kernel and
  Arrow.
- HTTP and protocol tests can run quickly without compiling Rust.
- Kernel conformance and stream lifecycle tests remain native and
  cross-platform.
- R/Rust crossings occur once per read setup and per Arrow stream pull, never
  once per row.
