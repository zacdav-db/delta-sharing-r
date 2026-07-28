# Performance and lifecycle benchmark contract

## Purpose

`bench/vnext-performance.R` is the reproducible local harness for the vNext R
control plane and the narrow Delta Kernel/Arrow C Stream boundary. It does not
move package behavior into Rust, add a Rust benchmark-only implementation, or
change CI. Its JSON artifact preserves raw samples, environment details,
controlled gate outcomes, and the limits of the evidence.

The harness deliberately separates:

- **controlled gates**, where a deterministic local assertion can fail the
  command; and
- **trend metrics**, where machine load, fixture size, or a missing comparator
  makes a hard pass/fail claim misleading.

An unavailable required comparator is `not_evaluable`, never silently treated
as a pass.

## Exact source and environment

Install the checkout under test into a clean temporary R library, then run the
harness from that same checkout:

```sh
BENCH_R_LIB="$(mktemp -d)"
R CMD INSTALL --preclean --library="$BENCH_R_LIB" .
R_LIBS="$BENCH_R_LIB" \
  Rscript bench/vnext-performance.R \
    --mode quick \
    --output /tmp/delta-sharing-r-performance-quick.json
R_LIBS="$BENCH_R_LIB" \
  Rscript tools/test_performance_harness.R
```

Use `--mode standard` for a baseline. Standard mode uses 100, 1,000, and
10,000-file manifests, five preparation/FFI samples, 30 local Kernel samples,
50 explicit-release samples, and larger stream-consumption ranges. Override
all timed repetition counts with `--repetitions N` only for investigation; do
not compare artifacts with different configurations as if they were the same
baseline.

The artifact records:

- exact Git commit, branch, and whether the worktree was dirty;
- R version/platform, operating system/release, machine architecture, and
  logical CPU count;
- package, nanoarrow, optional Arrow, Delta Kernel, and Arrow Rust versions;
- installed package path plus native-library byte size and MD5 identity;
- rustc and Cargo versions;
- full benchmark configuration and UTC capture time; and
- every raw sample rather than only a favorable minimum.

Run on an otherwise idle machine, plugged into power, with the same build
profile and fixture cache state. Warm-up samples are performed for native FFI
and Kernel paths and are not reported. For comparative release evidence,
repeat runs after a cold filesystem cache as a separate trend; do not combine
cold and warm samples.

## Workloads and interpretation

### R snapshot-manifest preparation

The existing Delta Sharing NDJSON fixture is decoded through package protocol
code once. Its validated add action is then expanded to unique, deterministic
HTTPS file actions at increasing manifest sizes. The harness measures two
separate stages:

1. construction and retention of the private validated file-action objects;
   and
2. synthetic-log preparation and cleanup from an already staged action list.

The scalable staging workload constructs the same post-decode private action
shape; it does not measure repeated NDJSON parsing and is not labelled wire
decode time.

Reported memory fields have intentionally narrow names:

- `r_allocation_bytes` and `r_allocation_count` are cumulative allocations
  observed by base R `Rprofmem()`;
- `r_heap_peak_proxy_bytes` is the increment in R's GC high-water columns after
  `gc(reset = TRUE)`; and
- `object_size_bytes` and `serialized_size_proxy_bytes` are two views of the
  retained validated file-action input. `object.size()` may undercount
  environment internals, so the serialized value is the primary retained-size
  scaling proxy.

None is peak resident set size. They exclude Rust, Arrow, Delta Kernel, memory
maps, libc allocators, and OS/object-store buffers. Use them to catch R-side
scaling discontinuities, not to claim NFR-PERF-04.

### Native Arrow C Stream pull cost

The existing deterministic synthetic native stream is pulled with 1,024-row
and 65,536-row batches. Results include creation, time to first batch,
steady-state batches/s and rows/s, explicit release latency, emitted batches,
and final active-stream count.

This isolates the registered C shim, Arrow C Stream handoff, Rust synthetic
array construction, and R/nanoarrow pulling. It is not a pure FFI timer and is
not a Delta Kernel or Parquet throughput result.

### Local Delta Kernel scan

The checked-in seven-row Delta table exercises the actual local Delta Kernel
Snapshot/Scan and Arrow C Stream path. It reports time to first batch,
end-to-end rows/s, exact `limit = 1` early-stop latency, emitted batch count,
and release state. Its size is appropriate for correctness and lifecycle
regression checks, not release throughput claims.

### Cancellation and demand-driven behavior

An intentionally unexhausted 10,000-batch synthetic stream is pulled once and
explicitly released. The controlled lifecycle gate requires one emitted batch,
one cancellation, and no leaked active stream.

A separate 10,000-batch, 65,536-row stream is left idle before one pull.
The demand-driven boundary gate requires:

1. one active stream after construction;
2. zero emitted batches while R is idle;
3. exactly one emitted batch after one pull; and
4. zero active streams after release.

Finite consumption sizes also record the R allocation and GC high-water
proxies. Standard mode compares 256, 2,048, and 8,192 batches so the trend can
show whether the R heap proxy reaches a plateau as total rows continue to grow.
This is useful bounded-memory evidence at the synchronous C Stream boundary,
but it does not measure process peak RSS or prove a future async producer
queue's capacity.

## Gates from the vNext plan

The source of truth remains `design/vnext-plan.md` and ADR 003:

| Requirement | Threshold | Harness disposition |
|---|---:|---|
| NFR-PERF-02 | R/FFI overhead `< 2%` versus the same Rust-only 64K+ scan | `not_evaluable` until a same-fixture Rust-only executable exists |
| NFR-PERF-03 | R Arrow stream throughput `>= 90%` of Rust-only Delta Kernel | `not_evaluable` until that comparator exists |
| NFR-PERF-04 | Peak RSS bounded by in-flight batches plus fixed overhead | `not_evaluable`; R allocation/heap proxies and demand-driven evidence are published |
| NFR-PERF-05 | bounded prefetch and backpressure | controlled demand-driven boundary gate |
| ADR 003 exception | `>= 25%` end-to-end wall-time improvement or `>= 50%` peak-RSS reduction | `not_evaluable` unless an R optimization and Rust prototype are compared end to end |

The harness also gates exact-limit correctness and explicit-release lifecycle.
Latency values themselves are trends because the design sets no portable
millisecond threshold. Cloud/object-store results, CPU utilization, network
concurrency, compressed/uncompressed MB/s, file/row-group skipping, and eager
materialization remain trend or release-suite work rather than local gates.

## Baseline and review protocol

Keep JSON artifacts as release evidence, not as universal golden numbers.
Artifacts contain a `metric_classes` section that explicitly identifies
controlled gates and noisy trend metrics, plus distribution summaries and
every underlying sample.
Before accepting a regression baseline:

1. build and run both commits on the same host and build profile;
2. require identical artifact configuration and fixture revision;
3. compare medians and distribution spread across raw samples;
4. repeat any apparent regression in interleaved old/new runs; and
5. fail only controlled correctness/lifecycle gates automatically.

The initial useful baselines are:

- validated-action staging time, retained-size proxies, and R allocation/heap
  proxies versus manifest file count;
- synthetic-log preparation time and R allocation/heap proxies from an already
  staged action list;
- absolute 1,024-row and 65,536-row synthetic C Stream pull rates;
- local Kernel time to first batch and exact-limit early stop;
- explicit release latency distribution; and
- idle-emission and finite-consumption heap-proxy evidence.

NFR-PERF-02, NFR-PERF-03, NFR-PERF-04, and any ADR 003 exception remain open
until their required comparator or process-level measurement exists. A future
peak-RSS runner should isolate each workload in a subprocess and use the
platform's process high-water facility; it must not relabel the R heap proxy.
