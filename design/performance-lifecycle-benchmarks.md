# Performance and lifecycle benchmark contract

## Purpose

`bench/vnext-performance.R` is the reproducible local harness for the vNext R
control plane and the narrow Delta Kernel/Arrow C Stream boundary.
`bench/vnext-performance-evidence.R` adds a direct Rust comparator and
subprocess peak-RSS evidence without changing package behavior, adding a native
entry point, or changing CI. The comparator compiles the package's existing
Kernel adapter source as a Cargo example; it is not an alternative
implementation and is never linked into the R package static library.

Both JSON artifacts preserve raw samples, environment details, controlled gate
outcomes, and the limits of the evidence.

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
  Rscript bench/vnext-performance-evidence.R \
    --base /tmp/delta-sharing-r-performance-quick.json \
    --mode quick \
    --output /tmp/delta-sharing-r-performance-evidence-quick.json
R_LIBS="$BENCH_R_LIB" \
  Rscript tools/test_performance_harness.R
R_LIBS="$BENCH_R_LIB" \
  Rscript tools/test_performance_evidence.R
```

Use `--mode standard` for a baseline. Standard mode uses 100, 1,000, and
10,000-file manifests, five preparation/FFI samples, 30 local Kernel samples,
50 explicit-release samples, and larger stream-consumption ranges. Override
all timed repetition counts with `--repetitions N` only for investigation; do
not compare artifacts with different configurations as if they were the same
baseline.

The standard evidence addendum generates an 8,388,608-row deterministic local
Delta table and alternates 15 R-first/Rust-first scan pairs. For process RSS it
generates 65,536-row, 4,194,304-row, and 16,777,216-row table variants and runs
each scan three times in a fresh subprocess. Quick evidence is an
instrumentation smoke test: its throughput and real-Kernel RSS results remain
`not_evaluable` even when the command succeeds.

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

### Direct Rust comparator and generated table

`src/rust/examples/kernel_scan_comparator.rs` includes the package's existing
`src/rust/src/kernel/adapter.rs` directly. Its internal timer starts immediately
before adapter construction and ends after the same scan is exhausted. The R
timer covers native stream construction and nanoarrow pulls over that adapter.
The evidence runner alternates execution order by sample to reduce systematic
warm-cache bias and records all raw values.

`src/rust/examples/generate_kernel_benchmark_table.rs` creates the exact local
Delta table consumed by both paths. It uses deterministic four-column Arrow
batches, 65,536-row Parquet row groups, Snappy compression, one protocol action,
one metadata action, and one add action. It writes only under a fresh temporary
directory and the evidence runner removes that directory after the sample.
The Parquet/log MD5 identities and source identities are recorded.

The generator's Parquet dependency is dev-only. Neither example is linked into
the installed native library, and neither changes the shipped Rust boundary.

Delta Kernel 0.22's `DefaultEngine` fixes its Parquet read batch size at 1,000
rows. The package adapter can slice a Kernel batch to a smaller requested
output but does not coalesce multiple Kernel batches. A generated table with
65,536-row Parquet row groups therefore still emits at most 1,000 rows per R or
Rust batch. This is an explicit blocker for NFR-PERF-02's 64K+ precondition:
meeting it requires production coalescing or a custom Kernel engine and is
outside this evidence-only work.

### Disposable coalescing prototype

A non-shipping adapter prototype accumulated successive 1,000-row Kernel
batches and concatenated them up to the requested output size. It changed only
the Arrow batch adapter plus a direct `arrow-select` dependency. After
measurement, both production changes were removed.

The controlled local run used R 4.5.1, rustc 1.92.0, Darwin arm64, the same
8,388,608-row generated table for timing, alternating R/Rust order, ten samples
per target, and three fresh subprocesses per target for the 16,777,216-row RSS
scan. A new non-coalescing R baseline taken in the same work period had median
total time 1.1269 seconds, time to first batch 2.239 ms, first-pull latency
1.443 ms, 8,448 output batches, and 206.5 MiB median peak RSS on the
16,777,216-row table.

Other package builds were active on the host, so these results establish a
large eligibility signal and target-size trade-off, not a release baseline.
Any production proposal must repeat the interleaved comparison on an idle host.

| Coalesced rows | R median seconds | Wall improvement | R/Rust throughput | Time to first batch | First pull | R batches | 16.8M-row peak RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 4,096 | 0.5429 | 51.8% | 55.5% | 2.773 ms | 1.831 ms | 2,048 | 239.9 MiB |
| 8,192 | 0.5101 | 54.7% | 64.7% | 3.037 ms | 2.087 ms | 1,024 | 274.4 MiB |
| 16,384 | 0.4667 | 58.6% | 69.7% | 3.474 ms | 2.373 ms | 512 | 395.3 MiB |
| 32,768 | 0.4335 | 61.5% | 78.1% | 4.004 ms | 2.774 ms | 256 | 457.4 MiB |
| 65,536 | 0.4053 | 64.0% | 77.9% | 5.103 ms | 4.045 ms | 128 | 401.9 MiB |

All targets exceeded ADR 003's 25% wall-time eligibility threshold, but none
met NFR-PERF-03's 90% throughput target and every target used more peak RSS
than the current path. The 4,096-row result is the conservative observed Pareto
candidate: it preserved near-baseline first-batch latency, improved wall time
by 51.8%, and had the smallest RSS increase. The non-monotonic RSS values at
larger targets show that the simple concatenate implementation is not a
memory-safe production design.

The 65,536-row prototype also passed exact limits of one row and 65,537 rows
(one 65,536-row batch plus one row), with zero active streams and pending
cleanups afterward. It is still not acceptable production code: if a later
Kernel source batch fails while a coalesced batch is being filled, the simple
prototype returns that error before rows already accumulated for the output
batch. A real implementation needs deferred-error state, bounded-copy or
builder-based assembly, schema/limit/error/lifecycle tests, cross-platform
proof, and a separately approved ADR.

Delta Kernel 0.22 does not expose Parquet batch size through
`DefaultEngineBuilder`: its public controls are construction, task-executor
replacement, and build. `DefaultParquetHandler` exposes readahead but calls the
private `DEFAULT_BATCH_SIZE = 1000` constant. Replacing the default engine only
to alter batch size would require disproportionate custom-engine ownership.
If approved, bounded coalescing remains the narrower Arrow-lifecycle glue
option and does not justify moving R-owned auth, HTTP, protocol, or planning
into Rust.

### Process peak RSS

`tools/performance_peak_rss_worker.R` isolates one baseline, synthetic stream,
or real Kernel scan in a fresh R subprocess. The parent uses the operating
system's process high-water facility:

- Darwin: `/usr/bin/time -l`, whose maximum-resident-set-size value is bytes;
- Linux: GNU `/usr/bin/time -v`, whose kbyte value is treated as KiB and
  multiplied by 1,024; and
- Windows or non-GNU Linux `time`: unavailable, with no guessed conversion or
  silent pass.

Peak RSS includes R startup, the loaded package/native libraries, Delta Kernel,
Arrow, allocators, and the workload. It is intentionally not baseline
subtracted. Each worker also requires zero leaked active streams and zero
new pending native cleanups. Standard mode treats the real-Kernel scaling check as controlled:
the largest minus smallest median peak RSS must be no more than the larger of
32 MiB or 20% of the smallest median while input rows grow 256-fold. The raw
baseline and synthetic-stream measurements are retained as supporting trends.

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
| NFR-PERF-02 | R/FFI overhead `< 2%` versus the same Rust-only 64K+ scan | `not_evaluable`; the direct comparator exists, but Kernel 0.22 DefaultEngine emits at most 1,000-row Parquet batches |
| NFR-PERF-03 | R Arrow stream throughput `>= 90%` of Rust-only Delta Kernel | evaluated only by standard evidence on the same generated table |
| NFR-PERF-04 | Peak RSS bounded by in-flight batches plus fixed overhead | evaluated only by standard evidence across isolated real-Kernel table-size variants on Darwin/GNU Linux |
| NFR-PERF-05 | bounded prefetch and backpressure | controlled demand-driven boundary gate |
| ADR 003 exception | `>= 25%` end-to-end wall-time improvement or `>= 50%` peak-RSS reduction | direct baseline remains `not_evaluable`; disposable coalescing met wall eligibility but failed memory/production-readiness gates |

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

NFR-PERF-02 remains open because the current Kernel engine cannot produce the
required 64K+ batches. NFR-PERF-03 and NFR-PERF-04 require a clean standard
evidence artifact on a supported RSS platform; quick artifacts cannot close
them. The durable artifact's ADR 003 gate remains `not_evaluable` because its
direct executable is the already-shipped Kernel adapter. The disposable
coalescing experiment separately crossed the wall-time eligibility threshold;
its memory regression and deferred-error flaw mean it authorizes only a
dedicated design review, not integration or broader Rust ownership.
