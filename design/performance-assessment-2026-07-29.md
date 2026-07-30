# Performance assessment — 29 July 2026

## Status and scope

This note began as a read-only performance review of the R6/Delta Kernel
implementation at commit `e56404c` on
`codex/delta-kernel-s7-overhaul`. Its final sections record the experiments
subsequently implemented on the integration branch. Descriptions of the
"current" path in the original assessment are therefore baseline descriptions;
the experiment sections record what was tested. The final decision removed
progress indicators and their native collection worker, leaving every public
materializer on the direct Arrow stream.

The review follows ADR 003:

- R continues to own profiles, authentication, HTTP, protocol handling,
  planning, synthetic logs, diagnostics, and adapters.
- Rust remains limited to Delta Kernel and the minimum Arrow/native lifecycle
  boundary.
- A responsibility moves from R to Rust only after an optimized R baseline and
  a representative prototype demonstrate at least 25% lower wall time or 50%
  lower peak memory.

The measurements below are diagnostic, not release benchmark results. Local
benchmarks were repeated and stable. Live results are medians where possible,
but remain subject to network, object-store, server, and cache variance.
Potential improvements are not additive.

## Executive conclusion

The package does not have a general performance problem, and it does not need a
large expansion of Rust.

The direct Arrow paths are already fast because Arrow consumers pull the native
C stream without an R callback for every batch. On the tested 250,000-row
nested/deletion-vector workload, the direct R implementation was directionally
faster than the Python connector. The native exact row limit is also better
suited to bounded reads than the Python path assessed here.

The original assessment identified four concentrated boundaries:

1. **Large snapshot and CDF manifests:** R currently materializes the HTTP body,
   parsed actions, file lists, and re-encoded synthetic-log lines. At 100,000
   file actions, the benchmark retained roughly 179 MiB in the parsed file
   objects alone and spent 17 seconds re-encoding them.
2. **Progress-enabled eager reads:** at the baseline, progress turned a direct
   C-stream materialization into one R call per Kernel batch, retained every
   batch, then replayed them. It was about 19% slower in a live paired
   comparison and 1.8–2.3 times slower in local eager-materialization tests.
3. **Presigned Parquet I/O inside Delta Kernel 0.22:** the Kernel default engine
   fetches each presigned Parquet object into one in-memory byte buffer before
   decoding it and overlaps only the next file with the current scan. This can
   limit network concurrency and makes peak native memory depend on Parquet file
   size.
4. **Repeated metadata negotiation:** `response_format = "auto"` adds a metadata
   request before every query. That request cost 1.78 seconds in the measured
   live read. Explicit Delta format avoids it today; careful table-level caching
   could avoid it without changing the public API.

The resulting work kept the ownership boundary intact. Snapshot manifests use
bounded R staging; the equivalent CDF prototype was rejected because its memory
reduction did not justify its wall-time cost. Kernel 0.26 supplied larger source
batches. The progress worker was tested and then removed because its modest UX
benefit did not justify a second materialization and lifecycle path. Metadata
caching remains a possible R-side experiment.

## Baseline execution model at `e56404c`

A snapshot read currently does the following:

1. Resolve `response_format = "auto"` with the metadata endpoint.
2. Call Query Table, read each page as a complete character body, split NDJSON,
   and parse every line into an R list.
3. Accumulate parsed actions across pages and split them into protocol,
   metadata, and file objects.
4. Re-encode the parsed file actions into a character vector representing a
   synthetic Delta log and write it to a temporary directory.
5. Give the local log path to Delta Kernel in Rust.
6. Expose the resulting record batches as an Arrow C stream.

The direct `to_arrow_stream()`, `to_arrow_reader()`, `to_arrow()`, and
`to_data_frame()` paths preserved the native C-stream boundary.
The progress path instead calls `$get_next()` in R for every batch, retains all
batches in a list, creates a second nanoarrow stream, and then materializes it.

CDF has the same HTTP/NDJSON costs plus bucketing all actions by version and
creating a synthetic commit for each version in the effective range.

## Measurements

### Live phase breakdown

Source:
`delta_sharing_r_vnext_share.delta_sharing_r_vnext.dv_nested_events_250m`

The source contains 250 million nested event rows, deletion vectors, 257
Parquet files, and approximately 1.8 GB of Parquet data. The measured read
projected all five top-level columns and limited output to 250,000 rows.

| Phase | Time |
|---|---:|
| Resolve automatic response format | 1.783 s |
| Query Table | 1.475 s |
| Encode synthetic log | 0.002 s |
| Write temporary log | 0.003 s |
| Construct native stream | 0.005 s |
| Obtain first batch | 2.274 s |
| Drain remaining batches | 5.989 s |

The query returned two files. Its parsed R object was only 24,480 bytes, and
the four synthetic log lines totalled 8,597 bytes. This is important: synthetic
log construction is negligible for an ordinary small manifest. The control
plane, particularly automatic format negotiation, is more material for a
small or bounded read.

The 250,000 rows arrived in 246 batches with a maximum of 1,024 rows per batch,
despite the public requested `batch_size` being 65,536.

### Direct versus progress-enabled materialization

For a local generated Delta table with 4,194,304 rows:

| Materializer | No progress | Progress | Ratio |
|---|---:|---:|---:|
| `to_arrow()` | 0.178 s | 0.415 s | 2.33x |

For a 1,048,576-row data-frame limit:

| Materializer | No progress | Progress | Ratio |
|---|---:|---:|---:|
| `to_data_frame()` | 0.061 s | 0.112 s | 1.84x |

On the live 250,000-row nested/deletion-vector source, with explicit Delta
format and alternating order:

| Path | Samples | Median |
|---|---|---:|
| Direct/no progress | 8.649, 9.428, 8.854, 8.809 s | 8.832 s |
| Progress | 10.272, 10.763, 15.169, 10.044 s | 10.517 s |

The live median overhead was approximately 19%. The outlier illustrates why
this result should be treated as directional until it is repeated in a
controlled release benchmark.

The cost is not primarily drawing the CLI bar. Progress changes the data path:
all record batches cross into R, are retained, and are replayed before the
eager result is built. It therefore adds per-batch overhead and weakens the
otherwise Arrow-native memory behavior.

### Batch-boundary cost

A generated local table with 8,388,608 rows was read seven times per requested
batch size. A manual R `$get_next()` drain was compared with a Rust-side direct
drain of the same adapter.

| Requested batch size | Emitted batches | Largest emitted batch | R median | Rust median | R/Rust |
|---:|---:|---:|---:|---:|---:|
| 128 | 67,200 | 128 | 6.486 s | 0.310 s | 20.94x |
| 1,024 | 8,448 | 1,000 | 1.237 s | 0.296 s | 4.19x |
| 65,536 | 8,448 | 1,000 | 1.240 s | 0.319 s | 3.89x |

Two conclusions follow:

- Very small requested batches are pathological whenever R participates in
  each batch boundary.
- Raising the public batch size above roughly 1,000 currently does not create
  larger source batches. Delta Kernel's default engine emits approximately
  1,000-row batches and the package adapter only slices batches that are too
  large; it does not coalesce adjacent small batches.

This benchmark should not be used to claim that normal Arrow consumers are four
times slow. Direct Arrow and nanoarrow consumption occurs across the C
interface and was fast in the eager-materialization benchmarks.

### Large-manifest scaling

Realistic synthetic Delta `add` actions were parsed into the current file
object shape and re-encoded as synthetic log lines.

| File actions | Encode time | Parsed file objects | Encoded lines | Commit text | Write time |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.155 s | 1.793 MiB | 0.344 MiB | 0.287 MiB | 0.004 s |
| 10,000 | 1.549 s | 17.929 MiB | 3.434 MiB | 2.879 MiB | 0.007 s |
| 100,000 | 17.031 s | 179.291 MiB | 34.333 MiB | 28.886 MiB | 0.041 s |

Disk writing is not the bottleneck. Parsing, retaining nested R lists, and
serializing those lists back to JSON are the costs. A real query also
temporarily owns the response body string and page action lists, so the
end-to-end peak is higher than the object sizes in the table.

The repeated `actions <- c(actions, page_actions)` operation can additionally
copy an increasingly large list when a response has many pages.

### Comparison with the Python connector

The comparison environment used:

- `delta-sharing` 1.4.1
- `delta-kernel-rust-sharing-wrapper` 0.3.1
- `pyarrow` 25.0.0
- `pandas` 3.0.3

Source inspection showed that Python streams the HTTP response with
`Response.iter_lines()`, retaining raw NDJSON strings before parsing and
writing its temporary Delta log. That representation is substantially leaner
than retaining every action as a deeply nested R list, although Python still
retains the full set of strings and is not fully bounded.

Python's automatic response format also performs a metadata request. Its
explicit `use_delta_format = TRUE` path skips that negotiation.

In the assessed Python Kernel path, the limit is sent to the server as a hint
but the resulting pandas object is truncated after Kernel materialization. The
R native adapter applies an exact limit to emitted record batches and can stop
the scan early.

For the same live 250,000-row nested/deletion-vector workload with explicit
Delta format and all five columns:

| Connector/path | Samples | Median |
|---|---|---:|
| Python to pandas | 10.599, 16.617, 12.695, 10.740 s | 11.717 s |
| R direct to data frame | 8.649, 9.428, 8.854, 8.809 s | 8.832 s |
| R with progress | 10.272, 10.763, 15.169, 10.044 s | 10.517 s |

By these medians, R direct was approximately 25% faster than Python, and R with
progress was approximately 10% faster. This is a directional comparison, not a
formal connector shootout: the language environments use different wrapper,
Arrow, and materialization layers, and cloud timings vary. It does demonstrate
that there is no evidence for moving general client logic to Rust.

## Ranked uplift opportunities

### 1. Stream large query manifests into the synthetic log

**Expected value:** high peak-memory reduction and high wall-time reduction for
tables or CDF ranges with tens of thousands of returned files/actions; little
effect on ordinary small manifests.

**Scope:** R.

The preferred direction is to process response pages and NDJSON lines
incrementally into an atomically prepared log:

- retain only protocol, metadata, the next page token, and small diagnostics;
- unwrap and write each file action without building one giant parsed action
  list;
- avoid a second full encoded-lines character vector;
- preserve cleanup and the current native ownership transfer;
- preserve useful protocol validation without recreating every wire object.

Snapshot responses can be streamed directly. CDF needs more care because
actions must be grouped into versioned commits. A bounded R design could spool
actions per version to temporary files while retaining only a small version
index. It should not assume server ordering unless the protocol guarantees it.

The 100,000-action benchmark is enough to prioritize a prototype, but not to
claim ADR 003's memory threshold. The next experiment should measure peak RSS
for the complete HTTP-to-first-batch path using a mock streamed response with
realistic pages.

### 2. Preserve direct Arrow consumption while reporting progress

**Expected value:** approximately 19% on the measured live eager read and
1.8–2.3x on fast local eager reads; potentially a substantial peak-memory
reduction because batches would no longer all be retained and replayed.

**Scope:** begin with R/API and narrow-adapter prototypes; do not move read
orchestration to Rust.

Candidate experiments:

1. Correct, bounded coalescing of adjacent small Kernel batches in the existing
   Rust Arrow adapter. This reduces R crossings while keeping the same ownership
   boundary.
2. A progress mode that updates at a coarser cadence without retaining every
   batch. This is technically constrained because a blocking C-stream consumer
   cannot safely call arbitrary R/CLI code from a worker thread.
3. An explicit UX choice between the fastest direct materialization and row
   progress. The current interactive default deliberately favors feedback; that
   policy should not be changed as a hidden optimization.

An earlier disposable coalescing prototype on the prior architecture reduced
wall time by 48–64% depending on target batch size, clearing ADR 003's time
threshold, but increased peak RSS and had a deferred-error correctness flaw.
It is evidence that the boundary is worth revisiting, not production code.

### 3. Assess a Delta Kernel upgrade before custom I/O

**Expected value:** potentially high for large Parquet objects, many-file
scans, narrow projections, and high-latency object storage; uncertain on the
current approximately 7 MiB average files.

**Scope:** dependency experiment inside the narrow Rust kernel boundary.

The package pins `delta_kernel = 0.22.0`. In that version's default-engine
source, the presigned URL opener:

- performs a GET and awaits the complete response bytes;
- builds the Parquet reader over that complete in-memory buffer;
- opens the next file while scanning the current file, providing one-file
  lookahead rather than broad configurable file concurrency;
- uses a roughly 1,000-row default decode batch.

Consequences:

- projection reduces decoding but may not reduce transferred bytes for
  presigned URLs;
- native peak memory is related to the current and prefetched Parquet object
  sizes;
- a stopped exact-limit scan may still have prefetched the next complete file;
- throughput on many small files may be limited by narrow file concurrency.

The currently advertised crate release is newer than the pinned version, but
this assessment has not established that it changes presigned I/O behavior.
The correct next action is an isolated Kernel upgrade branch with the existing
conformance suite plus:

- bytes transferred for narrow versus full projection;
- time to first batch and total scan time across different file sizes/counts;
- peak RSS with large Parquet files;
- cancellation and early-limit behavior;
- deletion-vector, column-mapping, nested-type, and CDF regression coverage.

Only if the released Kernel still has a measured user-visible bottleneck should
the package consider a small custom engine component. Reimplementing HTTP or
Parquet in package Rust up front would violate the desired architecture.

### 4. Avoid repeated format and metadata round trips

**Expected value:** medium to high for small reads and repeated operations on
one table; negligible for long scans.

**Scope:** R.

Automatic response-format negotiation cost 1.783 seconds in the live phase
breakdown. Users can already avoid it by selecting
`response_format = "delta"`. A private cache on a reusable table could preserve
the default UX while avoiding the repeated request.

The table methods `protocol()`, `metadata()`, and `schema()` currently issue
separate metadata requests even though the same response contains the
underlying information. The live spinner therefore makes multiple round trips
before scanning.

Any cache needs an explicit freshness policy:

- protocol and chosen response format are relatively stable;
- schema and metadata can change with table versions;
- a latest snapshot should not silently use stale version-dependent state;
- credential expiry and auth refresh remain owned by the existing R transport.

A safe prototype can cache one parsed metadata response together with its
`delta-table-version` and invalidate version-dependent fields after a newer
version is observed.

### 5. Make server-side pruning the primary scan optimization

**Expected value:** potentially very high when predicate hints or limits reduce
the signed-file manifest; workload dependent.

**Scope:** existing R planning/UX.

With the current presigned I/O path, selecting fewer columns does not
necessarily save network bytes because each selected Parquet file is fetched as
a complete response. Avoiding files altogether is therefore more valuable.
Structured R predicate hints and the server `limitHint` should be benchmarked
against partitioned and data-skipped sources.

Predicate hints are best effort at the sharing-server boundary. They should not
be described as exact local filtering unless an exact residual predicate is
also executed. No large predicate engine should be added to Rust solely for
this assessment.

### 6. Reuse native HTTP resources only if first-batch profiling justifies it

**Expected value:** probably low to medium; unmeasured.

**Scope:** narrow Rust lifecycle boundary.

Each native stream builds a Delta Kernel default engine. The presigned opener
creates a `reqwest::Client` for that scan, so repeated reads do not share a
long-lived package-level connection pool. Native construction itself took only
0.005 seconds locally, but DNS/TLS/connection setup is included in the
time-to-first-batch phase.

Persistent engine/client reuse would add lifecycle and concurrency complexity.
It should be considered only after a repeated-scan benchmark separates
connection setup from server and object-store latency.

### 7. Keep large results lazy

**Expected value:** high memory avoidance, but primarily usage guidance rather
than an implementation change.

`to_arrow()` and `to_data_frame()` are eager by definition. For large or
open-ended reads, `to_arrow_stream()`/`to_arrow_reader()` composed with Arrow
or DuckDB preserves streaming and predicate/projection opportunities. This is
the intended high-scale path and should remain prominent in documentation and
benchmarks.

## Lower-priority observations

- Native stream setup and temporary-log disk writes were negligible in the
  measured ordinary read. Optimizing them would not be useful now.
- `batch_size = 65,536` is currently misleading as a performance control
  because Kernel emits roughly 1,000-row batches. It still acts as an upper
  slicing bound in the adapter. Documentation or implementation should clarify
  this only after the coalescing decision.
- CDF preparation has a potentially expensive sparse-range case because it may
  need synthetic commits across the complete requested version interval. This
  needs a representative long-range CDF benchmark before redesign.
- Query pagination is necessarily sequential because the next page token comes
  from the current page. The optimization is to reduce retained/copying state,
  not to parallelize page requests.
- The C boundary checks R interrupts once per emitted batch. That is desirable
  responsiveness and not a current optimization target.

## Original recommended experiment order

1. Add durable, reproducible benchmark fixtures and record wall time, first
   batch, emitted batches, bytes transferred where observable, and peak RSS.
2. Prototype streamed snapshot-manifest preparation entirely in R and measure
   10,000/100,000/1,000,000 actions.
3. Extend that design to CDF using bounded per-version spooling and validate
   many changes, sparse versions, metadata evolution, and cleanup failures.
4. Build a corrected bounded batch-coalescing prototype in the existing native
   adapter and benchmark direct Arrow, progress, DuckDB, exact limits,
   cancellation, error propagation, and peak RSS.
5. Test the newest compatible Delta Kernel release against the live share and
   large local/presigned fixtures before designing custom I/O.
6. Prototype version-aware metadata/format caching and measure repeated table
   operations.
7. Make production decisions only from end-to-end results and ADR 003 gates.

## Decision summary

| Opportunity | Likely value | Primary owner | More Rust required? |
|---|---|---|---|
| Stream snapshot/CDF manifests | High on large manifests | R | No |
| Remove progress replay cost | High on eager interactive reads | R + narrow adapter experiment | Possibly a small amount |
| Improve presigned Parquet I/O | Potentially high | Delta Kernel dependency | Prefer upgrade; custom code only if proven |
| Cache format/metadata | Medium on repeated/small reads | R | No |
| Better server-side pruning | Workload dependent, potentially high | R/API + server | No |
| Reuse native engine/client | Unknown, probably medium at most | Narrow lifecycle boundary | Small, only if proven |
| Keep large reads lazy | High memory benefit | User-facing API/docs | No |

The evidence supports the accepted architecture: optimize the R control plane
where it retains too much state, keep ordinary reads Arrow-native, and use Rust
only where Delta Kernel or the Arrow batch/lifecycle boundary demonstrably
requires it.

## Implemented experiment: Kernel 0.26 source batches

On 2026-07-30 the integration branch upgraded from Delta Kernel 0.22 with
Arrow 57 to Delta Kernel 0.26 with Arrow 58. Kernel 0.26's split default engine
accepts a Parquet batch size, so the package adapter can request a bounded
source batch without implementing package-owned Arrow concatenation. The
source request follows the public output size, clamped to 1,000–65,536 rows;
the existing adapter still slices output when the caller requests a smaller
batch.

The paired installed-package benchmark used the same generated local Delta
table throughout: 8,388,608 rows, four columns, eight Parquet files, and a
requested output batch size of 65,536. Each timing series contained eight
eager Arrow materializations. The first cold observation is retained in the
raw series and the median is reported.

| Path | Kernel 0.22 median | Kernel 0.26 median | Change |
|---|---:|---:|---:|
| Direct Arrow materialization | 0.3645 s | 0.1425 s | 60.9% faster |
| Progress-enabled Arrow materialization | 0.7790 s | 0.1500 s | 80.7% faster |

The emitted native stream changed from 8,448 batches (maximum 1,000 rows) to
128 batches of exactly 65,536 rows. This clears ADR 003's 25% end-to-end time
gate while keeping the extra Rust surface limited to Delta Kernel engine
configuration. The upgrade also preserves the Rust 1.88 MSRV, passes all 35
native unit tests and strict Clippy, and resolves successfully from the
generated offline vendor archive.

This result supersedes the earlier recommendation to prototype a custom
coalescer. It materially reduces R/native crossings and progress callback
overhead, but it does not animate the CLI spinner while a single synchronous
Kernel pull is blocked on I/O. Continuous animation remains a distinct
lifecycle experiment rather than a reason to add a second batching layer.

## Retired experiment: live eager-read progress

The progress experiment moved an already-created Arrow C stream to one native
worker for eager collection. R polls only atomic row, batch, and completion
counters every 50 milliseconds, forcing a CLI repaint even when the next
Kernel batch is still blocked on object-store I/O. When collection completes,
the worker returns the retained Arrow record batches through the same Arrow C
Stream ABI. Lazy readers and eager reads with progress disabled remained on the
direct path.

For snapshots, the R query layer computes an exact denominator only when every
returned add has valid `stats.numRecords`. Deletion-vector cardinality is
subtracted and an exact read limit caps the result. Missing or malformed
statistics leave the indicator indeterminate. CDF is always indeterminate:
live `cdf_dv_interop` action statistics implied 4,906 rows while Kernel
correctly emitted 3,500 changes, so an action-based percentage would be false.

On the 8,388,608-row local table, eight progress-enabled Arrow
materializations had a 0.2025-second median, compared with 0.7790 seconds for
the pre-upgrade progress path. The corresponding direct current path had a
0.1750-second median. The remaining progress cost is bounded polling and one
in-memory Arrow-stream handoff rather than one R callback and retained
nanoarrow object per source batch.

The live 250-million-row deletion-vector table validated the visible behavior
with a 250,000-row exact limit. The spinner continued to repaint while the
first object was blocked, advanced to 25,000/250,000 after the first file, and
remained animated during the next blocked pull before completing at exactly
250,000 rows. Alternating remote samples were dominated by network variance:
the worker path took 12.5 and 14.3 seconds, while the direct current path took
17.3 and 11.8 seconds.

Cancellation never calls R from the worker. Dropping a job sets an atomic
cancel flag and returns immediately; a blocked Kernel pull observes it once
I/O returns. Normal completion joins the worker before returning its collected
stream. If cancellation must detach a blocked worker, the native library is
pinned for the rest of the R process rather than risking execution through an
unloaded DLL. Native tests cover a gated no-batch interval, successful joined
handoff, repeated finish, cancellation, detached-worker pinning, panic
containment, and active-job completion.

The experiment was subsequently removed. Its 0.2025-second local median was
15.7% slower than the paired 0.1750-second direct path, while the background
worker required separate FFI, cancellation, unload, and retained-batch
lifecycles. The clean vNext API now has no `progress` arguments: eager and lazy
materializers all consume the direct Arrow stream. Snapshot staging also no
longer parses per-file statistics solely to construct a progress denominator.

## Implemented experiment: bounded snapshot manifests in R

Snapshot Query Table responses are now consumed through httr2's pull-based
response connection in groups of at most 256 complete NDJSON lines. R retains
only protocol, metadata, pagination state, and the current chunk. Normalized
file actions are written to a private staging file and copied
behind the protocol/metadata header in the final commit. The stage lives inside
the ownership-marked log root and is deleted before the native cleanup guard
receives that root.

Focused tests cover both real `StreamingBody` behavior and httr2's in-memory
mock fallback, pagination, Delta and Parquet responses, deletion-vector row
counts, exact zero limits, malformed later-page cleanup, and the final private
layout. The generated commit remains byte-identical to the retained baseline.

Fresh-process `/usr/bin/time -l` measurements used realistic Delta adds with
statistics, partition values, signed-looking URLs, and periodic deletion
vectors:

| File actions | Path | Transform time | Maximum RSS | Peak memory footprint | Commit bytes |
|---:|---|---:|---:|---:|---:|
| 1,000 | Retained | 0.253 s | 97.6 MB | — | 246,563 |
| 1,000 | Staged | 0.259 s | 101.3 MB | — | 246,563 |
| 100,000 | Retained | 21.950 s | 398.6 MB | 340.9 MB | 24,829,165 |
| 100,000 | Staged | 20.196 s | 222.0 MB | 125.7 MB | 24,829,165 |

At 100,000 files, bounded staging reduced maximum RSS by 44.3%, reduced the
reported peak memory footprint by 63.1%, and reduced transformation time by
8.0%. Ordinary 1,000-file cost was effectively unchanged. This is a clear
R-first improvement, so the staged snapshot path was retained.

Reproduce with:

```sh
/usr/bin/time -l Rscript tools/snapshot_manifest_benchmark_worker.R retained 100000
/usr/bin/time -l Rscript tools/snapshot_manifest_benchmark_worker.R staged 100000
```

## Rejected experiment: bounded CDF spooling

The same principle was prototyped for CDF using one private staging file per
represented version and a compact version/timestamp index. It preserved
protocol placement, metadata evolution, action order within each version,
empty interior commits, timestamps, effective bounds, and the checkpoint
bootstrap. The complete credentialed CDF test also passed with the prototype:
version range, exact timestamp range, metadata-only open end, and all four
change types.

The 100,000-action, 100-version fresh-process comparison produced byte-identical
15,400,277-byte commit sets:

| Path | Transform time | Maximum RSS | Peak memory footprint |
|---|---:|---:|---:|
| Retained baseline | 14.527 s | 540.0 MB | 460.6 MB |
| Per-version staging | 23.709 s | 405.5 MB | 320.6 MB |

Spooling reduced maximum RSS by 24.9% and peak footprint by 30.4%, but made the
transformation 63.2% slower. It cleared neither ADR 003 threshold and imposed a
substantial wall-time regression. The production CDF path therefore remains
the concise Python-style retained action list. The reproducible rejected
prototype benchmark remains in
`tools/cdf_manifest_benchmark_worker.R` so a future provider with materially
larger CDF manifests can be assessed without guessing.

## Current connector comparison across the Desktop share

A fresh Python 3.13 environment used `delta-sharing` 1.4.1,
`delta-kernel-rust-sharing-wrapper` 0.3.1, PyArrow 25.0.0, and pandas 3.0.3.
Both connectors used the same Desktop profile, explicit Delta format, all
columns, eager data-frame materialization, and the same row/version bounds.
Snapshot entries are medians of three sequential remote reads; CDF is one read
because each connector took more than five minutes. Every comparison produced
the same row and column counts.

| Source | Scope | R | Python | Direction |
|---|---|---:|---:|---:|
| `empty_snapshot` | all rows (0 × 6) | 1.906 s | 1.720 s | R 10.8% slower |
| `small_snapshot` | all rows (25 × 6) | 32.256 s | 36.972 s | R 12.8% faster |
| `partitioned_orders` | all rows (1,000,000 × 5) | 16.963 s | 17.280 s | R 1.8% faster |
| `complex_types` | limit 1,000 (1,000 × 10) | 33.487 s | 36.591 s | R 8.5% faster |
| `snapshot_narrow_250m` | limit 250,000 (250,000 × 6) | 6.080 s | 6.639 s | R 8.4% faster |
| `dv_nested_events_250m` | limit 250,000 (250,000 × 5) | 10.418 s | 11.002 s | R 5.3% faster |
| `cdf_dv_interop` | versions 1–4 (3,500 × 7) | 317.697 s | 355.853 s | R 10.7% faster |

These are directional cloud measurements, not hard regression thresholds.
Cold-cache and object-store variance are visible, especially on the narrow
250-million-row source. The useful conclusion is parity: the R connector does
not show a general materialization deficit versus Python, and moving additional
client logic into Rust is not supported by this evidence.

CDF is the clear outlier for both languages. The source has 513 current table
files and the requested range represents hundreds of tiny change objects; its
five-to-six-minute wall time is dominated by remote object opens. The rejected
R spooling prototype cannot address that bottleneck. The next CDF performance
experiment should therefore be a Delta Kernel/default-engine I/O investigation
or a better-compacted provider fixture, not more R protocol complexity.

Reproduce individual entries with `tools/compare_connector.R` and
`tools/compare_connector.py`. Their third argument is `none`, a snapshot row
limit, or `cdf:START:END`.

## Implemented experiment: format negotiation and snapshot pruning

Automatic response-format negotiation is now cached by table for the lifetime
of one client. The cache stores only the stable selected `delta`/`parquet`
capability: explicit formats bypass it, failed requests are not cached, and
version-dependent protocol, metadata, and schema values continue to be fetched
fresh. On the credentialed `partitioned_orders` table, the first automatic
resolution took 1.504 seconds and the second resolution was a zero-I/O cache
hit.

`tools/profile_snapshot_pruning.R` measures the control-plane manifest without
downloading Parquet. On the six-file `partitioned_orders` table:

| Hint shape | Selected files | Synthetic log bytes |
|---|---:|---:|
| None | 6 | 14,793 |
| `limit = 1000` | 1 | 3,331 |
| `region == "us-west-2"` | 1 | 3,331 |
| Region predicate plus limit | 1 | 3,331 |
| `order_id == 105469` | 6 | 14,793 |
| Order predicate plus limit | 6 | 14,793 |

The package was already transmitting both structured predicates and limit
hints correctly. These measurements show why the hints must remain documented
as best effort: partition pruning was decisive, while the non-partition value
did not eliminate a file and prevented the server from safely applying the
one-file limit shortcut.

The partition-column probe also found a Delta Kernel 0.26 panic when the
requested logical projection has no physical Parquet columns. The native
adapter now detects that exact scan shape, adds one hidden physical data column,
and projects it away before exposing Arrow output. Ordinary projections are
unchanged.

## CDF presigned-I/O diagnosis

`tools/profile_cdf_io.R` separates the Sharing query, synthetic-log
preparation, native construction, and every Arrow batch pull without reporting
credentials or signed URLs.

| Source | R query + log | Native construction | Rows | Batches | Data pulls | Pull p50 | Pulls >1 s |
|---|---:|---:|---:|---:|---:|---:|---:|
| `cdf_dv_interop`, versions 1–4 | 6.583 s | 0.007 s | 3,500 | 258 | 314.766 s | 1.198 s | 258 |
| `cdf_no_dv`, versions 1–4 | 3.821 s | 0.008 s | 3,500,000 | 322 | 429.344 s | 1.240 s | 297 |

The DV response contained 257 add actions, one CDC action, one remove, and two
metadata actions. The non-DV response contained 256 adds, 41 CDC actions, and
two metadata actions. The batch/file-action relationship and pull distribution
localize the cost to remote object access rather than R parsing, log writing,
native setup, or Arrow conversion.

Kernel 0.26 exposes a general file-buffer setting, but its own default-engine
implementation explicitly bypasses that setting for presigned HTTPS URLs.
Those URLs use an ordered `FileStream` with one-file lookahead. A package-level
custom downloader or Parquet engine would materially expand Rust and duplicate
Kernel responsibility. The supported next directions are provider compaction
or an upstream Kernel presigned-concurrency improvement, followed by the same
end-to-end benchmark; no package implementation change is justified now.

## Final-candidate read-path evidence (2026-07-30)

Fresh R processes used the installed package and the credentialed Desktop
profile. Each case requested exactly 250,000 rows. `/usr/bin/time -l` measured
process peak RSS; the worker separately measured construction, first-batch,
total materializer time, sampled RSS, and pending native cleanups.

| Source | Path | First batch | Materializer total | Peak RSS |
|---|---|---:|---:|---:|
| `snapshot_narrow_250m` | C stream | 5.256 s | 8.322 s* | 159.4 MB |
| `snapshot_narrow_250m` | Arrow reader | 3.789 s | 5.605 s | 217.6 MB |
| `snapshot_narrow_250m` | Arrow table | — | 6.078 s | 219.4 MB |
| `snapshot_narrow_250m` | data frame | — | 5.757 s | 183.9 MB |
| `snapshot_narrow_250m` | DuckDB `count(*)` | — | 9.271 s | 254.1 MB |
| `dv_nested_events_250m` | C stream | 2.276 s | 17.306 s* | 198.8 MB |
| `dv_nested_events_250m` | Arrow reader | 2.221 s | 17.458 s | 251.5 MB |
| `dv_nested_events_250m` | Arrow table | — | 10.683 s | 245.2 MB |
| `dv_nested_events_250m` | data frame | — | 10.243 s | 329.0 MB |
| `dv_nested_events_250m` | DuckDB `count(*)` | — | 10.250 s | 281.0 MB |

`*` The stream total deliberately includes a one-second idle backpressure
probe. Sampled RSS did not change during that idle period on the narrow source
and increased by only 32 KB on the DV source. The no-read installed baseline
peaked at 86.8 MB.

Every path returned exactly 250,000 rows and reported zero pending prepared-log
cleanups. The measurements are single cloud reads, so latency differences are
directional rather than rankings. The stable conclusion is memory-related:
the lazy direct stream remains the lowest-RSS option, while eager nested
materialization costs more as expected. DuckDB composes through the standard
Arrow reader and introduces no package-specific Rust path.

The same DV manifest contained two signed HTTPS data URLs and one signed HTTPS
absolute deletion-vector URL (`storageType = "p"`). All three carried provider
expiry parameters and had about 3,599 seconds remaining when planned. A Rust
loopback test independently proves that an expired signed request becomes one
terminal, fixed/redacted data-scan error.
