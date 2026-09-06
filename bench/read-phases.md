# Local read and metadata measurements

These scripts measure separate costs without contacting a Sharing server.
Install the package being measured and run from the repository root:

```sh
Rscript bench/metadata.R 20000 1000
Rscript bench/generate-read-table.R /tmp/new-read-benchmark 1000000
Rscript bench/native-reader.R stream /tmp/new-read-benchmark 50 65536
Rscript bench/native-reader.R tibble /tmp/new-read-benchmark 10 65536
Rscript bench/native-reader.R arrow-close /tmp/new-read-benchmark 10 65536
```

Use a fresh directory for generation and a separate R process for each read
mode. The generator requires the optional Arrow package. `arrow-close` enables
Arrow threads and reads only the first batch before closing the reader; it
does not test DuckDB's early-termination path. Run it with an external timeout
when investigating shutdown failures.

`metadata.R` reports input size, parser time, cumulative R allocations, and
the retained R result size. Allocated bytes are **not** peak memory. The native
script reports constructor time, time to the first batch where observable,
total time, row counts, and Unix RSS checkpoints before/after reads and GC.
RSS checkpoints are **not** a continuous peak measurement; non-Unix platforms
report `NA`. Use an OS profiler for peaks (for example `/usr/bin/time -l` on
macOS or `/usr/bin/time -v` on Linux). Startup, filesystem caching, allocator
retention, and thread scheduling affect these results.

## Observations on main a24d70a

R 4.5.1, nanoarrow 0.9.0, Arrow 22.0.0, macOS arm64. These are local synthetic
measurements, not a production throughput claim:

- Parsing 20,000 actions with 1,000 bytes of URL padding used a 20.7 MiB body,
  took 0.312 s, allocated 89.1 MiB cumulatively in R, and retained a 39.2 MiB
  parsed result. This quantifies buffering overhead but does not validate a
  streaming parser for heterogeneous protocol/metadata/file actions.
- For one million rows and eight double columns, warm median constructor time
  was 2–3 ms. Streaming the full table took 21 ms, and eager tibble conversion
  took 49 ms (nine warm observations after the first read). The streaming
  median time to the first batch was about 4 ms. The OS-reported process peaks
  over ten reads were 372 MiB for streaming and 549 MiB for tibble conversion.
- In a separate 50-read streaming run, RSS after GC was 282.1 MiB at read 20,
  282.1 MiB at read 40, and 288.9 MiB at read 50. This run does not establish
  an unbounded native leak. It also does not prove memory is returned to the
  OS on every release.
- Ten threaded Arrow first-batch/close reads completed. Their warm median was
  4 ms, with a process peak of 194 MiB. This does not clear an intermittent
  DuckDB early-completion crash observed in a separate package test run.

The package still buffers metadata pages and stages all selected files before
opening a native stream. These scripts deliberately isolate later phases;
they do not measure network latency, cache-hit rate, cancellation during HTTP
staging, or time to first batch from the public Sharing API. A download/scan
pipeline or shared-engine design needs representative workload measurements
and lifecycle validation beyond these local timings. No such runtime change
is included here.
