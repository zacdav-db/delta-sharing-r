# Snapshot manifest memory validation

## Scope

`bench/vnext-manifest-memory.R` measures the production R path from streamed
Delta Sharing NDJSON through decoding, normalization, snapshot planning, and
private synthetic-log publication. It deliberately does not add a Rust entry
point or move protocol work across the native boundary.

The worker generates deterministic HTTPS add actions in bounded pull chunks.
Each measurement runs in a fresh R process under the platform high-water RSS
facility. A zero-file process provides the same-process-shape baseline. The
standard workload covers 1,000, 10,000, and 100,000 files; 100,000 is the
client's default requested page ceiling. The implementation separately rejects
more than 1,000,000 actions.

Install the exact checkout into a clean library and run:

```sh
BENCH_R_LIB="$(mktemp -d)"
R CMD INSTALL --preclean --library="$BENCH_R_LIB" .
R_LIBS="$BENCH_R_LIB" \
  Rscript tools/test_manifest_memory_harness.R
R_LIBS="$BENCH_R_LIB" \
  Rscript bench/vnext-manifest-memory.R \
    --mode standard \
    --output /tmp/delta-sharing-r-manifest-memory.json
```

Darwin uses `/usr/bin/time -l`; Linux requires GNU `/usr/bin/time -v`.
Unsupported platforms fail explicitly instead of guessing RSS units.

## Recorded Darwin arm64 evidence

Two compact, machine-labelled artifacts preserve the raw timing/RSS samples
and lifecycle outcomes:

- `evidence/manifest-memory-darwin-arm64-0c88b9e.json` is the original
  development capture based on integration commit `0c88b9e`. Its harness was
  still uncommitted, which the artifact records explicitly.
- `evidence/manifest-memory-darwin-arm64-09dbd9b.json` repeats the standard run
  from clean harness commit `09dbd9b`.
- `evidence/manifest-memory-darwin-arm64-d44c576.json` records the R-only
  bounded staging implementation from clean commit `d44c576`.

The clean capture used R 4.5.1 on Darwin 25.4.0 arm64, three fresh subprocesses
per successful workload, and a 157.812 MiB median zero-file peak-RSS baseline.

| Files | Median elapsed | Median peak RSS | RSS above baseline | Wire bytes | Commit bytes | Incremental RSS / wire |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.425 s | 157.609 MiB | 0 MiB | 0.250 MiB | 0.166 MiB | 0x |
| 10,000 | 4.229 s | 232.609 MiB | 74.797 MiB | 2.499 MiB | 1.660 MiB | 29.931x |
| 100,000 | 45.227 s | 607.922 MiB | 450.109 MiB | 24.987 MiB | 16.594 MiB | 18.014x |

The zero incremental 1,000-file median is baseline noise, not a zero-memory
claim. At 100,000 files the first and clean captures agree within 0.1 MiB on
incremental peak RSS and 1% on median elapsed time. This is a material local
signal: planning uses about 450 MiB above the zero-file process to transform a
25 MiB wire manifest into a 17 MiB commit. It is not a release threshold or
cross-platform proof.

The staged capture used the same host/workload with 1,024-action in-memory
runs and a 16-way R merge. Its zero-file baseline was 158.188 MiB.

| Files | Median elapsed | Median peak RSS | RSS above baseline | Incremental RSS / wire |
|---:|---:|---:|---:|---:|
| 1,000 | 0.471 s | 163.625 MiB | 5.438 MiB | 21.728x |
| 10,000 | 6.253 s | 241.062 MiB | 82.875 MiB | 33.163x |
| 100,000 | 76.753 s | 311.188 MiB | 153.000 MiB | 6.123x |

At 100,000 files, staged incremental peak RSS is 297.109 MiB lower, a 66.0%
reduction from the clean pre-staging capture. Median elapsed time increases
from 45.227 to 76.753 seconds, a 69.7% regression. At 10,000 files the staged
path is also worse: 47.9% slower with 10.8% more incremental peak RSS. The
implementation therefore resolves the material large-manifest retention
problem by making memory bounded, but it is explicitly a memory-for-time
tradeoff rather than a general performance win.

## Lifecycle cases

For the largest configured workload the harness also injects a commit-write
error after validation/encoding and drops an unreleased prepared snapshot for
finalization. Together with the successful explicit-release samples, the
controlled lifecycle gate requires:

- one response close;
- one published root while a successful prepared snapshot is live;
- zero roots after explicit release;
- zero roots after the injected publication error; and
- zero roots after finalization.

## Retention model and decision rule

The pre-staging planner pulled HTTP in bounded chunks but retained normalized
file actions, page lists, flattened files, validation/order views, and every
encoded commit line until publication. Its peak R memory was therefore
`O(file_count)` with several simultaneous representations.

The production planner now validates each decoded action immediately and keeps
at most 1,024 encoded records in memory. It writes permission-restricted action,
ID, and path runs, then performs shell-free 16-way R merges. Separate ID/path
merges enforce global duplicates; the action merge preserves deterministic
type/ID order. The final commit is streamed from the merged action run and the
run tree is removed before atomic publication. Pagination no longer retains
page file lists or constructs a flattened whole-manifest list.

The JSON artifact reports wall time, wire bytes, commit bytes, fresh-process
peak RSS, and peak RSS above the zero-file baseline. No release workload RSS
limit has yet been agreed, so the harness records action retention as
`not_evaluable` instead of inventing a passing threshold.

Focused production tests prove byte-identical commits against the retained-list
reference, multi-page behavior, duplicates within a run and across final merge
passes, mixed/malformed action rejection, Parquet totals/version checks,
deletion-vector protocol checks, commit-source early-return protection, and
cleanup after page mismatch, write error, explicit release, and finalization.

This remains R-owned protocol/planning work. The memory result has no Rust
comparator and does not satisfy ADR 003's gate for expanding Rust ownership.
An agreed release RSS/time envelope and non-Darwin capture remain open.
