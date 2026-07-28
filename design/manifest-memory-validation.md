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

The HTTP body is pulled in bounded chunks, but the current planner retains all
normalized file actions until publication. Pagination retains page lists and
then flattens them. Synthetic-log validation builds whole-manifest state,
ordering, and action views, then encodes every commit line before the writer
opens the commit. Peak R memory is therefore `O(file_count)` with several
simultaneous representations. It is hard count-bounded, not unbounded.

The JSON artifact reports wall time, wire bytes, commit bytes, fresh-process
peak RSS, and peak RSS above the zero-file baseline. No release workload RSS
limit has yet been agreed, so the harness records action retention as
`not_evaluable` instead of inventing a passing threshold.

The repeatable 100,000-file result justifies designing an R-side mitigation,
but a safe staged implementation is not a narrow patch: it must preserve
global duplicate detection, deterministic type/ID ordering, atomic
publication, redaction, expiry checks, and cleanup across success and failure.
This evidence-only change therefore does not alter production behavior.

If the observed memory is outside the agreed deployment envelope, the next
implementation should be a permission-restricted R staging sink that validates
and writes bounded action runs before deterministic merge/publication. This is
R-owned protocol and planning work. These measurements have no Rust comparator
and do not satisfy ADR 003's gate for expanding Rust ownership.
