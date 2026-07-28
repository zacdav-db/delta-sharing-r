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

If the observed memory is outside the agreed deployment envelope, the next
implementation should be a permission-restricted R staging sink that validates
and writes bounded action runs before deterministic merge/publication. This is
R-owned protocol and planning work. These measurements have no Rust comparator
and do not satisfy ADR 003's gate for expanding Rust ownership.
