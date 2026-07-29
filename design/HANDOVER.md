# Delta Sharing R — Handover

Date: 2026-07-30
Branch: `codex/delta-kernel-s7-overhaul`
Current recorded head: `92c4068` (`Keep eager read progress live between batches`)
Status: the lean R6 snapshot/CDF implementation is committed and live-proven;
the remaining work is release hardening, portability, lifecycle evidence, and
targeted R-side performance work.

---

## 1. What this project is

A ground-up redesign of the `delta.sharing` R package. The old package (on
`main`) was an R6 client that downloaded Parquet to disk and collected with
`arrow::open_dataset()`. An intermediate rewrite (the early history of this
branch) over-engineered it into ~12,800 lines of S7 classes with heavy
abstraction layers.

This branch rebuilds it as a **small, clean R6 package**
that leans on:
- **Delta Kernel 0.26 / Arrow 58** (Rust, via a narrow C ABI + nanoarrow) for
  the row-scan hot path,
- **httr2 / openssl** for HTTP + auth,
- **cli / rlang / purrr / tibble** for idiomatic R plumbing.

The user is the maintainer (Zac Davies). Directives that shaped it:
- Every line must justify itself (no defensive overkill, no wheel-reinvention).
- Internal functions are **not** dot-prefixed; cleanliness comes from not
  exporting them. Errors go through `abort()` (internal, in `conditions.R`).
- Mirror the Python delta-sharing client's proven design where sensible; use
  real integration tests, not just mocks.
- Keep Rust minimal but do not sacrifice performance.

## 2. Architecture (the key idea)

`client -> table -> snapshot()/changes() -> materializer`, mirroring the Python
staged object model. R does **all** protocol/auth/HTTP work and writes a private
local synthetic `_delta_log`; the Rust kernel reads that local path and streams
Arrow back. **Rust never touches the network or the sharing protocol** — this is
the same split the Python `delta-kernel-rust-sharing-wrapper` uses.

```
sharing_client(profile)                  # SharingClient (R6)
  $list_shares() / $list_schemas() / $list_tables()   # tibbles
  $table("share.schema.table")           # SharingTable (R6)
      $version() / $protocol() / $metadata() / $schema()
      $snapshot(version=, timestamp=, columns=, limit=, predicate=, response_format=)
          $to_data_frame() / $to_arrow() / $to_arrow_reader() / $to_arrow_stream()
      $changes(starting_version=, ending_version=, ...)
          $to_data_frame() / ...          # SharingChanges
```

Reads flow through **one** kernel Arrow stream; eager forms are adapters over
the lazy `to_arrow_stream()`, while `to_arrow_reader()` transfers ownership of
that stream to an Arrow `RecordBatchReader`. Eager reads with progress enabled
move that already-created C stream to one native collection worker so R can
repaint the CLI indicator while Kernel is blocked on object-store I/O. This
worker is Arrow/lifecycle glue only: it does not implement protocol, HTTP,
planning, or materialization policy.

## 3. Public API surface (exported)

`sharing_client()`, `SharingClient`, `SharingTable`, `SharingSnapshot`,
`SharingChanges`, and the `$.delta_sharing_interruptible_stream` S3 method.
Condition classes (public contract): `delta_sharing_error` + subclasses
`delta_sharing_{validation,auth,http,protocol,kernel,unsupported}_error` and
`delta_sharing_cancelled`.

## 4. File map (R/)

| File | Responsibility |
|------|----------------|
| `client.R` | `SharingClient` R6: profile, auth ctx, discovery, `$table()` |
| `table.R` | `SharingTable` R6: metadata methods + `snapshot()`/`changes()` factories |
| `readers.R` | `SharingReader` base + `SharingSnapshot`/`SharingChanges` materializers |
| `table-validation.R` | table identifier parsing and CDF range validation |
| `profile.R` | parse profile v1/v2 (path/JSON/list) -> plain list |
| `auth.R` | httr2/openssl auth: bearer, basic, oauth client-creds, private-key JWT |
| `http.R` | authenticated httr2 requests, pagination, retries, and error translation |
| `discovery.R` | list_shares/schemas/tables -> tibbles (purrr) |
| `table-metadata.R` | version, protocol, metadata, schema, and response-format resolution |
| `read-execution.R` | snapshot/CDF query orchestration, stream construction, and materializers |
| `kernel-log.R` | write the private `_delta_log` layout the native cleanup guard validates |
| `parquet-actions.R` | synthesize flat `add` actions for parquet-format responses |
| `native-bridge.R` | `.Call` shims, native argument validation, and stream lifecycle |
| `validation.R` | shared public argument normalization |
| `conditions.R` | `abort()` on `cli::cli_abort` with typed classes |
| `zzz.R` | `.onUnload`, useDynLib, importFrom |

Rust: `src/rust/src/{lib.rs (C ABI), collect.rs (progress worker),
kernel/adapter.rs, stream/mod.rs}`.
C shim: `src/native.c`. Header: `src/rust/include/delta_sharing_native.h`.

## 5. What works and is proven

**Snapshot reads are fully working and verified LIVE** against two endpoints:
1. Public open datasets (`sharing.delta.io`, profile in the integration helper).
2. A credentialed Databricks share at `~/Desktop/config.share` (v1 bearer,
   share `delta_sharing_r_vnext_share.delta_sharing_r_vnext.*`, 11 test tables).

All 8 snapshot variants read correctly live: `plain_snapshot_no_dv`,
`partitioned_orders`, `column_mapped_nested`, `complex_types` (nested structs,
arrays, timestamps, binary, unicode), `empty_snapshot` (0 rows, correct schema),
`small_snapshot`, `snapshot_narrow_250m`, `dv_nested_events_250m` (deletion
vectors).

**Offline test suite passes with 7 integration tests skipped** (six public
integration tests plus one credentialed CDF test). Rust: 38 tests; locked,
offline tests, formatting, and strict Clippy pass with Kernel 0.26 and Arrow
58.3. Run the R tests with
`Rscript -e 'options(Ncpus=1); pkgload::load_all("."); devtools::test(".")'`.

The 246,942-byte archive containing the current production implementation
passes
`R CMD check --as-cran --no-manual` on macOS arm64 with zero errors, zero
warnings, and one expected note for a new submission/development version. A
clean installed-package lifecycle gate passes repeated worker success, error,
finalizer, cancellation, handoff, cleanup, unload, and reload.

**Real bugs found via live testing and fixed** (all snapshot-path):
- `version()` uses `HEAD` on the table path (reference server 404s `GET /version`).
- Empty query body must serialize as `{}` not `[]` (the no-options snapshot case).
- Format resolution: `queryTable` rejects `responseformat=delta,parquet`; must
  resolve one format first via `/metadata` (`resolve_query_format`), then send it.
- Reader features are delta-only; omitted for parquet (`capability_header`).
- Synthetic-log layout must match the Rust cleanup guard's ownership contract
  (see Section 7).
- Structured predicate hints stay ergonomic R lists but are encoded as the
  protocol's JSON-string `jsonPredicateHints` field.
- Snapshot and CDF timestamps accept either `POSIXct` or protocol-native
  ISO-8601 strings; strings are passed through unchanged.
- Delta metadata projects `size` and `numFiles` from the Sharing wrapper rather
  than the nested Delta action, matching Python and live Databricks responses.
- Snapshot Query Table responses are consumed in bounded NDJSON chunks and
  staged directly into the private synthetic log. A 100,000-file benchmark
  reduced maximum RSS by 44.3% and transformation time by 8.0% while producing
  a byte-identical commit.

Profile parsing now follows Python's structural level: it extracts the fields
required by the selected profile shape and defers credential content checks to
httr2, openssl, the token endpoint, or the Sharing server. Older Phase 2
profile/auth/transport contract documents describe the superseded S7
implementation and are not the current R6 contract.

## 6. CDF (`changes()`)

CDF reads follow the Python Kernel path without interpreting CDF rows in R:

- `changes` uses **GET with query params** (version or timestamp bounds plus
  `includeHistoricalMetadata=true`). The ending bound is optional.
- CDF is forced to delta format; its capability header advertises only
  `deletionvectors,columnmapping`, matching Python's narrower CDF feature set.
- `sharing_query_changes()` and `bucket_cdf_actions()` retain protocol,
  versioned metadata, and verbatim `deltaSingleAction` file actions.
- The effective end is the last version represented by returned metadata or
  files. It can be earlier than the requested end; responses outside the
  requested range are rejected.
- `prepare_cdf_log()` writes every commit across that effective range, including
  empty interior commits. It puts protocol in the first commit, preserves
  metadata actions, sets file-action commit mtimes, and writes the fake
  `{start-1}` checkpoint when start > 0.
- The prepared-log handle carries the effective bounds. The log, Kernel
  `TableChanges`, and native cleanup guard all receive those same bounds.
- Fake checkpoint parquet bytes extracted from the Python client live at
  `inst/extdata/fake_checkpoint.parquet` (14175 bytes).

Live validation against `~/Desktop/config.share`,
`cdf_dv_interop`, versions 1 through 4 returned 3,500 rows:
500 deletes, 1,000 inserts, 1,000 update preimages, and 1,000 update postimages.
The row-producing commit versions are 1, 2, and 3; version 4 is a metadata-only
commit that closes the requested log range.

An exact timestamp range at `2026-07-29T07:35:42Z` is also live-proven: 500
`delete` rows at commit version 2. An open-ended read from metadata-only version
4 returns zero rows with the complete CDF schema.

For version ranges, an unversioned metadata action is assigned to the requested
starting version, matching Databricks' response shape. Timestamp-range metadata
must carry a version because that response supplies the synthetic log's lower
bound.

### How to verify CDF
```r
options(Ncpus=1); pkgload::load_all(".")
client <- sharing_client("~/Desktop/config.share")
tb <- client$table("delta_sharing_r_vnext_share.delta_sharing_r_vnext.cdf_no_dv")
tb$changes(starting_version = 1, ending_version = tb$version())$to_data_frame()
```

The credentialed test is gated separately:
```sh
DELTA_SHARING_RUN_CDF_INTEGRATION=1 \
DELTA_SHARING_TEST_PROFILE=~/Desktop/config.share \
Rscript -e 'pkgload::load_all("."); testthat::test_file("tests/testthat/test-cdf-integration.R")'
```

CDF progress is intentionally indeterminate. File-action statistics in the live
CDF response implied 4,906 rows while Kernel correctly emitted 3,500 changes,
so presenting those statistics as a percentage would be misleading.

### Debug-error decision still open
`src/rust/src/kernel/adapter.rs` (~line 237): the CDF error map was changed from
`.map_err(|_| "Delta Kernel CDF preparation failed".to_string())` to include the
real error (`format!("... {e}")`). Decide whether to retain that detail before
release; it may expose an internal temporary path.

## 7. Critical constraint: the native cleanup-guard log contract

The Rust `PreparedLogCleanup` (`src/rust/src/stream/mod.rs`) deletes the temp log
dir on stream release, so it validates ownership first. The R writer
(`kernel-log.R`) MUST produce exactly this layout or the native call fails:

```
<root, name starts ".delta-sharing-snapshot-", mode 0700>/
├── .delta-sharing-r-prepared-log        (marker, contents exactly "delta-sharing-r:vnext\n")
└── table/
    └── _delta_log/
        ├── 00000000000000000000.json    (snapshot: single commit)
        └── ...                          (CDF: one {version}.json per version, + {start-1}.checkpoint.parquet & _last_checkpoint when start>0)
```
The path handed to the kernel is `<root>/table`; `cleanup_root` is `<root>`.
Snapshot expects exactly `00...0.json`; CDF's `try_new_cdf` expects the
version-range JSONs (+ checkpoint bootstrap). Any layout drift = native failure.

## 8. Progress and performance

Commits `1850bcc` and `92c4068`, plus the current manifest/lifecycle slice,
contain the current performance work:

- Kernel 0.26's configurable source batches changed an 8,388,608-row local read
  from 8,448 approximately 1,000-row batches to 128 batches of 65,536 rows.
- Direct Arrow materialization improved from a 0.3645-second median on Kernel
  0.22 to 0.1425 seconds on Kernel 0.26.
- The current progress worker completed the same local progress-enabled read in
  a 0.2025-second median, down from 0.7790 seconds on the old synchronous
  R-per-batch replay path.
- On the live 250-million-row deletion-vector table, a 250,000-row bounded read
  displayed continuous progress while waiting for I/O and finished with the
  exact row count. Snapshot percentages are shown only when every returned file
  has trustworthy row statistics; deletion-vector cardinality and the exact
  limit are included. Otherwise the spinner remains live and reports rows
  without inventing a percentage.
- Lazy reads and eager reads with `progress = FALSE` remain on the direct Arrow
  C Stream path.
- Snapshot manifests no longer retain the complete nested action graph. At
  100,000 files, bounded R staging reduced maximum RSS from 398.6 MB to
  222.0 MB and transformation time from 21.950 to 20.196 seconds; the reported
  peak memory footprint fell by 63.1%.
- Bounded per-version CDF spooling was tested and rejected: it reduced maximum
  RSS by 24.9% but was 63.2% slower at 100,000 actions. Production CDF keeps
  the concise retained Python-style path until a representative workload
  justifies a different trade-off.
- A current same-profile matrix against Python `delta-sharing` 1.4.1 matched
  every result shape. R was within 2% on the 1M-row partitioned table and
  5–13% faster on the other non-empty snapshot sources tested. The empty
  control was 11% slower. One CDF 1–4 run took 318 seconds in R versus
  356 seconds in Python; both are dominated by hundreds of tiny remote files.

These results support the accepted boundary. No client, protocol, HTTP, or
planning responsibility should move to Rust. See
`design/performance-assessment-2026-07-29.md` for the measurements and caveats.

## 9. Known gaps / next gates

- **CDF large-manifest trade-off**: production CDF still retains its response
  actions. The bounded prototype was byte-equivalent and live-correct but
  failed the performance gate, so it was deliberately not integrated.
- **Progress lifecycle hardening**: local subprocess gates now prove typed
  SIGINT cancellation, cleanup, garbage collection, completed-worker
  unload/reload, detached-worker library pinning, and clean installed-package
  reuse. A genuinely blocked credentialed interrupt and hosted sanitizer/
  cross-platform evidence, especially Windows, remain open.
- **Cross-platform package proof**: minimum/release/development R and macOS,
  Linux, and Windows source/binary builds still require current-head evidence.
- **R coverage**: the first whole-tree measurement of the lean R6 rewrite is
  70.46%. The historical 91.83% S7 result is superseded; focused current-R6
  tests must raise this to the 90% release gate.
- **Credential rotation/history cleanup**: commit `f047384` copied the Desktop
  bearer token into `tools/spin-live.R`. The current tree now reads
  `~/Desktop/config.share`, but the credential remains in local branch history.
  Rotate it and clean that commit before any push or external handoff.
- **Release performance gates**: rerun controlled direct, progress, first-batch,
  RSS, backpressure, and cancellation benchmarks on the final candidate.
- **Optional integrations**: `{duckdb}` tests require the package to be
  installed; public CDF is unavailable, so credentialed CDF remains opt-in.
- **Provider-signed deletion vectors**: genuine signed-URL behavior still needs
  hosted, cross-platform proof.
- **CDF integration environment**: the public endpoint has no CDF table, so the
  live CDF test requires the credentialed Desktop profile and remains opt-in.
- **Debug-error decision**: `kernel/adapter.rs` currently includes the
  underlying Kernel error in CDF preparation failures. Confirm before release
  that this cannot expose a temporary local path.

`design/adr-004-r6-object-system.md` is the governing object-system decision
and supersedes ADR 001. ADR 002/003 continue to govern the Rust/Arrow boundary
and R-first scope.

## 10. Licensing note

The package is copyright **Zac Davies** (not Databricks — it is not
Databricks-funded). LICENSE is Apache-2.0. `DESCRIPTION` lists Zac as
`aut, cre, cph`; `zac@databricks.com` is a contact address only, not a
Databricks copyright claim.

## 11. Dev helpers

- `tools/spin.R` — offline spin against local fixtures.
- `tools/spin-live.R` — progress-enabled live spin against the credentialed
  250M-row deletion-vector and nested-data fixture.
- `tools/compare_connector.{R,py}` — same-profile snapshot/CDF timing harnesses
  for the development package and official Python connector.
- `tools/{snapshot,cdf}_manifest_benchmark_worker.R` — fresh-process retained
  versus bounded-staging memory/time evidence.
- `tests/testthat/fixtures/delta/` — real local Delta tables (snapshot + cdf)
  for kernel tests that need no network.
