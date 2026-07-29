# Delta Sharing R — Handover

Date: 2026-07-29
Branch: `codex/delta-kernel-s7-overhaul` (current refactor work is uncommitted)
Status: snapshot and CDF reads complete; version- and timestamp-bounded CDF are
proven live.

---

## 1. What this project is

A ground-up redesign of the `delta.sharing` R package. The old package (on
`main`) was an R6 client that downloaded Parquet to disk and collected with
`arrow::open_dataset()`. An intermediate rewrite (the early history of this
branch) over-engineered it into ~12,800 lines of S7 classes with heavy
abstraction layers.

This branch rebuilds it as a **small, clean R6 package** (~2,300 lines of R)
that leans on:
- **Delta Kernel** (Rust, via a narrow C ABI + nanoarrow) for the row-scan hot path,
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
that stream to an Arrow `RecordBatchReader`.

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

Rust: `src/rust/src/{lib.rs (C ABI), kernel/adapter.rs, stream/mod.rs}`.
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
integration tests plus one credentialed CDF test). Rust: 34 tests. Style: air-clean
(`air format --check R/ tests/`). Run tests with
`Rscript -e 'options(Ncpus=1); pkgload::load_all("."); devtools::test(".")'`.

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

## 8. Known gaps / TODO

- **CDF integration environment**: the public endpoint has no CDF table, so the
  live CDF test requires the credentialed E2 profile and remains opt-in.
- **`R CMD check` + pkgdown**: not run locally (no Pandoc in this env). CI has
  Pandoc. pkgdown config (`_pkgdown.yml`) + workflow exist.
- **Commit**: nothing committed. When ready: branch is
  `codex/delta-kernel-s7-overhaul`; end commit messages with the required
  Co-authored-by trailer.
- **ADRs**: `design/adr-004-r6-object-system.md` records the R6-over-S7 decision
  (supersedes adr-001). ADR-002/003 (Rust/Arrow boundary, R-first scope) still hold.
- Full running notes: the agent memory file
  `~/.claude/projects/.../memory/delta-sharing-r6-redesign.md` has the blow-by-blow.

## 9. Licensing note

Attribution was corrected this session: the package is copyright **Zac Davies**
(not Databricks — it is not Databricks-funded). LICENSE is Apache-2.0,
`DESCRIPTION` lists Zac as `aut, cre, cph`, contact `zac@databricks.com`
(contact only, not a copyright claim).

## 10. Dev helpers

- `tools/spin.R` — offline spin against local fixtures.
- `tools/spin-live.R` — progress-enabled live spin against the credentialed
  250M-row deletion-vector and nested-data fixture.
- `tests/testthat/fixtures/delta/` — real local Delta tables (snapshot + cdf)
  for kernel tests that need no network.
