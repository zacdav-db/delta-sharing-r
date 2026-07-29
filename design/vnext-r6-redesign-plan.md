# Delta Sharing R — R6 redesign plan

Status: implemented; retained as design history. See `design/HANDOVER.md` for
the current file map and validated behavior.
Date: 2026-07-29
Supersedes intent of: ADR-001 (S7 choice), auth portion of the vNext build

## 1. Goal

Trim the package to a small, clean, R6-based surface that leans on the Delta
Kernel (via a narrow Rust bridge) for the row hot path and on well-known R
packages (`httr2`, `openssl`, `jsonlite`, `nanoarrow`) for everything else.
No backwards-compatibility layer with either `main` or the current S7 branch.

Guiding decisions (agreed):

- **Object system:** R6, in the spirit of `main` — reference-semantics
  `client -> table -> reader`, methods on the object.
- **Auth:** bearer + OAuth (profile v2), but implemented with `httr2`'s
  `req_oauth_*` / `req_auth_bearer_token()` and `openssl` primitives rather than
  a hand-rolled JWT/base64/token-cache layer.
- **Rust boundary:** kernel scan + Arrow C Stream lifecycle only. The kernel
  stays the row hot path. Everything else is R unless profiling shows a
  *material* end-to-end win. Tiny per-call overheads are acceptable.
- **Execution:** written plan first (this document), then build.

## 2. Where the current branch stands (baseline)

- ~12,800 lines of R across 30 files; 26 exported symbols; 6 S7 classes plus a
  parallel set of functional generics.
- Notable heavy modules: `synthetic-log.R` (1,399), `snapshot-planning.R`
  (1,012), `auth.R` (819), `protocol-ndjson.R` (790), `http-transport.R` (717),
  `discovery-planning.R` (610), `parquet-response.R` (616).
- Abstraction layers that add indirection without user value:
  `execution-interface.R` (callback registry between public generics and work),
  private-handle registries + finalizers in `profile-context.R`, read-only S7
  property backing, a dedicated diagnostics object hierarchy.
- Rust: ~2,900 lines (kernel `adapter.rs`, `stream/mod.rs` lifecycle, `lib.rs`
  C ABI). Pinned `delta_kernel = 0.22.0`, `arrow 57.3`. Exposes snapshot + CDF
  Arrow-C-Stream population, cancellation, panic containment. **This is worth
  keeping largely as-is.**
- `main`: 6 R files, R6 `SharingClient` + `SharingTableReader`, bearer-only,
  downloads Parquet to disk and collects with `arrow::open_dataset()`.

The seam that makes this tractable: R does all protocol/auth work and writes a
**synthetic local `_delta_log`**; Rust's kernel reads from that local path. Rust
never touches the network, auth, or the sharing protocol. We keep that seam.

## 3. Target public API (R6)

Shape mirrors the Python staged object model (delta-io/delta-sharing #862/#949):
`client -> table -> snapshot/changes -> materializer`. Query options live on the
`snapshot()` / `changes()` factory call (not on the table, not as chained
setters), and materializers hang off the returned snapshot/changes object.

```r
library(delta.sharing)

# Client: owns credentials + auth, matches main's ergonomics
client <- sharing_client("~/config.share")          # path, or
client <- sharing_client(profile_list)               # parsed profile list

# Discovery (methods return tibbles/data frames)
client$list_shares()
client$list_schemas()
client$list_tables()

# Table handle: cheap, reusable
orders <- client$table("sales.default.orders")
events <- client$table(share = "product", schema = "default", name = "events.v2")

# Metadata (no row scan)
orders$version()
orders$metadata()
orders$schema()
orders$protocol()

# Snapshot: query options on the factory call, then materialize
snap <- orders$snapshot(version = 42, columns = c("order_id", "amount"), limit = 1000)
snap$to_data_frame()        # base data frame (nanoarrow)
snap$to_arrow()             # arrow Table (optional {arrow})
snap$to_arrow_reader()      # arrow RecordBatchReader (lazy, optional {arrow})
snap$to_arrow_stream(batch_size = 65536L)   # nanoarrow_array_stream (lazy, the one true path)

# Change data feed (same shape; Arrow CDF split to follow-up like the Python PR)
orders$changes(starting_version = 120, ending_version = 125)$to_data_frame()
```

Naming maps to Python but stays idiomatic R: `to_arrow()`/`to_data_frame()`/
`to_arrow_reader()` map to Python's `to_arrow()`/`to_pandas()`/
`to_record_batch_reader()`. All materializers consume the **one** kernel Arrow
stream; eager forms and the Arrow reader are adapters over the lazy stream.

Question #1 (read-config style) is now **resolved**: options on
`snapshot()`/`changes()`, matching the Python PR, not chained setters.

### Exported surface (target ~6–8 symbols)

- `sharing_client()` — constructor wrapper (keep the friendly free function).
- `SharingClient` (R6 generator), `SharingTable` / reader generator(s) as
  needed.
- `sharing_creds_from_env()` — carry over from `main`, useful and tiny.
- `print()` S3 methods for the classes.

Everything else becomes internal (not exported): protocol parsing, planning,
synthetic-log writing, the native bridge.

## 4. Module map (target)

| File | Responsibility | Source |
|------|----------------|--------|
| `client.R` | `SharingClient` R6: profile, auth, discovery, `$table()` | rebuild, informed by `main` |
| `table.R` | `SharingTable` R6: metadata + `$read*()` / `$changes()` | rebuild |
| `auth.R` | httr2/openssl-based auth: bearer + OAuth cc + JWT | **rewrite, ~100–150 lines** |
| `profile.R` | parse profile v1/v2 (path, json, list) | trim from `profile-context.R` |
| `requests.R` | httr2 request building, pagination, error mapping | trim `http-transport.R` + `main/requests.R` |
| `protocol.R` | parse discovery/metadata JSON responses (small) | trim `protocol-ndjson.R` |
| `synthetic-log.R` | stream Delta-format NDJSON -> local `_delta_log` (R) | rewrite to stream; collapse from 1,399 lines |
| `native.R` | `.Call` shim to Rust kernel scan on local log | keep `native-core.R` core |
| `materialize.R` | stream -> data.frame / arrow adapters | trim from `materializers.R` |
| `zzz.R` | `useDynLib`, onload | keep |
| `src/rust/**` | kernel adapter + Arrow stream lifecycle | **keep as-is** |

Expected R line count: roughly 2,000–3,500 (vs. 12,800), driven mostly by
deleting the auth machinery, the execution-interface indirection, the private
handle registries, the diagnostics hierarchy, and the S7 property scaffolding.

## 5. What to keep / rewrite / cut

**Keep (largely as-is):**
- The entire Rust crate and C ABI (`src/rust/**`, `src/native.c`, headers).
- `sharing_creds_from_env()` from `main`.

**Keep in R, but rewrite to stream (revised — see Sections 6.0/6.1):**
- **Delta-format synthetic-log write.** Python does this in Python in ~30 lines
  (unwrap `deltaSingleAction`, write bytes). Keep it in R and **stream NDJSON
  lines to the temp file** instead of materializing the manifest — this fixes
  the 450 MiB / 100k-file case without any Rust boundary change. Collapses most
  of `synthetic-log.R` (1,399 lines).
- **Parquet-format synthesis** (`parquet-response.R`, 616 lines) — **in scope**
  (match Python) but drastically simpler: synthesize a **flat** `add` per file
  (`path`/`partitionValues`/`size`/`stats`); Parquet format has no deletion
  vectors so no DV synthesis. Feed the same kernel path (Section 6.3). Target
  ~50–100 lines. Stays in R; no Rust change.

**Rewrite:**
- `auth.R` → thin `httr2`/`openssl` layer, modeled on brickster's
  `package-auth.R` (same author): `httr2::oauth_client()` + `httr2::req_oauth()`
  / `httr2::oauth_token()` own the token exchange and caching; bearer via
  `req_auth_bearer_token()`; private-key JWT assertion via `openssl` signing.
  Layered resolution (profile field -> env var -> explicit arg) as brickster
  does with `resolve_oauth_auth_mode()`/`db_auth_type()`. Target ~100–150 lines
  vs. 819; no hand-rolled base64url/JWT/token-cache.
- Client/table as R6 with methods, replacing S7 classes + functional generics.

**Cut:**
- `execution-interface.R` (callback registry) — call the work directly.
- Private-handle registries + finalizers in `profile-context.R` — R6 private
  fields hold auth state.
- The entire diagnostics feature (`diagnostics.R`, `SharingReadDiagnostics`,
  `read_diagnostics()`) — dropped, not folded. Not part of the public surface.
- S7 classes, generics, `.readonly_property` scaffolding, `s7-interface-naming-matrix.md`.
- Parallel "capabilities"/"conditions" abstraction where a simple `abort()` with
  a class does the job.

## 6. Rust/R boundary (unchanged intent, re-confirmed)

- Rust owns: kernel snapshot/CDF scan, Arrow C Stream export, cancellation,
  panic containment, lifecycle of the native stream + coupled temp-log cleanup.
- R owns: profiles, auth, HTTP, retry, pagination, NDJSON parse, synthetic-log
  write, planning, adapters.
- Boundary crossing frequency: once per read setup + once per Arrow batch pull.
  Never per row. This satisfies "no compromise on performance; tiny overheads
  fine."
- Moving an R responsibility into Rust requires a profiled, material
  end-to-end win — otherwise it stays in R. (Carry ADR-003's performance-gate
  spirit; drop the ceremony.)

### 6.0 What Python delta-sharing actually does (precedent)

Audited `delta-io/delta-sharing` PR #949 + current `reader.py`. Key facts:

- **Python writes the synthetic `_delta_log` in Python, not Rust.** The Rust
  side is `delta-kernel-rust-sharing-wrapper`, which only takes a local
  `table_path` and runs `ScanBuilder(snapshot).build().execute()`. Rust never
  sees the sharing protocol.
- **The Delta-format transform is ~30 lines and constructs nothing.** The
  server's Delta-format response already carries fully-formed Delta actions:
  `line["protocol"]["deltaProtocol"]`, `line["metaData"]["deltaMetadata"]`,
  `line["file"]["deltaSingleAction"]`. Python unwraps the field and writes the
  bytes straight to `00…0.json`. No `Add`/`Metadata` struct construction, no
  DV/stats/tags normalization.
- **Struct construction only matters on the legacy Parquet path**, where the
  server sends parquet file metadata and the client must *synthesize* Delta
  `add` actions. That is the source of most of R `synthetic-log.R`'s bulk.
- Object model: `client.table(name).snapshot(...).to_arrow()/to_record_batches()
  /to_record_batch_reader()`; `use_delta_format` selects the path.

**Implication:** the earlier "move the log to Rust for correctness of the action
structs" rationale is largely moot for the Delta-format path — there are no
structs to get right, just field unwrapping. See revised recommendation in 6.1.

### 6.1 Synthetic-log construction: revised recommendation

Original plan proposed moving the whole transform to Rust. The Python precedent
(6.0) reframes this:

- **Delta-format path (modern default):** keep it in **R**, like Python keeps it
  in Python. It is unwrap-`deltaSingleAction`-and-write, ~30 lines. The 450 MiB
  / 100k-file blowup in the current R branch is caused by materializing every
  NDJSON line as a nested R list, **not** by the transform. Fix it by
  **streaming NDJSON lines to the temp file** (read line, write bytes, discard)
  instead of holding the whole manifest. No Rust needed; no ADR-003 change for
  this path.
- **Parquet-format path (legacy):** this is the only part that genuinely
  synthesizes Delta actions (DV/stats/partition normalization). Decide whether
  we even support Parquet-format responses in v1 (Python treats Delta format as
  the path forward). If we do, this is where struct correctness matters and
  where a Rust move — or delta-rs structs (6.2) — could still be justified. If
  we defer Parquet format, most of `synthetic-log.R` disappears with no Rust
  boundary change at all.

**Net:** the strong version of the boundary expansion is no longer clearly
warranted. Prefer R-side streaming for Delta format; only revisit Rust/delta-rs
for the Parquet-format synthesis path, and only if we commit to supporting it.
This keeps Rust minimal (the original goal) *and* keeps the hot path fast.

### 6.3 Both formats, one kernel hot path (match Python's capability, not its structure)

"Match Python's level of support" = support **both** Delta- and Parquet-format
responses, autoresolving by default. Python's `snapshot(use_delta_format=None)`
autoresolves; `True` forces Delta (kernel); `False` forces Parquet.

Python achieves this with **two reader paths**: Delta -> `delta-kernel-rust`;
Parquet -> a separate pyarrow downloader that reads presigned URLs and
normalizes partition columns / timestamps / casing. Two hot paths, two
normalization surfaces.

**We keep the current R branch's better idea: one kernel hot path for both.**
Parquet-format responses are normalized into a synthetic `add` and fed to the
same kernel scan — no second downloader, no separate Arrow path. This is the
one architectural improvement worth carrying from the current branch.

Both format transforms are cheap once we stop materializing manifests:
- **Delta-format:** pass `file.deltaSingleAction` through verbatim; stream to
  disk (~30 lines, Section 6.0). DVs/stats/tags ride along pre-formed.
- **Parquet-format:** synthesize a **flat** `add` from `{path, partitionValues,
  size, stats}`. Parquet-format is the older protocol and carries **no deletion
  vectors**, so no DV synthesis is needed (~50–100 lines). The current branch's
  616-line `parquet-response.R` + DV handling in `synthetic-log.R` is
  over-engineered for what the Parquet path actually requires.

Both stream NDJSON/actions to the temp log in **R**; the kernel reads the local
log. **No Rust boundary change** for either format. Rust stays exactly as narrow
as today (kernel scan + Arrow stream). delta-rs / homegrown-struct question
(6.2) is moot unless a future DV-bearing case appears on a non-kernel path.

### 6.1a (superseded) Original "move to Rust" rationale, retained for context

The synthetic log is a **format transform**, not protocol I/O: take the Query
Table response actions (`protocol` / `metaData` / `file{url, partitionValues,
size, stats}`) and emit a local `_delta_log/00…0.json` whose `add` entries point
at the presigned URLs. The kernel then reads that log and fetches data directly
from the URLs. No second downloader.

**Why this crosses to Rust (clears the "material difference" bar):**
- Recorded 100,000-file manifest cost **450 MiB / 45 s** in R; the disk-backed
  staging workaround only reached 153 MiB / 77 s. That is a real end-to-end
  memory/time cost, not a tiny overhead.
- R stops allocating 100k action objects on its heap: R streams the raw NDJSON
  response bytes to a temp file (cheap) and passes the path; Rust parses and
  emits the log with `serde_json`.
- Deletes the largest R module (`synthetic-log.R` 1,399 + `parquet-response.R`
  616 lines).

**Boundary contract (new FFI op):** R passes `{ ndjson_path, response_format,
scan controls }`; Rust returns the `nanoarrow_array_stream` (or writes the log
and returns a prepared-table path, then reuses the existing snapshot/CDF FFI —
decide during scaffold). R still owns HTTP, auth, retry, pagination, and the
temp-file lifecycle handshake.

**Cost / caveats:**
- Rust gains knowledge of Delta Sharing action shapes (sharing `file` -> delta
  `add` mapping). This is deliberate protocol coupling and **requires a
  superseding ADR** (supersedes the synthetic-log clause of ADR-003).
- Parquet-format responses need the same sharing->add normalization on the Rust
  side (previously `parquet-response.R`).
- Keep the transform narrow and reviewable as one unit, per ADR-003's FFI rule.

### 6.2 How to source the action structs (decision + fallback)

Two options for the Rust structs that serialize to log JSON:

**A. Homegrown serde structs (chosen default).** Define our own
`#[derive(Serialize)]` structs in `src/rust/log.rs`. `serde_json` already
available transitively; make it a direct dependency.

- Pro: no new heavy dependency; stays decoupled from `delta_kernel` / `arrow`
  version churn; our exact pins (`delta_kernel = 0.22.0`, `arrow 57.3`) are
  unaffected; keeps Rust minimal per the redesign premise.
- Con: we own correctness of the JSON, and it is **not trivial** (see below).

**B. `deltalake-core` (delta-rs) public action structs.** Reuse its public,
serde-capable `Add`/`Metadata`/`Protocol`/`Remove` (delta_kernel 0.22's are
`pub(crate)`, verified — not reusable, so this is the only "reuse" path).

- Pro: battle-tested action JSON incl. deletion vectors.
- Con: large crate pulled in for ~3 structs; its own `delta_kernel`/`arrow`
  pins risk conflicting with ours (the pre-1.0 churn ADR-002/003 insulate
  against); we would use only its serialization surface, not its commit /
  object-store / transaction machinery (our kernel reads one synthetic commit
  from local disk).

**Decision: start with A.** The log is a single synthetic local commit, so we
need the serialization surface but none of delta-rs's table-management code.

**Fall back to B if, and only if:** correctness of the homegrown structs proves
fragile — most likely around **deletion vectors** (the audit found
`synthetic-log.R` already handles `deletionVector` / `deletionVectorFileId`,
`stats`, `tags`, `numRecords`, `partitionValues` maps, `remove`/`cdc` and
`deltaSingleAction` unwrapping, and protocol reader/writer feature negotiation —
this is ~150–200 lines of faithful struct surface, not a flat `add[]`). If DV or
stats passthrough is error-prone to hand-roll, delta-rs's structs are worth the
dependency cost. Reassess after porting the DV/stats path with fixture parity
tests against the current R output. Requirement either way: **lightweight, and
no correctness regression** vs. today's synthetic log.

## 7. Execution phases (once plan is approved)

1. **Scaffold R6 skeleton** on a fresh branch off the current one: `client.R`,
   `table.R`, snapshot/changes generators with method stubs; keep Rust + native
   bridge wired.
2. **Auth rewrite** on httr2/openssl (brickster model); prove bearer + one OAuth
   flow against a mock; port profile v1/v2 parsing.
3. **Discovery + metadata** methods returning data frames (incl.
   `list_all_schemas()`/`list_tables_in_share()`); port protocol parse.
4. **Read path**: `snapshot()$to_arrow_stream()` → kernel via streamed synthetic
   log; then `to_arrow()` / `to_data_frame()` adapters over the one stream.
   Wire the format control (autoresolve / Delta / Parquet) through the single
   kernel path (Section 6.3).
5. **CDF**: `changes()` object + materializers.
6. **Prune**: delete S7, execution-interface, registries, diagnostics; update
   NAMESPACE/DESCRIPTION (drop `S7`, add `R6`).
7. **Docs + tests**: roxygen, README, vignette, testthat against mock; keep the
   Rust lifecycle tests.
8. **pkgdown site + CI** (Section 7.1).

### 7.1 pkgdown site

Add a pkgdown site published via GitHub Actions to GitHub Pages.

- **`_pkgdown.yml`**: reference index grouped to mirror the staged API —
  *Client & profile* (`sharing_client`, `sharing_creds_from_env`,
  `SharingClient`), *Tables* (`SharingTable`, metadata methods), *Reads*
  (snapshot/changes generators + materializers), *Package* (overview). Articles
  section carries the vignette(s). Use bootstrap 5 (`template: bootstrap: 5`).
- **CI**: add `.github/workflows/pkgdown.yaml` from
  `usethis::use_pkgdown_github_pages()` (r-lib/actions `examples/pkgdown.yaml`).
  Builds on push to the default branch, deploys to the `gh-pages` branch.
  Guard: the Rust toolchain must be available in the pkgdown job (reuse the
  setup steps from `r-cmd-check.yaml`) because reference examples load the
  compiled package.
- **`.Rbuildignore`**: add `^_pkgdown\.yml$`, `^docs$`, `^pkgdown$`.
- **`DESCRIPTION`**: set `URL` to include the pages site; add pkgdown to
  `Config/Needs/website`.
- **R6 docs note:** R6 generators document methods via roxygen `@description`
  blocks on the class; ensure each public method has a doc entry so the
  reference renders method signatures (unlike S7 generics, R6 methods are not
  auto-listed as separate topics).
- Keep the site content minimal and current — no evidence/proof dumps from
  `design/` (that dir is already in `.Rbuildignore`).

## 8. Decisions (resolved)

1. **Read configuration style** — options on `snapshot()`/`changes()`
   (Python-PR shape), materializers on the returned object. Not chained setters.
2. **Synthetic-log move to Rust** — **reversed after auditing Python**
   (Sections 6.0/6.1). Delta-format log stays in **R**, rewritten to stream
   NDJSON to disk (Python does the same in ~30 lines; fixes the 450 MiB case
   without Rust). No ADR-003 boundary change for the Delta path. Rust/delta-rs
   only reconsidered for the legacy Parquet-format synthesis path, and only if
   we support Parquet format in v1 (open — decision #6).
3. **Diagnostics** — **dropped.** No read-diagnostics feature. The public
   surface is only the snapshot/CDF interface.
4. **Scope** — **snapshot + CDF both in.** (Note: this differs from the Python
   PR, which split Arrow CDF to a follow-up. Revisit sequencing during phases if
   CDF blocks snapshot delivery, but both are in scope.)
5. **`main` convenience endpoints** — **keep them.** `list_all_schemas()` /
   `list_tables_in_share()` are just already-exposed endpoints and trivial to
   wire, so include them.
6. **Parquet-format responses in v1** — **support both, match Python.**
   `snapshot()`/`changes()` take a format control (`use_delta_format`-equivalent,
   R-idiomatic name TBD) with three states: unset -> **autoresolve** from table
   capability (default), Delta -> kernel path, Parquet -> supported too.
   Implementation: **keep the current R branch's unified kernel path** (one hot
   path) rather than Python's separate parquet downloader — see Section 6.3.
```
