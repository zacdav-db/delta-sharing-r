# Slim native core build and ownership

Status: Phase 2 compact local snapshot invocation
Integration target: `codex/delta-kernel-s7-overhaul`

## Native scope

The native crate is deliberately limited to:

- a single adapter module that may reference concrete Delta Kernel APIs;
- construction and eventual execution of Kernel scans;
- conversion to a Rust `RecordBatchReader`;
- Arrow C Stream ownership, cancellation, and panic containment; and
- resources whose lifetime must exactly match an active Kernel stream.

It contains no profile, credential, auth, Delta Sharing HTTP, retry,
pagination, protocol, NDJSON, planning, or synthetic-log implementation.

The compact internal invocation accepts only:

- an absolute prepared local table path or `file://` URI;
- an optional ordered projection;
- an optional exact non-negative row limit; and
- a positive output batch-size ceiling.

It constructs a real Kernel `Snapshot` and `Scan` through the default engine,
converts `ArrowEngineData` to logical Arrow record batches, and stops pulling
as soon as the exact limit is satisfied. Output batches never exceed the
requested ceiling; Kernel file and row-group boundaries may yield smaller
batches. The deterministic synthetic callable remains only as an ABI/lifecycle
test and is not a second table reader.

## Binding boundary

The binding does not use `extendr`. A small registered C `.Call` shim:

1. validates R scalar arguments and the nanoarrow external pointer;
2. calls a pure Rust `extern "C"` function with plain values and a status
   buffer;
3. converts a completed Rust status into an R result or error; and
4. registers exact routine names and argument counts while disabling dynamic
   symbols.

Rust never calls the R API and never retains an R object. R errors are raised
only after the Rust call has returned, so an R long jump cannot cross Rust
frames. Arrow batches cross only through the Arrow C Stream ABI.

Kernel, object-store, reqwest, Parquet, and Arrow implementation errors are
mapped to fixed stage messages before crossing the ABI. Raw error formatting
is prohibited because a Kernel action can contain a presigned URL. Column
validation may identify a caller-supplied name, but table locations and action
URLs are never interpolated into native conditions. Panic payloads are also
discarded at the constructor, Arrow callback, and outer FFI boundaries.

## Pinned dependency stack

- `delta_kernel = 0.22.0`, with Arrow 57 and the default rustls engine
- `arrow-array = 57.3.0`
- `arrow-schema = 57.3.0`
- `same-file = 1.0.6` for stable cross-platform root identity (already in the
  locked Kernel graph, so this adds no resolved package)
- Rust MSRV 1.88

`src/rust/Cargo.lock` is committed and normal source builds use `--locked`.
There are no checked-in unpacked crate sources or path patches.
The release profile strips debug information and uses thin LTO. Full LTO
offered no end-to-end scan evidence to justify its source-build cost, while
disabling LTO produced a 45 MiB local shared library. Thin LTO is the bounded
packaging compromise until scan benchmarks can justify another profile.

The Kernel 0.22 pin is intentional interoperability scope, not a claim that it
is the newest crate: the official Delta Sharing Python wrapper on the current
delta-sharing integration line also pins Delta Kernel 0.22 with Arrow 57 and
the rustls default engine. Moving to Kernel 0.25 and its split default-engine
crate changes the API and packaging surface. Evaluate that as a dedicated
upgrade only with a new lockfile audit, MSRV check, offline archive measurement,
cross-platform link proof, and parity tests against the official wrapper.

Delta Kernel's default rustls engine transitively builds `aws-lc-sys`, which
requires a working C/CMake toolchain. Its complete source-build and native-link
requirements still need proof on Linux, macOS, and Windows.

## Offline source releases

The repository intentionally does not contain a partial vendor directory.
Before an offline/CRAN-style source release, generate a complete archive from
the committed lockfile:

```sh
cargo vendor \
  --manifest-path src/rust/Cargo.toml \
  --locked \
  --versioned-dirs \
  src/vendor
tar -cJf src/rust/vendor.tar.xz -C src vendor
```

Store Cargo's emitted source replacement as
`src/rust/vendor-config.toml`. The Makevars recipes detect both files, use a
package-local Cargo home, and build with `--frozen -j 2`. A clean source
tarball must be installed with network access disabled before release.

The current resolved graph is large because the pinned Kernel default engine
includes Arrow, Parquet, object-store, TLS, and cloud support. The resulting
archive and package binary must be measured rather than assuming they fit CRAN
size expectations.

## Ownership contract

```text
nanoarrow external pointer
  owns ArrowArrayStream
    owns panic-boundary RecordBatchReader
      owns cancellation and metrics
      owns Kernel scan/engine
      optionally owns one verified prepared-root cleanup token
```

Releasing or exhausting the stream drops active resources exactly once.
Emitted Arrow arrays retain their buffers independently. Panics during
construction or batch pulls are contained and returned as status/error
payloads.

The cleanup token is native lifecycle glue, not a Rust synthetic-log
implementation. R creates and populates the log. The token can only be
constructed for an absolute private `.delta-sharing-snapshot-*` root with mode
0700 on Unix, the package marker, the exact `table/_delta_log/version-zero`
shape, no symlinks, and a canonical table equal to `root/table`. Ordinary
local-table scans never receive deletion capability. The token records root
filesystem identity at construction, then repeats the exact shape, canonical
containment, no-link/reparse-point, permission, and identity checks immediately
before removal. A replaced or mutated root is deliberately left in place.
Terminal stream paths take and drop the Kernel reader before dropping the
cleanup token.

Cleanup uses only `remove_file` and `remove_dir` on the known version-zero
shape, never recursive removal. Each stage gets three immediate attempts. A
transient failure retains the canonical root, stable identity, and stage in a
process-local queue; every later native call and `.onUnload` run the bounded
reaper after revalidation. Diagnostics expose only the pending count. A
same-UID path race remains theoretically possible between validation and one
filesystem call, but it cannot induce recursive traversal: injected content,
non-empty directories, changed identity, links, and reparse points fail closed.

## Developer checks

```sh
cargo fmt --manifest-path src/rust/Cargo.toml --all -- --check
cargo clippy \
  --manifest-path src/rust/Cargo.toml \
  --workspace --all-targets --all-features --locked -- -D warnings
cargo test \
  --manifest-path src/rust/Cargo.toml \
  --workspace --all-features --locked
R CMD INSTALL --preclean .
NOT_CRAN=true Rscript -e \
  'testthat::test_dir("tests/testthat", package = "delta.sharing", load_package = "installed")'
R CMD build .
R CMD check --no-manual delta.sharing_*.tar.gz
```

The synthetic stream benchmark in `bench/native-stream.R` measures the FFI and
Arrow handoff only. It is not evidence of end-to-end Kernel scan performance.

## Current proof

The foundation was validated on macOS arm64 with R 4.5.1, rustc 1.92, and the
declared 1.88 MSRV across the resolved graph:

- 326 locked Rust packages, all from normal upstream registry dependencies;
- 28 Rust unit tests covering real Kernel Snapshot/Scan execution, logical
  schema and projection, exact limits, zero/one/multiple batches, the C ABI
  status boundary, early release, buffer lifetimes, prepared-root cleanup and
  reaping, fixed errors, and panic containment; the suite passed three
  consecutive full runs after the loopback-server hardening;
- focused installed-package native snapshot tests and the full installed R
  suite passed, with four unrelated tests skipped under their existing CRAN
  guard;
- a final clean `R CMD check --no-manual` of the exact post-hardening source
  archive completed with status `OK`;
- a 32,327,152-byte installed shared library (26,281,664 bytes after a
  separate `strip -x` measurement);
- a 118,389-byte source archive without unpacked dependency sources; and
- loopback HTTP evidence that the default engine follows a presigned object
  action without losing its query, plus fixed-message redaction tests for a
  downstream request failure.

This is local evidence, not cross-platform build proof. Linux and Windows
source builds, a network-isolated source install using a complete dependency
archive, dependency-license inventory, real TLS/HTTPS and deletion-vector
coverage, package-size disposition, and end-to-end Kernel scan performance
remain release gates.
