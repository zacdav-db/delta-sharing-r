# Slim native core build and ownership

Status: Phase 1 lifecycle foundation
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

The Phase 1 callable produces deterministic record batches solely to test the
Arrow ABI and lifecycle before the real compact Kernel invocation is added.
It is internal and is not a second reader implementation.

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

## Pinned dependency stack

- `delta_kernel = 0.22.0`, with Arrow 57 and the default rustls engine
- `arrow-array = 57.3.0`
- `arrow-schema = 57.3.0`
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
      owns Kernel scan/engine and temporary resources
```

Releasing the stream drops the owner exactly once. Emitted Arrow arrays retain
their buffers independently. Panics during construction or batch pulls are
contained and returned as status/error payloads.

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
- 15 Rust unit tests covering Kernel construction, the C ABI status boundary,
  Arrow stream ownership, early release, buffer lifetimes, errors, and panics;
- package installation and R tests using nanoarrow and arrow consumers;
- a 2.9 MiB stripped shared library (3,086,768 bytes for the direct install
  and 3,087,136 bytes in the clean package check);
- a 57,791-byte source archive without unpacked dependency sources;
- a 210.77-second clean local install (Cargo compilation reported 3m 25s); and
- a 233.40-second clean `R CMD check --no-manual` with status `OK`.

This is local evidence, not cross-platform build proof. Linux and Windows
source builds, a network-isolated source install using a complete dependency
archive, dependency-license inventory, and end-to-end Kernel scan performance
remain release gates.
