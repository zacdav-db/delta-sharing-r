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

The repository intentionally does not contain a partial vendor directory or a
development-branch archive. Before an offline/CRAN-style source release,
generate and verify a complete archive from the committed lockfile:

```sh
python3 tools/rust_vendor.py generate
python3 tools/rust_vendor.py check
```

The generator runs `cargo vendor --locked --offline --respect-source-config
--versioned-dirs` in a temporary copy, so a release machine may use its
configured registry mirror without encoding that mirror in the resulting
package. It validates every registry package and file checksum against
`Cargo.lock`, normalizes Cargo's emitted source replacement to the
package-local `vendor` directory, and writes a byte-reproducible
`src/rust/vendor.tar.xz`. Run `cargo fetch --manifest-path
src/rust/Cargo.toml --locked` first when the release machine does not already
cache every locked crate. The checker rejects unsafe or non-normalized archive
entries, revalidates all checksums, and runs `cargo metadata --frozen` with an
empty Cargo home and offline networking.

The Makevars recipes require `src/rust/vendor.tar.xz` and
`src/rust/vendor-config.toml` as a pair, use a package-local Cargo home, and
build with `--frozen -j 2`. Release evidence must also include a clean source
tarball installation with network access disabled, archive and installed
binary sizes, and the lock/archive SHA-256 values reported by the checker.

Do not carry a generated archive across lockfile changes. In particular, the
active CDF lane adds a direct `url` declaration to the locked graph. Regenerate
and re-run the complete offline source-install proof from the final integrated
post-CDF commit immediately before release packaging.

The current resolved graph is large because the pinned Kernel default engine
includes Arrow, Parquet, object-store, TLS, and cloud support. The resulting
archive and package binary must be measured rather than assuming they fit CRAN
size expectations.

### Isolated Linux offline source proof

Hosted checks build the package on Linux, macOS, and Windows. The local proof
below adds a different guarantee: the exact generated source archive installs
in a read-only Linux container with networking disabled, an empty Cargo home,
and only temporary writable storage. The proof also runs the Kernel smoke test
and rejects installed Cargo, target, or vendor directories.

First generate and check the vendor pair, then build the source archive:

```sh
python3 tools/rust_vendor.py generate
python3 tools/rust_vendor.py check
R CMD build .
```

Prepare the proof image while networking is available. The base must be an
approved image selected by digest, rather than a mutable tag:

```sh
engine=${CONTAINER_ENGINE:-podman}
base_image='rocker/r-ver:4.5.1@sha256:<approved-digest>'
"$engine" build \
  --build-arg BASE_IMAGE="$base_image" \
  --build-arg RUST_VERSION=1.88.0 \
  --file tools/linux-source-proof.Containerfile \
  --tag delta-sharing-linux-proof:local \
  .
proof_image=$("$engine" image inspect \
  --format '{{.Id}}' delta-sharing-linux-proof:local)
```

Run the package build with no container network:

```sh
CONTAINER_ENGINE="$engine" \
DELTA_SHARING_LINUX_IMAGE="$proof_image" \
tools/linux-source-proof.sh delta.sharing_*.tar.gz
```

`DELTA_SHARING_LINUX_IMAGE` accepts only a local `sha256:<id>` or a registry
reference with `@sha256:<digest>`. The image recipe installs the package's
declared R imports and exact Rust 1.88.0 while online; those dependencies are
already present when the network-isolated package installation starts. This
tool is release evidence in addition to the platform CI matrix, not a
replacement for any hosted check.

### macOS deployment target

The Unix Makevars exports a macOS deployment target to both R's compiler and
Cargo. It preserves an explicit caller value. When none is set, it uses
`rustc --print=deployment-target`, which is the Rust toolchain's supported
target default (11.0 on arm64 and 10.12 on x86_64 for Rust 1.88). This matters
because Cargo build scripts compile bundled C and assembly in zstd and
`aws-lc-sys`; without the environment value, current Apple Clang treats the
SDK version as those objects' minimum OS.

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

The registered C shim wraps each initialized stream with owner-thread interrupt
polling. It uses `R_ToplevelExec(R_CheckUserInterrupt, ...)` before owner-thread
batch pulls so an R interrupt cannot long-jump across Arrow or Rust ownership.
An interrupt first releases the inner native stream, then returns a fixed
secret-free C Stream error which R maps to `delta_sharing_cancelled`. Pulls on
foreign consumer threads never call the R API. Their consumer remains
responsible for releasing the imported stream, which uses the same exact-once
inner release path.

The R stream/materializer adapters also catch a normal R `interrupt` condition
when nanoarrow or Arrow observes it inside conversion code before the next
callback. That owner-thread fallback releases the outer stream and raises the
same typed condition. It is required because different consumers choose their
own safe interrupt polling points; it does not call R from a foreign thread.

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

Offline source packaging was separately exercised on macOS arm64 from
integration commit `63fa7a7`, before the active CDF lane:

- two independent generator runs produced byte-identical archives for 325
  registry packages (326 resolved packages total);
- the lockfile SHA-256 was
  `9e41b2b72ba747d56289ae7754a0078ed8e9a87aeb2df780cafe7454b7bdad2b`;
- the 427 MiB unpacked vendor tree compressed to a 28,805,176-byte archive with
  SHA-256
  `b69ef646822deb20bdcba36c0c9aec9a74536153b3144f6f5b73fa4484e601b8`;
- `R CMD build` produced a 28,966,232-byte source archive containing only the
  compressed vendor archive and its 101-byte config, not an unpacked vendor
  tree, Cargo home, build target, design packet, or packaging tools; and
- `R CMD INSTALL --preclean` completed from that source archive with
  `CARGO_NET_OFFLINE=true`, an empty package-local Cargo home, and HTTP(S)/all
  proxies directed at a closed loopback port. Loading the installed package
  reported Kernel 0.22.0, Arrow 57.3.0, a successful Kernel smoke test, and no
  active lifecycle resources. The installed shared library was 32,359,792
  bytes and no vendor, Cargo-home, or target tree remained installed.

These are reproducibility and local offline-install results for the pre-CDF
graph, not release artifacts to carry forward. The archive and config must be
regenerated from the final post-CDF lockfile.

The macOS portability follow-up from integration base `72f8e8b` reproduced the
same 325-package archive bytes and then installed the generated source archive
with Cargo offline and all proxy variables directed at a closed loopback port:

- the 28,968,818-byte source archive had SHA-256
  `d2cb7c2a12aac72dc1eb1385123e9fa53bc66ba32e6424922d62da3c4adeae07`
  and contained the compressed vendor pair but no unpacked vendor, Cargo home,
  target directory, design packet, or packaging tools;
- the original build emitted deployment-version warnings for bundled zstd and
  `aws-lc-sys` objects tagged macOS 26.1 while the package linked for 26.0;
- after exporting the Rust deployment target through Makevars, the exact
  offline source install emitted zero such warnings and produced a
  32,359,808-byte shared library tagged minimum macOS 11.0 with SDK 26.1; and
- the installed package reported Kernel 0.22.0, Arrow 57.3.0, a successful
  Kernel smoke test, zero active streams, zero pending cleanups, and no retained
  native build directories.

This validates the package-scoped macOS deployment-target handling and
pre-CDF offline archive. It does not remove the requirement to regenerate from
the final post-CDF lockfile.

An isolated Linux attempt from macOS on 2026-07-29 did not reach the package
build. Docker 27.1.1 had no running daemon. The existing Podman 5.8.0
`podman-machine-default` started its virtual machine but Fedora CoreOS entered
emergency mode: Ignition failed, and `systemd-fsck-root` could not find root
filesystem UUID `8d734016-8492-4487-a6db-27fc1ae5a7f0`. The Podman socket and
SSH port therefore never became available. The hung start was stopped without
deleting or recreating the user's machine. This is an exact local
infrastructure blocker, not Linux package-build evidence; run the isolated
proof above on a functioning engine.

This is local evidence, not cross-platform build proof. Linux and Windows
source builds, a final post-CDF network-isolated source install, dependency
license inventory, real TLS/HTTPS and deletion-vector coverage, package-size
disposition, and end-to-end Kernel scan performance remain release gates.
