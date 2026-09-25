# Release tooling

`package-check.yaml` creates one source package and checks that exact archive
offline on every supported runner. Download the `package-source` artifact from
a successful main-branch run when preparing a release; do not rebuild a
different archive for submission.

To reproduce the source artifact locally:

```sh
cargo fetch --manifest-path src/rust/Cargo.toml --locked
python3 tools/rust_vendor.py generate
python3 tools/rust_vendor.py check
python3 tools/dependency_licenses.py generate
python3 tools/dependency_licenses.py check
R CMD build .
R CMD check --as-cran --no-manual delta.sharing_*.tar.gz
```

`rust_vendor.py` creates `src/rust/vendor.tar.xz` and
`src/rust/vendor-config.toml`. They are generated release inputs and are
ignored by Git. Package installation extracts them temporarily and invokes
Cargo with `--frozen`, so no network access is needed.

The source bundle retains the union of runtime/build dependencies for Linux
x86_64 and arm64 (GNU and musl), macOS x86_64 and arm64, Windows x86_64 GNU and
arm64 GNU-LLVM, and FreeBSD x86_64. Cargo selects these dependencies from the
locked graph with all package features enabled, independently of the host.
Other targets are not included in the release bundle.

Upstream test, example, benchmark, and documentation directories are omitted
unless required for compilation (`zerocopy` currently needs its complete tree).
License, notice, authorship, and patent files are always retained using the same
selection rules as the installed license inventory. Inactive crates keep their
resolver metadata and a compile-error stub, so using an excluded target fails
explicitly rather than silently building an incomplete dependency. Our own
package code, tests, and vignettes are unchanged by this filtering.

Generation checks the original registry checksums before filtering, refreshes
file checksums after filtering, and verifies frozen offline resolution for every
listed target. Native builds are still validated by the package-check matrix;
dependency resolution alone is not a compilation test. The archive is ordered
and timestamp-normalized for reproducible generation.

CRAN accepted the reduced 15.9 MB source-package approach on 21 September 2026.
CI keeps subsequent release candidates within that size. The archive remains
self-contained; there is no dependency download during installation.

`dependency_licenses.py` rebuilds the installed Rust license inventory and
deduplicated legal-text bundle from the verified archive. Commit those two
outputs whenever `Cargo.lock` or `DESCRIPTION` changes. CI regenerates the
expected files offline and rejects stale or tampered outputs.

Before submission, copy the final check counts and environments into
`cran-comments.md`, inspect the source archive, and confirm that its checksum
matches the artifact checked by CI.
