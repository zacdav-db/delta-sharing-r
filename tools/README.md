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

`dependency_licenses.py` rebuilds the installed Rust license inventory and
deduplicated legal-text bundle from the verified archive. Commit those two
outputs whenever `Cargo.lock` or `DESCRIPTION` changes. CI regenerates the
expected files offline and rejects stale or tampered outputs.

Before submission, copy the final check counts and environments into
`cran-comments.md`, inspect the source archive, and confirm that its checksum
matches the artifact checked by CI.
