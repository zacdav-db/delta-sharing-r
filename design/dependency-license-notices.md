# Dependency license and notice policy

This policy covers the materials redistributed in source and binary
`delta.sharing` packages. It complements the Rust advisory and license
allow-list policy; `cargo-deny` decides whether a license is acceptable, while
this inventory preserves the license and notice materials that accompany the
code actually distributed.

## Distribution boundary

R packages named in `Depends`, `Imports`, `Suggests`, `LinkingTo`, `Enhances`,
and `Config/Needs/*` are installed and distributed separately.
`delta.sharing` does not copy or statically link their code. The generated
inventory therefore records each direct R package name, DESCRIPTION
relationship, and version requirement. The dependency's installed DESCRIPTION
`License` field and its own license files are the authority for the version a
user installs. Recording a local developer's current dependency version or
license would be misleading because this package does not lock those versions.

The native Rust crate and its dependencies are different: their code is
statically linked into the package shared library. Every one of the 326
packages in the locked graph is therefore inventoried, including build and
development packages retained by the complete locked resolution. Each entry
records:

- exact crate name and version;
- registry source and Cargo.lock checksum, or the local native-crate identity;
- the exact declared Cargo license expression and optional `license-file`;
- repository, authors, and `.cargo_vcs_info` revision when supplied upstream;
- every preserved legal file with its original path, SHA-256 digest, bundle
  member, and provenance.

## Preserved materials

`tools/dependency_licenses.py` starts from the checksum-verified
`src/rust/vendor.tar.xz` created by `tools/rust_vendor.py`. It collects explicit
Cargo `license-file` targets, files under `LICENSE` or `LICENSES` directories,
and conventional license, copying, notice, copyright, authors, contributors,
patent, and third-party-notice filenames. Bytes are preserved exactly and
deduplicated by SHA-256 in
`inst/dependency-licenses/rust-license-texts.tar.xz`. The JSON inventory maps
every original file to the corresponding content-addressed member.

Fourteen published crate archives in the current lock omit a workspace-root
license or notice file. Thirteen are repaired from their exact
`.cargo_vcs_info` revision using the locally committed, SHA-256-pinned files
declared in `tools/rust-license-overrides.json`. The remaining internal Android
crate uses the license files from its exact locked parent crate, which has the
same repository and dual-license declaration. Generation and checking never
fetch these overrides. A missing, changed, unpinned, unnecessary, or
wrong-repository override fails closed.

The pinned files under `tools/rust-license-overrides/` are generator inputs and
are excluded from the R source archive. Their verbatim bytes are present once
in the installed content-addressed bundle. This keeps the distributable
license material complete without duplicating common Apache, MIT, BSD, Unicode,
and other license texts hundreds of times.

## Reproducible release procedure

Run from the exact release commit:

```sh
python3 tools/rust_vendor.py generate
python3 tools/rust_vendor.py check
python3 tools/dependency_licenses.py generate
python3 tools/dependency_licenses.py check
```

Both generators run Cargo offline against the committed lock. If the local
Cargo cache is incomplete, populate it first with `cargo fetch --locked`;
fetching an unpinned source is not permitted. The dependency-license checker
re-extracts and checksum-validates the vendor archive, runs frozen offline
Cargo metadata with an empty Cargo home, proves that metadata and Cargo.lock
contain the same name/version set, rebuilds both outputs, and requires
byte-for-byte equality.

Any DESCRIPTION dependency, Cargo manifest, lockfile, vendor archive, crate
license file, or override change makes the checked-in inventory stale. A
release reviewer must inspect the manifest diff, re-run `cargo-deny`, and
review each new license expression or override before accepting regenerated
outputs.

The generated files are deliberately committed because binary R packages do
not carry the Rust source-vendor archive:

- `inst/dependency-licenses/dependency-inventory.json`
- `inst/dependency-licenses/rust-license-texts.tar.xz`

They are installed under `dependency-licenses/` and can be found from R with:

```r
system.file("dependency-licenses", package = "delta.sharing")
```

The inventory is ordinary UTF-8 JSON. The legal-text bundle can be inspected
with any xz-capable tar implementation; no package code executes it.

## Current locked-graph evidence

The 2026-07-29 final integration-tree generation produced:

- 326 resolved packages, of which 325 are checksum-pinned registry packages;
- 629 per-package legal-file references, including 19 notice-file references,
  deduplicated to 214 exact byte sequences;
- a 390,830-byte JSON inventory with SHA-256
  `8c9452300f09b260cc7cfbf864bcbfaa7457fee7202efbf00e53f3c5e066151e`;
- a 45,900-byte legal-text bundle with SHA-256
  `3b3648f0ebfa5dac41e948612000242ed829a1f92f426e9462603ab8d9b099d9`;
  and
- the unchanged 28,805,176-byte source-vendor archive with SHA-256
  `b69ef646822deb20bdcba36c0c9aec9a74536153b3144f6f5b73fa4484e601b8`.

The two generated outputs passed byte-for-byte regeneration, manifest-to-bundle
digest verification, and focused determinism, lock/metadata mismatch, stale
override, override-checksum tamper, inventory tamper, and bundle tamper tests.

The final compact vendor pair was included in a 29,187,334-byte source package
without an unpacked vendor directory, generator tools, or override-input
duplicates. Exact `R CMD check --as-cran --no-manual` installed that source,
passed all package tests and vignettes, and reported zero errors, zero warnings,
and one explained note for the development version and source size. The
portable shell recipe removed the earlier GNU-make warning. The installed
package kept the two dependency-license files and no vendor, Cargo-home, or
target tree. Repeat the complete procedure if the integration lockfile,
DESCRIPTION, or native sources change.
