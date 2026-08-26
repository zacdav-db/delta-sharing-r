## R CMD check results

Local `R CMD check --as-cran` on macOS arm64 with R 4.5.1:

- 0 errors
- 0 warnings
- 1 note

This is a new submission. The same note reports the source package size shown
below.

The exact source archive produced by CI is checked with `--as-cran` on R devel
4.7.0, R release 4.6.1, and the minimum supported R 4.3.3 across Linux, macOS,
and Windows. Those checks report 0 errors, 0 warnings, and 0 notes.

Win-builder confirmation is pending for the final release candidate.

## Bundled Rust sources

The source package is 24,183,086 bytes (approximately 23.1 MiB) because it
includes the complete locked Rust dependency graph as
`src/rust/vendor.tar.xz`. This follows CRAN's Rust guidance and allows Cargo to
build with `--frozen` and without network access. We request the corresponding
increase from the preferred 10 MB source package size.

Cargo is limited to two parallel jobs during package installation. The package
declares and checks its minimum rustc version, and reports both Cargo and rustc
versions in the installation log.
