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

The originally submitted source archive was also checked by Win-builder on R
release 4.6.1 and R devel 4.7.0. Both checks reported 0 errors, 0 warnings, and
only the expected new-submission note.

## Resubmission

This is the second resubmission. In response to CRAN's review, the standard
Apache License 2.0 text is omitted from the source package and the redundant
`+ file LICENSE` reference has been removed from `DESCRIPTION`. The repository
copy remains outside the R source package for repository license discovery.

The package authorship metadata now lists only the package author/maintainer
and copyright holder. Authors of bundled Rust dependencies remain credited in
the installed `NOTICE` and dependency license materials rather than being
listed as contributors to this R package.

## Bundled Rust sources

The source package is approximately 24.2 MB (23.1 MiB) because it includes the
complete locked Rust dependency graph as
`src/rust/vendor.tar.xz`. This follows CRAN's Rust guidance and allows Cargo to
build with `--frozen` and without network access. We request the corresponding
increase from the preferred 10 MB source package size.

Cargo is limited to two parallel jobs during package installation. The package
declares and checks its minimum rustc version, and reports both Cargo and rustc
versions in the installation log.
