## R CMD check results

Local `R CMD check --as-cran --no-manual` on macOS arm64 with R 4.5.1:

- 0 errors
- 0 warnings
- 1 note

This is a new submission. The note also reports the source package size shown
below.

The release source package is checked with `--as-cran` on R devel, release,
and the minimum supported R version across Linux, macOS, and Windows. The exact
source archive produced by CI is used by every platform check.

The exact release archive was also submitted to Win-builder for R-release and
R-devel.

## Bundled Rust sources

The source package is approximately 29.6 MB because it includes the complete
locked Rust dependency graph as `src/rust/vendor.tar.xz`. This follows CRAN's
Rust guidance and allows Cargo to build with `--frozen` and without network
access. We request the corresponding increase from the preferred 10 MB source
package size.

Cargo is limited to two parallel jobs during package installation. The package
declares and checks its minimum rustc version, and reports both Cargo and rustc
versions in the installation log.
