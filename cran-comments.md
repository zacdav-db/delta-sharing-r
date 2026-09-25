## R CMD check results

Local macOS arm64, R 4.5.1, `R CMD check --as-cran --no-manual`:

- 0 errors
- 0 warnings
- 1 note: new submission and source-package size

All 981 test assertions passed, with no warnings or skips.

## Resubmission

After trimming the bundled Rust sources as far as practical, we requested a
source-size exception. Bundled sources allow installation with Cargo `--frozen`
and without network access; compilation remains limited to two jobs.

The previous reviewer-requested license and authorship corrections remain
in place.
