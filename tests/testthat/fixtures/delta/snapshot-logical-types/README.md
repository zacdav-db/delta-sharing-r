# Snapshot logical-type conformance fixture

This package-owned fixture is generated specifically for `delta.sharing`; it
is not copied from an upstream project. It combines decimal and map values,
UTC and timezone-free microsecond timestamps, and nested arrays and structs
under Delta name-mode column mapping. Tests wrap its protocol, metadata, and
add action in a Delta Sharing response before the R planner stages the
response, prepares the private synthetic log, and invokes Delta Kernel.

Regenerate it from the package root with Arrow R 22.0.0:

```sh
Rscript tests/testthat/fixtures/delta/snapshot-logical-types/generate.R
```

The generator writes uncompressed Parquet without dictionaries or statistics
and refreshes `SHA256SUMS`. Re-running it with Arrow R 22.0.0 must leave both
generated hashes unchanged. The generator itself is retained so the physical
Arrow schema, logical Delta schema, and test values remain reviewable.

Delta Kernel 0.22.0 deliberately rejects Delta interval logical types during
schema parsing. No interval column is included in the Parquet fixture. The
companion test mutates a copy of the Sharing metadata to
`interval day to second` and asserts the public typed, redacted native failure
and lifecycle cleanup instead of claiming unsupported materialization.

The fixture is package test data under the repository's Apache-2.0 license.
