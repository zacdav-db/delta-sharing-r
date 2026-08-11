# Snapshot logical-type conformance fixture

This package-owned fixture is generated specifically for `delta.sharing`; it
is not copied from an upstream project. It combines decimal and map values,
UTC and timezone-free microsecond timestamps, and nested arrays and structs
under Delta name-mode column mapping. Tests wrap its protocol, metadata, and
add action in a Delta Sharing response before the R planner stages the
response, prepares the private synthetic log, and invokes Delta Kernel.

Regenerate it from the package root with Arrow R 22.0.0:

```sh
Rscript tests/testthat/fixtures/delta/logical-types/generate.R
```

The generator writes uncompressed Parquet without dictionaries or statistics
and refreshes `SHA256SUMS`. Re-running it with Arrow R 22.0.0 must leave both
generated hashes unchanged. The generator itself is retained so the physical
Arrow schema, logical Delta schema, and test values remain reviewable.

Interval logical types are not included; this fixture is limited to logical
types that the pinned Delta Kernel can materialize successfully.

The fixture is package test data under the repository's Apache-2.0 license.
