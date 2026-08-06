# Snapshot type-conformance fixture

This fixture is generated for `delta.sharing`; it is not copied from an
upstream project. It contains three rows spanning primitive Arrow values plus
Delta arrays and structs. The companion tests always wrap its protocol,
metadata, and add action in a Delta Sharing response before constructing the
private synthetic log and handing that log to Delta Kernel.

Regenerate it from the package root with Arrow R 22.0.0:

```sh
Rscript tests/testthat/fixtures/delta/snapshot-types/generate.R
```

The generator writes uncompressed Parquet without dictionaries or statistics.
Running it twice with Arrow R 22.0.0 produces these SHA-256 values:

- `part-00000.parquet`:
  `cd27bfcaa70eeb6d1fbd66d74f49e7428b8ca36439156f5419cf7e8b38105f00`
- `_delta_log/00000000000000000000.json`:
  `92de1457b01815cf789428ef83d5173d88cb75b54ab2c1a19306ab639df91645`

The fixture is package test data under the repository's Apache-2.0 license.
