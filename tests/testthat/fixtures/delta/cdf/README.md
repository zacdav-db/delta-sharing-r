# Bounded CDF fixture

This fixture is derived from `kernel/tests/data/cdf-table-simple` in
`delta-io/delta-kernel-rs` tag `v0.22.0` (Apache-2.0).

It deliberately contains only provider versions 1 and 2. Version 0 is an empty
bootstrap checkpoint from `delta-io/delta-sharing` commit
`4b790695e45bc66a7531f0ddd264725718ee2fcc`. The version 1 JSON commit includes
the protocol and metadata that were active at the bounded range start, followed
by the provider's real version 1 actions.

The four upstream Parquet payloads are byte-for-byte unchanged but use compact
`a.parquet` through `d.parquet` names so the R source tarball remains portable.

Rust tests copy the fixture and set distinct millisecond modification times on
the two JSON commits. Git does not preserve file modification times.
