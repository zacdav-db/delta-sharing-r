# CDF conformance fixtures

These fixtures are copied from
[`delta-io/delta-kernel-rs` tag `v0.22.0`](https://github.com/delta-io/delta-kernel-rs/tree/v0.22.0)
at commit `1c876015bb16902ae94f10916c7e78d7e6ced25e`:

- `b` is
  [`kernel/tests/data/cdf-table.tar.zst`](https://github.com/delta-io/delta-kernel-rs/blob/v0.22.0/kernel/tests/data/cdf-table.tar.zst);
- `n` is
  [`kernel/tests/data/cdf-column-mapping-name-mode.tar.zst`](https://github.com/delta-io/delta-kernel-rs/blob/v0.22.0/kernel/tests/data/cdf-column-mapping-name-mode.tar.zst);
- `i` is
  [`kernel/tests/data/cdf-column-mapping-id-mode.tar.zst`](https://github.com/delta-io/delta-kernel-rs/blob/v0.22.0/kernel/tests/data/cdf-column-mapping-id-mode.tar.zst);
- `s` is
  [`kernel/tests/data/table-with-cdf`](https://github.com/delta-io/delta-kernel-rs/tree/v0.22.0/kernel/tests/data/table-with-cdf).

Spark checksum sidecars are omitted. The remaining Parquet bytes and Delta
JSON commit bytes are unchanged. Directories and filenames are shortened for
portable R source archives. Within each fixture, `l` contains commits in
version order and `p` contains Parquet payloads in bytewise order of their
original relative paths.

Tests reconstruct the original path mapping, transform the committed Delta
actions into Delta Sharing wire actions, then exercise the production R
decoder, private synthetic-log builder, Delta Kernel CDF reader, and Arrow
stream. URLs are replaced with local fixture paths only after R has validated
and written the synthetic log.

The upstream Apache License 2.0 text is redistributed verbatim as `LICENSE`.
Its SHA-256 is
`c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4`.
`SHA256SUMS` records every redistributed fixture payload and Delta commit.

For independent source verification, the upstream compressed archives have
these SHA-256 digests:

- `cdf-table.tar.zst`:
  `0e298ad53e04cce705a961487c651628b13fdbae8a7446acab1dff0041ed1c42`;
- `cdf-column-mapping-name-mode.tar.zst`:
  `835277abcf8c68f816141c9efa8d3761603afeb03f473b76d701a4730487abec`;
- `cdf-column-mapping-id-mode.tar.zst`:
  `2464bbb1bbadf9e39b2b1d3940d8ac790fed919739ffb4a2c2c8e046455fab56`.
