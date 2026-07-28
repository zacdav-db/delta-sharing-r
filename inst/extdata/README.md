# Bundled native-reader assets

`cdf-empty-checkpoint.parquet` is the empty Delta checkpoint used to bootstrap
a bounded change-data-feed log whose first provider version is greater than
zero. It is copied from `delta-io/delta-sharing` commit
`4b790695e45bc66a7531f0ddd264725718ee2fcc` (`fake_checkpoint.py`,
Apache-2.0).
