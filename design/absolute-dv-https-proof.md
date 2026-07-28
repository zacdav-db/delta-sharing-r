# Absolute deletion-vector HTTPS proof

Status: implemented as an opt-in target-platform proof; capability remains
withheld pending a typed download condition and hosted Linux and Windows
results

## Gate

Delta Sharing servers resolve provider-relative on-disk deletion vectors and
return an absolute, presigned `storageType = "p"` URL. The package must not
advertise `deletionvectors` merely because inline deletion vectors work.
Restoring that reader feature requires evidence that the production path:

1. negotiates the feature in a Query Table request;
2. decodes the Delta Sharing wrappers in R;
3. validates and preserves the exact absolute HTTPS URL in the private
   synthetic log;
4. lets Delta Kernel's default engine fetch and apply the deletion vector;
5. returns the correctly filtered Arrow rows;
6. removes the synthetic log on success and failure; and
7. raises a typed, redacted condition for TLS or download failure;
8. never exposes the URL or its query in public output or errors.

The proof changes no Rust code and adds no downloader or TLS configuration.

## Deterministic object

The proof uses the on-disk deletion vector shipped in the official Delta
Kernel `v0.22.0` fixture, pinned to peeled tag commit
`1c876015bb16902ae94f10916c7e78d7e6ced25e`:

```text
kernel/tests/data/with-short-dv/
deletion_vector_ae7177f2-6d17-4ea8-819b-8d62fa2c5469.bin
```

The object is 47 bytes and has SHA-256:

```text
a4e7e6964f4d5271a10b9caae795508bfb293c1be8f74ad0f0aa1a200419a233
```

It contains one deletion vector at offset 1, with `sizeInBytes = 38` and
`cardinality = 3`, deleting physical row indexes 0, 1, and 2. The HTTPS URL
uses `raw.githubusercontent.com` with the immutable commit in its path. A
non-secret query sentinel stands in for a signed query. Before invoking
Kernel, the R proof downloads the same immutable object through ordinary
trusted TLS and verifies its SHA-256. The actual scan does not reuse those
bytes: Delta Kernel independently follows the absolute URL embedded in its
synthetic commit.

The Parquet object is the package-owned
`feature-deletion-vectors/part-00000.parquet` fixture. The test transport
returns real Delta Sharing protocol, metadata, and file wrappers. Only the
already-normalized data-file URL is substituted with the local fixture URI;
the absolute HTTPS deletion-vector descriptor remains unchanged.

## Run

Install the package normally, then run the focused test with network access:

```sh
DELTA_SHARING_HTTPS_DV_PROOF=true \
Rscript -e 'testthat::test_file(
  "tests/testthat/test-snapshot-conformance-matrix.R",
  package = "delta.sharing",
  load_package = "installed"
)'
```

The opt-in test temporarily widens the private reader-feature allowlist only
inside the test process. That makes the public `sharing_read()` request
negotiate `deletionvectors` and permits the production response validator to
accept the server-selected feature without changing the package's advertised
capabilities.

The success case must return only IDs 3 and 4, keep the query sentinel out of
stream and diagnostics printing, exhaust the stream, and remove the private
root. A second scan deliberately supplies the wrong DV size. Kernel must fetch
the same HTTPS object but expose only the fixed data-scan error. A third scan
uses the same trusted host and immutable commit path but requests a missing DV
object, exercising a real HTTPS download failure. In both failures the URL and
query sentinel must be absent and cleanup counts must balance.

## Remaining gate

This proof necessarily needs network access to exercise public-certificate TLS
and is skipped during ordinary offline package checks.

The successful scan, invalid-content failure, and missing-object download
failure are redacted. The latter two currently surface from the Arrow
materializer as base R `simpleError` conditions rather than the package's
`delta_sharing_kernel_error`. That is a release blocker: the package must wrap
mid-stream native failures in a typed, fixed-message condition without copying
their underlying text.

The package continues to omit `deletionvectors` from
`.snapshot_reader_features` until that typed-failure gap is closed and this
exact opt-in proof passes from installed source packages on hosted macOS,
Linux, and Windows. A local pass is implementation evidence, not
cross-platform release evidence.
