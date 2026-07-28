# Absolute deletion-vector HTTPS proof

Status: implemented as an opt-in local query-propagation proof; capability
remains withheld pending a genuine provider-signed target and hosted
cross-platform results

## Gate

Delta Sharing servers can resolve provider-relative on-disk deletion vectors
and return an absolute `storageType = "p"` HTTPS URL. The package must not
advertise `deletionvectors` merely because inline deletion vectors work.
Restoring that reader feature requires evidence that the production path:

1. negotiates the feature in a Query Table request;
2. decodes the Delta Sharing wrappers in R;
3. validates and preserves the exact absolute HTTPS URL in the private
   synthetic log;
4. lets Delta Kernel's default engine fetch and apply the deletion vector;
5. returns the correctly filtered Arrow rows;
6. removes the synthetic log on success and failure;
7. raises a typed, redacted condition for invalid content or download failure;
   and
8. never exposes the URL or its query in public output or errors.

The proof changes no Rust code and adds no downloader or TLS configuration.

## Deterministic query-required object

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
`cardinality = 3`, deleting physical row indexes 0, 1, and 2.

The HTTPS descriptor points to GitHub's official immutable blob URL with the
required `raw=1` query key. It also carries
`X-Amz-Signature=delta-kernel-query-proof`, a fixed, non-secret, intentionally
invalid marker. Delta Kernel 0.22 recognizes that key and selects its
per-object presigned-HTTP branch, so the proof exercises the upstream branch
that is supposed to preserve a signed URL. The marker is not accepted or
validated by an object-storage provider.

On every proof run, the test independently verifies the endpoint contract
before invoking Kernel:

- `raw=1` plus the marker returns HTTP 302 to GitHub's immutable raw-object
  URL;
- following that redirect returns the 47-byte object with the pinned hash;
- omitting the query returns HTML with a different hash; and
- changing the key to `not_raw=1` while retaining the Kernel marker also
  returns HTML with a different hash.

The successful Kernel scan can therefore obtain the expected bitmap only if
Kernel transmits the required query key and follows the trusted HTTPS
redirect. The preliminary `httr2` checks do not supply bytes to the scan:
Delta Kernel independently fetches the absolute URL embedded in its synthetic
commit.

This is deliberately described as a **query-propagation proof**, not a
presigned-URL proof. GitHub's `raw=1` query is neither secret nor a cloud
provider signature, and the marker only selects Kernel's relevant code path.
This test does not establish signature validation, expiry, authentication, or
provider-specific semantics.

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

The success case must return only IDs 3 and 4, keep the URL and query out of
stream and diagnostics printing, exhaust the stream, and remove the private
root. A second scan deliberately supplies the wrong DV size after fetching the
same object. A third changes the required query key so GitHub returns HTML
instead of the bitmap. A fourth uses the correct query against a missing
immutable object. All three failures must expose only the fixed, typed
data-scan condition, redact the complete URL and query, and balance cleanup
counts.

## Remaining gate and stability risks

This proof necessarily needs network access to exercise public-certificate TLS
and is skipped during ordinary offline package checks. It depends on the
public GitHub blob endpoint's longstanding `raw=1` redirect behavior and can
also be affected by GitHub availability or rate limiting. The test validates
that behavior directly before drawing a Kernel conclusion, while the
immutable commit and SHA-256 prevent a changed object from passing.

The package continues to omit `deletionvectors` from
`.snapshot_reader_features`. Capability restoration still requires:

- an installed-source pass on hosted macOS, Linux, and Windows; and
- an equivalent target owned by the project or provider that uses a genuine
  Delta Sharing-style provider-signed URL, including signature and expiry
  semantics.

The current local result is implementation evidence for HTTPS redirect,
required-query propagation, filtering, redaction, and lifecycle. It is not the
remaining provider-signed or cross-platform release evidence.
