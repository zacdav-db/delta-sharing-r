# Rust dependency policy

The native crate is checked with pinned `cargo-deny` 0.19.4 in hosted CI.
`src/rust/deny.toml` denies unknown registries and Git sources, rejects
unapproved licenses, and fails on every RustSec advisory except the reviewed
transitive cases recorded below. An exception is tied to one advisory ID and
does not suppress later advisories for the same crate.

License acceptability and redistribution notices are separate gates.
`design/dependency-license-notices.md` defines the deterministic inventory and
verbatim legal-text bundle shipped in source and binary R packages. It covers
the complete locked graph and fails closed when Cargo metadata, the vendor
archive, or a pinned missing-license override changes.

## Reviewed transitive advisory exceptions

| Advisory | Locked path | Disposition |
|---|---|---|
| `RUSTSEC-2024-0436` | `parquet 58.3.0 -> paste 1.0.15` | `paste` is an unmaintained compile-time macro dependency. It is not part of the runtime trust boundary, and the pinned Arrow line offers no replacement. |
| `RUSTSEC-2026-0194` | `object_store 0.13.2 -> quick-xml 0.39.4` | The advisory affects duplicate-attribute checking in untrusted XML. The supported Delta Sharing data path gives Kernel bounded presigned HTTP objects and does not invoke object-store cloud or WebDAV listing APIs. |
| `RUSTSEC-2026-0195` | `object_store 0.13.2 -> quick-xml 0.39.4` | The advisory affects namespace processing in untrusted XML. The same no-XML data-path restriction applies. |

Delta Kernel 0.26 with Arrow 58 requires `object_store 0.13`, whose
`quick-xml` requirement is restricted to the vulnerable 0.39 series. The
fixed `quick-xml >= 0.41` therefore cannot be selected by a lockfile-only
update. Do not add a source patch or move protocol/cloud responsibilities into
Rust to work around this constraint. The Kernel upgrade removed the previous
`rustls-pemfile` exception. Re-evaluate and remove the remaining three
exceptions when upgrading the pinned Kernel/Arrow line.

## Review procedure

Before integration:

1. Run the exact `cargo deny` command from `.github/workflows/rust.yaml`.
2. Review every new license and advisory; do not add wildcard exceptions.
3. Confirm any advisory exception is unreachable through the documented
   vNext boundary and records a removal condition.
4. Generate and check the vendor pair and dependency-license outputs; review
   every inventory or pinned-override diff.
5. Run locked MSRV, stable, platform, coverage, and package checks after any
   Kernel, Arrow, Parquet, object-store, or TLS dependency change.
