# Rust dependency policy

The native crate is checked with pinned `cargo-deny` 0.19.4 in hosted CI.
`src/rust/deny.toml` denies unknown registries and Git sources, rejects
unapproved licenses, and fails on every RustSec advisory except the reviewed
transitive cases recorded below. An exception is tied to one advisory ID and
does not suppress later advisories for the same crate.

## Reviewed transitive advisory exceptions

| Advisory | Locked path | Disposition |
|---|---|---|
| `RUSTSEC-2024-0436` | `parquet 57.3.0 -> paste 1.0.15` | `paste` is an unmaintained compile-time macro dependency. It is not part of the runtime trust boundary, and the pinned Arrow line offers no replacement. |
| `RUSTSEC-2025-0134` | `object_store 0.12.5 -> rustls-pemfile 2.2.0` | The crate is unmaintained rather than vulnerable. It is enabled by the transitive GCP feature; vNext keeps profile and cloud authentication in R and does not parse profile keys in Rust. |
| `RUSTSEC-2026-0194` | `object_store 0.12.5 -> quick-xml 0.38.4` | The advisory affects duplicate-attribute checking in untrusted XML. The supported Delta Sharing data path gives Kernel bounded presigned HTTP objects and does not invoke object-store cloud or WebDAV listing APIs. |
| `RUSTSEC-2026-0195` | `object_store 0.12.5 -> quick-xml 0.38.4` | The advisory affects namespace processing in untrusted XML. The same no-XML data-path restriction applies. |

Delta Kernel 0.22 with Arrow 57 requires `object_store 0.12`, whose
`quick-xml` requirement is restricted to the vulnerable 0.38 series. The
fixed `quick-xml >= 0.41` therefore cannot be selected by a lockfile-only
update. Do not add a source patch or move protocol/cloud responsibilities into
Rust to work around this constraint. Re-evaluate and remove all four
exceptions when upgrading the pinned Kernel/Arrow line.

## Review procedure

Before integration:

1. Run the exact `cargo deny` command from `.github/workflows/rust.yaml`.
2. Review every new license and advisory; do not add wildcard exceptions.
3. Confirm any advisory exception is unreachable through the documented
   vNext boundary and records a removal condition.
4. Run locked MSRV, stable, platform, coverage, and package checks after any
   Kernel, Arrow, Parquet, object-store, or TLS dependency change.
