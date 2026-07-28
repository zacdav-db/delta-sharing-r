# S7 interface and naming decision matrix

Status: accepted vNext direction
Decision owner: package maintainer
Date: 2026-07-28

This document defines a new S7 interface. Familiar client, table, read,
discovery, and Arrow terminology is retained because it describes the domain
well, not because an earlier package surface must continue to work.

There is intentionally no old-to-new name mapping. Removed functions, classes,
methods, arguments, mutation patterns, and return types receive no alias,
wrapper, warning period, or behavioral test.

## Naming principles

1. Use `sharing_` for package-specific constructors.
2. Use `Sharing` for public S7 class prefixes.
3. Keep the user model explicit: client, table, read, materializer.
4. Use ordinary verbs for generic operations (`list_*`, `read_*`,
   `table_*`); do not prefix every operation with `delta_`.
5. Keep query configuration in immutable read descriptors.
6. Reserve "stream" for the lazy Arrow C Stream and make eager outputs obvious.
7. Prefer base data frames for discovery/core eager output. `{arrow}` remains
   optional.
8. Names describe vNext semantics only. Familiar spelling does not imply
   familiar implementation or return values.

## Canonical user flow

```r
client <- sharing_client("recipient.share")

orders <- sharing_table(
  client,
  share = "sales",
  schema = "default",
  table = "orders"
)

orders_q2 <- sharing_read(
  orders,
  version = 125L,
  columns = c("order_id", "ordered_at", "amount"),
  limit = 1e6,
  response_format = "auto"
)

stream <- read_arrow_stream(orders_q2)
orders_arrow <- read_arrow(orders_q2)
orders_df <- read_data_frame(orders_q2)
```

Materializers accept a `SharingRead` or `SharingChanges`. A `SharingTable` is
not implicitly materialized; callers construct an explicit `sharing_read()`.
Calling `sharing_read(table)` is the concise latest-snapshot form.

## S7 class decisions

| Concept | Canonical S7 class | Role and state | Decision |
|---|---|---|---|
| Parsed profile | `SharingProfile` | Validated profile description and opaque credential-source reference; secrets are never printable properties | Public only if explicitly constructed; otherwise created internally |
| Client | `SharingClient` | Immutable descriptor containing safe endpoint metadata and an opaque Rust-owned client handle | Locked |
| Table | `SharingTable` | Immutable client reference plus structured share/schema/table identifier | Locked |
| Snapshot read | `SharingRead` | Immutable projection, time travel, predicate hint, limit, and response-format specification | Locked |
| Change read | `SharingChanges` | Immutable CDF bounds, projection, and response-format specification | Locked |
| Diagnostics | `SharingReadDiagnostics` | Safe snapshot of read counters and selected capabilities; never owns execution | Locked |
| Stream | `nanoarrow_array_stream` | Stateful, single-consumer native stream | Use the standard nanoarrow class; do not wrap it in S7 |

`SharingClient` may refer to Rust-owned state but does not expose or mutate it.
`SharingTable`, `SharingRead`, and `SharingChanges` are cheap descriptors.
Mutable scan state, HTTP clients, credentials, cancellation, Kernel objects,
temporary logs, buffers, and metrics accumulation stay in Rust.

## Constructor and operation decisions

| Concept | Canonical interface | Inputs and behavior | Decision rationale |
|---|---|---|---|
| Profile | `sharing_profile(source)` | File path, raw JSON, connection, or explicit fields; validates without leaking secrets | Gives explicit construction when needed without making it mandatory |
| Client | `sharing_client(profile)` | Accepts any supported profile source or a `SharingProfile`; returns `SharingClient` | Keeps the clear package/client terminology |
| Table | `sharing_table(client, name = NULL, share = NULL, schema = NULL, table = NULL)` | Exactly one three-part `name` or one complete structured triplet; returns `SharingTable` | Supports concise names and identifiers containing dots without URL concatenation |
| Snapshot read | `sharing_read(table, version = NULL, timestamp = NULL, columns = NULL, limit = NULL, predicate = NULL, response_format = "auto")` | Latest when both time-travel fields are absent; version/timestamp are exclusive | Separates reusable table identity from immutable query configuration |
| Change read | `sharing_changes(table, starting_version = NULL, ending_version = NULL, starting_timestamp = NULL, ending_timestamp = NULL, columns = NULL, response_format = "auto")` | Requires one start bound; version and timestamp modes cannot mix | Keeps CDF planning distinct while sharing materializers |
| Shares | `list_shares(client)` | Complete pagination; compact base data frame | Clear and unsurprising |
| Schemas | `list_schemas(client, share = NULL)` | One share when supplied; all accessible shares when omitted | One operation instead of separate "all" variants |
| Tables | `list_tables(client, share = NULL, schema = NULL)` | One schema, one share, or all accessible tables based on supplied filters | One operation instead of endpoint-shaped variants |
| Version | `table_version(table)` | Returns a stable scalar/version record without reading rows | Table-qualified and explicit |
| Protocol | `table_protocol(table)` | Returns parsed protocol capabilities | Metadata-only |
| Metadata | `table_metadata(table)` | Returns safe structured table metadata | Metadata-only |
| Schema | `table_schema(table)` and `read_schema(read)` | Logical table schema versus projected read schema | Avoids overloading one ambiguous schema call |
| Stream | `read_arrow_stream(read, batch_size = NULL, concurrency = NULL)` | Primary lazy, bounded `nanoarrow_array_stream` materializer | Makes laziness and Arrow boundary explicit |
| Arrow table | `read_arrow(read, ...)` | Optional eager `{arrow}` adapter over the same stream | Short familiar output name; eager behavior documented |
| Data frame | `read_data_frame(read, ...)` | Eager base data frame adapter over the same stream | States the memory-bearing output directly |
| Base conversion | `as.data.frame(read, ...)` | S3 interoperability method with the same semantics as `read_data_frame()` | Standard external generic, not a transition alias |
| Diagnostics | `read_diagnostics(stream)` | Returns `SharingReadDiagnostics`; safe before and after consumption | Tied to the stateful stream rather than immutable descriptors |
| Release | `stream$release()` | Standard nanoarrow deterministic release | Do not invent a second lifecycle wrapper |

The exact low-level predicate expression builder remains a pre-snapshot
implementation decision. Until exact residual filtering exists, the public
argument is named `predicate` but documented as a server-side best-effort hint.
Raw SQL mutation methods are not part of vNext.

## Argument conventions

| Area | Decision |
|---|---|
| Table identity | Preserve identifier case and spelling exactly; no silent normalization |
| Time travel | `version` and `timestamp` are mutually exclusive and validated before I/O |
| CDF range | Version and timestamp bounds cannot be mixed |
| Columns | A character vector in requested logical order |
| Limit | Non-negative whole-number scalar; server hint plus exact client enforcement |
| Response format | One of `"auto"`, `"delta"`, or `"parquet"`; selected format is diagnostic data |
| Batch size | Positive whole-number scalar or `NULL` for a safe automatic default |
| Concurrency | Positive whole-number scalar or `NULL` for a safe automatic default |
| Timestamps | `POSIXct`; normalized and validated with explicit UTC semantics |
| Discovery output | Compact base data frames with stable documented columns |
| Errors | All public errors inherit from `delta_sharing_error` and a narrower typed class |

## Non-interface decisions

- Direct S7 property access is not a normal workflow and is not the public
  contract.
- There are no public R6 classes or mutable setters.
- Query objects are not serializable if they capture a live native client
  handle; failure must be explicit.
- There is no implicit package-managed download directory.
- There is no row-by-row or batch-by-batch R conversion in the stream path.
- There is no table-to-latest-read materialization shortcut.
- There is no second set of `delta_*` constructors.
- There are no aliases, shims, soft deprecations, transition warnings, or
  prior-version behavior tests.
