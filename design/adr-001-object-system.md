# ADR 001: R object system for vNext

Status: accepted
Decision owner: package maintainer  
Decision date: 2026-07-28

## Context

The current package exposes mutable R6 clients and table readers. Query options
are set by mutating a reader before materialization. The desired API instead has
reusable table handles, immutable snapshot/CDF query specifications, several
materializers, and a stateful Arrow stream whose lifetime is native.

The object system does not affect the record-batch hot path, but it does affect
API stability, validation, extension, documentation, and how easily execution
state leaks into user objects.

## Options

### R6

Advantages:

- familiar client/table/read terminology;
- natural reference semantics and private fields;
- straightforward external-pointer ownership.

Costs:

- repeats the mutable architecture already being replaced;
- methods and data live inside the class rather than open generics;
- pipe-based composition and third-party extension are secondary;
- query state can accidentally be reused or mutated between scans;
- preserving the current R6 surface would constrain the redesign.

### S3

Advantages:

- mature, dependency-free, idiomatic, and easy to compose;
- excellent compatibility with base and tidyverse generics;
- thin objects are easy to construct around external pointers.

Costs:

- no formal property types or constructor contract;
- invariants rely on conventions and repeated manual checks;
- method discovery and multiple dispatch are less explicit;
- misspelled/missing internal attributes can fail late.

### S7

Advantages:

- formal classes, validated properties, and method signatures;
- functional generics fit the desired API;
- designed for S3 interoperability;
- class and generic objects improve introspection;
- immutable snapshot/CDF descriptors are a natural fit.

Costs:

- S7 is new and its own documentation calls it somewhat experimental;
- another runtime dependency and package-registration mechanism;
- public S7 representation may evolve;
- it does not provide true private properties, so it should not be asked to
  encapsulate mutable native execution state.

## Decision

Use S7 for high-level value-like descriptors and functional generics. Keep
mutable execution state in Rust external pointers and nanoarrow streams. Add S3
methods only for established external generics such as `print()` and
`as.data.frame()`.

The exported functions—not direct property access—are the public contract.
There is no S3 fallback and no requirement to preserve the prior R6 surface.
The canonical names are recorded in `s7-interface-naming-matrix.md`.

## Guardrails

- Users never need `@` for normal work.
- Query configuration returns a new object; it does not mutate table/client
  state.
- `SharingClient` may contain a validated external pointer, but scan state never
  lives in an S7 property.
- The stateful stream is the standard nanoarrow external-pointer class.
- Serialization of clients/streams is unsupported and fails clearly.
- S7's experimental environment base class is not used.
- Cross-platform package and lifetime proofs are required before reader
  implementation.

## Consequences

The API will look more like idiomatic R:

```r
orders <- sharing_table(
  client,
  share = "sales",
  schema = "default",
  table = "orders"
)
read <- sharing_read(orders, version = 42L)
stream <- read_arrow_stream(read)
```

It will not attempt to mimic Python syntax at the cost of R composability.
Familiar terminology is design inspiration only; vNext does not provide
aliases, deprecations, or transition behavior for earlier package releases.
