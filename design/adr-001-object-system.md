# ADR 001: R object system for vNext

Status: proposed  
Decision owner: package maintainer  
Decision required before: Phase 1

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

- familiar `client$table()$snapshot()$to_arrow()` chaining;
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

## Proposed decision

Use S7 for high-level value-like descriptors and functional generics. Keep
mutable execution state in Rust external pointers and nanoarrow streams. Add S3
methods only for established external generics such as `print()` and
`as.data.frame()`.

The exported functions—not direct property access—are the compatibility
contract. They should be written so their return representation can become a
thin S3 object if the decision spike rejects S7.

## Guardrails

- Users never need `@` for normal work.
- Query configuration returns a new object; it does not mutate table/client
  state.
- `DeltaClient` may contain a validated external pointer, but scan state never
  lives in an S7 property.
- The stateful stream is the standard nanoarrow external-pointer class.
- Serialization of clients/streams is unsupported and fails clearly.
- S7's experimental environment base class is not used.
- A two-day cross-platform package/lifetime spike precedes implementation.

## Consequences

The API will look more like idiomatic R:

```r
orders <- delta_table(client, "sales.default.orders")
read <- delta_snapshot(orders, version = 42L)
stream <- to_arrow_stream(read)
```

It will not attempt to mimic Python syntax at the cost of R composability.

