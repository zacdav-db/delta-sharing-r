# vNext design packet

The design packet is read in this order:

1. `integration-roadmap.md` — active checklist, specialist ownership,
   integration rules, phase order, and completion gates.
2. `s7-interface-naming-matrix.md` — canonical public S7 concepts and names.
3. `adr-001-object-system.md` — accepted S7 descriptor decision.
4. `adr-003-rust-scope.md` — accepted R-first implementation boundary and the
   performance gate for any additional Rust.
5. `adr-002-rust-arrow-boundary.md` — minimal Kernel/Arrow native boundary and
   required implementation proof.
6. `vnext-plan.md` — detailed requirements, architecture, tests, benchmarks,
   risks, and technical references.
7. `rust-dependency-policy.md` — enforced native advisory, license, and source
   policy plus reviewed transitive exceptions.
8. `r-execution-wiring-contract.md` — production R discovery/metadata
   execution, injection, and lifecycle boundary.
9. `parquet-response-kernel-proof.md` — Phase 6 wire-to-log mapping, R/Kernel
   ownership, safety gates, and executable fixture evidence.
10. `api-mock.R` — non-executable illustration of the canonical API.

Implemented R subsystems also carry focused contracts. Snapshot synthetic-log
mapping, atomic publication, privacy, and lifetime are recorded in
`r-synthetic-log-contract.md`.

When documents differ, the maintainer decisions and clean-break policy in the
roadmap and interface matrix take precedence.

vNext has no prior-package compatibility requirement. The existing R6 code can
inform terminology and problem discovery, but it is neither a public contract
nor a test oracle.
