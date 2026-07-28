# vNext design packet

The design packet is read in this order:

1. `integration-roadmap.md` — active checklist, specialist ownership,
   integration rules, phase order, and completion gates.
2. `s7-interface-naming-matrix.md` — canonical public S7 concepts and names.
3. `adr-001-object-system.md` — accepted S7 descriptor decision.
4. `adr-002-rust-arrow-boundary.md` — accepted Rust/Kernel/Arrow architecture
   and required implementation proof.
5. `vnext-plan.md` — detailed requirements, architecture, tests, benchmarks,
   risks, and technical references.
6. `api-mock.R` — non-executable illustration of the canonical API.

When documents differ, the maintainer decisions and clean-break policy in the
roadmap and interface matrix take precedence.

vNext has no prior-package compatibility requirement. The existing R6 code can
inform terminology and problem discovery, but it is neither a public contract
nor a test oracle.
