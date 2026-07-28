# R discovery planning contract

Status: implemented and connected to R HTTP execution
Branch owner: `codex/r-discovery-planning-vnext`

This contract defines the pure-R discovery boundary. Production execution uses
the authenticated httr2 adapter; hermetic tests inject the same private
transport interface.

## Routes and pagination

Discovery request plans carry validated raw path-segment vectors relative to
the normalized profile endpoint. The transport performs the only encoding
step. Provider-supplied share and schema names therefore remain one segment
even when they contain spaces, slashes, percent signs, query markers,
fragments, or non-ASCII names. `.discovery_route()` remains an internal
deterministic encoded-route helper for assertions; its output is never passed
back through the transport.

The route families are:

- `/shares`;
- `/shares/{share}/schemas`;
- `/shares/{share}/schemas/{schema}/tables`;
- `/shares/{share}/all-tables`.

Each route is collected through the shared bounded pagination control. Page
tokens remain separate query values and are never interpolated into paths.

## Omitted-filter fan-out

`list_schemas(client)` first resolves the complete share list, then plans one
schema route per share. `list_tables(client)` similarly resolves shares and
plans one `all-tables` route per share. Explicit share and schema filters plan
only the narrow route required by the request.

Provider order is preserved across pages and fan-out routes. Empty discovery
results retain the same documented data-frame columns and column types.

## Public record safety

Normalized discovery results are base data frames with fixed schemas:

- shares: `name`, `id`, `display_name`, `comment`;
- schemas: `share`, `name`;
- tables: `share`, `schema`, `name`, `share_id`, `id`, `access_modes`.

`access_modes` is a base list-column of character vectors. Provider identifiers
are preserved exactly.

Storage `location`, `auxiliaryLocations`, arbitrary properties, credential
fields, and unknown response fields are never copied into public records.
Validation and collection failures use fixed conditions and do not include
record bodies, paths, page tokens, URLs, or underlying error text.
