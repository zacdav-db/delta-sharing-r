# R discovery planning contract

Status: implemented Phase 2 planning foundation
Branch owner: `codex/r-discovery-planning-vnext`

This slice defines the pure-R discovery boundary without selecting or invoking
an HTTP transport. It does not install the internal planning functions as
public execution callbacks.

## Routes and pagination

Discovery routes are relative to the normalized profile endpoint. Every
provider-supplied share and schema name is encoded as one path segment, so
spaces, slashes, percent signs, query markers, fragments, and non-ASCII names
cannot change the route hierarchy.

The route families are:

- `/shares`;
- `/shares/{share}/schemas`;
- `/shares/{share}/schemas/{schema}/tables`;
- `/shares/{share}/all-tables`.

Each route is collected through the shared bounded pagination control.
Transport remains an injected internal callback until authenticated HTTP is
ready.

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
