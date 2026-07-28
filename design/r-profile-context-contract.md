# R profile and client-context contract

Status: implemented Phase 2 foundation
Branch owner: `codex/r-profile-context-vnext`

This contract records the clean-vNext R boundary for profile configuration and
client state. It adds no compatibility aliases, migration behavior, or
prior-version behavior tests.

## Public descriptors

`SharingProfile` parses its source immediately and exposes only safe,
read-only metadata:

- source type and a non-sensitive label;
- profile version;
- normalized sharing endpoint;
- authentication type;
- optional bearer expiration time.

`SharingClient` exposes only its `SharingProfile`. Mutable authentication state
is not an S7 property. Printing a profile or client shows safe metadata and
never renders credential fields.

Supported sources are a file path, inline JSON character or raw data, a
binary-readable connection, and an explicitly named list. JSON-bearing sources
are limited to 1 MiB. Text-only connections are rejected because base R cannot
read them in bounded chunks; callers can use a `rawConnection` for in-memory
input. Construction performs no sharing request, token exchange, or
private-key read.

## Accepted profile descriptors

The parser accepts:

- version 1 bearer credentials;
- version 2 `bearer_token`;
- version 2 `basic`;
- version 2 `oauth_client_credentials`;
- version 2 `oauth_jwt_bearer_private_key_jwt`.

OAuth client descriptors validate token endpoint, client identity, secret, and
optional scope. Private-key descriptors validate their nested token endpoint,
client identity, issuer, audience, optional scope, key-file descriptor,
optional key id, and signing algorithm. Only `RS256` descriptors are accepted;
missing algorithms default to `RS256`. Construction does not open the key.
Authentication opens and parses it only when a fresh assertion is required.

Sharing and token endpoints must be absolute HTTP(S) URLs without embedded
credentials, query strings, or fragments. Bearer expiration times must be RFC
3339. Profile versions newer than 2 fail with an actionable unsupported-feature
condition.

## Private R state

Credential values and client contexts live in package-private registries.
Public S7 objects retain opaque environment handles whose finalizers remove
their registry entries. A client context holds:

- normalized endpoint and authentication type;
- the validated credential descriptor;
- mutable authentication state;
- cached OAuth access-token timing and generation state.

The internal `.client_context()` boundary is the handoff for the R HTTP/auth
workstream. It must not be exported or added as a public S7 property. Future
token refresh mutates this context, never a `SharingClient`, `SharingTable`, or
read descriptor.

## Secret-safety rules

- Raw source data is discarded after parsing.
- Credentials, passwords, client secrets, and private-key paths are not S7
  properties.
- Conditions contain fixed messages and allowlisted safe metadata only.
- JSON parser, file, and connection errors are translated without forwarding
  source content, OS error text, or a path.
- Unsupported authentication errors do not repeat the configured type string.
- Downstream authentication and HTTP code must preserve these rules when using
  `.client_context()`.
