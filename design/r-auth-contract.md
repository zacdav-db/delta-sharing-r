# R authentication-state contract

Status: implemented bounded auth slice
Branch owner: `codex/r-auth-vnext`

This contract extends the private R client context without adding public
functions or compatibility surfaces. It covers profile bearer credentials,
HTTP Basic credentials, and OAuth client credentials. Discovery, sharing
requests, private-key reads and signing, and 401 replay remain separate work.

## Internal authorization boundary

`.client_authorization()` accepts a `SharingClient` and returns:

- the authorization header required for the next request;
- the profile authentication type;
- for cached OAuth credentials, a non-secret cache generation.

The result is internal and contains credential material. It must be passed
directly to the future request adapter and must never be printed, attached to a
condition, or included in diagnostics.

Profile bearer credentials are checked against their optional expiration time
on every authorization request. HTTP Basic values are UTF-8 encoded and
Base64-wrapped in R. A colon in a Basic username is rejected because it would
make the RFC 7617 credential ambiguous.

## OAuth client credentials

The OAuth request is transport-neutral:

- method `POST`;
- validated profile token endpoint;
- HTTP Basic client authentication;
- form body with `grant_type=client_credentials` and optional scope;
- JSON response requested.

The injected transport is a private list of `send`, `status`, and `body`
functions plus an optional `retry_after` function. It uses the shared R HTTP
retry control. Client-credential exchanges are explicitly replayable for that
control. Tests inject both transport and clock; the internal R HTTP layer now
supplies the httr2 production adapter through the same contract.

Successful responses must be JSON-style objects containing an `access_token`
and a positive numeric or numeric-string `expires_in`. `token_type` may be
absent; when present it must identify Bearer authentication
case-insensitively. Response bodies are limited to 1 MiB. Parser, transport,
HTTP, and identity-provider errors are translated without retaining response
content or credential values.

## Token cache and invalidation

The private client context stores the access token, issue time, expiration
time, refresh time, and a monotonically increasing generation. The proactive
refresh threshold is:

```text
min(600 seconds, token lifetime / 2)
```

A token is reused only strictly before both its refresh and expiration times.
At the refresh boundary a new exchange is required.

`.invalidate_client_auth(client, cache_generation)` is the only invalidation
primitive. It clears the OAuth cache only when the supplied non-secret
generation still identifies the current token. This prevents a delayed 401
from invalidating a token refreshed by a newer request. The future replay layer
may retry at most once after a successful generation-matched invalidation; that
replay behavior is not implemented here.

## Explicit exclusions

- No discovery or Delta Sharing request dispatch.
- No forced 401 replay.
- No private-key file access, JWT assertion, or crypto dependency.
- No native or Rust code.
- No exported auth provider, header, token, or invalidation API.
- No credentials, request bodies, or identity-provider error text in public
  conditions or printing.
