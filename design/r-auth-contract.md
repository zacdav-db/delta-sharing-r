# R authentication-state contract

Status: implemented R authentication layer
Latest branch owner: `codex/r-jwt-auth-vnext`

This contract extends the private R client context without adding public
functions or compatibility surfaces. It covers profile bearer credentials,
HTTP Basic credentials, OAuth client credentials, and OAuth private-key JWT
client assertions. Discovery and table control-plane requests use the separate
R execution layer.

## Internal authorization boundary

`.client_authorization()` accepts a `SharingClient` and returns:

- the authorization header required for the next request;
- the profile authentication type;
- for cached OAuth credentials, a non-secret cache generation.

The result is internal and contains credential material. It is passed directly
to the request adapter and must never be printed, attached to a condition, or
included in diagnostics.

Profile bearer credentials are checked against their optional expiration time
on every authorization request. HTTP Basic values are UTF-8 encoded and
Base64-wrapped in R. A colon in a Basic username is rejected because it would
make the RFC 7617 credential ambiguous.

## OAuth client credentials

The client-secret OAuth request is transport-neutral:

- method `POST`;
- validated profile token endpoint;
- HTTP Basic client authentication;
- form body with `grant_type=client_credentials` and optional scope;
- JSON response requested.

The injected transport is a private list of `send`, `status`, and `body`
functions plus an optional `retry_after` function. It uses the shared R HTTP
retry control. OAuth exchanges are explicitly replayable for that control.
The internal R HTTP layer supplies the httr2 production adapter through the
same contract.

Successful responses must be JSON-style objects containing an `access_token`
and a positive numeric or numeric-string `expires_in`. `token_type` may be
absent; when present it must identify Bearer authentication
case-insensitively. Response bodies are limited to 1 MiB. Parser, transport,
HTTP, and identity-provider errors are translated without retaining response
content or credential values.

## OAuth private-key JWT

Private-key JWT profiles use the same bounded OAuth response parser, access
token cache, refresh threshold, and generation counter as client-secret OAuth.
The form request contains:

- `grant_type=client_credentials`;
- the profile `clientId`;
- the standard JWT bearer client-assertion type;
- the compact signed assertion;
- optional profile scope.

It does not contain HTTP Basic authorization or a client secret.

The assertion is deterministic for supplied clock, random, and signer hooks.
Its protected header is `alg=RS256`, `typ=JWT`, plus `kid` only when the
profile supplies `keyId`. Claim mapping follows the committed v2 profile
descriptor exactly:

- `iss` is the profile `issuer`;
- `sub` is the profile `clientId`;
- `aud` is the profile `audience` without substituting the token endpoint;
- `iat` is the whole current epoch second minus the bounded skew;
- `exp` is the whole current epoch second plus the bounded lifetime;
- `jti` is the unpadded base64url form of 32 cryptographically random bytes.

Production assertions have a 300-second lifetime and a 30-second backward
clock skew. Internal validation caps lifetime at 600 seconds and skew at 60
seconds. Header, claims, and signature use unpadded base64url encoding. The
signing input is capped at 16 KiB and the resulting signature at 1 KiB.

The only added crypto dependency is `openssl` (MIT). It reads at most 64 KiB
from the configured file, accepts an RSA private key, and signs RS256 using
OpenSSL SHA-256/RSA primitives. Unencrypted PEM and DER keys are accepted.
Encrypted keys fail closed without prompting because the profile has no
passphrase field. Empty, oversized, unreadable, malformed, public-only, and
non-RSA keys also fail with one fixed auth condition. File access follows host
OS permissions; this layer does not weaken permissions or require a specific
mode. R and the openssl package do not provide a portable proof of immediate
private-key memory zeroization, so the parsed key is kept only in a local
signing call and is never cached in the client context.

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
primitive. It clears either OAuth cache only when the supplied non-secret
generation still identifies the current token. This prevents a delayed 401
from invalidating a token refreshed by a newer request. The HTTP layer retries
an explicitly replayable sharing request at most once after a successful
generation-matched invalidation. Private-key replay creates a new assertion and
performs one new token exchange before replacing the sharing Authorization
header.

## Explicit exclusions

- No exported authentication, assertion, signer, or key-management API.
- No interactive encrypted-key password flow or passphrase persistence.
- No native or Rust authentication code.
- No credentials, key paths/material, assertions, JTIs, request bodies, access
  tokens, or identity-provider error text in public conditions or printing.
