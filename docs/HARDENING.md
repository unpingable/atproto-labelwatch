# Hardening Guide

This repo started as a batch processor + static site. We now have an HTTP
surface (`/v1/climate/*`) and a homepage link to it. That changes the threat
model.

This document defines *hardening invariants* and a tiered checklist so we can
increase safety without turning this into a security crusade.

## Threat model

### What we're defending against
- Commodity abuse: scanners, opportunistic DoS, malformed requests,
  path/encoding weirdness.
- Targeted abuse: people who understand what Labelwatch reveals and want it
  offline or weaponized.
- Self-inflicted exposure: turning "observatory" features into
  "enumeration / stalking / harassment" primitives.

### What we're NOT defending against (non-goals)
- A determined nation-state actor.
- A fully compromised host or root.
- Side-channel/traffic analysis.
- Perfect privacy against someone who already has the underlying data.

## Core invariants (do not regress)

### 1) Read-only query layer cannot write
- HTTP server opens SQLite with `mode=ro` (URI form).
- `query_only=ON` pragma set on readonly connections.
- Service user has no write access to DB path (enforced by systemd
  `ReadOnlyPaths` + filesystem perms).
- No `init_db()` in the server path.

### 2) No XSS on our domain
- All untrusted strings are `html.escape()`'d at render time: handles, label
  namespaces/values, URIs, regime strings, DIDs.
- Never inject untrusted strings into inline JS.
- `X-Content-Type-Options: nosniff` on all responses.
- `Content-Type` always includes explicit charset.

### 3) DoS is boring
- Concurrency cap (semaphore, non-blocking) returns 503 when saturated.
- Rate limiting applies before cache check (protects bandwidth too).
- Caddy-level rate limiting for `/v1/climate/*` (covers everything).
- Hard caps: DID max 256 chars, window clamped to [1, 60].
- Generation timeout (10s default) prevents thread pile-up.
- Atomic cache writes (tempfile + rename) — never serve half files.

### 4) No "stalking endpoint" by accident
**Current policy: public aggregates only.**
- Public output: counts, series, top labelers, top values, week deltas.
- Stripped from public: `recent_receipts` (per-post URIs).
- Enforcement: `public_climate_payload()` uses a **whitelist** (not blacklist).
  Future fields are private by default.
- CLI retains full output (local use = trusted).

To change this policy: update this file, update `_PUBLIC_KEYS`, and accept the
adversarial implications.

### 5) Kill switch exists
- `CLIMATE_API_DISABLED=1` → all `/v1/climate/*` return 503 immediately.
- No redeploy needed. Set in env, restart service.
- Used during incidents/abuse.

### 6) Proxy is the public boundary
- HTTP server binds to `127.0.0.1` only (default, enforced in code).
- Caddy handles TLS, public exposure, global rate limits.
- Server is never directly reachable from the internet.

## Tier 0 checklist (ship-now, cheap, high leverage)

All items below are implemented as of Phase 2 deploy.

- [x] DID validation: `did:` prefix, max 256 chars, no `/` or control chars
- [x] URL-decode DID path segment once (`urllib.parse.unquote`)
- [x] Window clamped to [1, 60]
- [x] Unknown routes → 404, extra path segments → 404
- [x] Invalid format values → default to html
- [x] SQLite opened `mode=ro` + `query_only=ON`
- [x] No `init_db()` in server path
- [x] All HTML rendering uses `html.escape()` on untrusted content
- [x] `X-Content-Type-Options: nosniff` on all responses
- [x] `Content-Type` with explicit charset on HTML
- [x] `Cache-Control: private, max-age=300` on climate responses
- [x] `Cache-Control: no-store` on health endpoint
- [x] Atomic cache writes (tempfile + `os.replace`)
- [x] Cache key: DID slug + window + format
- [x] Cache TTL: 300s default, configurable
- [x] Concurrency gate: semaphore(2), non-blocking, 503 on saturation
- [x] Rate limiter: token bucket (30/min default), applied before cache
- [x] Generation timeout: 10s, returns 503 on timeout
- [x] Kill switch: `CLIMATE_API_DISABLED=1`
- [x] Public payload whitelist: `public_climate_payload()` strips private fields
- [x] DID truncated to 20 chars in logs (no log amplification)
- [x] Per-minute STATS aggregation (counters only, no DIDs)
- [x] Systemd: separate user, `ProtectSystem=strict`, `NoNewPrivileges`,
      `PrivateTmp`, `ReadWritePaths` cache only, `ReadOnlyPaths` DB,
      `MemoryMax=512M`, `CPUQuota=25%`
- [x] Bind `127.0.0.1` only

## Tier 1 (once publicly promoted / adversarial interest)

### Access policy: make "public vs private" explicit
Pick one when the time comes:
- **Aggregates-only public** (current): no per-post URIs in public output.
- **Proof-of-control** for detail views: shared secret token or DID auth.
- **Allowlist**: only serve climate for configured DIDs (self-only mode).

### Proxy-level controls
- Global rate limiting for `/v1/climate/*` at Caddy.
- Request header/body timeouts.
- Upstream response timeout to API.

### Abuse telemetry / alerting
- Alert on: sustained 429 spikes, sustained 503 busy spikes, sudden
  cache-miss collapse, unusual 400/404 scanning patterns.

### Output shaping
- If per-post tables are ever exposed publicly: truncate, require token for
  "show more", return only counts/hashes, or time-bucket instead of listing.

## Tier 2 (later, if it becomes a real service)

- Strong auth for detail views (DID auth flow).
- CSP (requires moving inline JS out of HTML or accepting `unsafe-inline`).
- Per-IP rate limiting (if upstream headers are trustworthy).
- Fuzzing of request parsing and encoding edge cases.
- Threat-model review before adding any write endpoints.

## Acceptance checks (quick regression tests)

Encoded in `tests/test_server.py`:
- HTML output cannot execute `<script>` embedded in handle/value/URI.
- `/v1/climate/did%3Aplc%3A...` works (path decoding).
- `recent_receipts` absent from public JSON response.
- Rate limiting triggers (429 with Retry-After).
- Unknown routes → 404, extra segments → 404.
- Invalid DID → 400.

Manual checks:
- Server cannot write DB (readonly + filesystem perms).
- Cache files written atomically (no partial reads).
- Kill switch disables climate endpoints immediately.

## When to escalate

Do the Tier 1 pass when any of these become true:
- You announce it / it gets attention.
- You allow querying *any* DID with detailed outputs.
- You add write-ish endpoints (even "admin" ones).
- You start accepting user input beyond "DID + window".
