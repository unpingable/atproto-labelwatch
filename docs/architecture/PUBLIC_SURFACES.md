# labelwatch — Public Surfaces

## Approved successor scope (October 6, off-production)

Existing issue #7 implements the bounded recent-observation contract in
`LABELWATCH-SEGMENTED-DECISION.md`. Its account view reports recently observed
label applications/removals, with summary, timeline, filters, attribution and
coverage gaps. This is receiving-end observation, not behavioral prediction.
Current effective state remains unknown unless separately qualified; an absent
removal is not evidence that a label is still active.

The explicit successor preview offers `/`, `/account` and `/exports` only on
loopback. Export snapshots contain a public-field allowlist, coverage, schema,
content identity and fixed expiry; pages cannot keep storage generations alive.
Internal probes, quarantine, operational evidence and arbitrary tables are not
export surfaces. Dense requests refuse explicitly and request a shorter interval;
they never silently truncate. The initial30-day window is a capacity candidate,
not a deployed guarantee or an accepted production resource envelope.

Production remains in safe hold with public503 responses. The inventory below
describes the earlier service; no successor surface is selected in production.
Migration must explicitly reconcile named existing consumers with windowed
semantics, without silently representing a recent-window answer as lifetime data.

**Status**: v0 starter.
**Last updated**: 2026-09-10

## The job

This doc names the public surfaces labelwatch exposes, the architectural rules that govern them, and the boundary against per-poster surveillance. It is not an ethics doc. It determines the shape of the system: storage may be permissive, the API must not be.

## Surfaces inventory

| Surface | Shape | Reachability | Key gates |
|---------|-------|--------------|-----------|
| Static report | HTML + JSON | public web (via Caddy) | atomic dir swap; no dynamic queries |
| Climate API `/v1/climate/{did_or_handle}` | per-DID receiving-end accounting | loopback + Caddy | token bucket per IP, disk cache (5min TTL), kill switch (`CLIMATE_API_DISABLED`), payload whitelist strips `recent_receipts`, generation timeout, concurrency semaphore |
| Registry endpoint `/v1/registry` | labeler directory | loopback + Caddy | rate-limited |
| Health endpoint `/health` | liveness plus gate state | loopback + Caddy | reports frontdoor receipt readiness without turning other healthy surfaces into a non-200 |
| Bsky bot `@labelwatch.neutral.zone` | summary posts | ATProto | manual or scheduled, never per-DID |

## The aggregate / per-DID / behavioral distinction

Surfaces are categorized by what they expose:

1. **Aggregate** — sums, distributions, censuses, boundary edges across labelers. *Default-permitted.*
2. **Per-DID receiving-end** — what labelers have done *to* a specific DID. Climate API. *Permitted with gates.* The DID is the recipient of action; the surface reports what was done to them.
3. **Per-DID behavioral-end** — what a specific DID did or will do. Volatility, posting risk, "discourse weather" on an account. *Forbidden.* This is dossier production.

The architectural distinction is load-bearing. Receiving-end accounting is descriptive of others' actions; behavioral-end forecasting is prescriptive about a person. The first is observation; the second is surveillance.

## The forbidden shape

```
GET /poster/{did}/weather           # forbidden
GET /poster/{did}/volatility         # forbidden
GET /poster/{did}/risk_class         # forbidden
GET /v1/forecast/{did}              # forbidden
```

Or any equivalent per-handle behavioral forecast surface. The shape is the hazard, not just the implementation. Adding such an endpoint as a "stub" or "placeholder" still creates the schema.

## The load-bearing rule

> **If the tables can answer dossier-shaped questions, the API still must not.**

Storage may need to be permissive (for derive passes, rollups, internal joins). The publishable API surface is a separate decision. The fact that `derived_author_day` could be queried per-DID for behavioral aggregates does not mean that a behavioral-aggregate endpoint should exist. The publishable contract is *not* "everything the storage can compute."

## What the climate API is and isn't

The climate API (`/v1/climate/{did_or_handle}`) is **receiving-end accounting**. Specifically:

- ✓ "what labelers labeled this DID, when, with what values" (label history)
- ✓ "which labelers are most active on this DID" (top labelers)
- ✓ "label volume on this DID over time" (daily series)
- ✗ "what this DID is likely to post next"
- ✗ "this DID's posting risk class"
- ✗ "this DID's volatility score"
- ✗ "this DID's discourse-weather forecast"

The first three describe public observable actions taken by *labelers*, indexed by their *target*. The last four would describe inferred properties of the *target* themselves. Different surfaces, even though both are keyed on `{did}`.

## Adding a new surface — checklist

Any new published surface must answer, in order:

1. **Aggregate or per-DID?** If aggregate, jump to step 4.
2. **If per-DID, is it receiving-end or behavioral-end?** Receiving-end → continue. Behavioral-end → stop. Don't build it.
3. **What rate / cache / kill-switch / payload-whitelist gates apply?** Match or exceed the climate API's posture.
4. **What's the receipt?** Surfaces without receipts can't be verified. Verification is a precondition for publication.
5. **What's the threshold gate?** What suppresses noise? Warmup, sparse, hysteresis, cooldown — pick or design one.
6. **What's the doc?** Update the inventory table above.

## Stage gating (future)

All present surfaces are Stage 0–1: read-only observation, descriptive language, receipted, gated. Stage 2+ would include public ATProto label emission, which is currently `LABELER_EMIT_MODE=detect-only` and gated by an explicit confirm step. That decision is deliberate and reversible — it is not a default that drifts on by accident.

## Cross-reference

- `../../NON_GOALS.md` — the bullet-form version of these prohibitions.
- `PUBLICATION_MODEL.md` — the full path from raw events to surfaced outputs.
- `OVERVIEW.md` — system context.
- `../HARDENING.md` — Tier-0 hardening on the climate API specifically.
