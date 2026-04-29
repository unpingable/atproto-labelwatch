# labelwatch — Publication Model

**Status**: v0 starter.
**Last updated**: 2026-04-28

## The job

This doc describes how labelwatch turns raw observation into published surfaces — the path from `label_events` rows to "what shows up on the dashboard or API." It also names what is *deliberately not published* and why.

## The pipeline

```
raw label_events
     │
     ▼
  detection rules ──▶ alerts (receipted)
     │
     ▼
  derive pass    ──▶ per-labeler signals (regime, risk, coherence)
     │
     ▼
  boundary pass  ──▶ inter-labeler edges (JSD divergence, contradiction)
     │
     ▼
  rollups        ──▶ derived_author_day, derived_author_labeler_day
     │
     ▼
   ┌────────────────────────────────┐
   │  publication tier evaluation   │
   │  (thresholded; see below)      │
   └────────────────────────────────┘
     │
     ▼
   ┌────────────────────────────────┐
   │  surfaced outputs              │
   │  - report.html (static)        │
   │  - climate API (per-DID)       │
   │  - registry endpoint           │
   └────────────────────────────────┘
```

## What is published

- **Static report** (`report.html` + JSON) — labelers, alerts, census, triage views, boundary cards, health cards. Atomic dir swap. No dynamic queries; no XSS surface.
- **Climate API** (`/v1/climate/{did_or_handle}`) — per-DID receiving-end accounting: what labelers have done *to* this DID. Rate-limited (token bucket per IP), disk-cached (5min TTL), public payload whitelist strips `recent_receipts`, kill switch (`CLIMATE_API_DISABLED`).
- **Registry endpoint** (`/v1/registry`) — labeler directory.
- **Health endpoint** (`/health`) — liveness only.

## What is not published

- **No labeler verdicts.** The dashboard shows behavioral patterns and signals; it does not say "this labeler is bad."
- **No truth claims about content.** Alerts describe *labeler* behavior, not whether the underlying labels are correct.
- **No motive inference.** Patterns are observable; intent isn't.
- **No poster behavioral profiles.** No per-DID volatility scores, posting risk classes, or "discourse weather" reads on accounts. The climate API reports what was done *to* a DID, not what the DID is likely to do. See `PUBLIC_SURFACES.md` (TODO) and `../../NON_GOALS.md`.
- **No collapsed trust score.** Auditability, inference risk, temporal coherence, and regime state are four separate dials. They are not summed, multiplied, or otherwise collapsed into a single number.

## Tiers and gates

The publication model uses thresholded gates to decide which signals are surfaced vs suppressed:

- **Warm-up gate** — new labelers are tagged `warming_up`. Rate-based alerts (spike, churn) are suppressed until enough event volume accumulates; pattern alerts (flip-flop, concentration) can still fire.
- **Sparse gate** — labelers with insufficient volume have rate rules suppressed entirely.
- **Hysteresis gate** — regime state changes require N consecutive passes before surfacing. No flapping.
- **Cooldown / dedupe** — repeated detections on the same `det_id` suppressed for `COOLDOWN_WINDOWS` unless severity escalates or score increases by ≥ `COOLDOWN_SCORE_DELTA`.
- **Fight gate (boundary)** — only moderation-vs-moderation conflicts with ≥2 shared targets surface as fight cards. Random taxonomy mismatch is filtered.

Publication tier thresholds are not yet fully validated against live data. See the `labelwatch assess` CLI and the next-steps section in project memory.

## Receipts and reproducibility

Every alert published includes a SHA-256 receipt hash over `(rule_id, labeler_did, ts, inputs_json, evidence_hashes_json, config_hash)`. Given the same inputs and config, the same alert produces the same hash. Receipts let downstream consumers verify that what they read matches what was computed.

The receipt is not a cryptographic signature — it's not attributable to a specific signer. It is reproducible: anyone with the same inputs can recompute the digest and verify the alert wasn't mutated.

## Dashboard language

The dashboard avoids accusation-shaped phrasing. Behavior is described as patterns, not motives:

- "rate spike" not "labeler attacking targets"
- "target concentration" not "labeler obsessed with"
- "flip-flop" not "labeler indecisive"
- "boundary contradiction" not "labeler wrong about"

The dashboard's job is to surface observables that warrant operator attention, not to render verdicts.

## What this means for new published surfaces

Any new published surface must answer:

1. **Aggregate or per-DID?** Aggregate is default-permitted. Per-DID requires step 2.
2. **If per-DID, is it receiving-end or behavioral-end?** Receiving-end (what was done *to* the DID by labelers) is permitted. Behavioral-end (what the DID did or will do) is not.
3. **If behavioral-end on a poster — stop.** That's outside the publication model. See `PUBLIC_SURFACES.md` (TODO).
4. **What's the receipt?** Surfaces without receipts can't be verified; verification is a precondition for publication.
5. **What's the gate?** What suppresses noise? Warmup, sparse, hysteresis, cooldown — pick one or design a new one.
