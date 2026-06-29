# Gap spec: resolver tail-health surface — show tail, not just current coverage

**Status:** CANDIDATE handle, **not ratified, not authorized to build.** Filed
2026-06-29 as a review handle while the shape is fresh (name-early / ratify-lazily).
No forcing *case* to implement yet; build only on explicit greenlight.

**Provenance:** the 2026-06-29 Driftwatch resolver aged-tail thread. The whole episode
turned on a distinction a single coverage number hides — see
`atproto-driftwatch/docs/resolver-pending-aged-tail.md`. **Gated on the upstream facts
contract:** this surface should consume the
[facts snapshot manifest](../../../driftwatch/specs/gaps/gap-spec-facts-snapshot-manifest.md)
(also candidate), not read Driftwatch's ad hoc tail JSON directly. No direct coupling
to internal Driftwatch artifacts.

## Problem

A "resolver coverage: X%" readout is a charismatic liar: coverage can improve while the
aged tail rots (2026-06-29 — pending total fell 21.8k while `gt168` grew 21.5k and the
oldest floor slid 10.4d→11.0d). A current-state-only surface cannot show that.

## Shape (non-binding sketch)

Surface the **tail**, not just the snapshot:

```
resolver tail:
  pending_total
  pending_gt_72h
  pending_gt_168h
  oldest_pending_hours
  new_pending/hour
  resolved/hour
  aged_tail_delta/hour
```

With a plain-language badge that names the divergence:

```
"coverage improving, tail worsening"
"total draining, aged tail still growing"
```

These map onto Driftwatch's read-classification taxonomy candidate
(`weather_win` vs `capacity_win` vs `coverage_loss`).

## Non-goals

- No direct read of Driftwatch internal DBs or ad hoc JSON — consume declared facts +
  coverage/refusal metadata via the manifest only.
- No new enforcement/verdict surface. "Weather, never verdict" holds.
- Source values must carry their denominator (the facts contract's standing rule).

## Why candidate, not build

The forcing evidence (a real coverage-vs-tail divergence) exists; the consumption
contract it depends on (facts manifest) is itself only a candidate. Promote this
**after** the manifest is ratified, so the surface reads declared facts, not a
hand-copied artifact.
