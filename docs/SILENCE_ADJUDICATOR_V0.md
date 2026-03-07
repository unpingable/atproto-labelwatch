# Labelwatch Silence Adjudicator v0

**Status:** Working note. Not a publication. Not a spec. A pinned ontology so future-me can't cheat.

**Purpose:** Determine *why* a labeler went quiet. Not whether it's quiet — that's a timestamp comparison. The question is: what regime is this silence evidence of?

---

## Target failure class

Labeler silence. A labeler that was emitting labels has stopped emitting labels. This is the most common ambiguous signal in Labelwatch operations and the one where static alerting fails hardest, because silence has multiple causes with radically different implications.

## Regime taxonomy

These are the plausible explanations for a labeler going quiet. They are not mutually exclusive — a labeler can be in a legitimate burst gap that is also masking a behavioral drift — but the adjudicator should pick the most probable primary regime and flag secondary possibilities.

### 1. Normal silence

The labeler has a known bursty or sparse cadence and this gap is within historical norms. No action needed. Most common regime by far — the adjudicator's main job is to correctly classify silence as *this* and shut up about it.

### 2. Burst gap

The labeler typically fires in clusters tied to external events (moderation waves, content spikes, etc.) and is currently between bursts. Distinguishable from normal silence by cadence shape — inter-event intervals are bimodal rather than roughly uniform.

### 3. Upstream PDS/hosting issue

The labeler's PDS is unreachable, degraded, or has changed behavior. Evidence: other labelers co-hosted on the same PDS or infrastructure are also quiet or degraded. This is the regime where peer comparison earns its keep.

### 4. Labeler death

The labeler is gone. DID doc may be stale, PDS endpoint may 404, no events from any source. Distinct from upstream issue because it's isolated to this specific labeler.

### 5. Behavioral drift

The labeler is still alive but has changed what it does — different label schemas, different targeting patterns, different volume profile. It hasn't gone silent; it's gone *different* in a way that looks like silence from the old baseline. This is the sneaky one.

### 6. Firehose issue

The silence isn't about the labeler at all. The firehose itself is dropping events, lagging, or filtering differently. Evidence: multiple unrelated labelers show correlated silence onset.

### 7. Local ingest issue

Labelwatch's own ingest pipeline is the problem. Events are arriving but not being recorded, or being recorded with incorrect timestamps, or the cursor is advancing without consuming. The system is lying to itself. Evidence: firehose cursor is moving but no new records for *any* labeler, or freshness timestamps are suspiciously uniform.

### 8. Unresolved

Insufficient evidence to classify. This is a legitimate output. The adjudicator should be comfortable here and say what would disambiguate.

---

## Evidence surface

Everything here must already exist in Labelwatch or be trivially derivable. Do not build new instrumentation for the monitor.

| Signal | Source | Notes |
|--------|--------|-------|
| Last-seen timestamp per labeler | Labelwatch DB | Primary silence detection input |
| Cadence sketch | Derived from historical inter-event intervals | Rolling mean + variance, bucketed into behavioral classes (metronomic / bursty / sparse / dormant). Start dumb. |
| Firehose cursor position + velocity | Labelwatch ingest state | Is the cursor advancing? At what rate vs historical? |
| Peer labeler activity | Labelwatch DB, grouped by PDS/hosting | Are co-hosted labelers also quiet? |
| Recent schema changes | Labelwatch schema version tracking | Did this labeler recently change its label definitions? |
| Local ingest health | Labelwatch process metrics | Write rate, error rate, WAL size, busy_timeout hits |
| Row freshness distribution | Labelwatch DB | Are freshness timestamps uniform across labelers? (uniform = suspicious) |

### What's NOT in the evidence surface for v0

- External PDS health probes (maybe v1, rate-budgeted)
- DID doc checks (maybe v1, cached)
- ATProto firehose health from other consumers (no access)
- Anything requiring network calls to third parties beyond one bounded lookup

---

## Cadence sketch design

This is the hard part. Start stupid.

**v0 approach:** For each labeler, maintain a rolling window of the last N inter-event intervals (N = 100 or last 30 days, whichever is smaller). Compute:

- **mean interval** — expected gap between events
- **variance** — how regular the labeler is
- **bimodality indicator** — are there two distinct clusters of intervals? (crude: ratio of intervals above vs below the mean)
- **max observed gap** — longest silence that was followed by a resumption

A silence is **expected** if the current gap is within, say, 3x the max observed gap. This threshold is deliberately loose for v0. Tighten after real data.

**Behavioral classes** (derived from cadence sketch):

- **Metronomic:** low variance, regular interval. Silence is surprising early.
- **Bursty:** high variance, bimodal intervals. Silence is expected between bursts.
- **Sparse:** high mean interval, low-moderate variance. Rarely fires but when it does, it's steady.
- **Dormant:** hasn't fired in a very long time but DID is still active. Silence is the default state.

These classes determine the baseline against which silence is measured. A metronomic labeler going quiet for 2 hours is a different signal than a bursty labeler going quiet for 2 hours.

---

## Allowed probes (v0)

### Free (no budget)

- Read Labelwatch's own SQLite state
- Compare labelers against each other
- Inspect firehose cursor history
- Check local process metrics

### Budgeted (max 1 per adjudication cycle per labeler)

- One external metadata lookup (PDS endpoint liveness check, HEAD request only)

### Not allowed in v0

- Any write to Labelwatch state
- Any probe that could affect labeler behavior
- Any probe requiring authentication
- Sustained polling of external endpoints

Governor wraps all probe execution. Every probe emits a receipt.

---

## Adjudication output schema

```json
{
  "labeler_did": "string",
  "timestamp": "ISO8601",
  "silence_duration": "seconds",

  "primary_regime": "one of taxonomy above",
  "confidence": "low | medium | high",
  "secondary_regimes": ["regime", "..."],

  "evidence": [
    { "signal": "string", "value": "any", "interpretation": "string" }
  ],

  "suggested_next_probe": {
    "type": "string",
    "rationale": "string",
    "budget_class": "free | budgeted"
  },

  "receipt_id": "string",
  "governor_policy_version": "string"
}
```

**Confidence criteria:**

- **High:** Evidence clearly disambiguates. E.g., five co-hosted labelers all went silent simultaneously → upstream issue.
- **Medium:** Most likely regime is identifiable but alternatives aren't ruled out. E.g., silence exceeds 3x max historical gap and cadence is metronomic, but no peer data available → probable death, but could be upstream.
- **Low / Unresolved:** Multiple regimes are equally plausible. Output should focus on what evidence would disambiguate.

---

## Ground truth: what counts as "right" later

This is the part that makes the receipt trail into a learning signal. For each adjudication, define what future evidence would confirm or refute it.

| Regime | Confirming evidence | Refuting evidence |
|--------|-------------------|-------------------|
| Normal silence | Labeler resumes within historical cadence bounds | Labeler never resumes, or resumes with different behavior |
| Burst gap | Labeler resumes with burst-like volume | Labeler resumes at steady low rate, or doesn't resume |
| Upstream PDS issue | PDS recovers, multiple co-hosted labelers resume together | Only this labeler was affected |
| Labeler death | DID goes stale, PDS 404s persistently, no recovery | Labeler resumes |
| Behavioral drift | Labeler resumes but with different schema/targeting/volume profile | Labeler resumes with same profile |
| Firehose issue | Multiple unrelated labelers resume simultaneously, cursor behavior normalizes | Only correlated labelers were affected (→ upstream, not firehose) |
| Local ingest issue | Fix to ingest pipeline restores event flow, backfill reveals missed events | Events were genuinely not emitted |

**Retrospective process:** Periodically (weekly? after incidents?) review past adjudications against what actually happened. Update cadence sketches. Adjust confidence thresholds. Add new regimes if reality produced one the taxonomy missed. This is manual for v0. Automating it is a v2 problem.

---

## Architecture (v0, deliberately minimal)

```
┌─────────────────────────────┐
│     Labelwatch (existing)    │
│  SQLite DB + ingest process  │
└──────────┬──────────────────┘
           │ reads (local, read-only)
           ▼
┌─────────────────────────────┐
│   Silence Adjudicator v0     │
│  Python process + own SQLite │
│  ┌───────────────────────┐  │
│  │  Governor constraint   │  │
│  │  envelope              │  │
│  └───────────────────────┘  │
│                              │
│  - cadence sketch builder    │
│  - regime classifier         │
│  - evidence gatherer         │
│  - receipt emitter           │
└──────────┬──────────────────┘
           │ outputs
           ▼
┌─────────────────────────────┐
│  Adjudication log (SQLite)   │
│  - regime classifications    │
│  - evidence snapshots        │
│  - confidence scores         │
│  - suggested probes          │
│  - governor receipts         │
│  - ground truth (filled later)│
└─────────────────────────────┘
```

No UI for v0. Adjudications go to a log. You read the log. If you find yourself wanting a dashboard, that's a signal that it's working, not a signal to build one yet.

---

## What success looks like

Not "it feels smart."

**Minimum viable success:**

1. It correctly classifies a silence you would have manually investigated, and narrows the cause faster than you would have by eyeballing timestamps.

2. It preserves ambiguity honestly when evidence is insufficient, and the suggested next probe is actually the thing you'd check.

3. It catches a local-ingest lie — a case where Labelwatch's own state says things are fine but the adjudicator notices the joint distribution shifted.

**Failure modes to watch for:**

- It classifies everything as "normal silence" because the cadence sketches are too loose → tighten thresholds, but carefully.
- It classifies too much as suspicious because sparse/dormant labelers trigger false alarms → behavioral classes need tuning.
- It never reaches high confidence because the evidence surface is too thin → you need a signal you didn't include.
- It's right but useless because you already knew → the target failure class is too obvious; look for subtler regimes.

---

## What this is NOT

- Not a product. Not a startup. Not a platform.
- Not general-purpose. This is for Labelwatch's specific failure geometry.
- Not autonomous. It diagnoses. It does not act. Action is a v-later problem.
- Not a paper yet. It becomes a paper if it catches something real and the write-up is honest about what worked and what didn't.
- Not the whole governor story. Governor constrains this; this doesn't define governor.

---

## Sequence

1. **This note** ← you are here
2. Build cadence sketch infrastructure against historical Labelwatch data
3. Implement regime classifier (rule-based for v0, not ML)
4. Wire governor constraints and receipt emission
5. Run against live Labelwatch, diagnostic only
6. Watch it be wrong. Fix the ontology. Repeat.
7. Wait for it to catch something real.
8. Then decide if it's worth writing up.
