# Temporal Ratio Analysis — Gap Spec

**Origin:** Paper 22 ("No Universal Plant Clock: Temporal Failure Geometry
in Distributed Control Systems"). Four-layer temporal failure model +
timescale ratio analysis applied to labelwatch.

**Core thesis:** There is no universal observer clock. Each labeler evolves
at its own rate. Fixed temporal thresholds become context-blind when plant
dynamics are heterogeneous.

**The question this answers:**
> Which labelers are we currently observing too slowly relative to their
> own rate of change?

## The gap

Labelwatch already has extensive temporal sensing:
- Regime detection (10 states, hysteresis)
- Signal health (gone_dark/degrading/surging via 7d/30d EPS ratio)
- Temporal coherence band (0-100 score)
- Burstiness index, cadence irregularity
- Observation staleness cards, stream liveness, read health

**What's missing: ratio normalization.**

All freshness judgments are absolute. All staleness thresholds are
one-size-fits-all. A labeler emitting 50 labels/minute and one emitting
10 labels/week get judged with the same ruler.

Without `T_o / T_p`, those look artificially similar. They're not.

## Definitions

- **T_p** — plant timescale. How fast the labeler's behavior is changing.
  Estimated from recent emission cadence (interarrival times).
- **T_o** — observation latency. Time since last successful ingest for
  this labeler.
- **T_s** — synchronization uncertainty. Timestamp reliability (clock skew,
  missing timestamps).
- **T_c** — clock divergence horizon. How long before cadence drift
  invalidates threshold contracts.

## The ratios

- **T_o / T_p** — observation freshness ratio.
  If >> 1: "we're watching this labeler too slowly."
  If ~1: borderline. If << 1: comfortable.
- **T_s / T_p** — timestamp quality relative to emission rate.
  If >> 1: timestamps are meaningless for ordering events.
- **T_c / contract** — cadence drift vs threshold validity.
  If >> 1: derived state (regime, coherence) is based on stale contracts.

## Implementation plan

### Phase 1: Per-labeler T_p estimation

Add to `scan.py` signal computation:
- `tp_mean_secs` — mean interarrival time (last 7d)
- `tp_median_secs` — median interarrival (robust to bursts)
- `tp_p10_secs` — 10th percentile (fastest sustained cadence)

Store on `labelers` table: `tp_mean`, `tp_median`.

Use **median** as primary T_p estimate (mean lies when bursts are present).

### Phase 2: Observation freshness ratio

Compute per labeler per derive pass:
```
T_o = now - last_successful_ingest_for_this_labeler
observation_ratio = T_o / T_p
```

Bucket into coarse bands:
- `healthy` — ratio < 0.5 (observing much faster than they change)
- `adequate` — ratio 0.5–2.0
- `lagging` — ratio 2.0–10.0 (missing events between observations)
- `blind` — ratio > 10.0 (keyhole observation, party already over)
- `indeterminate` — insufficient data for T_p estimate

Store: `observation_ratio` (float), `observation_ratio_band` (text).

Surface in report: new card or column showing which labelers we're
observing too slowly.

### Phase 3: Temporal dominant failure classification

For each labeler, diagnose which temporal layer is the primary concern:

- `stale_observation` — T_o/T_p is the dominant problem
- `clock_unreliable` — timestamps missing/skewed, T_s/T_p high
- `cadence_drift` — emission rate changing faster than our thresholds adapt
- `probe_instability` — endpoint flapping, can't maintain observation channel
- `none` — temporal health is adequate

Store as `temporal_dominant_failure` on labelers table.

### Phase 4: Measurement provenance

On derived signals (regime, coherence, signal health):
- `computed_at` — when the derivation ran
- `source_data_as_of` — max(ts) of the data it used
- `measurement_age = computed_at - source_data_as_of`

Prevents the dashboard from looking current while being epistemically old.

## What this does NOT do

- Does not change ingest intervals per labeler (that's adaptive control,
  not observation quality measurement)
- Does not make the system "relativistic" — it makes temporal assumptions
  explicit and auditable
- Does not require new data sources — everything computes from existing
  label_events and ingest_outcomes tables

## Schema changes (estimated)

Add to `labelers` table:
- `tp_mean REAL` — mean interarrival seconds
- `tp_median REAL` — median interarrival seconds
- `observation_ratio REAL` — T_o / T_p
- `observation_ratio_band TEXT` — healthy/adequate/lagging/blind/indeterminate
- `temporal_dominant_failure TEXT` — which layer is the primary concern

Schema version: v21.
