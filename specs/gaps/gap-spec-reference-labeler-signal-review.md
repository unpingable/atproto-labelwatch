# Gap spec: reference_labeler_signal_review — corroboration before degraded/gone_dark

**Status:** CANDIDATE handle, **not ratified, not authorized to build.** Filed
2026-06-29 as a review handle while the shape is fresh (name-early / ratify-lazily).
No forcing *case* to implement yet; build only on explicit greenlight.

**Provenance:** the 2026-06-29 observatory-family hardening pass. Composes with the
existing skywatch.blue / hailey.at degradation watch-items and the
authority-failure-modes "Reference capture" doctrine — those track *that* a reference
labeler is going quiet; this names the evidence bar for *calling* it real.

## Problem

When a labeler is about to be marked `degraded` or `gone_dark`, a single signal (e.g.
"0 events in 7d") can be a real labeler change **or** an ingest artifact / platform
window. Labelwatch already avoids a single collapsed trust score; the gap is a
**named cross-check** before a degradation verdict, so a platform-side ingest dip does
not get laundered into a claim about the labeler.

## Shape (non-binding sketch)

Before emitting a degradation verdict, corroborate across independent signals:

```
- queryLabels response behavior
- discovery registry state
- recent label-event volume
- upstream endpoint reachability
- labeler service record presence
- network/platform health during the window
- whether OTHER labelers show simultaneous ingest depression
```

Output is a classification, **never a sheriff verdict**:

```
likely_real_labeler_change
possible_ingest_artifact
platform_window_suspect
insufficient_evidence
```

Explicitly NOT `bad labeler` / `dead labeler` / `trust score changed`.

## Non-goals

- No trust score, no enforcement, no emit (detect-only is structural).
- "Weather, never verdict" still governs: this classifies *evidence sufficiency*, not
  the labeler's character.

## Why candidate, not build

The watch-items (skywatch.blue degrading, hailey.at retired) are the firing evidence,
but the retirement ledger is currently handled by hand and reads fine. Promote when a
degradation call is contested, or when reference-set churn outpaces manual review.
