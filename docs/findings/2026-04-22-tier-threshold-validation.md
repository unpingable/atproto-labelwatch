# Publication tier threshold validation — 2026-04-22 findings

**Protocol**: `docs/ADMISSIBILITY-PROTOCOL.md` style, single-pass calibration.
**Snapshot**: `out/tier-validation/2026-04-22_assess.json` (assess --json, live VM).
**DB**: `/var/lib/labelwatch/labelwatch.db` → `/mnt/zonestorage/labelwatch/labelwatch.db`, 26GB.
**Window**: last 7d, family_version=v3.

## Verdict

**No threshold changes recommended this pass. Binding constraint is upstream.**

The tier thresholds in `publication.py` can't be meaningfully calibrated from
the current live data, because the moderation-domain gate upstream eliminates
~95%+ of candidate pairs before thresholds ever fire. `assess --json`
returned a **single finding** against a 7d window that contains 103 pair
candidates and 49,924 contradiction edges.

## What the snapshot showed

`labelwatch assess --json` returned 1 finding:

| field | value |
|---|---|
| pair | `moderation.blacksky.app` vs `skywatch.blue` |
| type | claim_vs_action |
| n_targets | 3 |
| median_jsd | 1.00 |
| top_share | 100% / 100% |
| n_windows | 4 |
| tier | internal (blocked by `n_targets < 15`) |

Every non-target dimension was maxed. The only reason this landed in
`internal` rather than `ready` was target count.

## Pair-level distribution (last 7d, contradiction edges)

| distinct targets | pairs | fate in assess |
|---|---:|---|
| 1 | 40 | dropped (< 2 distinct targets filter) |
| 2–4 | 21 | internal (below reviewable floor of 15) |
| 5–14 | 21 | internal |
| 15–24 | 4 | **would be reviewable** by thresholds |
| ≥25 | 17 | **would be ready** by thresholds |

So ~21 pairs cleared the target-count threshold. 20 of them never
reached the tier logic because the `classify_domain != "moderation"`
filter in `cmd_assess` discarded them.

## Top families driving the filtered-out pairs

From the 21 pairs with ≥15 distinct targets:

| family | edges in big pairs | domain (DOMAIN_MAP) |
|---|---:|---|
| internal-independent | 3,830 | fallback/unknown |
| trumpface | 3,192 | political |
| monthly-posts-over-twenty-per-day | 3,036 | metadata |
| no-gap-more-than-four-hours | 2,742 | metadata |
| made-over-thirty-posts-yesterday | 2,696 | metadata |
| made-over-fifty-posts-yesterday | 2,346 | metadata |
| atlas-user | 2,077 | novelty (heuristic) |
| crushed-piano | 1,977 | unknown (skywatch idiolect) |
| inauthenticity | 1,135 | **moderation** ✓ |
| monthly-posts-over-fifteen-per-day | 877 | metadata |

Moderation families in this set: `inauthenticity`, `harassment`,
`mod-takedown`. Everything else is metadata, novelty, political, or
unmapped. The moderation filter is doing epistemic work
(taxonomy-shear suppression — the 2026-03-13 Phase 2 lesson), but on
the current live corpus it leaves essentially nothing to assess.

## Applying the rubric (chatty's frame)

Only one finding surfaced, so the rubric was applied mostly to the
*filtered-out* pairs to ask whether any of them *should* have made it.
Sampled pairs:

- **blacksky × skywatch / claim_vs_action, 3 targets, JSD=1.00** — the
  one that surfaced. Story clarity: clear (two moderation labelers with
  near-zero overlap). Signal distinctness: high. Stability: 4 windows.
  Target breadth: **thin**. Editorial dignity: marginal — "they
  disagree on 3 accounts" is not a headline. **Correct call to keep
  internal**; the target-count floor worked.
- **fqfzpua2… × oubsy… / crushed-piano vs monthly-posts-over-twenty,
  1,524 targets** — filtered out by moderation gate. This pair is
  skywatch vs hailey. It's not a moderation fight — one is a
  behavioral stats labeler, the other is… whatever skywatch is. The
  moderation gate is correctly dropping it from *moderation*
  disagreement findings. But the underlying disagreement (two labelers
  describing the same 1,524 accounts with totally different framings)
  is interesting signal that `assess` currently has no seat for.
- **adzprud… × r5ju6… / trumpface vs uspol, 699 targets** — taxonomy
  shear on political topic labels. Correctly filtered. Not a moderation
  story.

No sampled pair felt misbinned *given the current gate semantics*.

## Diagnosis

Two separate questions got tangled under "validate publication tier
thresholds":

1. **Are the tier thresholds right?** — unanswerable from this data. The
   moderation gate upstream leaves a sample size of effectively zero.
   The one finding that surfaces lands correctly (internal due to low
   targets).
2. **Is the moderation gate the right gate?** — it's doing what it was
   designed to do (kill 2026-03 Phase 2 synonym-shear noise), but it's
   also excluding a meaningful tier of observable-layer disagreement
   (stats-vs-moderation, descriptive-vs-descriptive) that might deserve
   its own surfacing track rather than being invisible.

## Recommendation

**No threshold changes.** The thresholds are currently unvalidatable by
live data and changing them would be noise.

**Follow-up question to decide later, not this pass**: should `assess`
grow a second track for non-moderation disagreement, scoped and
rendered differently? E.g.:

- `moderation disagreements` (current behavior, current thresholds)
- `observational disagreements` (stats vs moderation, stats vs stats,
  topic vs topic) — different copy, different thresholds, explicitly
  framed as "two labelers see this account differently," not
  "moderators disagree."

This is a design question, not a threshold-tuning question. Defer until
there's a concrete reason to surface observational disagreements — for
instance, if the upcoming dashboard pass wants a second panel.

## Artifacts

- `out/tier-validation/2026-04-22_assess.json` — assess --json snapshot (1 finding, scratch/gitignored)
- `docs/findings/2026-04-22-tier-threshold-validation.md` — this file (durable)

## Anti-patterns checked

- Not tuning thresholds based on a sample-of-one. (Would be
  post-hoc-ish even if pre-registered, because n=1.)
- Not promoting the one surfaced pair to `reviewable` by dropping
  target floor. 3 targets is genuinely thin.
- Not broadening the moderation gate without a pre-registered reason.
  The gate exists because of a specific prior incident; widening it
  needs its own pass with its own criteria.
