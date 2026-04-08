# Hosting Skew: First-Pass Findings

**Date**: 2026-04-08
**Coverage**: 13.8% (43,683 labeled targets resolved out of 317,483 overall)
**Window**: 7 days
**Status**: Provisional, directional. Not publication-grade.

## Summary

The label surface over-represents Bluesky-hosted accounts and under-represents
most alternative PDS operators. Four host families show anomalous skew worth
investigation. Two are likely real signals. Two need more observation.

## Baseline

| Host Family | Overall % | Labeled % | Delta | Accounts |
|---|---|---|---|---|
| host.bsky.network | 94.2% | 97.0% | +2.8% | 299,169 |
| sprk.so | 1.6% | 0.0% | -1.6% | 5,011 |
| blacksky.app | 1.6% | 0.4% | -1.2% | 5,022 |
| brid.gy (Bridgy Fed) | 0.5% | 1.4% | +0.9% | 1,584 |
| skystack.xyz | 0.09% | 0.6% | +0.5% | 276 |

## Anomaly Drilldowns

### 1. skystack.xyz — CONCENTRATED, likely real

- 276 accounts, 263 labeled in 7d (95.3% of the population)
- **One labeler dominates**: `labeler.antisubstack.fyi` → 259/263 targets (83.8%)
- `pef-moderation.org` labels 47.
- This is not a governance signal. It's one labeler carpet-bombing a small PDS.
  The host name ("skystack") suggests a Substack-related bridge or mirror.
  `antisubstack.fyi` labeling 95% of a Substack-adjacent PDS is consistent with
  its declared purpose, not anomalous behavior.
- **Verdict**: Artifact of purpose-built labeler + special-purpose PDS. Not a
  blind spot or skew problem.

### 2. brid.gy (Bridgy Fed) — DISTRIBUTED, likely real

- 1,584 accounts, 624 labeled in 7d (39.4% of population)
- **Distributed across 12 labelers**. Top labeler share: 31.7%.
  - pef-moderation.org: 282 targets (31.7%)
  - alt-text-labeler.bsky.social: 196 (22.0%)
  - labeler.antisubstack.fyi: 177 (19.9%)
  - skywatch.blue: 64
  - xblock.aendra.dev: 59
  - moderation.bsky.app (official): 54
- Bridged accounts genuinely draw more labeler attention per capita than native
  accounts. This is broad-based, not one labeler's crusade.
- **Verdict**: Likely real. Bridged content has different characteristics
  (cross-protocol, different norms, often no alt text). Multiple independent
  labelers flag it more often. Worth tracking over time.

### 3. blacksky.app — NOT CONCENTRATED, likely real

- 5,022 accounts, 169 labeled in 7d (3.4%), 933 in 30d (18.6%)
- **Distributed across 13 labelers**. Top labeler share: 29.0%.
  - xblock.aendra.dev: 101 (29.0%)
  - uspol-labeler.bsky.social: 56 (16.1%)
  - moderation.bsky.app: 45 (12.9%)
  - Notably, `moderation.blacksky.app` (their own labeler) labeled only 3 targets.
- Under-labeled relative to population share (1.6% of accounts, 0.4% of labels).
- The 7d→30d drop (169 vs 933) suggests recent decline, possibly correlated with
  skywatch.blue going degraded (skywatch labeled 23 blacksky targets in 7d).
- **Verdict**: Likely real under-representation. Distributed across many labelers,
  so it's not one labeler ignoring them — it's structural. Blacksky accounts may
  generate less content that triggers labeling rules, or labelers may have less
  coverage of that population. Needs longer observation.

### 4. sprk.so — CONCENTRATED, possibly artifact

- 5,011 accounts, **0 labeled in 7d**, 463 in 30d.
- 30d labeling was **98.9% skywatch.blue** (463/463 meaningful targets).
- skywatch.blue is degrading (down 66% from 30d baseline). When skywatch stopped
  labeling, sprk.so went dark in the label surface.
- **Verdict**: Not a governance blind spot per se. sprk.so was visible to exactly
  one labeler, and that labeler is failing. This is a **single-labeler dependency**
  that manifests as a blind spot. If skywatch recovers, sprk.so reappears. If not,
  5k accounts remain unlabeled.

## What's probably real

1. **Bridged content draws disproportionate labeler attention** (distributed signal)
2. **Blacksky is under-labeled** relative to its population share (structural)
3. **Single-labeler dependencies create blind spots** when labelers degrade (sprk.so)

## What might be artifact

1. **skystack.xyz skew** is a purpose-built labeler doing its job on a purpose-built PDS
2. **sprk.so** may reappear if skywatch.blue recovers
3. Coverage is still 13.8% — all percentages could shift as the resolver fills in

## What needs longer observation

- Whether blacksky under-representation is stable or fluctuates with labeler activity
- Whether brid.gy over-representation tracks Bridgy Fed growth or labeler behavior
- Rerun this comparison weekly; snapshot diffing now available via `--snapshot-dir`

## Publication assessment (boundary fights)

Ran `labelwatch assess` on 7d window. 159,688 contradiction edges filtered to
**1 ready finding**:

**moderation.blacksky.app vs skywatch.blue** — claim vs action disagreement.
29 shared targets, JSD=1.0, 8 windows. Blacksky calls "mod-takedown",
skywatch calls "inauthenticity" on the same accounts.

**Decision: HOLD for one more window.**
- skywatch.blue is degrading (~66% volume drop). Posting now risks freezing a
  transient state into a governance claim.
- Better as part of a bundle: blacksky structurally under-labeled at population
  level + contradiction with a degrading labeler = sharper frame than "two
  labelers disagree."
- Recheck week of 2026-04-15. If skywatch is still emitting and the
  contradiction persists, post with broader context.

Note: the tier system only covers boundary fights. Hosting-locus findings
(population-level skew, single-labeler dependencies) are a different kind
of finding with no publication pathway yet.

## How to reproduce

```bash
# Overall comparison
labelwatch hosting-locus --compare --days 7

# Per-family drilldown
labelwatch hosting-locus --drilldown brid.gy --days 7
labelwatch hosting-locus --drilldown skystack.xyz --days 7

# Save snapshot for later diffing
labelwatch hosting-locus --compare --days 7 --snapshot-dir data/snapshots/
```
