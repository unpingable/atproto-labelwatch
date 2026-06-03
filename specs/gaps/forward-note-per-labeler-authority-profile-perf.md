# Forward note: per-labeler authority profile report-gen cost

**Status:** forward note, not a gap-spec. Filed 2026-06-03.
**Sibling:** [`gap-spec-report-generation-workload-isolation.md`](gap-spec-report-generation-workload-isolation.md) — the structural answer if this becomes more than a forward note.

## The observation

The per-labeler authority profile slice landed 2026-06-03 (commit `d1bbc53`). The first post-deploy report cycle grew from ~4:26 to ~13:39.

The added cost is shape-clear: `build_authority_effect_inventory(conn, start_7d, now_ts, labeler_did=did)` runs two indexed range queries per labeler (one `GROUP BY (val, labeler_did)`, one `COUNT(DISTINCT target_did) GROUP BY val`), and there are ~493 labelers. With ~37.7M rows in `label_events` and an indexed lookup that is still doing `COUNT(DISTINCT)` work per group, ~1000 such queries per cycle adds up.

`EXPLAIN QUERY PLAN` confirms the queries use `idx_label_events_labeler_ts (labeler_did=? AND ts>? AND ts<?)`. The cost is not a missing index; it is the cumulative cost of N indexed lookups.

## Why this is a forward note, not a gap-spec

Hourly report cycle. ~14 min report-gen time. Disk-bound; main thread is also active on derive passes. Under the pressure-aware gate from the report-gen-workload-isolation gap-spec, this is comfortably within tolerance — WAL stays well below the 80MB skip threshold during the cycle.

It is, however, the kind of "fine" that quietly becomes 30 min, then 45 min, as label volume grows and labeler count grows together. Worth naming as a known cost so the next person reading top-of-mind perf does not have to rediscover it.

## Shape of a fix, if one becomes needed

Two collapse-into-one-query candidates, in increasing surgery cost:

1. **One pass, Python-side bucketing.** Replace per-labeler `build_authority_effect_inventory(..., labeler_did=did)` calls with a single `GROUP BY (val, labeler_did)` over the full window, then bucket per labeler in Python. Same shape of output; one big query instead of N small ones. Removes the per-labeler `COUNT(DISTINCT target_did)` cost too, since the global query can compute per-(val, labeler_did) target counts in one pass.

2. **Materialize a derived rollup.** A `derived_labeler_authority_day` table (parallel to the existing `derived_author_day` / `derived_author_labeler_day` rollups) that pre-aggregates (val, labeler_did, day) tuples with target counts. Report-gen then reads from the rollup, not from `label_events`. More complex; aligns with the existing rollup pattern; would benefit other lenses too.

Candidate (1) is the obvious first cut and has no schema cost. Candidate (2) is structural and probably only worth doing once a second consumer wants the same shape.

## Trigger

Promote this to a gap-spec — or just do candidate (1) inline — when any of the following hold:

- Report-gen consistently crosses 30 minutes on the hourly cadence.
- Pressure-aware gate begins skipping cycles in steady state (not just during incident windows).
- A second consumer of (val, labeler_did) aggregates emerges and would also benefit from rollup.

Until then: known cost, named handle, no action.
