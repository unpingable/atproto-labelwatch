# Frontdoor SQL-side aggregation — gap spec, 2026-06-10

> **Status: named gap.** Filed in response to the
> `labelwatch.load_probe.v1` verdict `refused_unbounded` from
> 2026-06-10T07:18:56Z. The frontdoor v0 is gated via the
> `subject_too_dense` circuit breaker; this spec describes the proper fix.

## What the probe found

Live audit on 2026-06-10 against the top-100 labeled subjects on the
production DB:

```
verdict:  refused_unbounded
rationale: p99 wall time 24309.7ms exceeds 5000ms threshold
percentiles (wall_ms):  p50 5524 · p90 12843 · p99 24310 · max 37568
slowest subject:  did:plc:o6ggjvnj4ze3mnrpnv5oravg
  100,624 events  ·  12 labelers  ·  37.6 s wall time
```

Receipt:
`docs/analysis/receipts/labelwatch.load_probe.whatsonme.frontdoor.v0.20260610T071856Z.json`

## Root cause

The shape audit (`labelwatch.index_audit.v1`) verdicts whether the
planner picks the right index. It does — every query in the inventory
SEARCHes via `idx_label_events_target_did_ts`. The audit's synthetic
zero-match probe DID returns 0 rows, so per-query SQLite time is
sub-millisecond.

But Q8 fetches **all events for the subject** as per-row tuples for two
purposes:
1. Temporal coherence (did the labeler flip its claim about this subject?)
2. Attachment-locus aggregation (account vs post vs record breakdown).

For a subject with 100k events, that's 100k rows pulled back to Python,
walked twice in `_build_labeler_card` (once for locus, once for
classification flips). The SQL is fast; the Python is O(events).

## Surgical fix (in scope for the remediation slice)

Push the aggregation into SQL. Replace Q8's per-row fetch with two
small GROUP BY queries:

### Q8a — temporal coherence (cardinality-bounded)

```sql
SELECT labeler_did, COUNT(DISTINCT val || '|' || neg) AS distinct_states
FROM label_events
WHERE target_did = ?
GROUP BY labeler_did
```

Result set: one row per labeler. `classification_changed = (distinct_states > 1)`.

### Q8b — locus aggregation (bucketed in SQL)

```sql
SELECT
  labeler_did,
  SUM(CASE WHEN uri LIKE 'did:%'                                 THEN 1 ELSE 0 END) AS locus_account,
  SUM(CASE WHEN uri LIKE 'at://%/app.bsky.feed.post/%'           THEN 1 ELSE 0 END) AS locus_post,
  SUM(CASE WHEN uri LIKE 'at://%/app.bsky.actor.profile/%'       THEN 1 ELSE 0 END) AS locus_profile,
  SUM(CASE WHEN uri LIKE 'at://%/app.bsky.graph.list/%'          THEN 1 ELSE 0 END) AS locus_list,
  SUM(CASE WHEN uri LIKE 'at://%/app.bsky.graph.listitem/%'      THEN 1 ELSE 0 END) AS locus_list_item,
  SUM(CASE WHEN uri LIKE 'at://%/app.bsky.feed.generator/%'      THEN 1 ELSE 0 END) AS locus_feed_generator,
  SUM(CASE WHEN uri LIKE 'at://%/app.bsky.graph.starterpack/%'   THEN 1 ELSE 0 END) AS locus_starterpack,
  SUM(CASE WHEN uri LIKE 'at://%'                                THEN 1 ELSE 0 END)
    - SUM(CASE WHEN uri LIKE 'at://%/app.bsky.%'                 THEN 1 ELSE 0 END) AS locus_record_other,
  COUNT(*) AS total
FROM label_events
WHERE target_did = ?
GROUP BY labeler_did
```

Result set: one row per labeler. The Python side just renders the buckets;
no per-event walking.

### Q8c — labeled records (top-N per labeler)

```sql
SELECT labeler_did, uri, COUNT(*) AS event_count,
       MIN(ts) AS first_seen, MAX(ts) AS last_seen
FROM label_events
WHERE target_did = ? AND uri != ?  -- exclude account-locus rows
GROUP BY labeler_did, uri
ORDER BY labeler_did, event_count DESC
```

Then the Python side caps to `MAX_LABELED_RECORDS_PER_LABELER` (50) per labeler.
Result set: small — at most ~50 × num_labelers rows, regardless of total events.

## Audit implication

Q8 is currently in `labelwatch.index_audit.v1` as a single query. Replacing
it with Q8a+Q8b+Q8c is a contract change to the audit inventory. The
post-remediation audit should:

- Drop Q8 from the inventory.
- Add Q8a, Q8b, Q8c.
- Re-run `labelwatch index-audit` to verdict the new shape.

Q8a/Q8b should both be SEARCH against `idx_label_events_target_did_ts`
with small GROUP BY work; should be sub-ms even on real subjects.
Q8c is also bounded by subject; SQLite's GROUP BY over the index can
do the per-URI rollup without sorting all rows by hand.

## Verification plan

After the remediation lands:

1. Re-run `labelwatch index-audit` (Q8a+Q8b+Q8c verdicts).
2. Re-run `labelwatch load-probe --sample-size 100` against the same
   top-100 sample that motivated this work.
3. New receipt should show `admissible_for_publication` (p99 < 500ms).
4. Remove the `subject_too_dense` circuit breaker (or raise the cap to
   "infinity").

## What this gap-spec does NOT do

- Does not commit to the exact SQL formulations above — they're a
  starting point; the remediation slice may pick different cuts.
- Does not redesign the FrontdoorResult/LabelerCard shapes (the result
  surface stays the same; only the way the data lands changes).
- Does not gate the surface live — that's done now via
  `subject_too_dense` (`frontdoor.MAX_EVENTS_FOR_AGGREGATION`,
  default 10,000). Sparse subjects keep working; dense subjects refuse
  honestly.
- Does not extend to load probe sampling improvements (top-100 is a
  worst-case probe; representative-sampling probes are a separate
  follow-up).

## Composes with

- `docs/analysis/subject-lookup-frontdoor-001.md` — the surface contract;
  Q8 lives there as a single query.
- `docs/analysis/subject-lookup-load-probe-001.md` — the probe's
  purpose; this is the first instance of its acceptance criterion firing.
- `docs/analysis/receipts/labelwatch.load_probe.whatsonme.frontdoor.v0.20260610T071856Z.json`
  — the evidence the probe surfaced.
