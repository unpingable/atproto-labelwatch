# Storage runway sizing — Labelwatch, 2026-06-09

> **Phase −1 research spike.** This note characterises labelwatch's storage footprint and growth, identifies retention candidates, and proposes a minimum-runway floor. It does NOT commit to a Phase 0 implementation (cold-path Parquet/DuckDB, index pruning, or any other structural change). The sizing note exists so that the Phase 0 decision is informed, not so that the Phase 0 build is queued.

## TL;DR

- **The volume problem is mostly driftwatch, not labelwatch.** `/mnt/zonestorage` is 196 GB / 171 GB used / 15 GB free / 92%. Driftwatch holds 133 GB; labelwatch holds 38 GB.
- **Labelwatch growth is structural, not a retention tuning issue.** `label_events` is append-only by doctrine. It and its 7 indexes account for **87% of labelwatch's 40.5 GB DB**. All other retention-pruneable tables combined add up to ~5 GB.
- **Current burn:** ~290 MB/day (~9 GB/month, ~110 GB/year extrapolated). A phase shift on or around 2026-05-20 doubled the daily rate from ~150K to ~330K events/day.
- **Index footprint:** 19.9 GB of 7 label_events indexes (60% of the events footprint). One-time savings of 1–3 GB *may* be available via index audit; not pursued in Phase −1.
- **The only recurring multi-GB lever for labelwatch alone is cold-path migration** of historical `label_events` partitions off the working DB. The append-only doctrine constrains the shape: partitions move off, no rows mutate in place.
- **Proposed labelwatch runway floor: 5 GB** of free volume dedicated to labelwatch ops (WAL peak + 14d growth + derive workspace + restart spike). Current 15 GB satisfies this comfortably *in isolation*, but the volume is shared with driftwatch; its retention shape is the binding constraint.

## Volume picture

```
/dev/sdc (zonestorage)   196G total · 171G used · 15G free · 92% used
  ├── driftwatch/         133G   (78% of consumption)
  ├── labelwatch/          38G   (22% of consumption)
  ├── lost+found, etc.     <1G
```

Driftwatch is the dominant consumer by a factor of 3.5×. Any runway story for `/mnt/zonestorage` runs through driftwatch first; labelwatch is the smaller half. The labelwatch-side actions in this note are therefore **necessary but not sufficient** for volume runway. The driftwatch-side picture is tracked separately in driftwatch's project_state and `gap-spec-cold-path-parquet-duckdb.md`.

## Labelwatch DB internals

### File-level

```
labelwatch.db          40.50 GB   (9,907,891 pages × 4096 B)
labelwatch.db-wal      131  MB    (transient post-restart; see WAL section)
labelwatch.db-shm       13  MB
labelwatch_state.db      0  B     (sidecar provisioned but unwired — `1033f7e`)
freelist_count          0          (no internal fragmentation; VACUUM-eligible space ~0)
```

The freelist being zero is notable: there's nothing for a `VACUUM INTO` to reclaim. Every page is either active or in the WAL. Compaction is not a runway lever.

### Per-table page distribution (top 16)

| Object | Size | % of DB | Type |
|---|---:|---:|---|
| `label_events` | 15.54 GB | 38.4% | table |
| `idx_label_events_state` | 5.69 GB | 14.1% | index |
| `idx_label_events_uri_ts` | 4.11 GB | 10.1% | index |
| `sqlite_autoindex_label_events_1` (event_hash UNIQUE) | 3.16 GB | 7.8% | index |
| `idx_label_events_labeler_ts` | 2.86 GB | 7.1% | index |
| `idx_label_events_target_did_ts` | 2.48 GB | 6.1% | index |
| `idx_label_events_ts` | 1.51 GB | 3.7% | index |
| `derived_label_fp` | 326 MB | 0.8% | table |
| `derived_author_labeler_day` (table + autoidx + idx) | 755 MB | 1.9% | derived |
| `alerts` | 254 MB | 0.6% | table |
| `discovery_events` | 216 MB | 0.5% | table |
| `derived_author_day` (table + autoidx) | 329 MB | 0.8% | derived |
| `labeler_evidence` | 131 MB | 0.3% | table |
| `boundary_edges` + `boundary_targets` (+ autoidx + idx) | 226 MB | 0.6% | table |
| `labeler_probe_history` | 19.7 MB | <0.1% | table |
| `ingest_outcomes` | 9.8 MB | <0.1% | table |

**label_events table:** 15.5 GB
**label_events indexes (7 total):** 19.9 GB
**label_events footprint total:** **35.4 GB = 87.4% of DB**

All other tables + their indexes combined: ~5.1 GB.

### Row counts

| Table | Rows | Bytes/row (incl. indexes) |
|---|---:|---:|
| `label_events` | 39,903,383 | ~890 |
| `derived_author_labeler_day` | 2,881,335 | ~270 |
| `derived_author_day` | 2,570,056 | ~135 |
| `derived_label_fp` | 1,610,630 | ~265 |
| `labeler_evidence` | 1,240,372 | ~110 |
| `boundary_edges` | 192,450 | ~600 |
| `boundary_targets` | 190,249 | ~470 |
| `discovery_events` | 146,049 | ~1,500 |
| `labeler_probe_history` | 130,141 | ~230 |
| `alerts` | 81,706 | ~3,260 |
| `ingest_outcomes` | 78,766 | ~125 |
| `labelers` | 501 | — |

The ~890 B/row figure on `label_events` is the structural unit cost for storage planning: a typical row carries `uri`, `target_did`, `val`, `ts`, `event_hash`, `sig`, plus the 7 index entries.

## WAL behaviour

Layered ceilings, all in code:

| Threshold | Where | Behaviour |
|---|---|---|
| 64 MB | `db.py` `PRAGMA journal_size_limit=67108864` | Post-checkpoint truncation target |
| 80 MB | `runner.py` `LABELWATCH_REPORT_WAL_SKIP_MB` | Report defers (skips this cycle) if WAL > 80 MB at trigger time |
| 200 MB | `scan.py` `LABELWATCH_UPDATE_AUTHOR_DAY_WAL_MB` | UAD derive enters pressure-aware chunking |
| 300 MB | `scan.py` `LABELWATCH_UPDATE_AUTHOR_LABELER_DAY_WAL_MB` | UALD derive enters pressure-aware chunking |

Currently 131 MB — sits between "checkpoint target" and "report-defer." This is a post-restart transient (service restarted 2026-06-09T15:56Z for the publication-fix deploy); the next checkpoint cycle reduces WAL toward the 64 MB target, and the report-defer guard means a 131 MB WAL will skip the next report rather than pin the WAL.

**Conclusion:** WAL behaviour is well-bounded by code. WAL is not the runway concern.

## Growth rate

```
span:              2026-02-24 → 2026-06-09 = 105 days
total accumulated: 39,903,383 rows
average:           ~380,000 rows/day
```

Daily counts (last 30 days):

```
2026-06-09: 258,990    2026-05-31: 300,719    2026-05-22: 426,058
2026-06-08: 330,794    2026-05-30: 322,642    2026-05-21: 458,225
2026-06-07: 375,979    2026-05-29: 359,869    2026-05-20: 236,603
2026-06-06: 362,537    2026-05-28: 317,862    2026-05-19: 182,405
2026-06-05: 363,880    2026-05-27: 314,894    2026-05-18: 214,477
2026-06-04: 314,077    2026-05-26: 298,483    2026-05-17: 152,527
2026-06-03: 338,908    2026-05-25: 354,388    2026-05-16: 148,564
2026-06-02: 329,616    2026-05-24: 355,281    2026-05-15: 152,529
2026-06-01: 298,945    2026-05-23: 392,418    2026-05-14: 135,094
                                              2026-05-13: 153,090
                                              2026-05-12: 135,315
                                              2026-05-11: 145,893
                                              2026-05-10: 155,010
```

**Phase shift on or around 2026-05-20** — daily rate doubled from ~150K to ~360K. The cause is not characterized in this note; possible explanations include a new high-volume labeler reaching steady state, a coverage delta from the discovery sidecar, or a Jetstream firehose pattern change. Worth a follow-up but not blocking for sizing.

**Burn rate (current, last 14 days):**

```
~330,000 rows/day × ~890 B/row ≈ 290 MB/day
                                ≈ 9 GB/month
                                ≈ 110 GB/year (linear extrapolation)
```

The annualised figure assumes the post-2026-05-20 rate holds. If the phase shift was a one-time step change (rather than ongoing acceleration), 110 GB/year is the right order. If volume continues to grow non-linearly, the figure is a floor.

## Retention map

**Tables WITH retention:**

| Table | Cutoff | Mechanism |
|---|---|---|
| `ingest_outcomes` | 7 days | `scan.py::_cleanup_ingest_outcomes` |
| `boundary_edges`, `boundary_targets` | windowed | `boundary.py` cutoff prune + same-window replace |
| `derived_val_dist_day` | 60 days | `scan.py` `_prune_*` family |
| `derived_author_day` | 60 days | same |
| `derived_author_labeler_day` | 60 days | same |
| `derived_labeler_lag_7d`, `_reversal_7d` | bulk delete | regenerated each cycle |

**Tables WITHOUT retention:**

| Table | Size | Append-only by doctrine? |
|---|---:|---|
| `label_events` | 15.5 GB (+19.9 GB indexes) | **Yes** — explicit in CLAUDE.md: "rows are never updated or deleted" |
| `labeler_evidence` | 131 MB | Yes — "append-only classification evidence" |
| `alerts` | 254 MB | Yes by convention (receipt-hash audit chain) |
| `discovery_events` | 216 MB | No explicit doctrine; per-revision archive of `app.bsky.labeler.service` records |
| `labeler_probe_history` | 19.7 MB | No explicit doctrine; per-probe records |
| `posted_findings` | <1 MB | Append-only dedup ledger |

Of the no-retention tables, only `label_events` matters at runway scale. The four sub-300 MB tables would buy at most ~700 MB total even with full purge; not worth the doctrine cost.

`discovery_events` and `labeler_probe_history` are doctrinally retainable (no explicit append-only commitment) but trimming them is also negligible at this scale.

## Existing exports and off-volume candidates

- `/var/www/labelwatch` (284 MB) — already on root volume (`/dev/sda`, 109 GB free), not on `/mnt/zonestorage`. Not a runway concern.
- No labelwatch-side cold exports exist.
- Adjacent: driftwatch `facts.sqlite` (3.1 GB) is parked-by-design on `/mnt/zonestorage` pending DuckDB cutover (per driftwatch project_state). Not labelwatch's to act on, but it's 3 GB sitting unused.
- `labelwatch_state.db` (0 bytes) — provisioned for the label_state sidecar (`1033f7e`); when populated, this becomes an OUT-of-DB store on the root volume per the existing pattern, not a runway concern for `/mnt/zonestorage`.

## Index audit candidates (Phase 0 maybe, ~1–3 GB one-time)

Not pursued in Phase −1. The seven label_events indexes total 19.9 GB. Possible redundancies to investigate before Phase 0:

- `idx_label_events_state` is `(labeler_did, uri, val, ts)`. If it can serve queries currently using `idx_label_events_labeler_ts` (`labeler_did, ts`) via skip-scan, the smaller index becomes droppable (~2.9 GB savings). Skip-scan economics depend on cardinality of intermediate columns; needs query-plan inspection.
- `idx_label_events_ts` (1.5 GB) — used for bare-window scans. If most windowed queries also have `labeler_did` or `uri`, this index is at risk of being underused; would need usage profiling to confirm.
- `idx_label_events_hide` (partial WHERE val='!hide') — not in top 30 by size; likely already small.

**Note:** index audit produces a one-time saving, not recurring runway. The 1–3 GB headroom buys ~10 days of growth at current burn — useful but not the long-term lever.

## Cold-path migration (the only multi-GB recurring lever)

Restated for completeness; the operational plan lives in driftwatch as `specs/gaps/gap-spec-cold-path-parquet-duckdb.md`. For labelwatch specifically:

- `label_events` partitioned by month (or week) → Parquet on cold storage.
- Working DB retains a rolling N-month window.
- The append-only doctrine *is preserved*: partitions move off, no rows mutate. The published surface is unchanged.
- DuckDB (or another OLAP) covers historical queries against the Parquet partitions.

Decision NOT to commit in Phase −1 was the operator instruction. Phase 0 is the decision; this note is the input.

## Minimum runway threshold

Working numbers for the labelwatch-only floor (a function of current burn + WAL + workspace):

| Component | Reservation |
|---|---:|
| WAL peak headroom (above derive-pressure ceiling) | 500 MB |
| 14 days of growth at current burn | ~4.0 GB |
| Retention + derive workspace (UAD/UALD chunking, prune scratch) | 500 MB |
| Crash/restart spike envelope + checkpoint catchup | 500 MB |
| **Labelwatch operational floor** | **~5.5 GB** |

Current 15 GB free comfortably satisfies this **for labelwatch in isolation**. The volume is shared with driftwatch; driftwatch's burn rate and runway are the binding constraint and live in its own project_state.

A more conservative joint-floor for the volume (both projects, plus VACUUM INTO headroom for either project) lands around 10–15 GB. At today's 15 GB free, the volume is operating at the edge of that floor.

## Recommendations (Phase −1 outputs; Phase 0 inputs)

1. **Do not yet commit to cold-path migration.** This note characterises the problem; commitment requires the cross-project (driftwatch + labelwatch) joint sizing and Phase 0 acceptance criteria.
2. **Driftwatch sizing is the binding analysis.** Labelwatch's runway story cannot be decided in isolation. Pair this note with a similar Phase −1 against driftwatch's `labeler.sqlite` (120+ GB) before proposing volume-level surgery.
3. **Index audit is a candidate Phase 0 sub-slice.** ~1–3 GB one-time savings at low risk. Requires query-plan profiling to identify redundant indexes. Cheap to investigate; cheap to abandon.
4. **WAL behaviour is well-managed.** Not a runway concern. Do not include WAL tuning in Phase 0 scope unless something changes.
5. **Label_events partition scheme** is the long-term lever. When Phase 0 fires, the partition shape must preserve the append-only doctrine: no row mutation, no in-place deletion. Partitions cold-archive whole, are never re-written. This is the same custody pattern as driftwatch's `claim_history` JSONL archive — and aligns with the observation-export-custody doctrine (this note's filing peer at `docs/observation-export-custody.md`).
6. **Watch the 2026-05-20 phase shift.** A doubling of daily event rate is a measurement event in its own right. If it represents a new high-volume labeler at steady state, the runway projection holds; if it represents ongoing acceleration, every Phase 0 estimate based on 290 MB/day is a floor.

## Open questions for Phase 0

- What's the actual driftwatch runway projection? (Binding constraint.)
- Are any of the seven label_events indexes redundant under query-plan inspection?
- What's the cause of the 2026-05-20 phase shift, and is it stable?
- Does the Parquet/DuckDB Phase −1 (driftwatch-side) generalise cleanly to labelwatch, or does the architecture diverge meaningfully because labelwatch's events have signatures and labeler_evidence side data?
- What's the cost of an index audit in lock-contention terms — can it run during live ingest, or does it need a maintenance window?

## Provenance

Data collected 2026-06-09 from prod `/var/lib/labelwatch/labelwatch.db` via read-only `sqlite3 -readonly` and `dbstat` virtual table. `dbstat` query took several minutes on the 40 GB DB; row counts took similar. Growth distribution computed from `label_events.ts` per-day aggregation, last 30 days. All numbers as-of `2026-06-09T16:30Z`.

Sizing methodology assumes the recent burn rate is representative going forward and that the storage cost per row is stable (~890 B/row including indexes). Phase shift around 2026-05-20 is noted but not characterised; a follow-up "what changed on 2026-05-20" investigation is queued.

This note is research; nothing here is a build commitment.
