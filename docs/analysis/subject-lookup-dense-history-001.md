# Frontdoor dense-subject history — gap spec, 2026-06-11

> **Status: named gap.** Successor to
> `subject-lookup-sql-aggregation-001.md` after its load probe
> verdicted `refused_unbounded` despite shape-admissible SQL queries.
> This spec restates the problem without the falsified premise
> (Python aggregation was the bottleneck) and surfaces the candidate
> strategies that survive the receipts.

## Statement

Dense Labelwatch subjects cannot be served from raw `label_events`
history within the frontdoor latency envelope. The system currently
refuses dense subjects via `subject_too_dense` (cap = 2000 events);
this refusal is correct but blocks the highest-interest lookups
(main characters, popular accounts, anything actually worth looking
up). SQL-side aggregation improved query shape but did not bound the
scan. A different storage / serving strategy is required.

## Evidence the previous gap closed against

`subject-lookup-sql-aggregation-001` predicted that SQL-side
aggregation (replacing Python event walks) would close the gap. It
did not.

Post-implementation receipts:

- Shape audit
  (`labelwatch.index_audit.whatsonme.frontdoor.v0.20260611T153621Z.json`):
  overall_verdict `admissible`, Q8a/Q8b/Q8c all sub-millisecond on the
  synthetic zero-row probe against the 40.5M-row production DB.
- Load probe
  (`labelwatch.load_probe.whatsonme.frontdoor.v0.20260611T154213Z.json`):
  verdict `refused_unbounded`, p99 wall = 28315 ms, p50 = 5223 ms,
  max = 45290 ms. The pre-implementation probe (2026-06-10T07:18:56Z,
  old Q8) reported p99 = 24309 ms. Same ballpark.

Diagnosis: per-row scan / materialization cost over dense histories
dominates, regardless of aggregation shape. The index walks 100k+
entries for forcing subjects; SQL aggregation pays that walk as much
as Python walks did, plus Q8c's CTE materializations.

## Candidate strategies

### 1. Pre-aggregated per-subject summary table (recommended)

Maintain a `subject_summary` table populated on background passes.
Frontdoor reads the summary; raw `label_events` stay available as
backing evidence for drill-downs.

The frontdoor surface needs roughly:

- per-labeler event count, last_seen, first_seen
- per-labeler distinct (val, neg) tuples (drives `classification_changed`)
- per-labeler locus bucket counts
- per-labeler top-N labeled-record URIs (with val breakdown)
- per-labeler classification flip evidence

All of these are computable as scheduled rollups on the existing
write path (similar shape to `derived_author_day` and
`derived_author_labeler_day`), keyed on `target_did`.

**Pros.**
- Frontdoor reads are bounded by summary cardinality, not event
  history. Hot lookups become trivially fast.
- Same custody pattern as `label_events` (append-only history with
  derived rollups) — no new doctrine.
- Backfill is finite work, then incremental on the writer thread.

**Cons.**
- Schema migration; new background pass; staleness vs freshness
  trade-offs.
- Summary table grows with subject count, not event volume — but it
  is unbounded over time. Needs its own retention discipline.
- Raw-event drill-down (`labeled_records` per URI) needs a separate
  bounded path; this can be a secondary query gated on a "show more"
  affordance.

### 2. Historical-tail cold path via Parquet/DuckDB

Older event partitions move to Parquet, queried via DuckDB. Hot
window (last 30/90 days?) stays in SQLite for ingest.

**Pros.**
- Composes with the existing cold-path doctrine
  (`docs/analysis/storage-runway-sizing-001.md`,
  `gap-spec-cold-path-parquet-duckdb.md` in driftwatch).
- Reduces SQLite working set, which helps everyone.

**Cons.**
- Frontdoor request path becomes multi-source; latency budget gets
  worse, not better, unless the result is cached.
- Doesn't solve dense-subject lookups on its own — a 100k-event
  subject is dense whether the events are in SQLite or Parquet.
- Better as an enabling layer for #1 than a standalone fix.

### 3. New covering / index shape

A covering index over `(target_did, labeler_did, val, neg, uri)` would
let the planner answer Q8a/Q8b/Q8c without heap reads. Might cut wall
time meaningfully.

**Pros.**
- Cheap to test.
- No schema migration; no background pass.

**Cons.**
- Worth investigating, but distrust until proven against the 101k×12
  specimen. Index wizardry often just moves the dragon into a smaller
  cave.
- `label_events` indexes already total 19.9 GB / 49% of DB. Adding
  another covering index has real storage cost (see
  `docs/analysis/storage-runway-sizing-001.md`).

## Bias

**Pre-aggregated subject summary table** is the real frontdoor fix.

The frontdoor page should not be deriving civilization from sediment
every time someone asks "what's on this account?" It should read a
maintained testimony summary, with raw events available as backing
evidence.

## Forcing case

Already met. The load probe verdicted `refused_unbounded` on real
production subjects. The dense-subject refusal is blocking the
highest-interest lookups — exactly the witnesses that would test the
publication discipline of the frontdoor surface.

## Acceptance for the eventual remediation slice

- New load probe (`labelwatch.load_probe.v1`) against top-100 labeled
  subjects verdicts `admissible_for_publication` (p99 < 500 ms).
- `subject_too_dense` cap removed (or raised to a defensive maximum
  much higher than the heaviest observed subject).
- Backing evidence (raw events) still reachable via a bounded drill-
  down path; the summary surface is not the only place the data
  lives.

## What this gap-spec does NOT do

- Does not commit to one of the three strategies. The next slice
  picks one (or composes them) with explicit rationale.
- Does not commit to a summary table schema. That is part of the
  remediation slice.
- Does not specify refresh cadence, staleness budget, or back-fill
  shape — all open until the strategy is picked.
- Does not propose retiring the SQL aggregation (`Q8a/Q8b/Q8c`)
  shipped at `25a6e23`. It is retained as the cleaner shape for the
  raw-event drill-down path; it is no worse than Q8 was.

## Composes with

- `docs/analysis/subject-lookup-frontdoor-001.md` — the surface
  contract this gap protects.
- `docs/analysis/subject-lookup-load-probe-001.md` — the probe
  contract that surfaced the failure.
- `docs/analysis/subject-lookup-sql-aggregation-001.md` — the
  predecessor gap that closed PARTIAL.
- `docs/analysis/storage-runway-sizing-001.md` — the storage
  constraint any "more indexes" answer has to price in.
- `driftwatch/specs/gaps/gap-spec-cold-path-parquet-duckdb.md` — the
  cold-path doctrine that #2 would compose with.
