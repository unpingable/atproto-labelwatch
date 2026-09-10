# Cadence read work and honest derive outcomes

Date:2026-09-08. Baseline rc9: `d520981171bb24a196dd5aaf027abf4b91c4a925`.
Successor candidate: immutable `v0.1.0-rc.10`. Deployment outcome remains separate.

## Why this followed the first repairs

The [acquisition and homepage repair](2026-09-08-acquisition-custody-and-homepage-budget.md)
worked under production observation: the two formerly unobserved durable cursors
gained observation timestamps, and three loopback samples returned in about two
seconds with the optional weather strip explicitly unavailable. Discovery made
progress with zero reported queue/parse/worker losses. These results did not
establish report or derive completion.

At the 30-minute observation, ingest and scan progressed while report and derive
success timestamps still preceded cutover. A nonblocking stack sample identified
two expensive reads: the main thread counted all historical events grouped by
source for rule thresholds, while the report thread repeatedly searched historical
service-record JSON for individual emitter definitions. The report requested 30
such lookups. Its configured one-hour interval starts after completion and imposes
no bound on the current report.

Measured memory.current was2.88GB against a2.95GB memory.high, with194MB anonymous
memory and2.61GB file cache (2.06GB inactive). Large memory.high event deltas showed
real reclaim work. Derive's existing pressure check runs after the expensive scan
and can defer repeatedly without an upper bound. No memory limit or pressure
threshold was loosened by this successor.

## What changed

Rule event counts serve only the confidence and warmup threshold comparisons.
The scan now reads at most the larger configured threshold per catalog source
through the existing source index. Standalone exact-count behavior remains
available. This preserves rule outcomes and their receipts without recounting
an entire large source history for a threshold it has long exceeded.

Report emitter enrichment now resolves all requested identifiers during one
chunked newest-first history traversal. Historical definitions remain eligible;
delete records are excluded, duplicate input values share a lookup, and the first
matching definition within a record is preserved. Timestamp ties use descending
record id, matching the old timestamp-index traversal in the parity fixture.
NULL identifiers do not match a requested NULL. Each chunk finalizes its cursor
before processing and continuing, preserving the existing reader-yield boundary.
The run bounds its input by the initial maximum record id; records appended after
that point are eligible next report. Missing identifiers can still require one
full traversal. Invalid JSON encountered during traversal raises instead of being
silently treated as a missing definition. No persistent classifier cache was added.

The driver also now returns explicit derive step outcomes. Failed steps keep
their rollback behavior and successful earlier steps remain committed. Disabled
derive, unavailable/stale configured facts, and pending day-range work cannot
advance `last_derive_ok_ts`. An unchanged already-synchronized facts source needs
no repeated work and remains a completed step. New private metadata records the
last attempt and full step outcome separately from the success timestamp.

This corrects a historical evidence limitation: previously a returned driver,
including one with a failed substep, could advance the success heartbeat. Old
timestamps are preserved; they are not retroactively upgraded into all-step
success evidence. Required public concerns, cursor-v2 meaning, schema23, and the
memory pressure policy remain unchanged.

## Qualification and remaining operational gate

An expanded237-test focused suite passed. The final eight-test delta rerun covers
the NULL-identifier guard and genuine complete derive outcome. Tests compare full
rule outputs and threshold decisions with the old exact count cache; SQLite
instruction counting shows less than one tenth of the work on a skewed fixture.
Emitter fixtures cover newest/historical definitions, timestamp ties, duplicate
inputs, absent/NULL definitions, malformed JSON, provenance equality, and bounded
query count. Derive fixtures distinguish a failed middle step, pending work,
skipped source, disabled driver, and complete pass without a false success tick.

```sh
python -m pytest -q tests/test_cadence_read_work.py tests/test_runner_pressure.py tests/test_derive.py tests/test_warmup.py tests/test_rules_overlap.py tests/test_rules_spikes.py
```

Private campaign `2026-09-08-observatory-correctness-recovery`, lane `labelwatch`,
retains selected resource/stack evidence and tests. Artifact qualification and
the dated production receipt bind exact candidates separately. Reduced redundant
work is locally demonstrated; complete production cadence and recovery remain
measured gates. If pressure persists, it must remain visible rather than granting
derive an unconditional exemption.
