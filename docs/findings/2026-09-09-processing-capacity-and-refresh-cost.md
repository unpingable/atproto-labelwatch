# Processing capacity and repeated refresh cost

Assessment date: 2026-09-09. Baseline: deployed rc10
`5ed0a00d132ac3ec9d912b5100baa113b545e954`.
Implementation: `4e11c8a` on the campaign branch; package candidate `0.1.0rc11`.
Artifact qualification and deployment acceptance remain separate gates.

## What the overnight history corrected

The [previous observation](2026-09-08-production-correctness-observation.md)
truthfully ended with an incomplete derive outcome. Subsequent retained evidence
shows that rc10 did complete all required steps at 04:23:41 UTC. Across twelve
completed passes, author-labeler backlog subsets drained three times; normal
seven-day refreshes then created new pending work. The same backlog count at two
times is not evidence that no work completed between them.

Twelve measured derive passes took approximately 14.8–28.9 minutes, with one
aggregate COMPLETE outcome. Report completions were approximately 95–104 minutes
apart: the old loop slept the configured 60 minutes **after** generation, whose
observed duration was roughly 35–44 minutes. The report freshness question's
existing limit is 60 minutes. This was a scheduling throughput mismatch, not a
reason to redefine freshness as healthy.

The per-day helper intentionally drains a deferred closed range before returning
to newest-first normal refresh. Normal refresh recomputes seven eligible days,
including historical days whose source facts may have changed. Consequently,
`last_completed_day_epoch` is the last day processed in that traversal, not a
monotonic global coverage watermark. The new repair preserves those semantics.

## Three bounded repairs

1. The report WAL guard appended `-wal` to the configured database symlink.
   SQLite put its real sidecar beside the resolved database. The alias sidecar
   was absent while the real WAL existed, so report logs incorrectly recorded
   zero at admission. The helper now resolves the database pathname first.
   Missing WAL remains zero; unexpected stat failures explicitly defer instead
   of pretending there is no pressure. The 80 MiB report threshold is unchanged.

2. A process-local coordinator excludes report generation and derive attempts.
   Acquisition is nonblocking: due derive work gets priority; a report already
   running causes an explicit derive deferral. Neither ingest nor scan acquires
   this lock. Memory admission remains effective, including pending priority
   while pressure prevents derive. Completed attempts, even an honest INCOMPLETE
   result, release priority until the existing next eligibility. Exceptions
   release ownership. Report skips retry after 60 seconds, and generation uses
   start-to-start timing rather than adding its duration to the configured
   interval. This is not a new scheduler or cross-process writer authority.

3. Author-labeler day refresh stages the exact existing aggregate in one
   connection-local temporary table, then deletes disappeared keys and updates
   only changed metrics. New keys, late changes, removals and NULL source
   negations retain the old aggregate meaning. Existing connections use
   `temp_store=FILE`; only one day's staging is retained. Main-database writer
   ownership is acquired before reading source rows, preserving the previous
   DELETE-first transaction boundary instead of introducing a read-upgrade race.
   Each day remains atomic under the existing caller's commit/rollback. Schema
   23, retention, and WAL/budget guards are unchanged.

## Evidence and limits

A synthetic fixture contains 120,000 retained derived rows over 60 days and
4,000 source events for a 2,000-key target day. It compares exact full-table
outputs, not merely status generation. A day-leading index changed the delete
plan from SCAN to SEARCH but did not materially improve whole-refresh time or
WAL bytes; it was not adopted.

Conditional refresh reduced the unchanged-repeat main WAL from 11,861,512 bytes
to zero. A changed-metric/deleted-key case fell from 11,845,032 to 12,392 bytes;
a new-key/NULL-negation case fell to 28,872 bytes. Initial changed metrics used
156,592 bytes instead of 11,968,632. All compared output hashes matched, and
rollback preserved the pre-merge derived rows. Temporary page allocation was
270,336 bytes for this fixture. These are bounded fixture results, not estimates
of production table size, every dependency, or end-to-end throughput.

The production deferral observed 638.3 MiB against the 500 MiB author-labeler
guard. Removing that particular excess would require reducing total WAL by
138.3 MiB, about 21.7%; retained receipts do not attribute that WAL to individual
earlier steps. A large fixture reduction in this substep does not establish that
the production excess will disappear. Existing WAL can still defer a later day.
No threshold is raised and no pending day is silently dropped by this repair.

The scheduling capacity model also has a real boundary: serialized report and
derive work spans about 50–73 minutes using the observed ranges. The low end can
fit the unchanged 60-minute freshness limit; the upper end cannot. Coordination
prevents one known overlap but is not proof that every cycle will meet freshness.
Unexpected pressure can still defer work, and a missing/old result stays visible.

## Qualification and next acceptance

The affected 259-test suite passed, including existing acquisition, cursor,
derive, day-resume, report and boundary checks. New tests cover canonical WAL
paths, fail-closed stat errors, nonblocking admission, ingest/scan during report
ownership, finally-release, monotonic start-to-start delays, truthful incomplete
outcomes, exact changed/deleted/new/NULL aggregates, rollback, main-writer
ownership before source reads, and zero main-WAL writes on unchanged refresh.

```sh
PYTHONPATH=src python -m pytest -q tests/test_runner_wal_path.py tests/test_heavy_work_schedule.py tests/test_conditional_author_labeler_day.py tests/test_runner_pressure.py tests/test_update_author_labeler_day_chunked.py
```

Private campaign `2026-09-09-processing-capacity-recovery`, Labelwatch lane,
retains selected phase/resource evidence, fixture scripts/results and durable
test terminals. Next acceptance requires exact candidate artifacts, offline
installation/recovery, adequate production staging/rollback and temporary space,
and observation of completed day boundaries versus newly eligible work. Storage
capacity remains a separate preflight decision; this slice does not add raw-event
retention or authorize cleanup, index creation, or deletion of production data.
