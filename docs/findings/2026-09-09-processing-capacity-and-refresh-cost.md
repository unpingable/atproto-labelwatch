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

## Production observation: progress, remaining work, and a disk gate

Candidate source `941c8d62708c180f202ff9110630cbbeb81eb6e2` was qualified with
two matching clean wheel builds and two matching source archives. Its exact
target-platform dependencies installed with network access disabled; the installed
instrument demonstration, synthetic application recovery, and previous rc10
runtime reading candidate-written schema23 state passed. These do not establish
reproducible rebuilding of every third-party dependency.

The first deployment attempt exposed a readiness-check race, not an established
instrument failure. Its automatic fallback retained the current database, but
the same immediate process-directory assertion also rejected fallback readiness.
Later read-only observations found the expected rc10 process identity. That
failed receipt remains failed. The packet was repaired to require two stable
successful identity samples within a bounded deadline, and the next coordinated
cutover passed startup checks at 15:09:27 UTC. No database/schema, memory policy,
WAL threshold or freshness profile was changed.

Two observer invocation failures were separately retained: an output-directory
guard refusal and a host journal timestamp-parser error. The repaired observer
converted the cutover's timezone-aware timestamp to explicit epoch syntax and
distinguished genuinely empty journal matches from capture errors. Neither
failed capture was counted as a successful observation window.

The subsequent finite observation collected 29 samples from 15:15:34 to 17:35:34
UTC, ending after 140 minutes at the predeclared disk-space failure condition:
available root bytes were 7,723,184,128, below the unchanged 8 GiB cutover floor.
The observer recorded ROLLBACK_REQUIRED and performed no service changes itself.
The larger existing 15 GiB operations floor remained unmet throughout; it was not
waived. This was an early terminal, not a completed three-hour qualification.

Within that window:

- All three code-sharing units retained the exact candidate identity with no
  reported restarts. The event high-water ID advanced 95,830; 173 acquisition cursor
  records remained present, with 22 distinct aggregate cursor hashes observed.
  This supports observed progression, not complete source coverage or gap-free
  acquisition. Discovery reported no queue drops or worker failures.
- One derive pass completed all 12 required steps at 15:40:04 UTC. It drained four
  pending author days and completed a seven-day author-labeler refresh. The next
  pass completed a further seven-day author-labeler refresh but was truthfully
  INCOMPLETE: author-day refresh processed three days, then deferred four days
  (September 3–6) when its observed 427 MiB WAL exceeded the unchanged 350 MiB guard.
  The later derive admission deferred on memory-pressure evidence. The repaired
  author-labeler step progressed, but a persistent-capacity claim did not pass.
- Two reports completed at 16:22:16 and 17:27:57 UTC. Their 3940.185-second gap
  improved on the preceding 95–104-minute observations but still exceeded the
  unchanged 3600-second freshness profile. This is not a controlled performance
  comparison, and faster completion does not make the freshness failure pass.
- All 29 sampled homepages returned 200 within 2.016 seconds. The optional strip
  explicitly remained unavailable in every sample; bounded page availability
  did not establish successful optional computation.

Available root space fell 3,872,509,952 bytes while the canonical instrument
database grew 93,077,504 bytes. Those measurements do not attribute the remaining
filesystem growth to this candidate. The smallest next investigation is to reconcile
the disk-space trigger, then address the remaining
author-day and report-throughput limits without raising their guards or silently
discarding pending work. No second complete derive or clear final author-day
backlog was demonstrated in this terminated window.

Private campaign receipts retain the exact 29 samples, run identity, result and
terminal, with remote-to-local SHA256 comparisons passing for all 32 receipt
files. Service fallback execution has a separate integrator-owned receipt.

The coordinating integrator completed fallback at 17:37:25 UTC to exact rc10
`5ed0a00d132ac3ec9d912b5100baa113b545e954` in all three code-sharing units,
active with zero reported restarts. The current database was retained; this was
not a restoration of older observations. Root free space recovered from
7,723,184,128 to 11,075,129,344 bytes after the coordinated process replacement.
That recovery correlates with stopping the former processes and is consistent
with release of temporary or open-file allocations, but no retained allocation
identity proves their ownership or attributes a specific defect to rc11.
Fallback service availability is established; a new full ingestion/capacity
observation of the restored baseline is a separate, uncompleted claim here.

A bounded post-fallback descriptor check at 18:12:18 UTC examined all 24 open
descriptors across the three service processes, without reading contents. The
rc10 main process held one deleted-open regular file with a SQLite-style temporary
name on the root filesystem: 280,185,876 logical bytes and 280,190,976 allocated
bytes. Discovery and API held no matching temporary/deleted-open files. There
were no enumeration errors or truncated process/descriptor sets. Root free space
was then 10,336,948,224 bytes. This establishes a temporary allocation on the
restored baseline too; without its earlier allocation size or the former rc11
process's descriptor identity, it does not explain the entire post-fallback
decline or establish the cause of the earlier multi-gigabyte release.

The final identical check at 18:30:55 UTC found the same three processes active
but no matching temporary/deleted-open allocation among their 23 descriptors.
The earlier temporary allocation was no longer held. Root free space was
10,201,415,680 bytes: 135,532,544 fewer bytes than at 18:12, but still
1,611,481,088 bytes above the 8 GiB cutover floor. These two snapshots do not
demonstrate monotonically growing temporary allocation; the remaining filesystem
decline is unattributed. The larger operations floor remained unmet, and no
additional cleanup or pressure-policy change was performed by this diagnosis.
