# Gap spec: derive workload isolation

**Status:** active steady-state incident. Filed 2026-05-16, after report-gen workload isolation landed and exposed the next layer.

**This is the active gap. Acceptance criteria are operational, not architectural.** The structural pass is now load-bearing; this is not a "name and defer" record.

Companion to:
- `gap-spec-report-generation-workload-isolation.md` — same family, prior layer (chunked-reads patch landed 2026-05-15)
- Workspace doctrine: *A derived table is not allowed to destroy the evidence it derives from*
- Labelwatch doctrine: *Discovery ingest outranks derive catch-up*

## Finding

After the report-gen WAL-pin pathology was fixed via chunked reads (2026-05-15), discovery loss continued at ~5 drops/min over a 20.5-hour observation window — 6,747 dropped events and 4,999 `database is locked` retries on `labelwatch-discovery`, distributed in 5-10 minute bursts every 15-30 minutes across all 18 clean report cycles in the window.

py-spy at 2026-05-16T18:00Z caught `MainThread` in `labelwatch/scan.py:1011` — `_update_author_labeler_day` — the same writer line called out in the 2026-05-14 incident memory as contending with the report-gen reader. The report-gen reader was removed by the chunked-reads fix; the writer is still here, now contending directly with `labelwatch-discovery` for the SQLite single-writer lock.

## Root cause: `run_derive` is one giant transaction

`run_derive` (`scan.py:1211-1266`) calls 12+ sub-derive operations on a single connection with a single `conn.commit()` at the very end. Every DELETE/INSERT/UPDATE issued by `_run_derive_pass`, `_update_coverage_columns`, `_cleanup_ingest_outcomes`, `_sync_driftwatch_facts`, `_compute_labeler_lag_7d`, `_compute_reversal_stats_7d`, `_compute_boundary_load_7d`, `_update_val_dist_day`, `_compute_entropy_7d`, `_update_author_day`, `_update_author_labeler_day`, and `run_boundary_pass` is buffered into one transaction. The SQLite writer lock is held for the entire duration, which is long enough that `labelwatch-discovery`'s 120s busy_timeout is insufficient on each derive pass.

## Keeper rule

> Derived state is subordinate to discovery ingest. A derived table is not allowed to destroy the evidence it derives from.

## Structural fixes

Ordered conservative → invasive. Land the cheap ones first; let the harder ones earn their way in only if cheaper layers don't close the gap.

1. **Commit + yield between sub-derive steps.** Smallest surface change with the largest leverage. Each sub-step becomes its own short transaction; writer lock is released between steps; discovery gets scheduling windows. No logic restructuring. Pattern mirrors report-gen `_yield_between_chunks()`.

2. **Pressure-gate derive against ingest health.** Before each derive pass (or between sub-steps), check:
   - cgroup memory near `MemoryHigh` (cheap, directly observable from `/sys/fs/cgroup/memory.current`)
   - WAL frame count growth rate (proxy for sustained writer pressure)
   - **Discovery drop counter** — currently unobservable from main process; requires a sidecar→main signal channel (DB meta row, file flag, or shared counter file written by `labelwatch-discovery`)
   - **db-locked retry counter** — same observability gap
   - **Work queue backlog** — same

   Skip or defer derive when pressure exceeds threshold. Surface the skip as an explicit log line + heartbeat counter so degradation is visible, not silent.

3. **Chunk the heaviest individual ops.** `_update_author_labeler_day`'s GROUP BY INSERT over the last 7 days of `label_events` is the largest single statement. If commit-between-steps + pressure gating don't close the gap, split this query into per-day or per-author-range chunks with commit between chunks.

4. **Decouple derive cadence from scan loop.** Currently derive runs inside the scan pass when `(now - last_derive) > derive_interval`. Consider a separate scheduler or a slower default cadence; derive doesn't need to fire as often as it currently does if it's blocking ingest.

5. **Snapshot/cold-path for derive reads.** If the SELECT side of derive (the materialization that feeds `_update_author_labeler_day` and friends) is contributing to page-cache pressure against `MemoryHigh`, move the analytics path off the live SQLite onto a periodically-refreshed snapshot or DuckDB/Parquet. Composes with the workspace evidence-store doctrine and driftwatch's planned cold-path direction.

## Non-goals

- **Do not merely raise `labelwatch-discovery`'s busy_timeout.** It hides contention as tail latency without changing priority. The asymmetric-recoverability argument doesn't care whether the loss shows up as a drop log line or a 60-second stall — discovery still loses.
- **Do not restart as the fix.** Restart resets the visible state without addressing the root cause; the same shape returns within hours (verified twice in this incident family already).
- **Do not lower report cadence further.** Report-gen is not the current culprit — 18 clean cycles confirms that. Touching it again would be noise.
- **Do not declare the system healthy because rollback loss is zero.** Discovery loss is the live signal; absence of crash is not health.
- **Do not chunk so aggressively that derive can't finish.** The point is to yield, not to prevent progress.

## Acceptance

Whichever combination of fixes lands, the outcome must produce all of these in a one-hour observation window:

- Discovery drops drop to 0 (or near-0 with documented bound)
- `database is locked` retries drop to 0
- Derive still makes forward progress (heartbeat advances, derived tables aren't stale)
- WAL remains bounded (PASSIVE checkpoint advancing, file at high-water mark not growing)
- Report-gen stays clean (cycles continue at ~71min cadence)
- Derive degradation, if it occurs under pressure, is **explicit** — logged, surfaced in health, not silent

## Magnitude for calibration

20.5-hour baseline (2026-05-15T21:28 → 2026-05-16T18:00):
- 6,747 dropped discovery events (~5.5/min average, bursty)
- 4,999 db-locked retries
- 18 clean report cycles
- WAL frame count never pinned (checkpoint advancing throughout)
- cgroup `MemoryCurrent` consistently 1.9GB / `MemoryHigh` 2GB — page-cache pressure (separate symptom, not anonymous leak)

## Composes with

- `gap-spec-report-generation-workload-isolation.md` — prior layer in the same plant doctrine
- Workspace constraint: *A derived table is not allowed to destroy the evidence it derives from*
- Labelwatch constraint: *Discovery ingest outranks derive catch-up*
- `lesson_self_shedding_queue_boundary.md` (workspace) — same family: per-step sync writes in a shared transaction are the smell; chunked txn + yield is the fix shape
- Doctrine: *Layered failure is the expected discovery shape* — three layers visible now (cadence collision → reader pin → writer monopoly); each was real, each unmasked the next
