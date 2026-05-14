# Gap spec: report generation workload isolation

**Status:** proposed / structural debt. Filed 2026-05-14, after the report-gen WAL-pin incident.

**This is not authorization to build.** It is the named handle for the structural fix that the 2026-05-14 acute recovery (restart of main labelwatch) deliberately did not address.

Companion to:
- `lesson_report_gen_is_workload.md` (auto-memory) — the generalized lesson
- `project_labelwatch_wal_watch_2026_05.md` (auto-memory) — the watch entry, now resolved
- `lesson_wal_bloat.md` (auto-memory) — the external-reader variant of the same WAL-pin mechanism

## Architecture sentence

> Report generation is a workload. The main process is not a magic priesthood exempt from the plant.

## Core finding

Same-process read-heavy report threads can pin SQLite WAL just like external long-lived readers. "Read-only" does not mean operationally harmless. Background work is workload.

## Incident evidence (2026-05-14)

- Main `labelwatch` process was 22 days old (PID 182861, replaced by PID 2570969 on restart)
- `py-spy` showed two simultaneous heavy threads:
  - MainThread: `_update_author_labeler_day` (`labelwatch/scan.py:1017`) — write-heavy derive pass
  - report-gen thread: `_count_naive_timestamps` (`labelwatch/report.py:982`) — long read snapshot iterating rollup tables for report generation
- Checkpoint state was `busy=1, log≈198000 frames, checkpointed≈480` — the read snapshot was holding the WAL frontier, preventing checkpoint advancement
- WAL grew 891 MB (2026-05-13) → 1.1 GB (2026-05-14 afternoon) → 1.6 GB (incident peak)
- `labelwatch-discovery-stream` was dropping events every ~500ms with `database is locked` retries exhausted (`Dropping discovery after 5 busy retries`)
- cgroup `MemoryCurrent = 2.0 GB`, `MemoryHigh = 2.0 GB` — page cache pressed against the soft limit, separate symptom (not anonymous memory leak; RSS was only 277 MB)
- `systemctl restart labelwatch` (main service only, not discovery or api) released the snapshot; WAL dropped to **64 MB** within seconds; discovery resumed normal processing

## Why the existing `lesson_wal_bloat.md` recovery was overkill

The original `lesson_wal_bloat.md` recovery was "stop all 3 services, TRUNCATE checkpoint, restart." That assumed an external connection held the WAL pin. For this incident, the pin was in-process — restarting just the main service was sufficient because the offending snapshot lived inside the process being restarted. `labelwatch-discovery-stream` and `labelwatch-api` did not need to be touched.

## Structural fix candidates

These are not ranked or pre-selected. Each has different operational characteristics; the choice depends on whether report freshness, code complexity, or storage architecture are most load-bearing.

1. **Report generation runs against a periodic snapshot copy.**
   - `cp` the DB to a snapshot path on a schedule
   - Report generation reads from the snapshot, never the live DB
   - Snapshot freshness becomes an explicit knob, not a side effect of locking
   - Adds disk usage (one extra DB-size copy) and snapshot lag
   - Cleanest separation; most operationally legible

2. **Chunked report reads with read-transaction refresh between chunks.**
   - Report generation iterates rollups in chunks
   - Closes and reopens the read transaction between chunks
   - WAL can advance during refreshes
   - Smallest code change but doesn't fully eliminate the contention window
   - Skip-on-pressure semantics still needed

3. **Move report/history reads to DuckDB-over-Parquet.**
   - Composes with the evidence-store doctrine (workspace-level)
   - Report-gen reads from a Parquet snapshot instead of live SQLite
   - Long-term consistent with driftwatch's planned cold-path direction
   - Significant refactor; not a tonight fix
   - See `gap-spec-cold-path-parquet-duckdb.md` (driftwatch) for the same shape applied to driftwatch's facts_export

4. **Split report-gen into its own service/process.**
   - Separate systemd unit
   - Own cgroup memory/CPU limits, independent restart semantics
   - Still reads the same DB but its lifecycle is decoupled
   - Doesn't fix WAL pin by itself — must compose with one of the above

5. **Report-gen pressure gates.**
   - Skip / defer report cycles when:
     - WAL size > threshold
     - Discovery dropped event count rising
     - Discovery work queue > threshold
     - Checkpoint busy across multiple windows
     - Writer back-pressured / event queue backlog
   - Report freshness degrades explicitly under pressure rather than silently dragging the plant
   - Composes well with #1 or #2

## Non-goals

- Do not change report generation tonight.
- Do not introduce a DuckDB migration in this patch.
- Do not treat "restart when WAL gets big" as the durable answer; that is containment, not architecture.
- Do not collapse discovery loss into WAL-size-only health monitoring — WAL bloat was smoke; discovery drops were the actual incident.

## Acceptance for the future fix

Whichever candidate is chosen, the fix must produce all of these:

- Report generation cannot pin WAL long enough to cause discovery drops.
- `labelwatch-discovery-stream` writes continue uninterrupted during report generation.
- WAL remains bounded across multi-day uptime (no monotonic growth from report pressure).
- `wal_checkpoint(PASSIVE)` `busy=0` returns reliably between report cycles.
- Report freshness degradation, if it occurs under pressure, is explicit (logged, surfaced in health) rather than silent.
- The 22-day-old process does not need to be the trigger for the next incident.

## Diagnostic pattern worth preserving

The 2026-05-14 incident was diagnosed entirely with non-invasive tools, in roughly this order:

1. `lsof` on DB files → identify which processes hold what FDs
2. `py-spy dump` on the main PID → see what each thread is actually doing
3. Recent journal logs filtered for `lock|checkpoint|busy|wal` → confirm reader-pinned WAL via `busy=1` checkpoint logs
4. cgroup memory state via `systemctl show -p MemoryCurrent -p MemoryHigh` → catch page-cache pressure that's separate from process RSS
5. Discovery-stream `database is locked` retry pattern → confirm downstream drop is happening *now*
6. Pre-restart state capture (timestamp, WAL size, drop counters) → enable before/after comparison
7. Restart only the suspected offending service unit
8. Post-restart verification within 30s

This is the diagnostic shape for any "WAL is big and writes are getting locked out" scenario. Reusable across labelwatch / driftwatch / future observatories.

## Composes with

- `lesson_report_gen_is_workload.md` — same insight, generalized past labelwatch
- `lesson_wal_bloat.md` — external-reader variant, different recovery
- Driftwatch's analytical-workload-boundary doctrine — same family of fixes ("background work is workload, don't run it against the live writer")
- Evidence-store doctrine — DuckDB-over-Parquet path is the structural answer if labelwatch follows driftwatch's cold-path migration
