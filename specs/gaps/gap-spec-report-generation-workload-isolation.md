# Gap spec: report generation workload isolation

**Status:** proposed / structural debt. Filed 2026-05-14. **Revised 2026-05-15 after recurrence within ~20h of the recovery restart.**

**This is not authorization to build.** It is the named handle for the structural fix that the 2026-05-14 acute recovery (restart of main labelwatch) deliberately did not address.

## Revision 2026-05-15: not a long-uptime buildup

The 2026-05-14 incident was framed as "22-day-old process accumulated state." The 2026-05-15 recurrence falsifies that. The main process was only ~20 hours old when the same shape returned: discovery drops in 10–25/min bursts, `database is locked` retries on the worker, WAL climbing past baseline.

The recurring trigger is **report-generation cadence colliding with derive / discovery SQLite writes**, not process age. The original report thread slept `max(scan_interval, 300) = 300s` between runs while each report took ~7 minutes — so the live DB was being held under a long readonly snapshot for ~70% of every ~12-minute cycle, with no time for the WAL to checkpoint cleanly between snapshots.

**Restart is emergency relief only. It is not a fix even at the containment layer; it just resets the snapshot-stack so the next collision starts from a clean WAL.**

### Containment patch landed 2026-05-15

Implemented two of the candidates below as immediate mitigation (not structural):

- **Candidate 5 (pressure gate):** `_report_loop` checks WAL size before each cycle; if `wal_size_mb > LABELWATCH_REPORT_WAL_SKIP_MB` (default 80), skip and re-evaluate next interval. Implemented in `src/labelwatch/runner.py` (`_wal_size_mb`, `_report_loop`).
- **Cadence dial:** new `--report-interval` arg (default 1800s, min 300s) decouples report cadence from scan cadence. The report cycle is now ~30 min instead of ~12 min, giving the WAL time to checkpoint between long readonly snapshots.

Operational rule added by this revision:

> **Report freshness is subordinate to discovery ingest.** A slightly stale report is acceptable. Dropping discovery events because the report thread wants to count timestamps every twelve minutes is not.

The structural fix candidates below remain the durable answer — the containment patch buys time for choosing among them, not for skipping them.

### Structural fix landed 2026-05-15 (candidate #2: chunked reads)

The chunked-reads + per-chunk snapshot-release fix (gap-spec candidate #2) was implemented and validated under controlled load against the production DB:

- **`_count_naive_timestamps`** — replaced single full-scan SELECT with rowid-bounded chunks (default 200k rows/chunk), `time.sleep(0.05)` between chunks so the cursor finalizes and the WAL snapshot releases
- **`_stream_alerts_json`** — replaced cursor iteration with id-bounded pagination (default 1000 rows/page); each page is a separate short SELECT
- **Per-labeler loops** (nonref table + per-labeler pages, ~470+ labelers each) — yield every 50 iterations
- **Explicit degradation path** — `LABELWATCH_REPORT_SKIP_NAIVE_TS=1` skips the timestamp scan entirely, surfaced as a banner + `naive_timestamp_count_skipped` JSON flag

**Controlled fault test result:**
- Runtime: 509s (~8.5min, comparable to pre-patch baseline; the per-chunk yields add a small constant)
- Discovery drops during run: **0**
- `database is locked` retries during run: **0**
- WAL stable at 64MB throughout — never grew or shrunk
- Output artifacts complete (alerts.json 10MB, index.html, per-labeler pages, claims, census)

**Current operational setting:** `--report-interval 3600` via `/etc/systemd/system/labelwatch.service.d/report-interval.conf` (replaced the 6h emergency drop-in). 3600s is a conservative decompression step, not the durable end state — one natural cycle should be observed clean before considering a return to 1800s default.

The remaining structural candidates (#1 snapshot copy, #3 DuckDB/Parquet, #4 separate service) are still useful escape hatches if the workload outgrows even chunked SQLite, but chunked reads alone resolved the WAL-pin pathology that produced the 2026-05-14 incident.

### Containment patch failed verification 2026-05-15 (historical)

The 30min-cadence + 80MB-WAL-gate patch passed its first post-restart cycle (cold start, WAL≈0) and then failed its second cycle in the same shape as the original incident: 46 drops + 33 `database is locked` retries on `labelwatch-discovery` within 5 minutes while the in-flight report ran.

**Key finding: the WAL-size gate threshold was the wrong shape.** During the failing cycle the WAL was pinned at **64 MB — below the 80 MB skip threshold** — because the readonly snapshot holds the checkpoint frontier wherever the WAL happened to be when the snapshot opened. WAL size is not a leading indicator of writer contention under this failure mode; it's a *trailing* indicator of an external long-running reader. By the time WAL crosses 80 MB the drops have already been happening for minutes.

What this rules out as cheap fixes:
- WAL-size gates alone (any threshold) — the snapshot pins WAL well below any threshold worth picking, because checkpoint advancement is what's blocked, not file growth
- Cadence reduction alone — even at 30min spacing, every cycle still spends ~7 minutes pinning live writes; the failure mode is per-cycle, not per-frequency

Fallback applied: `--report-interval 21600` (6h) via systemd drop-in (`/etc/systemd/system/labelwatch.service.d/report-interval-fallback.conf`). This is hold-the-line behavior — one drop window every 6h is tolerable while structural work lands; 30min cadence is not.

Pressure signals that would actually work (deferred to structural):
- Discovery dropped-event counter rising (the actual incident signal — but requires sidecar→main signal channel)
- `wal_checkpoint(PASSIVE) busy>0` across N cycles (probe-able from the report thread itself)
- Writer-thread back-pressure / event queue backlog (requires instrumentation)

These are not cheap to add cleanly. The structural candidates (chunked reads, snapshot copy, DuckDB/Parquet) eliminate the pin entirely rather than gating around it.

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

5. **Report-gen pressure gates.** *(partially landed 2026-05-15 as containment, WAL-size gate only)*
   - Skip / defer report cycles when:
     - WAL size > threshold *(landed: `LABELWATCH_REPORT_WAL_SKIP_MB`, default 80)*
     - Discovery dropped event count rising *(not yet — requires sidecar→main signal channel)*
     - Discovery work queue > threshold *(not yet — same)*
     - Checkpoint busy across multiple windows *(not yet — requires checkpoint probe)*
     - Writer back-pressured / event queue backlog *(not yet)*
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
