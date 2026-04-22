# Operational hazards

Things that have bitten us in production and how to recover. Not
security (see `HARDENING.md`), not protocol limits (see `KNOWN_GAPS.md`).
Just landmines to know about.

---

## SQLite WAL bloat from long-lived reader

**Symptom**: after `systemctl restart labelwatch`, report generation
hangs for hours on the first cycle. py-spy shows the report-gen thread
stuck on a line that does a cheap query; main DB file mtime is days
old; WAL file is larger than the main DB.

**Root cause**: `labelwatch-discovery.service` (and possibly
`labelwatch-api.service`) keeps a long-lived read transaction open. In
SQLite WAL mode, a reader's open transaction pins the WAL at whatever
frame the transaction started on. Passive checkpoints (the default,
and what `PRAGMA wal_autocheckpoint` triggers) cannot advance past a
pinned frame. Writes continue to append to the WAL; the WAL grows
unbounded.

When a new reader (e.g., report-gen after a service restart) opens a
transaction against that WAL, every query has to scan through millions
of WAL frames to find the current page versions. Queries that normally
take milliseconds take minutes.

**How it looked the first time (2026-04-22)**:

- Main DB: 26.6 GB, mtime Apr 17 (5 days stale)
- WAL: 38.5 GB, mtime current
- `PRAGMA wal_checkpoint` (passive): returned `0|9351313|490` —
  9.3M frames in WAL, only 490 checkpointable
- Report thread hung for 2+ hours on `generate_report` at a line that
  normally takes <1s
- Nightly `PRAGMA optimize + wal_checkpoint` cron was running but
  couldn't make progress for the same reason

**Recovery procedure**:

1. Capture receipts — `ls -la labelwatch.db*`, `py-spy dump`,
   `lsof labelwatch.db-wal` to confirm which processes hold open fds.
2. Stop *all* labelwatch services:
   ```
   systemctl stop labelwatch labelwatch-discovery labelwatch-api
   ```
   Confirm via `lsof labelwatch.db-wal` — should show no processes.
3. Run a blocking TRUNCATE checkpoint:
   ```
   sudo -u labelwatch sqlite3 /var/lib/labelwatch/labelwatch.db \
     'PRAGMA wal_checkpoint(TRUNCATE);'
   ```
   Expect this to take several minutes on a large WAL (6m40s for 38GB
   in the 2026-04-22 incident). The WAL and SHM files should disappear
   afterward.
4. Restart services in order: main, then discovery, then api.
5. Verify report regeneration completes — first post-checkpoint regen
   took ~6 minutes in the 2026-04-22 incident (vs. hung indefinitely).

**Structural fix** (not yet implemented):

Either (a) make discover-stream periodically close and reopen its read
transaction to release the WAL pin, (b) run a scheduled TRUNCATE
checkpoint with services briefly paused, or (c) accept that restarts
will require manual remediation and document that in the deploy runbook.

The nightly `wal_checkpoint` cron is currently PASSIVE, which is what
failed. Upgrading it to TRUNCATE would require coordinating service
pauses.

**Detection**: add a check for `size(labelwatch.db-wal) > size(labelwatch.db)`
to ops monitoring. If the WAL exceeds the DB, remediation is overdue.
