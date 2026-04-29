# labelwatch — Failure Modes

**Status**: v0 starter.
**Last updated**: 2026-04-28

## The job

This doc names failure modes the architecture already knows about — the ones encoded in invariants, gates, and recovery paths, plus the ones we've hit in production. It is the architectural-frame counterpart to `../OPS_HAZARDS.md` (which is the runbook).

The point of writing this down is so that "the architecture handles X" is a checkable claim, not a story we tell ourselves.

## Storage failures

### WAL bloat (long-lived reader pins WAL)

**Shape**: a long-lived reader (e.g., the API service holding a connection) prevents WAL checkpoint. WAL grows. On restart, regen of WAL hangs.

**What the architecture knows**: SQLite WAL mode requires checkpointing; long-lived connections can pin it; recovery requires stopping all readers/writers, TRUNCATE checkpoint, restart.

**Recovery**: see `../OPS_HAZARDS.md`. Stop all three services, `PRAGMA wal_checkpoint(TRUNCATE)`, restart.

**Architectural mitigation**: nightly maintenance cron at 06:00 UTC runs `PRAGMA optimize` + WAL checkpoint. `busy_timeout=60s` on writers. `lesson_wal_bloat.md` in memory.

### Schema migration drift

**Shape**: code expects schema version N+1; DB is at N. Or vice versa.

**What the architecture knows**: schema version tracked in `meta` table (`key = "schema_version"`). `init_db()` walks migrations sequentially. Idempotent.

**Architectural mitigation**: migrations run on startup; refuse to run if version is unknown. Sticky evidence fields documented to prevent downgrade-on-reprobe.

### DB write failure (discovery stream)

**Shape**: discovery stream catches an event, fails to write, continues running.

**What the architecture knows**: this is the "dead but optimistic" anti-pattern. A daemon that swallows write failures looks healthy and is silently broken.

**Architectural mitigation**: discovery stream **crashes loud** on DB write failure. systemd restarts. Cursor + 3s rewind on reconnect ensures gapless replay.

## Ingest failures

### Polling gaps

**Shape**: HTTP timeout / 4xx / network blip during a poll. Events between cursor and resume are missed.

**What the architecture knows**: cursor persistence covers most of this. Event hash dedup is the safety net — if cursor drifts, we re-fetch but `INSERT OR IGNORE` prevents duplicates.

**Architectural mitigation**: per-labeler cursors in `ingest_cursors`. SHA-256 `event_hash` on every event.

### bsky.social returns 401

**Shape**: as of 2026-03-05, `bsky.social` `queryLabels` requires auth.

**What the architecture knows**: primary aggregator can fail; multi-ingest mode polls each labeler directly as fallback.

**Architectural mitigation**: primary auto-disables on 401. Multi-ingest handles all labelers individually. Documented in MEMORY.

### Sparse labeler false signals

**Shape**: a new labeler with 5 events triggers rate-spike rule because baseline is zero.

**What the architecture knows**: low-volume labelers produce noise on rate-based rules. Pattern rules are still meaningful.

**Architectural mitigation**: warm-up gate (tagged `warming_up`, rate rules suppressed); sparse gate (rate rules suppressed entirely below threshold). Pattern rules (flip-flop, concentration) still fire.

## Detection / publication failures

### Flapping regime states

**Shape**: noise pushes a labeler back and forth between two regime states each scan.

**What the architecture knows**: instantaneous classification on noisy signals causes thrash.

**Architectural mitigation**: hysteresis — regime state changes require N consecutive passes. No flapping.

### Repeated identical alerts

**Shape**: same `det_id` fires every scan, drowning out new signals.

**What the architecture knows**: alert spam is a symptom of insufficient cooldown.

**Architectural mitigation**: cooldown filter — same `det_id` suppressed for `COOLDOWN_WINDOWS` unless severity escalates or score increases by ≥ `COOLDOWN_SCORE_DELTA`.

### Fake "moderation conflicts" from synonym mismatch

**Shape**: pre-Phase-2, 624 false fight pairs from labelers using different synonyms for the same family.

**What the architecture knows**: label families need normalization before cross-labeler comparison.

**Architectural mitigation**: `label_family.py` v2 with FAMILY_MAP, DOMAIN_MAP expansion, word-boundary keywords. Reports filter by `family_version` for clean transition. Boundary phase 2 deployed 2026-03-13.

## Climate API failures

### API overload / resource exhaustion

**Shape**: surge of `/v1/climate/{did}` requests blocks the server.

**What the architecture knows**: per-IP rate limiting alone is insufficient; total concurrency must be capped.

**Architectural mitigation**: token bucket per IP + concurrency semaphore + generation timeout (10s) + disk cache (5min TTL, atomic writes) + kill switch (`CLIMATE_API_DISABLED`).

### Sensitive payload leak

**Shape**: receipt fields included in public response would expose internal config.

**What the architecture knows**: internal payloads and public payloads are different shapes.

**Architectural mitigation**: public payload whitelist strips `recent_receipts`.

### Path traversal on DID parameter

**Shape**: malformed DID query attempts directory traversal.

**Architectural mitigation**: input validation on DID parameter; loopback-only binding.

## Subsystem cascade failures

### One subsystem crash kills others

**Shape**: scan crashes; ingest stops too because they share a process.

**What the architecture knows**: subsystem isolation via try/except is essential.

**Architectural mitigation**: runner wraps each subsystem in try/except. A scan crash doesn't kill ingest. systemd restarts the process if the runner dies.

### Half-dead state (heartbeats stale)

**Shape**: process is up; one subsystem is silently broken; no operator notices.

**Architectural mitigation**: heartbeat timestamps in `meta` (`last_ingest_ok_ts`, `last_scan_ok_ts`, `last_report_ok_ts`, `last_discovery_ok_ts`). Report card flags stale heartbeats.

## The doctrine layer

Some failures are not implementation bugs but framing failures the architecture is designed to refuse.

### Operationally up vs epistemically degraded

A live, green system can be silently producing loss-conditioned outputs. Liveness ≠ coverage ≠ truthfulness. The architecture must not let "the dashboard updated" stand in for "the dashboard is correct."

**Architectural mitigation**: drop-aware coverage stats; warmup banners; staleness indicators; clock-skew detection; the four-dial discipline (no collapsed trust score).

### Tolerability horizon

Sometimes the system is bad-but-tolerable; sometimes it's actually broken. Both states must be declarable. The architecture must not collapse them.

**Architectural mitigation**: regime states distinguish degraded / flapping / inactive / ghost_declared / dark_operational. No single "broken" or "fine" boolean.

### Co-presence is not corroboration

Two labels (or claim + receipt) appearing together does not validate either.

**Architectural mitigation**: receipts attest to *one* decision, not to other receipts that happen to sit nearby. Cross-reference is operator judgment, not architectural truth.

## Cross-reference

- `OVERVIEW.md` — system context.
- `PUBLICATION_MODEL.md` — gates that prevent some of these from publishing.
- `PUBLIC_SURFACES.md` — boundary surfaces.
- `../OPS_HAZARDS.md` — operational runbook (recovery procedures).
- `../HARDENING.md` — Tier-0 hardening details.
- Memory: `lesson_wal_bloat.md`, `lesson_operationally_up_epistemically_degraded.md`, `project_tolerability_horizon.md`, `constraint_copresence_not_corroboration.md`.
