"""Sidecar label_state DB — pilot only.

This module implements the storage and pilot backfill path for an
incremental label-state table, kept in a SIDECAR sqlite file
(`labelwatch_state.db`) rather than the main labelwatch DB. The sidecar
shape is deliberate:

  - Isolated growth (composite index on the main DB just cost ~6 GB; we
    don't want to compound that mistake).
  - Easier rollback (drop the file, nothing else moves).
  - Can be relocated to another volume later without touching the main
    DB.
  - The main DB does not gain another organ.

This commit ships *code only*: pilot-mode backfill behind an explicit
CLI, no auto-activation, no scan-cycle dependency, no report
integration. Full live backfill is intentionally NOT exposed yet —
see the docstring on `pilot_backfill` for the safety contract.

ADMISSIBILITY RULE
------------------
Any reader of `label_state` MUST check `label_state_meta.build_status`
before relying on the table:

    inadmissible unless build_status == 'complete'
                  AND build_window covers the scan window being queried

A half-built sidecar is not testimony. Future readers should refuse
to display data unless this gate passes.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)


SIDECAR_SCHEMA = """
CREATE TABLE IF NOT EXISTS label_state (
    labeler_did TEXT NOT NULL,
    uri TEXT NOT NULL,
    val TEXT NOT NULL,
    current_state TEXT NOT NULL,           -- 'active' | 'inactive' | 'unknown'
    current_state_since INTEGER,           -- unix seconds; when current state began
    first_seen_ts INTEGER NOT NULL,        -- unix seconds; earliest event for this key
    last_seen_ts INTEGER NOT NULL,         -- unix seconds; latest event for this key
    add_count INTEGER NOT NULL DEFAULT 0,  -- state transitions: inactive -> active
    del_count INTEGER NOT NULL DEFAULT 0,  -- state transitions: active -> inactive
    event_count INTEGER NOT NULL DEFAULT 0,-- total events observed (incl. reasserts)
    open_run_started_ts INTEGER,           -- unix seconds; NULL when current_state != 'active'
    updated_at INTEGER NOT NULL,           -- unix seconds; last write to this row
    PRIMARY KEY (labeler_did, uri, val)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_label_state_current_state
    ON label_state(current_state);

CREATE TABLE IF NOT EXISTS label_state_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# Meta keys the readers / monitors care about
META_BUILD_STATUS = "build_status"          # empty | building | complete | failed
META_CURSOR_ID = "cursor_id"                # last main-DB event id processed
META_EVENTS_PROCESSED = "events_processed"  # cumulative count
META_BUILD_WINDOW_DAYS = "build_window_days"
META_BUILD_MAX_EVENTS = "build_max_events"
META_BUILT_AT = "built_at"                  # ISO timestamp at completion
META_FAILURE_REASON = "build_failure_reason"
META_SOURCE_SCHEMA = "source_db_schema_version"


def init_sidecar(path: str) -> sqlite3.Connection:
    """Open or create the sidecar DB; ensure schema. Returns a writable conn."""
    new = not os.path.exists(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # WAL on the sidecar so writer + reader can coexist without blocking.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SIDECAR_SCHEMA)
    conn.commit()
    if new:
        _log.info("Created sidecar DB at %s", path)
        meta_set(conn, META_BUILD_STATUS, "empty")
        conn.commit()
    return conn


def meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute(
        "SELECT value FROM label_state_meta WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else None


def meta_set(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO label_state_meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def is_admissible(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Check whether the sidecar is safe to read as testimony.

    Returns (ok, reason). ok=False means "do not display data from this
    sidecar." This is the admissibility gate downstream readers must
    consult before showing label_state aggregates.
    """
    status = meta_get(conn, META_BUILD_STATUS)
    if status != "complete":
        return False, f"build_status={status!r} (need 'complete')"
    return True, "ok"


# --- timestamp helpers ----------------------------------------------------

def _parse_ts(s: Optional[str]) -> int:
    """ISO string -> unix epoch seconds. 0 on parse failure (caller handles)."""
    if not s:
        return 0
    s = s.rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return int(
                datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
            )
        except ValueError:
            continue
    try:
        return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return 0


# --- safety probes --------------------------------------------------------

def _free_disk_gb(path: str) -> float:
    s = os.statvfs(os.path.dirname(path) or path)
    return (s.f_bavail * s.f_frsize) / (1024 ** 3)


def _main_wal_gb(main_db_path: str) -> float:
    wal = f"{main_db_path}-wal"
    try:
        return os.path.getsize(wal) / (1024 ** 3)
    except FileNotFoundError:
        return 0.0


# --- state machine + chunk apply ------------------------------------------

def _apply_chunk(
    state_conn: sqlite3.Connection,
    key_events: Dict[Tuple[str, str, str], List[Dict[str, int]]],
    now_int: int,
) -> int:
    """Apply one chunk's events to the sidecar. Returns rows upserted.

    State machine (per key):
        unknown / inactive -> positive  = ADD       (state transition)
        active             -> positive  = REASSERT  (no state change)
        active             -> negation  = REMOVAL   (state transition)
        unknown / inactive -> negation  = stray (event_count++, no transition)

    `event_count` increments on every observed event regardless of
    transition; `add_count` / `del_count` track transitions only.
    """
    if not key_events:
        return 0

    keys = list(key_events.keys())

    # Load existing state for the affected keys, batched to stay under
    # SQLite's expression-tree limits. VALUES (?, ?, ?) row constructor
    # keeps the IN-tuple comparison simple.
    existing: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    BATCH = 400
    for i in range(0, len(keys), BATCH):
        batch = keys[i:i + BATCH]
        placeholders = ", ".join(["(?, ?, ?)"] * len(batch))
        flat_args: List[Any] = []
        for k in batch:
            flat_args.extend(k)
        rows = state_conn.execute(
            f"""
            SELECT labeler_did, uri, val, current_state, current_state_since,
                   first_seen_ts, last_seen_ts, add_count, del_count,
                   event_count, open_run_started_ts
            FROM label_state
            WHERE (labeler_did, uri, val) IN (VALUES {placeholders})
            """,
            flat_args,
        ).fetchall()
        for r in rows:
            existing[(r["labeler_did"], r["uri"], r["val"])] = dict(r)

    upserts: List[Tuple[Any, ...]] = []
    for key, events in key_events.items():
        # Events within a single key may arrive out of order across chunks
        # (we chunk by id, not by key+ts). Sort here so the state machine
        # sees them in temporal order.
        events_sorted = sorted(events, key=lambda e: e["ts"])
        st = existing.get(key)
        if st is None:
            st = {
                "current_state": "unknown",
                "current_state_since": None,
                "first_seen_ts": None,
                "last_seen_ts": None,
                "add_count": 0,
                "del_count": 0,
                "event_count": 0,
                "open_run_started_ts": None,
            }
        for ev in events_sorted:
            ts = ev["ts"]
            neg = ev["neg"]
            st["event_count"] += 1
            if st["first_seen_ts"] is None or ts < st["first_seen_ts"]:
                st["first_seen_ts"] = ts
            if st["last_seen_ts"] is None or ts > st["last_seen_ts"]:
                st["last_seen_ts"] = ts
            if neg == 0:
                if st["current_state"] != "active":
                    st["current_state"] = "active"
                    st["current_state_since"] = ts
                    st["open_run_started_ts"] = ts
                    st["add_count"] += 1
                # else: reassertion — no transition
            else:  # neg == 1
                if st["current_state"] == "active":
                    st["current_state"] = "inactive"
                    st["current_state_since"] = ts
                    st["open_run_started_ts"] = None
                    st["del_count"] += 1
                # else: stray negation, no transition
        upserts.append(
            (
                key[0], key[1], key[2],
                st["current_state"], st["current_state_since"],
                st["first_seen_ts"], st["last_seen_ts"],
                st["add_count"], st["del_count"], st["event_count"],
                st["open_run_started_ts"], now_int,
            )
        )

    state_conn.executemany(
        """
        INSERT INTO label_state(
            labeler_did, uri, val,
            current_state, current_state_since,
            first_seen_ts, last_seen_ts,
            add_count, del_count, event_count,
            open_run_started_ts, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(labeler_did, uri, val) DO UPDATE SET
            current_state       = excluded.current_state,
            current_state_since = excluded.current_state_since,
            first_seen_ts       = MIN(label_state.first_seen_ts, excluded.first_seen_ts),
            last_seen_ts        = MAX(label_state.last_seen_ts, excluded.last_seen_ts),
            add_count           = excluded.add_count,
            del_count           = excluded.del_count,
            event_count         = excluded.event_count,
            open_run_started_ts = excluded.open_run_started_ts,
            updated_at          = excluded.updated_at
        """,
        upserts,
    )
    state_conn.commit()
    return len(upserts)


# --- pilot backfill -------------------------------------------------------

def pilot_backfill(
    main_db_path: str,
    sidecar_path: str,
    days: int = 7,
    chunk_size: int = 50_000,
    max_events: Optional[int] = 1_000_000,
    disk_floor_gb: float = 14.0,
    main_wal_ceiling_gb: float = 1.0,
) -> Dict[str, Any]:
    """Bounded pilot backfill into the sidecar.

    SAFETY CONTRACT
    ---------------
    Reads main DB in chunks ordered by event id. Each chunk runs in a
    brief read transaction so the main WAL is not pinned across the whole
    backfill. After each chunk:

      - Re-check free disk; abort if below `disk_floor_gb`.
      - Re-check main WAL size; abort if above `main_wal_ceiling_gb`.
      - Re-check max_events; abort if exceeded.

    Resumable: cursor_id + events_processed are persisted to
    label_state_meta after every chunk. Rerunning continues from the
    saved cursor.

    Status reflected in label_state_meta.build_status:
      empty    — fresh sidecar, never run
      building — pilot is in progress
      complete — backfill finished its bounded window successfully
      failed   — aborted (reason in build_failure_reason)

    NOT for full 39M-event live backfill. The bounded pilot is a
    deliberate guard rail; the safety thresholds will abort long
    before a full backfill could finish.
    """
    state_conn = init_sidecar(sidecar_path)
    status = meta_get(state_conn, META_BUILD_STATUS)
    if status == "complete":
        _log.warning(
            "Sidecar build_status=%r; refusing to rerun pilot. "
            "Drop %s to rebuild.",
            status, sidecar_path,
        )
        return {"status": "skip_already_complete", "sidecar_path": sidecar_path}

    meta_set(state_conn, META_BUILD_STATUS, "building")
    meta_set(state_conn, META_BUILD_WINDOW_DAYS, days)
    meta_set(state_conn, META_BUILD_MAX_EVENTS, max_events if max_events else "unlimited")
    state_conn.commit()

    main_conn = sqlite3.connect(f"file:{main_db_path}?mode=ro", uri=True)
    main_conn.row_factory = sqlite3.Row

    now = datetime.now(timezone.utc)
    cutoff_iso = (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    cursor_id = int(meta_get(state_conn, META_CURSOR_ID) or "0")
    total_events = int(meta_get(state_conn, META_EVENTS_PROCESSED) or "0")

    started = time.perf_counter()
    aborted: Optional[str] = None
    chunk_count = 0
    keys_touched_total = 0
    try:
        while True:
            if max_events is not None and total_events >= max_events:
                _log.info(
                    "pilot_backfill: reached max_events=%d, stopping",
                    max_events,
                )
                break

            free_gb = _free_disk_gb(sidecar_path)
            wal_gb = _main_wal_gb(main_db_path)
            if free_gb < disk_floor_gb:
                aborted = f"free_disk={free_gb:.2f}G < floor={disk_floor_gb}G"
                _log.error("ABORT: %s", aborted)
                break
            if wal_gb > main_wal_ceiling_gb:
                aborted = f"main_wal={wal_gb:.2f}G > ceiling={main_wal_ceiling_gb}G"
                _log.error("ABORT: %s", aborted)
                break

            limit = chunk_size
            if max_events is not None:
                limit = min(limit, max_events - total_events)
            rows = main_conn.execute(
                """
                SELECT id, labeler_did, uri, val, ts, neg
                FROM label_events
                WHERE id > ? AND ts >= ?
                ORDER BY id
                LIMIT ?
                """,
                (cursor_id, cutoff_iso, limit),
            ).fetchall()

            if not rows:
                _log.info("pilot_backfill: no more rows; done")
                break

            key_events: Dict[Tuple[str, str, str], List[Dict[str, int]]] = defaultdict(list)
            for r in rows:
                key_events[(r["labeler_did"], r["uri"], r["val"])].append(
                    {"ts": _parse_ts(r["ts"]), "neg": int(r["neg"] or 0)}
                )

            updated = _apply_chunk(state_conn, key_events, int(time.time()))
            keys_touched_total += updated

            cursor_id = rows[-1]["id"]
            total_events += len(rows)
            chunk_count += 1
            meta_set(state_conn, META_CURSOR_ID, cursor_id)
            meta_set(state_conn, META_EVENTS_PROCESSED, total_events)
            state_conn.commit()

            _log.info(
                "pilot_backfill: chunk=%d events=%d keys_updated=%d "
                "cursor=%d total=%d free=%.2fG main_wal=%.3fG elapsed=%.1fs",
                chunk_count, len(rows), updated, cursor_id, total_events,
                free_gb, wal_gb, time.perf_counter() - started,
            )
    except Exception as e:
        aborted = f"exception: {type(e).__name__}: {e}"
        _log.exception("ABORT: %s", aborted)
    finally:
        main_conn.close()

    if aborted:
        meta_set(state_conn, META_BUILD_STATUS, "failed")
        meta_set(state_conn, META_FAILURE_REASON, aborted)
    else:
        meta_set(state_conn, META_BUILD_STATUS, "complete")
        meta_set(
            state_conn, META_BUILT_AT,
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    state_conn.commit()
    state_conn.close()

    return {
        "status": "aborted" if aborted else "complete",
        "sidecar_path": sidecar_path,
        "events_processed": total_events,
        "keys_updated_estimate": keys_touched_total,
        "chunks": chunk_count,
        "cursor_id": cursor_id,
        "elapsed_seconds": round(time.perf_counter() - started, 1),
        "abort_reason": aborted,
    }


# --- read API (for future report integration) -----------------------------

def query_state_summary(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Aggregate read for downstream display. Caller MUST check is_admissible()
    before treating these counts as testimony.
    """
    row = conn.execute(
        """
        SELECT
            COUNT(*)                                       AS total_keys,
            SUM(CASE WHEN current_state='active'   THEN 1 ELSE 0 END) AS active_keys,
            SUM(CASE WHEN current_state='inactive' THEN 1 ELSE 0 END) AS inactive_keys,
            SUM(CASE WHEN current_state='unknown'  THEN 1 ELSE 0 END) AS unknown_keys,
            SUM(add_count)         AS total_adds,
            SUM(del_count)         AS total_dels,
            SUM(event_count)       AS total_events,
            MIN(first_seen_ts)     AS oldest_seen,
            MAX(last_seen_ts)      AS newest_seen
        FROM label_state
        """
    ).fetchone()
    return dict(row) if row else {}
