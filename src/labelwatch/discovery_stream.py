"""Jetstream-based live discovery of ATProto labelers.

Connects to Jetstream WebSocket, watches for app.bsky.labeler.service
creates/updates/deletes. Runs as a separate process from the main
ingest/scan loop for failure isolation.

Architecture invariants:
  - Single worker task does ALL DB writes (single-writer guarantee).
  - SQLITE_BUSY/SQLITE_LOCKED in the worker is recoverable contention, not a
    write failure: bounded retry with exponential backoff, drop the item and
    continue if exhausted. Other sqlite3.Error is fatal (re-raised via
    fatal_error). "Crash loud" applies to genuine corruption/IO/schema
    errors, not to losing a brief race for the write lock.
  - Backstop loop serialized with asyncio.Lock to prevent overlap.
  - Dedupe key is (labeler_did, commit_rev, operation) — anchored on commit
    identity, not time_us, so reconnect rewind can't create duplicates even
    if time_us shifts.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import websockets
import websockets.exceptions

from . import db
from .discover import backstop_from_lists, upsert_discovered_labeler
from .resolve import fetch_did_doc, resolve_label_key, resolve_service_endpoint
from .utils import format_ts, hash_sha256, now_utc, stable_json

log = logging.getLogger(__name__)

JETSTREAM_URL = os.environ.get(
    "JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe")
WANTED_COLLECTIONS = ["app.bsky.labeler.service"]
CURSOR_SAVE_INTERVAL = 60  # seconds
CURSOR_REWIND_US = 3_000_000  # 3s rewind on reconnect
IDENTITY_COOLDOWN = 1800  # 30 min cooldown per DID for identity refreshes
STATS_INTERVAL = 60  # seconds


def _build_ws_url(cursor: Optional[int] = None) -> str:
    """Build Jetstream WebSocket URL with collection filter."""
    params = "&".join(
        f"wantedCollections={c}" for c in WANTED_COLLECTIONS
    )
    url = f"{JETSTREAM_URL}?{params}"
    if cursor is not None:
        resume_cursor = max(0, cursor - CURSOR_REWIND_US)
        url += f"&cursor={resume_cursor}"
    return url


def _load_known_labelers(conn) -> set:
    """Load all known labeler DIDs from DB into memory."""
    rows = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    return {r["labeler_did"] for r in rows}


class _Stats:
    """Simple counters for STATS line."""

    def __init__(self):
        self.msgs = 0
        self.discoveries = 0
        self.identity_refreshes = 0
        self.deletes = 0
        self.errors = 0
        self.started_at = time.monotonic()

    def log(self, cursor: Optional[int]):
        uptime = int(time.monotonic() - self.started_at)
        log.info(
            "STATS msgs=%d discoveries=%d identity_refreshes=%d deletes=%d "
            "errors=%d cursor=%s uptime=%ds",
            self.msgs, self.discoveries, self.identity_refreshes,
            self.deletes, self.errors,
            cursor if cursor is not None else "none",
            uptime,
        )


def _process_commit(msg: dict) -> Optional[dict]:
    """Extract labeler-relevant commit info, or None if irrelevant."""
    commit = msg.get("commit")
    if not commit:
        return None
    if commit.get("collection") != "app.bsky.labeler.service":
        return None
    if commit.get("rkey") != "self":
        return None
    return commit


def _resolve_did_sync(did: str, timeout: int = 10) -> dict:
    """Resolve DID doc and extract labeler info. Thread-safe (no DB)."""
    result = {"did": did, "handle": None, "endpoint": None,
              "has_label_key": False, "did_doc": None}
    doc = fetch_did_doc(did, timeout=timeout)
    if doc is None:
        return result
    result["did_doc"] = doc
    result["endpoint"] = resolve_service_endpoint(doc)
    result["has_label_key"] = resolve_label_key(doc)
    for aka in doc.get("alsoKnownAs", []):
        if aka.startswith("at://"):
            result["handle"] = aka[len("at://"):]
            break
    return result


_WORKER_BUSY_RETRY_MAX = 5
_WORKER_BUSY_BACKOFF_CAP_SEC = 60


async def _worker(conn, queue: asyncio.Queue, known_labelers: set,
                  stats: _Stats, fatal_error: asyncio.Event):
    """Process work items: DID resolution + DB writes.

    This is the ONLY task that writes to the DB (single-writer guarantee).
    SQLITE_BUSY/SQLITE_LOCKED is treated as recoverable contention with
    bounded retry. Other sqlite3 errors set fatal_error — "dead but
    optimistic" remains the worst failure mode for genuine corruption.
    """
    while True:
        item = await queue.get()
        try:
            for retry in range(_WORKER_BUSY_RETRY_MAX + 1):
                try:
                    kind = item.get("kind")
                    if kind == "discovery":
                        await _handle_discovery(conn, item, known_labelers, stats)
                    elif kind == "identity_refresh":
                        await _handle_identity_refresh(conn, item, stats)
                    break  # success
                except sqlite3.OperationalError as e:
                    err_str = str(e).lower()
                    if "locked" in err_str or "busy" in err_str:
                        if retry == _WORKER_BUSY_RETRY_MAX:
                            stats.errors += 1
                            log.error("Dropping %s after %d busy retries: %s",
                                      item.get("kind"), retry, e)
                            break
                        delay = min(2 ** retry, _WORKER_BUSY_BACKOFF_CAP_SEC) \
                            + random.uniform(0, 1)
                        log.warning("DB busy/locked in worker, retrying in %.1fs (%d/%d): %s",
                                    delay, retry + 1, _WORKER_BUSY_RETRY_MAX, e)
                        await asyncio.sleep(delay)
                        continue
                    log.critical("DB OperationalError in worker, forcing exit: %s", e)
                    fatal_error.set()
                    return
                except sqlite3.Error as e:
                    log.critical("DB error in worker, forcing exit: %s", e)
                    fatal_error.set()
                    return
        except Exception:
            stats.errors += 1
            log.exception("Worker error processing %s", item.get("kind"))
        finally:
            queue.task_done()


async def _handle_discovery(conn, item: dict, known_labelers: set, stats: _Stats):
    """Handle a labeler create/update/delete event."""
    did = item["did"]
    operation = item["operation"]
    commit = item["commit"]
    record = commit.get("record")
    time_us = item.get("time_us")
    seen_ts = format_ts(now_utc())

    if operation == "delete":
        # Log but don't delete — sticky
        db.insert_discovery_event(
            conn, did, "delete", "jetstream",
            discovered_at=seen_ts,
            time_us=time_us,
            commit_cid=commit.get("cid"),
            commit_rev=commit.get("rev"),
        )
        conn.commit()
        stats.deletes += 1
        log.warning("LABELER DELETE %s via jetstream", did[:40])
        return

    # create or update — resolve DID doc first (off event loop, no DB held)
    info = await asyncio.to_thread(_resolve_did_sync, did)

    record_json = stable_json(record) if record else None
    record_sha = hash_sha256(record_json) if record_json else None

    upsert_discovered_labeler(
        conn, did,
        handle=info["handle"],
        endpoint=info["endpoint"],
        has_service=info["endpoint"] is not None,
        has_label_key=info["has_label_key"],
        declared_record=True,
        seen_ts=seen_ts,
        evidence_source="jetstream",
    )
    db.insert_discovery_event(
        conn, did, operation, "jetstream",
        discovered_at=seen_ts,
        time_us=time_us,
        commit_cid=commit.get("cid"),
        commit_rev=commit.get("rev"),
        record_json=record_json,
        record_sha256=record_sha,
        resolved_endpoint=info["endpoint"],
    )
    conn.commit()

    known_labelers.add(did)
    stats.discoveries += 1
    log.info(
        "DISCOVERED %s via jetstream (%s) endpoint=%s",
        did[:40], operation, info["endpoint"],
    )


async def _handle_identity_refresh(conn, item: dict, stats: _Stats):
    """Re-resolve DID doc for a known labeler after identity event."""
    did = item["did"]
    info = await asyncio.to_thread(_resolve_did_sync, did)
    if info["did_doc"] is None:
        return

    seen_ts = format_ts(now_utc())
    upsert_discovered_labeler(
        conn, did,
        handle=info["handle"],
        endpoint=info["endpoint"],
        has_service=info["endpoint"] is not None,
        has_label_key=info["has_label_key"],
        seen_ts=seen_ts,
        evidence_source="jetstream_identity",
    )
    conn.commit()
    stats.identity_refreshes += 1
    log.debug("Identity refresh for %s", did[:40])


async def _stream_loop(conn, work_queue: asyncio.Queue,
                       known_labelers: set, stats: _Stats,
                       last_refresh: dict, fatal_error: asyncio.Event):
    """Connect to Jetstream and process messages until disconnect."""
    cursor_val = db.get_meta(conn, "jetstream_discovery_cursor")
    cursor = int(cursor_val) if cursor_val else None

    url = _build_ws_url(cursor)
    log.info("Connecting to %s", url)

    last_cursor_save = time.monotonic()
    last_stats_log = time.monotonic()
    last_msg_time_us = cursor

    async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
        async for raw in ws:
            # Check fatal_error every message — don't keep consuming if
            # the worker died on a DB error.
            if fatal_error.is_set():
                return

            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                stats.errors += 1
                log.debug("JSON parse error, skipping")
                continue

            stats.msgs += 1
            msg_time_us = msg.get("time_us")
            if msg_time_us is not None:
                last_msg_time_us = msg_time_us

            now_mono = time.monotonic()

            # Cursor save (every 60s)
            if now_mono - last_cursor_save >= CURSOR_SAVE_INTERVAL:
                if last_msg_time_us is not None:
                    db.set_meta(conn, "jetstream_discovery_cursor",
                                str(last_msg_time_us))
                    ts_iso = datetime.fromtimestamp(
                        last_msg_time_us / 1_000_000, tz=timezone.utc
                    ).isoformat()
                    db.set_meta(conn, "jetstream_discovery_last_msg_at", ts_iso)
                    conn.commit()
                last_cursor_save = now_mono

            # STATS (every 60s)
            if now_mono - last_stats_log >= STATS_INTERVAL:
                stats.log(last_msg_time_us)
                last_stats_log = now_mono

            kind = msg.get("kind")

            # Identity/account events → refresh known labelers
            if kind in ("identity", "account"):
                event_did = msg.get("did")
                if event_did and event_did in known_labelers:
                    last_t = last_refresh.get(event_did, 0)
                    if now_mono - last_t >= IDENTITY_COOLDOWN:
                        try:
                            work_queue.put_nowait({
                                "kind": "identity_refresh",
                                "did": event_did,
                            })
                            last_refresh[event_did] = now_mono
                        except asyncio.QueueFull:
                            pass  # drop, not critical
                continue

            if kind != "commit":
                continue

            commit = _process_commit(msg)
            if commit is None:
                continue

            operation = commit.get("operation")
            if operation not in ("create", "update", "delete"):
                continue

            did = msg.get("did")
            if not did:
                continue

            try:
                work_queue.put_nowait({
                    "kind": "discovery",
                    "did": did,
                    "operation": operation,
                    "commit": commit,
                    "time_us": msg_time_us,
                })
            except asyncio.QueueFull:
                log.warning("Work queue full, dropping discovery event for %s", did[:40])

    # Save final cursor on clean disconnect
    if last_msg_time_us is not None:
        db.set_meta(conn, "jetstream_discovery_cursor", str(last_msg_time_us))
        conn.commit()


async def _backstop_loop(conn, interval_hours: int, lock: asyncio.Lock):
    """Periodically run backstop_from_lists.

    Guarded by lock to prevent overlap if a slow scrape exceeds the interval.
    backstop_from_lists has its own 10-minute budget + per-call HTTP timeouts.
    """
    if interval_hours <= 0:
        return
    while True:
        await asyncio.sleep(interval_hours * 3600)
        if lock.locked():
            log.warning("Backstop still running from previous cycle, skipping")
            continue
        async with lock:
            try:
                log.info("Running backstop discovery from labeler-lists")
                await asyncio.to_thread(backstop_from_lists, conn)
            except Exception:
                log.exception("Backstop discovery failed")


async def run(db_path: str, backstop_interval_hours: int = 6):
    """Main entry point for discovery stream daemon."""
    conn = db.connect(db_path)
    conn.execute("PRAGMA busy_timeout=120000")  # 120s — generous for sensor daemon
    # Skip full init_db on large DBs. The main process owns schema migrations;
    # we just verify the version is sufficient.
    schema_ver = db.get_schema_version(conn)
    if schema_ver is None or schema_ver < 17:
        db.init_db(conn)
    else:
        log.info("Schema v%d OK, skipping init", schema_ver)

    known_labelers = _load_known_labelers(conn)
    log.info("Loaded %d known labelers", len(known_labelers))

    # Record startup — exponential backoff + jitter if main process holds
    # a write lock during batch ingest. Total budget ~3 min: must exceed the
    # main process's busy_timeout (120s) plus typical run_derive duration so
    # we don't give up before the natural lock window arrives.
    for _attempt in range(10):
        try:
            db.set_meta(conn, "jetstream_discovery_started_at", format_ts(now_utc()))
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and _attempt < 9:
                delay = min(0.5 * (2 ** _attempt), 30) + random.uniform(0, 1)
                log.warning("DB locked during startup, retrying in %.1fs (%d/10)", delay, _attempt + 1)
                await asyncio.sleep(delay)
            else:
                raise

    stats = _Stats()
    work_queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    last_refresh: dict = {}  # did -> monotonic time of last refresh
    backstop_lock = asyncio.Lock()

    # Fatal error: set by worker on DB write failure → forces process exit.
    # Without this, a dead worker means "cursor advances, discovery silently drops."
    fatal_error = asyncio.Event()

    # Shutdown handling
    shutdown_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def _signal_handler():
        log.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    worker_task = asyncio.create_task(
        _worker(conn, work_queue, known_labelers, stats, fatal_error))
    backstop_task = asyncio.create_task(
        _backstop_loop(conn, backstop_interval_hours, backstop_lock))

    def _check_task_health(task: asyncio.Task):
        """Callback: if worker or backstop dies unexpectedly, force exit."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.critical("Background task died: %s", exc)
            fatal_error.set()

    worker_task.add_done_callback(_check_task_health)
    backstop_task.add_done_callback(_check_task_health)

    try:
        while not shutdown_event.is_set() and not fatal_error.is_set():
            try:
                await _stream_loop(conn, work_queue, known_labelers,
                                   stats, last_refresh, fatal_error)
            except websockets.exceptions.ConnectionClosed:
                log.warning("Jetstream disconnected, reconnecting in 5s")
            except OSError as e:
                log.warning("Connection error: %s, reconnecting in 5s", e)
            except Exception:
                log.exception("Unexpected error in stream loop")

            if shutdown_event.is_set() or fatal_error.is_set():
                break
            await asyncio.sleep(5)
    finally:
        worker_task.cancel()
        backstop_task.cancel()

        # Final cursor save
        cursor_val = db.get_meta(conn, "jetstream_discovery_cursor")
        if cursor_val:
            log.info("Final cursor: %s", cursor_val)

        stats.log(int(cursor_val) if cursor_val else None)
        conn.close()

        if fatal_error.is_set():
            log.critical("Exiting due to fatal error (DB write failure)")
            sys.exit(1)

        log.info("Discovery stream shut down cleanly")
