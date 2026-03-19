from __future__ import annotations

import ctypes
import gc
import logging
import threading
import time
import urllib.error
from typing import Optional

from . import db, discover, ingest, report as report_mod, resolve, scan
from .config import Config
from .utils import format_ts, now_utc

log = logging.getLogger(__name__)


def _sleep_until(next_ingest: float, next_scan: float) -> None:
    next_due = min(next_ingest, next_scan)
    delay = max(1.0, next_due - time.monotonic())
    time.sleep(min(delay, 60.0))


def _heartbeat(conn, key: str) -> None:
    """Write a heartbeat timestamp to meta for observability."""
    db.set_meta(conn, key, format_ts(now_utc()))
    conn.commit()


def _rss_mb() -> str:
    """Read RSS from /proc/self/status (Linux only)."""
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    return f"{kb / 1024:.1f}MB"
    except (FileNotFoundError, ValueError):
        pass
    return "n/a"


def _release_memory(conn) -> None:
    """Force Python + SQLite to release memory back to OS."""
    gc.collect()
    conn.execute("PRAGMA shrink_memory")
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except (OSError, AttributeError):
        pass  # Not on Linux or libc not found
    log.info("rss=%s", _rss_mb())


def _report_loop(db_path: str, report_out: str, interval: int, facts_path: str = "") -> None:
    """Dedicated report generation thread.

    Uses its own readonly DB connection so report generation is never
    blocked by long-running ingest passes in the main loop.
    """
    log.info("Report thread started (interval=%ds, out=%s)", interval, report_out)
    while True:
        try:
            conn = db.connect(db_path, readonly=True)
            try:
                report_mod.generate_report(conn, report_out, now=now_utc(), facts_path=facts_path or None)
            finally:
                conn.close()
            log.info("Report generated successfully")
            # Heartbeat via a separate writable connection
            try:
                wconn = db.connect(db_path)
                _heartbeat(wconn, "last_report_ok_ts")
                wconn.close()
            except Exception:
                pass  # non-critical
        except Exception:
            log.error("Report generation failed", exc_info=True)
        time.sleep(interval)


def run_loop(
    cfg: Config,
    ingest_interval: int,
    scan_interval: int,
    report_out: Optional[str] = None,
) -> None:
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    last_ingest = 0.0
    last_scan = 0.0
    last_derive = 0.0
    last_discovery = 0.0
    discovery_interval = cfg.discovery_interval_hours * 3600
    derive_interval = cfg.derive_interval_minutes * 60
    primary_ingest_disabled = False

    # Start report generation on its own thread so it's never blocked by ingest
    if report_out:
        report_interval = max(scan_interval, 300)  # at least every 5 min
        t = threading.Thread(
            target=_report_loop,
            args=(cfg.db_path, report_out, report_interval, cfg.driftwatch_facts_path),
            daemon=True,
            name="report-gen",
        )
        t.start()

    while True:
        now_mono = time.monotonic()

        # Discovery pass
        if cfg.discovery_enabled and now_mono - last_discovery >= discovery_interval:
            try:
                discover.run_discovery(conn, cfg)
                _heartbeat(conn, "last_discovery_ok_ts")
            except Exception:
                log.warning("Discovery failed", exc_info=True)
            last_discovery = now_mono

        # Ingest pass
        if ingest_interval > 0 and now_mono - last_ingest >= ingest_interval:
            if not cfg.labeler_dids and not cfg.discovery_enabled:
                raise SystemExit("labeler_dids must be configured for ingest")

            # Primary ingest (queryLabels via bsky.social aggregator)
            if cfg.labeler_dids and not primary_ingest_disabled:
                try:
                    ingest.ingest_from_service(conn, cfg)
                except urllib.error.HTTPError as exc:
                    if exc.code == 401:
                        log.warning(
                            "Primary ingest returned 401 — endpoint requires auth. "
                            "Disabling; multi-ingest will handle all labelers."
                        )
                        primary_ingest_disabled = True
                    else:
                        log.error("Primary ingest failed", exc_info=True)
                except Exception:
                    log.error("Primary ingest failed", exc_info=True)

            # Multi-source ingest from discovered labelers (runs even if primary fails)
            if cfg.discovery_enabled:
                try:
                    ingest.ingest_multi(conn, cfg)
                except Exception:
                    log.error("Multi-ingest failed", exc_info=True)

            try:
                resolve.resolve_handles_for_labelers(conn)
                _heartbeat(conn, "last_ingest_ok_ts")
            except Exception:
                log.error("Resolve/heartbeat failed", exc_info=True)
            _release_memory(conn)
            last_ingest = now_mono

        # Scan + derive pass (report moved to its own thread)
        if scan_interval > 0 and now_mono - last_scan >= scan_interval:
            try:
                scan_time = now_utc()
                scan.run_scan(conn, cfg, now=scan_time)
                _heartbeat(conn, "last_scan_ok_ts")
                _release_memory(conn)

                # Derive pass (expensive — runs on its own interval)
                if now_mono - last_derive >= derive_interval:
                    scan.run_derive(conn, cfg, now=scan_time)
                    _heartbeat(conn, "last_derive_ok_ts")
                    last_derive = now_mono
                    _release_memory(conn)
            except Exception:
                log.error("Scan/derive failed", exc_info=True)
            _release_memory(conn)
            last_scan = now_mono

        _sleep_until(last_ingest + ingest_interval, last_scan + scan_interval)
