from __future__ import annotations

import ctypes
import gc
import logging
import time
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


def _release_memory(conn) -> None:
    """Force Python + SQLite to release memory back to OS."""
    gc.collect()
    conn.execute("PRAGMA shrink_memory")
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except (OSError, AttributeError):
        pass  # Not on Linux or libc not found


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
            try:
                if not cfg.labeler_dids:
                    raise SystemExit("labeler_dids must be configured for ingest")
                ingest.ingest_from_service(conn, cfg)

                # Multi-source ingest from discovered labelers
                if cfg.discovery_enabled:
                    ingest.ingest_multi(conn, cfg)

                resolve.resolve_handles_for_labelers(conn)
                _heartbeat(conn, "last_ingest_ok_ts")
            except SystemExit:
                raise
            except Exception:
                log.error("Ingest failed", exc_info=True)
            _release_memory(conn)
            last_ingest = now_mono

        # Scan + report pass
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

                if report_out:
                    report_mod.generate_report(conn, report_out, now=scan_time)
                    _heartbeat(conn, "last_report_ok_ts")
            except Exception:
                log.error("Scan/report failed", exc_info=True)
            _release_memory(conn)
            last_scan = now_mono

        _sleep_until(last_ingest + ingest_interval, last_scan + scan_interval)
