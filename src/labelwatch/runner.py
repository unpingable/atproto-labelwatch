from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from . import db, discover, ingest, report as report_mod, resolve, scan
from .config import Config
from .utils import now_utc

log = logging.getLogger(__name__)


def _sleep_until(next_ingest: float, next_scan: float) -> None:
    next_due = min(next_ingest, next_scan)
    delay = max(1.0, next_due - time.monotonic())
    time.sleep(min(delay, 60.0))


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
    last_discovery = 0.0
    discovery_interval = cfg.discovery_interval_hours * 3600

    while True:
        now_mono = time.monotonic()

        # Discovery pass
        if cfg.discovery_enabled and now_mono - last_discovery >= discovery_interval:
            try:
                discover.run_discovery(conn, cfg)
            except Exception:
                log.warning("Discovery failed", exc_info=True)
            last_discovery = now_mono

        if ingest_interval > 0 and now_mono - last_ingest >= ingest_interval:
            if not cfg.labeler_dids:
                raise SystemExit("labeler_dids must be configured for ingest")
            ingest.ingest_from_service(conn, cfg)

            # Multi-source ingest from discovered labelers
            if cfg.discovery_enabled:
                try:
                    ingest.ingest_multi(conn, cfg)
                except Exception:
                    log.warning("Multi-ingest failed", exc_info=True)

            resolve.resolve_handles_for_labelers(conn)
            last_ingest = now_mono

        if scan_interval > 0 and now_mono - last_scan >= scan_interval:
            scan_time = now_utc()
            scan.run_scan(conn, cfg, now=scan_time)
            if report_out:
                report_mod.generate_report(conn, report_out, now=scan_time)
            last_scan = now_mono

        _sleep_until(last_ingest + ingest_interval, last_scan + scan_interval)
