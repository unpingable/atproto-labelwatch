from __future__ import annotations

import ctypes
import gc
import json
import logging
import os
import threading
import time
import urllib.error
from pathlib import Path
from typing import Optional

from . import db, discover, ingest, report as report_mod, resolve, scan
from .config import Config
from .utils import format_ts, now_utc

log = logging.getLogger(__name__)

_DERIVE_MEMORY_HIGH_RATIO = float(
    os.environ.get("LABELWATCH_DERIVE_MEMORY_HIGH_RATIO", "0.90")
)
_DERIVE_MEMORY_HIGH_EVENT_DELTA = int(
    os.environ.get("LABELWATCH_DERIVE_MEMORY_HIGH_EVENT_DELTA", "1000")
)


def _read_cgroup_memory_snapshot() -> dict[str, int] | None:
    """Read this process's cgroup-v2 memory pressure counters.

    Absence, cgroup v1, and an unlimited ``memory.high`` are all treated as
    unavailable rather than as pressure. This is an operational guard, not a
    reason to make Labelwatch Linux-only.
    """
    try:
        cgroup_path = None
        for line in Path("/proc/self/cgroup").read_text(encoding="utf-8").splitlines():
            hierarchy, controllers, path = line.split(":", 2)
            if hierarchy == "0" and not controllers:
                cgroup_path = path
                break
        if cgroup_path is None:
            return None
        memory_dir = Path("/sys/fs/cgroup") / cgroup_path.lstrip("/")
        high_raw = (memory_dir / "memory.high").read_text(encoding="ascii").strip()
        if high_raw == "max":
            return None
        events = {}
        for line in (memory_dir / "memory.events").read_text(encoding="ascii").splitlines():
            key, value = line.split()
            events[key] = int(value)
        return {
            "current": int((memory_dir / "memory.current").read_text(encoding="ascii")),
            "high": int(high_raw),
            "high_events": events.get("high", 0),
        }
    except (OSError, ValueError):
        return None


def _derive_memory_pressure_reason(
    snapshot: dict[str, int] | None,
    previous_high_events: int | None,
) -> str | None:
    """Return a stable reason when derive should yield to primary work."""
    if not snapshot or snapshot["high"] <= 0:
        return None
    ratio = snapshot["current"] / snapshot["high"]
    event_delta = (
        max(0, snapshot["high_events"] - previous_high_events)
        if previous_high_events is not None
        else 0
    )
    reasons = []
    if ratio >= _DERIVE_MEMORY_HIGH_RATIO:
        reasons.append(
            f"current_ratio:{ratio:.3f}>={_DERIVE_MEMORY_HIGH_RATIO:.3f}"
        )
    if event_delta >= _DERIVE_MEMORY_HIGH_EVENT_DELTA:
        reasons.append(
            f"high_events_delta:{event_delta}>={_DERIVE_MEMORY_HIGH_EVENT_DELTA}"
        )
    return ",".join(reasons) or None


def _record_derive_deferred(conn, reason: str) -> None:
    observed_at = format_ts(now_utc())
    previous = db.get_meta(conn, "ops:derive:deferred_count")
    try:
        count = int(previous or "0") + 1
    except ValueError:
        count = 1
    db.set_meta(conn, "ops:derive:deferred_at", observed_at)
    db.set_meta(conn, "ops:derive:deferred_reason", reason)
    db.set_meta(conn, "ops:derive:deferred_count", str(count))
    conn.commit()
    log.warning("derive.deferred reason=memory_pressure detail=%s count=%d", reason, count)


def _record_derive_outcome(conn, outcome: dict) -> bool:
    """Retain completed/failed/pending/skipped work without a false success tick."""
    db.set_meta(conn, 'ops:derive:last_attempt_at', format_ts(now_utc()))
    db.set_meta(conn, 'ops:derive:last_outcome', json.dumps(outcome, sort_keys=True))
    complete = outcome.get('state') == 'COMPLETE'
    if complete:
        db.set_meta(conn, 'last_derive_ok_ts', format_ts(now_utc()))
    conn.commit()
    return complete


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


def _wal_size_mb(db_path: str) -> float:
    try:
        return os.path.getsize(db_path + "-wal") / (1024 * 1024)
    except OSError:
        return 0.0


def _report_loop(
    db_path: str,
    report_out: str,
    interval: int,
    facts_path: str = "",
    wal_skip_mb: float = 80.0,
    coverage_window_minutes: Optional[int] = None,
    coverage_threshold: Optional[float] = None,
) -> None:
    """Dedicated report generation thread.

    Uses its own readonly DB connection so report generation is never
    blocked by long-running ingest passes in the main loop.

    Report freshness is subordinate to discovery ingest: skip when WAL is
    over wal_skip_mb (writer/checkpoint contention) and re-evaluate next cycle.
    """
    log.info(
        "Report thread started (interval=%ds, wal_skip=%.0fMB, out=%s)",
        interval, wal_skip_mb, report_out,
    )
    while True:
        wal_mb = _wal_size_mb(db_path)
        if wal_mb > wal_skip_mb:
            log.warning(
                "Report skipped: WAL=%.1fMB > %.0fMB (writer pressure, deferring)",
                wal_mb, wal_skip_mb,
            )
            time.sleep(interval)
            continue
        try:
            conn = db.connect(db_path, readonly=True)
            try:
                # Rebuild the coverage contract the scan pass is using, so the
                # report's observation-adequacy verdict matches the rules'.
                report_cfg = Config()
                if coverage_window_minutes is not None:
                    report_cfg.coverage_window_minutes = coverage_window_minutes
                if coverage_threshold is not None:
                    report_cfg.coverage_threshold = coverage_threshold
                report_mod.generate_report(
                    conn, report_out, now=now_utc(),
                    facts_path=facts_path or None, config=report_cfg,
                )
            finally:
                conn.close()
            log.info("Report generated successfully (WAL=%.1fMB at start)", wal_mb)
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
    report_interval: Optional[int] = None,
) -> None:
    from .maintenance_hold import wait_before_writer_start
    wait_before_writer_start(cfg.db_path, 'main')
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    last_ingest = 0.0
    last_scan = 0.0
    last_derive = 0.0
    last_discovery = 0.0
    discovery_interval = cfg.discovery_interval_hours * 3600
    derive_interval = cfg.derive_interval_minutes * 60
    derive_memory_snapshot = _read_cgroup_memory_snapshot()
    derive_high_events = (
        derive_memory_snapshot["high_events"] if derive_memory_snapshot else None
    )
    primary_ingest_disabled = False

    # Start report generation on its own thread so it's never blocked by ingest.
    # Report freshness is subordinate to discovery ingest (see gap-spec
    # report-generation-workload-isolation): long readonly snapshots pin the WAL
    # and starve discovery writes, so we run reports infrequently and gate on
    # WAL pressure.
    if report_out:
        eff_interval = report_interval if report_interval is not None else 1800
        eff_interval = max(eff_interval, 300)
        wal_skip_mb = float(os.environ.get("LABELWATCH_REPORT_WAL_SKIP_MB", "80"))
        t = threading.Thread(
            target=_report_loop,
            args=(cfg.db_path, report_out, eff_interval,
                  cfg.driftwatch_facts_path, wal_skip_mb,
                  cfg.coverage_window_minutes, cfg.coverage_threshold),
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
                    derive_memory_snapshot = _read_cgroup_memory_snapshot()
                    pressure_reason = _derive_memory_pressure_reason(
                        derive_memory_snapshot, derive_high_events
                    )
                    if derive_memory_snapshot:
                        derive_high_events = derive_memory_snapshot["high_events"]
                    if pressure_reason:
                        _record_derive_deferred(conn, pressure_reason)
                    else:
                        outcome = scan.run_derive(conn, cfg, now=scan_time)
                        _record_derive_outcome(conn, outcome)
                        last_derive = now_mono
                        _release_memory(conn)
            except Exception:
                log.error("Scan/derive failed", exc_info=True)
            _release_memory(conn)
            last_scan = now_mono

        _sleep_until(last_ingest + ingest_interval, last_scan + scan_interval)
