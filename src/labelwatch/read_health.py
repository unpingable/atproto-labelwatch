"""Read-side health tracking for outbound HTTP calls.

Tracks success/failure/latency of calls to external APIs (AppView, PLC
directory) so we can detect "process alive, purpose deceased" scenarios
where the server is up but its data sources are degraded.

Thread-safe. Designed for use from the API server's request threads.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Outcome recording
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReadOutcome:
    """Single outbound read attempt."""
    endpoint: str          # e.g. "handle_resolve", "profile_fetch", "did_doc"
    ok: bool
    latency_ms: float
    ts: float              # monotonic timestamp
    empty: bool = False    # success but empty/null response
    error: str = ""        # error description if not ok


class EndpointStats:
    """Sliding-window stats for one endpoint category."""

    def __init__(self, window_seconds: int = 600, max_entries: int = 200):
        self._window = window_seconds
        self._max = max_entries
        self._lock = threading.Lock()
        self._outcomes: deque[ReadOutcome] = deque(maxlen=max_entries)

    def record(self, outcome: ReadOutcome) -> None:
        with self._lock:
            self._outcomes.append(outcome)

    def snapshot(self) -> Dict[str, Any]:
        """Compute stats over the sliding window."""
        now = time.monotonic()
        cutoff = now - self._window

        with self._lock:
            recent = [o for o in self._outcomes if o.ts >= cutoff]

        if not recent:
            return {
                "calls": 0,
                "success_rate": None,
                "empty_rate": None,
                "avg_latency_ms": None,
                "p95_latency_ms": None,
                "last_success": None,
                "last_failure": None,
                "last_error": None,
            }

        total = len(recent)
        ok_count = sum(1 for o in recent if o.ok)
        empty_count = sum(1 for o in recent if o.ok and o.empty)
        latencies = sorted(o.latency_ms for o in recent)
        p95_idx = min(int(total * 0.95), total - 1)

        successes = [o for o in recent if o.ok]
        failures = [o for o in recent if not o.ok]

        return {
            "calls": total,
            "success_rate": round(ok_count / total, 3) if total else None,
            "empty_rate": round(empty_count / ok_count, 3) if ok_count else None,
            "avg_latency_ms": round(sum(latencies) / total, 1),
            "p95_latency_ms": round(latencies[p95_idx], 1),
            "last_success": successes[-1].ts if successes else None,
            "last_failure": failures[-1].ts if failures else None,
            "last_error": failures[-1].error if failures else None,
        }


# ---------------------------------------------------------------------------
# Global tracker
# ---------------------------------------------------------------------------

class ReadHealthTracker:
    """Global tracker for all outbound read health."""

    def __init__(self, window_seconds: int = 600):
        self._window = window_seconds
        self._lock = threading.Lock()
        self._endpoints: Dict[str, EndpointStats] = {}
        self._boot_time = time.monotonic()

    def _get_stats(self, endpoint: str) -> EndpointStats:
        with self._lock:
            if endpoint not in self._endpoints:
                self._endpoints[endpoint] = EndpointStats(self._window)
            return self._endpoints[endpoint]

    def record(self, endpoint: str, ok: bool, latency_ms: float,
               empty: bool = False, error: str = "") -> None:
        """Record an outbound read outcome."""
        outcome = ReadOutcome(
            endpoint=endpoint,
            ok=ok,
            latency_ms=latency_ms,
            ts=time.monotonic(),
            empty=empty,
            error=error[:200],
        )
        self._get_stats(endpoint).record(outcome)

    def snapshot(self) -> Dict[str, Any]:
        """Full health snapshot across all endpoints."""
        with self._lock:
            endpoints = dict(self._endpoints)

        per_endpoint = {}
        for name, stats in endpoints.items():
            per_endpoint[name] = stats.snapshot()

        # Aggregate verdict
        all_rates = [
            s["success_rate"] for s in per_endpoint.values()
            if s["success_rate"] is not None
        ]
        if not all_rates:
            verdict = "NO_DATA"
        elif min(all_rates) < 0.5:
            verdict = "DEGRADED"
        elif min(all_rates) < 0.9:
            verdict = "WARN"
        else:
            verdict = "OK"

        return {
            "verdict": verdict,
            "uptime_seconds": round(time.monotonic() - self._boot_time, 0),
            "endpoints": per_endpoint,
        }


# Module-level singleton
_tracker: Optional[ReadHealthTracker] = None
_tracker_lock = threading.Lock()


def get_tracker() -> ReadHealthTracker:
    """Get or create the global read health tracker."""
    global _tracker
    if _tracker is None:
        with _tracker_lock:
            if _tracker is None:
                _tracker = ReadHealthTracker()
    return _tracker


def reset_tracker() -> None:
    """Reset the global tracker (for testing)."""
    global _tracker
    with _tracker_lock:
        _tracker = None


# ---------------------------------------------------------------------------
# Instrumented HTTP helper
# ---------------------------------------------------------------------------

def tracked_urlopen(endpoint: str, url: str, timeout: int = 10,
                    headers: Optional[Dict[str, str]] = None) -> Optional[bytes]:
    """urlopen wrapper that records outcome to the read health tracker.

    Returns response body bytes on success, None on failure.
    Caller decides whether None/empty constitutes an error for their use case.
    """
    import urllib.request

    tracker = get_tracker()
    t0 = time.monotonic()

    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            latency = (time.monotonic() - t0) * 1000
            empty = len(body) == 0 or body.strip() in (b"", b"null", b"{}", b"[]")
            tracker.record(endpoint, ok=True, latency_ms=latency, empty=empty)
            return body
    except Exception as exc:
        latency = (time.monotonic() - t0) * 1000
        tracker.record(endpoint, ok=False, latency_ms=latency,
                       error=f"{type(exc).__name__}: {exc}")
        return None
