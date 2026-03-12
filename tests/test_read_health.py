"""Tests for read-side health tracking."""
import time
from unittest.mock import patch, MagicMock

from labelwatch.read_health import (
    EndpointStats,
    ReadHealthTracker,
    ReadOutcome,
    get_tracker,
    reset_tracker,
    tracked_urlopen,
)


def test_endpoint_stats_empty():
    stats = EndpointStats()
    snap = stats.snapshot()
    assert snap["calls"] == 0
    assert snap["success_rate"] is None


def test_endpoint_stats_records():
    stats = EndpointStats()
    now = time.monotonic()
    stats.record(ReadOutcome("test", ok=True, latency_ms=50.0, ts=now))
    stats.record(ReadOutcome("test", ok=True, latency_ms=100.0, ts=now))
    stats.record(ReadOutcome("test", ok=False, latency_ms=200.0, ts=now,
                             error="timeout"))

    snap = stats.snapshot()
    assert snap["calls"] == 3
    assert snap["success_rate"] == round(2 / 3, 3)
    assert snap["avg_latency_ms"] == round((50 + 100 + 200) / 3, 1)
    assert snap["last_error"] == "timeout"


def test_endpoint_stats_empty_detection():
    stats = EndpointStats()
    now = time.monotonic()
    stats.record(ReadOutcome("test", ok=True, latency_ms=10.0, ts=now, empty=True))
    stats.record(ReadOutcome("test", ok=True, latency_ms=10.0, ts=now, empty=False))

    snap = stats.snapshot()
    assert snap["empty_rate"] == 0.5


def test_tracker_verdict_ok():
    tracker = ReadHealthTracker()
    for _ in range(10):
        tracker.record("test", ok=True, latency_ms=50.0)
    snap = tracker.snapshot()
    assert snap["verdict"] == "OK"


def test_tracker_verdict_degraded():
    tracker = ReadHealthTracker()
    for _ in range(10):
        tracker.record("test", ok=False, latency_ms=50.0, error="down")
    snap = tracker.snapshot()
    assert snap["verdict"] == "DEGRADED"


def test_tracker_verdict_warn():
    tracker = ReadHealthTracker()
    for i in range(10):
        tracker.record("test", ok=(i < 8), latency_ms=50.0)
    snap = tracker.snapshot()
    assert snap["verdict"] == "WARN"


def test_tracker_verdict_no_data():
    tracker = ReadHealthTracker()
    snap = tracker.snapshot()
    assert snap["verdict"] == "NO_DATA"


def test_tracker_multiple_endpoints():
    tracker = ReadHealthTracker()
    tracker.record("handle_resolve", ok=True, latency_ms=30.0)
    tracker.record("profile_fetch", ok=False, latency_ms=500.0, error="timeout")

    snap = tracker.snapshot()
    assert "handle_resolve" in snap["endpoints"]
    assert "profile_fetch" in snap["endpoints"]
    assert snap["endpoints"]["handle_resolve"]["success_rate"] == 1.0
    assert snap["endpoints"]["profile_fetch"]["success_rate"] == 0.0


def test_global_singleton():
    reset_tracker()
    t1 = get_tracker()
    t2 = get_tracker()
    assert t1 is t2
    reset_tracker()


def test_tracked_urlopen_success():
    reset_tracker()
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"did": "did:plc:test"}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        body = tracked_urlopen("test_ep", "https://example.com/test")
        assert body == b'{"did": "did:plc:test"}'

    snap = get_tracker().snapshot()
    assert snap["endpoints"]["test_ep"]["calls"] == 1
    assert snap["endpoints"]["test_ep"]["success_rate"] == 1.0
    reset_tracker()


def test_tracked_urlopen_failure():
    reset_tracker()
    with patch("urllib.request.urlopen", side_effect=ConnectionError("refused")):
        body = tracked_urlopen("test_ep", "https://example.com/fail")
        assert body is None

    snap = get_tracker().snapshot()
    assert snap["endpoints"]["test_ep"]["calls"] == 1
    assert snap["endpoints"]["test_ep"]["success_rate"] == 0.0
    assert "ConnectionError" in snap["endpoints"]["test_ep"]["last_error"]
    reset_tracker()
