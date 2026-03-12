"""Tests for the climate HTTP server."""
import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest

from labelwatch import db
from labelwatch.climate import public_climate_payload
from labelwatch.scan import _update_author_day, _update_author_labeler_day
from labelwatch.server import (
    ClimateHandler,
    _DiskCache,
    _TokenBucket,
    _validate_did,
    configure_handler,
)

TARGET = "did:plc:testauthor"
LABELER1 = "did:plc:labeler1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(tmp_path):
    """Create a test DB on disk (server needs a file path)."""
    db_path = str(tmp_path / "test.db")
    conn = db.connect(db_path)
    db.init_db(conn)
    return conn, db_path


def _day_iso(days_ago: int = 0):
    epoch = int(time.time()) - days_ago * 86400
    return time.strftime("%Y-%m-%dT12:00:00Z", time.gmtime(epoch))


def _seed_events(conn, events):
    for i, e in enumerate(events):
        conn.execute(
            "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                e["labeler_did"],
                e["uri"],
                e["val"],
                e.get("neg", 0),
                e["ts"],
                e.get("event_hash", f"hash_{i}_{time.monotonic_ns()}"),
                e["target_did"],
            ),
        )
    conn.commit()


def _seed_labeler(conn, labeler_did, handle=None, regime_state=None):
    conn.execute(
        "INSERT OR REPLACE INTO labelers(labeler_did, handle, regime_state) VALUES(?, ?, ?)",
        (labeler_did, handle, regime_state),
    )
    conn.commit()


def _seed_and_rollup(conn, events, labelers=None):
    _seed_events(conn, events)
    if labelers:
        for l in labelers:
            _seed_labeler(conn, **l)
    _update_author_day(conn)
    _update_author_labeler_day(conn)


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
def seeded_server(tmp_path):
    """Start a server with seeded data, yield (base_url, conn)."""
    conn, db_path = _make_db(tmp_path)
    events = [
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/abc",
         "val": "spam", "ts": _day_iso(1), "target_did": TARGET},
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/def",
         "val": "nsfw", "ts": _day_iso(2), "target_did": TARGET},
    ]
    _seed_and_rollup(conn, events, [{"labeler_did": LABELER1, "handle": "lab1.test"}])
    conn.close()

    port = _free_port()
    cache_dir = str(tmp_path / "cache")
    handler_cls = configure_handler(
        db_path, cache_dir, max_concurrent=2, rate_limit=100,
        generation_timeout=10,
    )
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    base = f"http://127.0.0.1:{port}"
    yield base
    server.shutdown()


def _get(url, expect_status=200):
    """GET a URL, return (status, headers, body_bytes)."""
    req = urllib.request.Request(url)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_endpoint(self, seeded_server):
        status, headers, body = _get(f"{seeded_server}/health")
        assert status == 200
        data = json.loads(body)
        assert data["ok"] is True
        assert headers.get("Cache-Control") == "no-store"


class TestClimateJSON:
    def test_climate_json(self, seeded_server):
        status, headers, body = _get(
            f"{seeded_server}/v1/climate/{TARGET}?format=json&window=30"
        )
        assert status == 200
        data = json.loads(body)
        assert data["target_did"] == TARGET
        assert data["window_days"] == 30
        # May be empty (rollup timing) or have summary
        assert "empty" in data or "summary" in data
        ct = headers.get("Content-Type", "")
        assert "application/json" in ct

    def test_climate_no_recent_receipts(self, seeded_server):
        status, _, body = _get(
            f"{seeded_server}/v1/climate/{TARGET}?format=json"
        )
        assert status == 200
        data = json.loads(body)
        assert "recent_receipts" not in data


class TestClimateHTML:
    def test_climate_html(self, seeded_server):
        status, headers, body = _get(
            f"{seeded_server}/v1/climate/{TARGET}?format=html"
        )
        assert status == 200
        ct = headers.get("Content-Type", "")
        assert "text/html" in ct
        assert headers.get("X-Content-Type-Options") == "nosniff"
        assert b"Label Climate" in body


class TestClimateEmpty:
    def test_climate_empty_did(self, seeded_server):
        status, _, body = _get(
            f"{seeded_server}/v1/climate/did:plc:nonexistent?format=json"
        )
        assert status == 200
        data = json.loads(body)
        assert data["empty"] is True


class TestClimateValidation:
    def test_climate_invalid_did(self, seeded_server):
        # Non-DID strings are now treated as handles for resolution
        status, _, body = _get(f"{seeded_server}/v1/climate/notadid")
        assert status == 404
        data = json.loads(body)
        assert "resolve" in data["error"].lower()

    def test_climate_did_too_long(self, seeded_server):
        long_did = "did:plc:" + "x" * 300
        status, _, _ = _get(f"{seeded_server}/v1/climate/{long_did}")
        assert status == 400

    def test_climate_url_encoded_did(self, seeded_server):
        encoded = TARGET.replace(":", "%3A")
        status, _, body = _get(
            f"{seeded_server}/v1/climate/{encoded}?format=json"
        )
        assert status == 200
        data = json.loads(body)
        assert data["target_did"] == TARGET


class TestClimateWindow:
    def test_climate_window_clamped(self, seeded_server):
        status, _, body = _get(
            f"{seeded_server}/v1/climate/{TARGET}?format=json&window=90"
        )
        assert status == 200
        data = json.loads(body)
        assert data["window_days"] == 60


class TestCache:
    def test_cache_hit(self, seeded_server):
        url = f"{seeded_server}/v1/climate/{TARGET}?format=json&window=30"
        _, _, body1 = _get(url)
        _, _, body2 = _get(url)
        assert body1 == body2


class TestRateLimit:
    def test_rate_limit(self, tmp_path):
        """Exhaust a tight bucket and verify 429."""
        conn, db_path = _make_db(tmp_path)
        conn.close()
        port = _free_port()
        cache_dir = str(tmp_path / "cache")
        # Very tight: 2 tokens, slow refill
        handler_cls = configure_handler(
            db_path, cache_dir, max_concurrent=2, rate_limit=2,
            generation_timeout=10,
        )
        from http.server import ThreadingHTTPServer
        server = ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        base = f"http://127.0.0.1:{port}"
        try:
            # Burn through tokens
            for _ in range(3):
                _get(f"{base}/v1/climate/did:plc:x?format=json")
            # Next should be 429
            status, headers, body = _get(f"{base}/v1/climate/did:plc:y?format=json")
            assert status == 429
            data = json.loads(body)
            assert "Rate limited" in data["error"]
            assert "Retry-After" in headers
        finally:
            server.shutdown()


class TestRouting:
    def test_unknown_route(self, seeded_server):
        status, _, _ = _get(f"{seeded_server}/v1/foo")
        assert status == 404

    def test_extra_path_segments(self, seeded_server):
        status, _, _ = _get(f"{seeded_server}/v1/climate/did:plc:x/extra")
        assert status == 404


class TestPublicPayload:
    def test_strips_private_fields(self):
        payload = {
            "empty": False,
            "target_did": "did:plc:test",
            "window_days": 30,
            "summary": {"label_actions": 5},
            "recent_receipts": [{"uri": "at://..."}],
            "generated_at": "2026-01-01T00:00:00Z",
            "top_labelers": [],
            "top_values": [],
            "daily_series": [],
            "week_deltas": {},
        }
        public = public_climate_payload(payload)
        assert "recent_receipts" not in public
        assert "summary" in public
        assert "target_did" in public


class TestDidValidation:
    def test_valid_did(self):
        assert _validate_did("did:plc:abc123") is None

    def test_no_prefix(self):
        assert _validate_did("notadid") is not None

    def test_too_long(self):
        assert _validate_did("did:plc:" + "x" * 300) is not None

    def test_slash_rejected(self):
        assert _validate_did("did:plc:abc/def") is not None

    def test_control_chars_rejected(self):
        assert _validate_did("did:plc:abc\x00def") is not None
