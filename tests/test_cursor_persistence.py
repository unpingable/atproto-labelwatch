"""Cursor persistence: restart resumes from last cursor, no duplicate events."""

from unittest.mock import patch

from labelwatch import db, ingest
from labelwatch.config import Config


def _make_labels(start: int, count: int, labeler: str = "did:plc:labelerA") -> list:
    """Generate synthetic label dicts with sequential URIs."""
    out = []
    for i in range(start, start + count):
        out.append({
            "src": labeler,
            "uri": f"at://{labeler}/app.bsky.feed.post/{i}",
            "val": "test-label",
            "ts": f"2024-01-01T00:{i:02d}:00Z",
        })
    return out


def _make_config() -> Config:
    return Config(service_url="https://fake.test")


def test_cold_start_no_cursor():
    """First run starts with no cursor (None)."""
    conn = db.connect(":memory:")
    db.init_db(conn)
    cfg = _make_config()
    source = ingest._cursor_key(cfg)
    assert db.get_cursor(conn, source) is None


def test_cursor_persisted_after_ingest():
    """After ingest, cursor is saved to meta table."""
    conn = db.connect(":memory:")
    db.init_db(conn)
    cfg = _make_config()
    source = ingest._cursor_key(cfg)

    page1 = {"labels": _make_labels(0, 5), "cursor": "cursor_page1"}
    page2 = {"labels": _make_labels(5, 5), "cursor": None}

    call_count = 0

    def fake_fetch(service_url, sources, cursor=None, limit=100):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            assert cursor is None  # cold start
            return page1
        return page2

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        total = ingest.ingest_from_service(conn, cfg)

    assert total == 10
    assert db.get_cursor(conn, source) == "cursor_page1"


def test_restart_resumes_from_cursor():
    """Simulated restart: second ingest starts from persisted cursor."""
    conn = db.connect(":memory:")
    db.init_db(conn)
    cfg = _make_config()
    source = ingest._cursor_key(cfg)

    # First run: returns 5 events + cursor
    page1 = {"labels": _make_labels(0, 5), "cursor": "cursor_after_5"}
    page1_end = {"labels": [], "cursor": None}

    first_run_calls = 0

    def fake_fetch_run1(service_url, sources, cursor=None, limit=100):
        nonlocal first_run_calls
        first_run_calls += 1
        if first_run_calls == 1:
            return page1
        return page1_end

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch_run1):
        ingest.ingest_from_service(conn, cfg)

    assert db.get_cursor(conn, source) == "cursor_after_5"

    # Second run: should resume from cursor_after_5
    page2 = {"labels": _make_labels(5, 5), "cursor": None}

    def fake_fetch_run2(service_url, sources, cursor=None, limit=100):
        assert cursor == "cursor_after_5"  # resumed!
        return page2

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch_run2):
        total = ingest.ingest_from_service(conn, cfg)

    assert total == 5
    count = conn.execute("SELECT COUNT(*) AS c FROM label_events").fetchone()["c"]
    assert count == 10  # no dupes, 5 + 5


def test_replay_deduplicates():
    """Even without cursor, replayed events are deduplicated by event_hash."""
    conn = db.connect(":memory:")
    db.init_db(conn)
    cfg = _make_config()

    same_labels = _make_labels(0, 5)
    page = {"labels": same_labels, "cursor": None}

    def fake_fetch(service_url, sources, cursor=None, limit=100):
        return page

    # Ingest twice with same data
    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        first = ingest.ingest_from_service(conn, cfg)
    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        second = ingest.ingest_from_service(conn, cfg)

    assert first == 5
    assert second == 0  # all deduplicated
    count = conn.execute("SELECT COUNT(*) AS c FROM label_events").fetchone()["c"]
    assert count == 5
