"""Tests for the live working-set retention floor."""
import pytest

from labelwatch import db, retention

FLOOR = "2026-08-14T00:00:00Z"


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv("LABELWATCH_RETENTION_FLOOR", raising=False)
    monkeypatch.delenv("LABELWATCH_RETENTION_ARCHIVE_REF", raising=False)


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def test_no_floor_returns_none():
    conn = _make_db()
    assert retention.live_floor(conn) is None
    assert retention.cold_history_block(conn, None) is None
    assert retention.cold_history_block(conn, "2020-01-01T00:00:00Z") is None


def test_env_overrides_meta(monkeypatch):
    conn = _make_db()
    retention.set_retention_floor(conn, FLOOR, "archive://meta")
    assert retention.live_floor(conn) == FLOOR
    assert retention.archive_ref(conn) == "archive://meta"
    monkeypatch.setenv("LABELWATCH_RETENTION_FLOOR", "2026-09-01T00:00:00Z")
    monkeypatch.setenv("LABELWATCH_RETENTION_ARCHIVE_REF", "archive://env")
    assert retention.live_floor(conn) == "2026-09-01T00:00:00Z"
    assert retention.archive_ref(conn) == "archive://env"


def test_start_at_or_after_floor_has_no_block():
    conn = _make_db()
    retention.set_retention_floor(conn, FLOOR, None)
    assert retention.cold_history_block(conn, FLOOR) is None
    assert retention.cold_history_block(conn, "2026-09-01T00:00:00Z") is None


def test_start_before_floor_returns_block():
    conn = _make_db()
    retention.set_retention_floor(conn, FLOOR, "archive://x")
    block = retention.cold_history_block(conn, "2026-08-03T00:00:00Z")
    assert block == {
        "status": "unavailable",
        "live_floor": FLOOR,
        "archive": "archive://x",
        "requested_start": "2026-08-03T00:00:00Z",
        "scope": "partial",
    }
    all_history = retention.cold_history_block(conn, None)
    assert all_history["scope"] == "all_history"
    assert all_history["requested_start"] is None


@pytest.mark.parametrize("bad", ["yesterday", "2026-08-14T00:00:00", "2026-08-14T00:00:00+02:00"])
def test_invalid_floor_raises(monkeypatch, bad):
    conn = _make_db()
    monkeypatch.setenv("LABELWATCH_RETENTION_FLOOR", bad)
    with pytest.raises(ValueError):
        retention.live_floor(conn)
    with pytest.raises(ValueError):
        retention.cold_history_block(conn, None)


def test_invalid_meta_floor_raises():
    conn = _make_db()
    db.set_meta(conn, retention.RETENTION_FLOOR_KEY, "not-a-time")
    with pytest.raises(ValueError):
        retention.live_floor(conn)
