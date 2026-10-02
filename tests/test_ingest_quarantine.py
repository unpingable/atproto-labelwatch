"""Ingest guard: events below the retention floor are quarantined."""
from pathlib import Path
from unittest.mock import patch

import pytest

from labelwatch import db, ingest, ops_status, retention
from labelwatch.config import Config

FLOOR = "2026-08-14T00:00:00Z"
LABELER = "did:plc:labeler"


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for name in ("LABELWATCH_RETENTION_FLOOR", "LABELWATCH_BELOW_FLOOR_POLICY"):
        monkeypatch.delenv(name, raising=False)


def _label(i, ts):
    return {"src": LABELER, "uri": f"at://did:plc:t{i}/app.bsky.feed.post/{i}",
            "val": "spam", "ts": ts}


BELOW = [_label(1, "2026-08-13T23:59:59Z"), _label(2, "2026-03-01T00:00:00Z")]
ABOVE = [_label(3, FLOOR), _label(4, "2026-09-01T00:00:00Z")]


def _db(path=":memory:", floor=FLOOR):
    conn = db.connect(str(path))
    db.init_db(conn)
    if floor:
        retention.set_retention_floor(conn, floor, None)
        conn.commit()
    return conn


def _live_ts(conn):
    return sorted(r[0] for r in conn.execute("SELECT ts FROM label_events"))


def _quarantined(conn):
    return sorted((r["ts"], r["reason"], r["seen_count"], r["live_floor"]) for r in
                  conn.execute("SELECT * FROM quarantined_events"))


def test_below_floor_events_are_quarantined_not_inserted():
    conn = _db()
    inserted = ingest.ingest_from_iter(conn, BELOW + ABOVE)
    assert inserted == 2
    assert _live_ts(conn) == [FLOOR, "2026-09-01T00:00:00Z"]
    assert _quarantined(conn) == [
        ("2026-03-01T00:00:00Z", "below_retention_floor", 1, FLOOR),
        ("2026-08-13T23:59:59Z", "below_retention_floor", 1, FLOOR),
    ]


def test_redelivery_after_trim_is_not_reinserted():
    # The original was live, then archived and trimmed; a cursor reset
    # re-delivers it. event_hash dedup no longer sees it.
    conn = _db(floor=None)
    ingest.ingest_from_iter(conn, BELOW)
    assert len(_live_ts(conn)) == 2
    conn.execute("DELETE FROM label_events WHERE ts < ?", (FLOOR,))
    retention.set_retention_floor(conn, FLOOR, "archive://p")
    conn.commit()
    assert ingest.ingest_from_iter(conn, BELOW) == 0
    assert ingest.ingest_from_iter(conn, BELOW) == 0
    assert _live_ts(conn) == []
    assert [q[2] for q in _quarantined(conn)] == [2, 2]


def test_no_floor_inserts_everything():
    conn = _db(floor=None)
    assert ingest.ingest_from_iter(conn, BELOW + ABOVE) == 4
    assert _quarantined(conn) == []


def test_env_policy_insert_disables_guard(monkeypatch):
    monkeypatch.setenv("LABELWATCH_BELOW_FLOOR_POLICY", "insert")
    conn = _db()
    assert ingest.ingest_from_iter(conn, BELOW + ABOVE) == 4
    assert _quarantined(conn) == []


def test_config_policy_and_env_precedence(monkeypatch):
    cfg = Config(below_floor_policy="insert")
    assert ingest.below_floor_policy(cfg) == "insert"
    assert ingest.below_floor_policy(Config()) == "quarantine"
    monkeypatch.setenv("LABELWATCH_BELOW_FLOOR_POLICY", "quarantine")
    assert ingest.below_floor_policy(cfg) == "quarantine"


def test_invalid_policy_fails_closed(monkeypatch):
    monkeypatch.setenv("LABELWATCH_BELOW_FLOOR_POLICY", "drop")
    conn = _db()
    with pytest.raises(ValueError):
        ingest.ingest_from_iter(conn, BELOW)
    assert _live_ts(conn) == []


def test_multi_ingest_path_quarantines():
    conn = _db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, service_endpoint, endpoint_status, first_seen, last_seen) "
        "VALUES(?, 'https://l.example', 'accessible', ?, ?)", (LABELER, FLOOR, FLOOR))
    conn.commit()
    with patch.object(ingest, "fetch_labels",
                      return_value={"labels": BELOW + ABOVE, "cursor": None}):
        ingest.ingest_multi(conn, Config(discovery_enabled=True))
    assert _live_ts(conn) == [FLOOR, "2026-09-01T00:00:00Z"]
    assert len(_quarantined(conn)) == 2


def test_service_ingest_path_quarantines():
    conn = _db()
    cfg = Config(labeler_dids=[LABELER])
    with patch.object(ingest, "fetch_labels",
                      return_value={"labels": BELOW + ABOVE, "cursor": None}):
        assert ingest.ingest_from_service(conn, cfg) == 2
    assert len(_quarantined(conn)) == 2


def test_ops_status_counts_quarantine(tmp_path):
    path = tmp_path / "lw.db"
    conn = _db(path)
    ingest.ingest_from_iter(conn, BELOW + ABOVE)
    ingest.ingest_from_iter(conn, BELOW[:1])
    conn.close()
    status = ops_status.build_status(path)
    ext = status["extensions"]["labelwatch.retention"]
    assert ext["live_floor"] == FLOOR
    assert ext["quarantined_events"]["events"] == 2
    assert ext["quarantined_events"]["deliveries"] == 3
    assert ext["quarantined_events"]["by_reason"]["below_retention_floor"]["events"] == 2


def test_ops_status_without_database(tmp_path):
    status = ops_status.build_status(tmp_path / "missing.db")
    assert status["extensions"]["labelwatch.retention"]["error"] == "database_absent"
