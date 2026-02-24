"""Churn index (Jaccard distance) rule tests."""

from datetime import datetime, timezone

from labelwatch import db, scan
from labelwatch.config import Config
from labelwatch.utils import hash_sha256


def _insert_events_at(conn, labeler: str, uris: list[str], hour: int):
    """Insert one label per URI at a given hour."""
    rows = []
    for i, uri in enumerate(uris):
        ts = f"2024-01-01T{hour:02d}:{i % 60:02d}:00Z"
        canonical = f'{{"labeler_did":"{labeler}","uri":"{uri}","hour":{hour},"i":{i}}}'
        eh = hash_sha256(canonical)
        rows.append((labeler, labeler, uri, None, "test", 0, None, None, ts, eh))
        db.upsert_labeler(conn, labeler, ts)
    db.insert_label_events(conn, rows)
    conn.commit()


def test_high_churn_triggers():
    """Completely different target sets in each half-window should trigger."""
    conn = db.connect(":memory:")
    db.init_db(conn)

    # First half: targets 0-9. Second half: targets 10-19. Zero overlap.
    first_uris = [f"at://user/post/{i}" for i in range(10)]
    second_uris = [f"at://user/post/{i}" for i in range(10, 20)]
    _insert_events_at(conn, "did:plc:churner", first_uris, hour=0)
    _insert_events_at(conn, "did:plc:churner", second_uris, hour=12)

    cfg = Config(
        churn_window_hours=24,
        churn_threshold=0.8,
        churn_min_targets=10,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 1, 23, 59, 0, tzinfo=timezone.utc)
    scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='churn_index'").fetchall()
    assert len(rows) == 1
    assert rows[0]["labeler_did"] == "did:plc:churner"


def test_stable_targets_no_churn():
    """Same targets in both halves should not trigger."""
    conn = db.connect(":memory:")
    db.init_db(conn)

    same_uris = [f"at://user/post/{i}" for i in range(15)]
    _insert_events_at(conn, "did:plc:stable", same_uris, hour=0)
    _insert_events_at(conn, "did:plc:stable", same_uris, hour=12)

    cfg = Config(
        churn_window_hours=24,
        churn_threshold=0.8,
        churn_min_targets=10,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 1, 23, 59, 0, tzinfo=timezone.utc)
    scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='churn_index'").fetchall()
    assert len(rows) == 0


def test_below_min_targets_no_churn():
    """Too few total targets should not trigger even with full churn."""
    conn = db.connect(":memory:")
    db.init_db(conn)

    _insert_events_at(conn, "did:plc:small", ["at://a/1", "at://a/2"], hour=0)
    _insert_events_at(conn, "did:plc:small", ["at://a/3", "at://a/4"], hour=12)

    cfg = Config(
        churn_window_hours=24,
        churn_threshold=0.5,
        churn_min_targets=10,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 1, 23, 59, 0, tzinfo=timezone.utc)
    scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='churn_index'").fetchall()
    assert len(rows) == 0
