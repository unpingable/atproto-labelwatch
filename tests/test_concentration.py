"""Target concentration (HHI) rule tests."""

from datetime import datetime, timezone

from labelwatch import db, ingest, scan
from labelwatch.config import Config


def _insert_events(conn, labeler: str, targets: list[tuple[str, int]], base_hour: int = 0):
    """Insert label events. targets: list of (uri, count) pairs."""
    rows = []
    i = 0
    for uri, count in targets:
        for _ in range(count):
            ts = f"2024-01-01T{base_hour:02d}:{i % 60:02d}:00Z"
            canonical = f'{{"labeler_did":"{labeler}","src":"{labeler}","uri":"{uri}","val":"test","neg":0,"ts":"{ts}"}}'
            from labelwatch.utils import hash_sha256
            eh = hash_sha256(canonical + str(i))
            rows.append((labeler, labeler, uri, None, "test", 0, None, None, ts, eh))
            db.upsert_labeler(conn, labeler, ts)
            i += 1
    db.insert_label_events(conn, rows)
    conn.commit()


def test_concentrated_labeler_triggers():
    """Labeler targeting one URI heavily should trigger concentration alert."""
    conn = db.connect(":memory:")
    db.init_db(conn)

    # 50 labels on one target, 1 each on 5 others = highly concentrated
    targets = [("at://user/post/1", 50)] + [(f"at://user/post/{i}", 1) for i in range(2, 7)]
    _insert_events(conn, "did:plc:concentrated", targets)

    cfg = Config(
        concentration_window_hours=24,
        concentration_threshold=0.1,
        concentration_min_labels=10,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    total = scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='target_concentration'").fetchall()
    assert len(rows) == 1
    assert rows[0]["labeler_did"] == "did:plc:concentrated"


def test_distributed_labeler_does_not_trigger():
    """Labeler with evenly distributed targets should not trigger."""
    conn = db.connect(":memory:")
    db.init_db(conn)

    # 5 labels each on 20 targets = very distributed
    targets = [(f"at://user/post/{i}", 5) for i in range(20)]
    _insert_events(conn, "did:plc:distributed", targets)

    cfg = Config(
        concentration_window_hours=24,
        concentration_threshold=0.25,
        concentration_min_labels=10,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='target_concentration'").fetchall()
    assert len(rows) == 0


def test_below_min_labels_does_not_trigger():
    """Too few labels should not trigger even if concentrated."""
    conn = db.connect(":memory:")
    db.init_db(conn)

    targets = [("at://user/post/1", 5)]
    _insert_events(conn, "did:plc:tiny", targets)

    cfg = Config(
        concentration_window_hours=24,
        concentration_threshold=0.1,
        concentration_min_labels=20,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='target_concentration'").fetchall()
    assert len(rows) == 0
