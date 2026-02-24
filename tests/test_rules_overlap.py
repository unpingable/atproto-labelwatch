from datetime import datetime, timezone

from labelwatch import db, ingest, scan
from labelwatch.config import Config


FIXTURE = "tests/fixtures/synthetic_streams.jsonl"


def test_flip_flop_triggers():
    conn = db.connect(":memory:")
    db.init_db(conn)
    ingest.ingest_from_fixture(conn, FIXTURE)

    cfg = Config(
        flip_flop_window_hours=24,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='flip_flop'").fetchall()
    assert len(rows) == 1
    assert rows[0]["labeler_did"] == "did:plc:labelerB"
