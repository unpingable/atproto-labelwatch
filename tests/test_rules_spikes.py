from datetime import datetime, timezone

from labelwatch import db, ingest, scan
from labelwatch.config import Config


FIXTURE = "tests/fixtures/synthetic_streams.jsonl"


def test_label_rate_spike_triggers():
    conn = db.connect(":memory:")
    db.init_db(conn)
    ingest.ingest_from_fixture(conn, FIXTURE)

    cfg = Config(
        window_minutes=15,
        baseline_hours=24,
        spike_k=5.0,
        min_current_count=10,
    )
    now = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    total = scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='label_rate_spike'").fetchall()
    assert total >= 1
    assert len(rows) == 1
    assert rows[0]["labeler_did"] == "did:plc:labelerA"
