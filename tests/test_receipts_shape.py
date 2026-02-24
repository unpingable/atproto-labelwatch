import json
from datetime import datetime, timezone

from labelwatch import db, ingest, scan
from labelwatch.config import Config


FIXTURE = "tests/fixtures/synthetic_streams.jsonl"


def test_receipt_shape():
    conn = db.connect(":memory:")
    db.init_db(conn)
    ingest.ingest_from_fixture(conn, FIXTURE)

    cfg = Config(spike_k=5.0, min_current_count=10, warmup_enabled=False)
    now = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    scan.run_scan(conn, cfg, now=now)

    row = conn.execute("SELECT * FROM alerts LIMIT 1").fetchone()
    assert row is not None
    assert row["rule_id"]
    assert row["config_hash"]
    assert row["receipt_hash"]
    evidence = json.loads(row["evidence_hashes_json"])
    assert isinstance(evidence, list)
