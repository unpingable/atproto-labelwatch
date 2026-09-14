from __future__ import annotations

import hashlib
import json
import sqlite3
import sys

from labelwatch import db
from tools.label_events_cold_archive import export_day
from tools.label_events_working_set_reconstruct import main


def test_reconstructs_complete_interval_with_stable_ids(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.db"
    conn = db.connect(str(source))
    db.init_db(conn)
    subject = "did:plc:subject"
    rows = [
        ("did:plc:labeler", subject, subject, None, "one", 0, None, "sig1",
         "2026-09-01T01:00:00Z", "one", subject),
        ("did:plc:labeler", subject, subject, None, "two", 1, None, "sig2",
         "2026-09-02T01:00:00Z", "two", subject),
    ]
    db.insert_label_events(conn, rows)
    ids = [row[0] for row in conn.execute("SELECT id FROM label_events ORDER BY id")]
    conn.commit()
    conn.close()
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    partitions = tmp_path / "partitions"
    for day in ("2026-09-01", "2026-09-02"):
        export_day(source, partitions / day, day, source_sha)
    destination = tmp_path / "restored.db"
    result = tmp_path / "result.json"
    monkeypatch.setattr(sys, "argv", [
        "reconstruct", "--partitions", str(partitions),
        "--source-sha256", source_sha, "--start-day", "2026-09-01",
        "--end-day-exclusive", "2026-09-03", "--destination", str(destination),
        "--result", str(result),
    ])
    main()
    receipt = json.loads(result.read_text())
    assert receipt["rows"] == 2
    restored = sqlite3.connect(destination)
    try:
        assert [row[0] for row in restored.execute(
            "SELECT id FROM label_events ORDER BY id"
        )] == ids
        assert restored.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        restored.close()
