from __future__ import annotations

import hashlib
import json
import sys

import pytest

pytest.importorskip("pyarrow")

from labelwatch import db
from tools.label_events_working_set import _days, main


def test_days_returns_complete_half_open_interval() -> None:
    assert _days("2026-02-27", "2026-03-02") == [
        "2026-02-27",
        "2026-02-28",
        "2026-03-01",
    ]


@pytest.mark.parametrize(
    ("start", "end"),
    [("2026-03-02", "2026-03-02"), ("2026-03-03", "2026-03-02")],
)
def test_days_refuses_empty_or_reversed_interval(start: str, end: str) -> None:
    with pytest.raises(ValueError, match="start must precede"):
        _days(start, end)


def test_resume_reuses_verified_days_and_rebuilds_catalog(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.db"
    conn = db.connect(str(source))
    db.init_db(conn)
    subject = "did:plc:subject"
    db.insert_label_events(conn, [
        ("did:plc:labeler", subject, subject, None, "one", 0, None, None,
         "2026-09-01T01:00:00Z", "one", subject),
        ("did:plc:labeler", subject, subject, None, "two", 0, None, None,
         "2026-09-02T01:00:00Z", "two", subject),
    ])
    conn.commit()
    conn.close()
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    output = tmp_path / "output"
    argv = [
        "label_events_working_set.py", "--source-db", str(source),
        "--source-sha256", source_sha, "--start-day", "2026-09-01",
        "--end-day-exclusive", "2026-09-03", "--output", str(output),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    main()
    first = json.loads((output / "qualification.json").read_text())
    (output / "qualification.json").unlink()
    (output / "frontdoor-cold-catalog.sqlite").unlink()
    (output / "frontdoor-cold-catalog.sqlite.manifest.json").unlink()

    monkeypatch.setattr(sys, "argv", argv + ["--resume"])
    main()
    resumed = json.loads((output / "qualification.json").read_text())
    assert resumed["archived_rows"] == first["archived_rows"] == 2
    assert resumed["catalog_included_events"] == first["catalog_included_events"] == 2
    assert resumed["catalog"]["sha256"] == first["catalog"]["sha256"]
