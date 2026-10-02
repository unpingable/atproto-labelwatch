import json
import sqlite3

import pytest

from labelwatch import db
from tools.label_events_cold_archive import export_day, reconstruct, verify_manifest


SOURCE_SHA = "a" * 64


def source(tmp_path):
    path = tmp_path / "source.db"
    conn = db.connect(str(path))
    db.init_db(conn)
    rows = [
        ("did:plc:one", "did:plc:subject", "at://did:plc:subject/app.bsky.feed.post/1", None, "spam", 0, None, "sig-one", "2026-09-01T00:00:01Z", "hash-one", "did:plc:subject"),
        ("did:plc:one", "did:plc:subject", "did:plc:subject", "cid-two", "hide", 1, None, "sig-two", "2026-09-01T23:59:59Z", "hash-two", "did:plc:subject"),
        ("did:plc:two", None, "did:plc:later", None, "test", 0, None, None, "2026-09-02T00:00:00Z", "hash-three", "did:plc:later"),
    ]
    db.insert_label_events(conn, rows)
    conn.commit()
    conn.close()
    return path


def test_export_verify_and_reconstruct_preserve_event_identity(tmp_path):
    src = source(tmp_path)
    before = src.read_bytes()
    manifest_path = export_day(src, tmp_path / "archive", "2026-09-01", SOURCE_SHA, batch_size=1)
    manifest = verify_manifest(manifest_path)
    assert manifest["partition"]["rows"] == 2
    assert manifest["semantics"]["source_rows_deleted"] is False
    assert src.read_bytes() == before

    restored = tmp_path / "restored.db"
    result = reconstruct(manifest_path, restored)
    assert result["rows"] == 2
    assert result["quick_check"] == "ok"
    conn = sqlite3.connect(restored)
    values = conn.execute("SELECT event_hash,sig,ts FROM label_events ORDER BY id").fetchall()
    conn.close()
    assert values == [
        ("hash-one", "sig-one", "2026-09-01T00:00:01Z"),
        ("hash-two", "sig-two", "2026-09-01T23:59:59Z"),
    ]


def test_existing_outputs_and_empty_days_refuse(tmp_path):
    src = source(tmp_path)
    archive = tmp_path / "archive"
    export_day(src, archive, "2026-09-01", SOURCE_SHA)
    with pytest.raises(FileExistsError):
        export_day(src, archive, "2026-09-01", SOURCE_SHA)
    with pytest.raises(RuntimeError, match="contains no events"):
        export_day(src, archive, "2026-08-01", SOURCE_SHA)


def test_tampered_partition_and_manifest_refuse(tmp_path):
    src = source(tmp_path)
    manifest_path = export_day(src, tmp_path / "archive", "2026-09-01", SOURCE_SHA)
    manifest = json.loads(manifest_path.read_text())
    parquet = manifest_path.parent / manifest["partition"]["file"]
    with parquet.open("ab") as handle:
        handle.write(b"changed")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        verify_manifest(manifest_path)

    manifest["format_version"] = 2
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match="unsupported archive format"):
        verify_manifest(manifest_path)


def test_reconstruct_refuses_existing_destination(tmp_path):
    src = source(tmp_path)
    manifest = export_day(src, tmp_path / "archive", "2026-09-01", SOURCE_SHA)
    destination = tmp_path / "exists.db"
    destination.write_bytes(b"preserve")
    with pytest.raises(FileExistsError):
        reconstruct(manifest, destination)
    assert destination.read_bytes() == b"preserve"


@pytest.mark.parametrize("day", ["2026-9-1", "not-a-day", "2026-02-30"])
def test_invalid_day_refuses(tmp_path, day):
    src = source(tmp_path)
    with pytest.raises(ValueError):
        export_day(src, tmp_path / "archive", day, SOURCE_SHA)
