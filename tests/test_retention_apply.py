"""Coordinator authority boundary: no host change without current NFS custody."""
from datetime import datetime, timedelta, timezone

import pytest

from labelwatch.trim import TrimRefused
from tools.label_events_retention_apply import custody_check


def custody(**overrides):
    value = {"result": "NFS_ARCHIVE_CUSTODY_VERIFIED", "verified_at": datetime.now(timezone.utc).isoformat(),
             **{kind: {"archive_path": "/verified/" + kind, "sha256": "a" * 64}
                for kind in ("partition", "catalog", "old_catalog")}}
    value.update(overrides)
    return {"nfs_custody": value, "old_catalog_sha256": "a" * 64,
            "partition_manifest": "/must-not-be-opened", "candidate_manifest": "/must-not-be-opened"}


def test_missing_archive_custody_refuses_before_local_files_or_host_changes():
    with pytest.raises(TrimRefused, match="fresh verified NFS"):
        custody_check({})


def test_stale_receipt_cannot_authorize_another_occurrence():
    value = custody(verified_at=(datetime.now(timezone.utc) - timedelta(hours=2)).isoformat())
    with pytest.raises(TrimRefused, match="freshly verified"):
        custody_check(value)


@pytest.mark.parametrize("kind", ["partition", "catalog", "old_catalog"])
def test_each_nfs_dependency_is_required(kind):
    value = custody()
    del value["nfs_custody"][kind]
    with pytest.raises(TrimRefused, match=f"NFS {kind} identity"):
        custody_check(value)


def test_old_root_copy_requires_matching_nfs_rollback_identity():
    value = custody()
    value["old_catalog_sha256"] = "b" * 64
    with pytest.raises(TrimRefused, match="old NFS catalog custody differs"):
        custody_check(value)


def test_same_admitted_occurrence_reverifies_bytes_after_supervisor_gap(monkeypatch):
    from types import SimpleNamespace
    from tools import label_events_retention_apply as module
    value = custody(verified_at=(datetime.now(timezone.utc) - timedelta(hours=2)).isoformat())
    manifest = {"floor": "2026-08-15T00:00:00Z", "partition": {"sha256": "a" * 64},
                "selection": {"watermark": 42}}
    catalog = SimpleNamespace(database_sha256="a" * 64, end_day_exclusive="2026-08-15", source_event_id_upper_bound=42)
    observed = []
    monkeypatch.setattr(module, "verify_partition", lambda path: observed.append(path) or manifest)
    monkeypatch.setattr(module, "load_catalog", lambda path: observed.append(path) or catalog)
    assert module.custody_check(value, resuming=True) == (manifest, catalog)
    assert observed == ["/must-not-be-opened", "/must-not-be-opened"]
    with pytest.raises(TrimRefused, match="freshly verified"):
        module.custody_check(value)


def test_completed_floor_recovers_database_receipt_without_another_delete(tmp_path):
    from labelwatch import db, retention, trim
    from _cold_catalog_fixture import build_test_catalog
    from tools.label_events_retention_apply import completed_trim
    path = tmp_path / "live.db"
    conn = db.connect(str(path))
    db.init_db(conn)
    row = ("did:plc:labeler", None, "did:plc:subject", None, "spam", 0, None,
           None, "2026-08-14T12:00:00Z", "event", "did:plc:subject")
    db.insert_label_events(conn, [row])
    retention.set_retention_floor(conn, "2026-08-14T00:00:00Z", "archive://base")
    db.set_meta(conn, retention.RETENTION_HISTORY_START_KEY, "2026-08-14T00:00:00Z")
    conn.commit()
    part = trim.export_partition(str(path), "2026-08-15T00:00:00Z", str(tmp_path / "archive"))
    catalog = build_test_catalog(tmp_path / "catalog", [(row[0], row[2], row[4], row[5], row[8], row[10])],
                                 start_day="2026-08-14", end_day_exclusive="2026-08-15", watermark=1)
    expected = trim.run_trim(conn, "2026-08-15T00:00:00Z", part,
                             catalog_custody=retention.load_catalog_custody(str(catalog)))
    changes = conn.total_changes
    manifest = trim.verify_partition(part)
    assert completed_trim(conn, manifest, part) == expected
    assert conn.total_changes == changes
    assert not db.get_meta(conn, trim.TRIM_PENDING_KEY)
    db.set_meta(conn, trim.TRIM_LAST_KEY, '{}')
    conn.commit()
    with pytest.raises(TrimRefused, match="another trim occurrence"):
        completed_trim(conn, manifest, part)
    conn.close()
