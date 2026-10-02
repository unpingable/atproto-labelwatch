"""Weekly delta export and retention trim."""
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from labelwatch import cli, db, retention, trim

from _cold_catalog_fixture import build_test_catalog

OLD_FLOOR = "2026-08-14T00:00:00Z"
NEW_FLOOR = "2026-08-21T00:00:00Z"
NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for name in ("LABELWATCH_RETENTION_FLOOR", "LABELWATCH_RETENTION_ARCHIVE_REF",
                 "LABELWATCH_FRONTDOOR_COLD_CATALOG", "LABELWATCH_BELOW_FLOOR_POLICY"):
        monkeypatch.delenv(name, raising=False)


def _synthetic_rows(days=28, per_day=120):
    rows = []
    start = datetime(2026, 8, 14, tzinfo=timezone.utc)
    for d in range(days):
        for i in range(per_day):
            ts = (start + timedelta(days=d, seconds=i * 700)).strftime("%Y-%m-%dT%H:%M:%SZ")
            target = f"did:plc:t{i % 37}"
            uri = target if i % 5 == 0 else f"at://{target}/app.bsky.feed.post/{d}-{i}"
            rows.append((f"did:plc:l{i % 4}", None, uri, None, ["spam", "warn", "!hide"][i % 3],
                         i % 7 == 0, None, None, ts, f"h-{d}-{i}", target if i % 5 else None))
    return rows


@pytest.fixture
def live(tmp_path):
    path = tmp_path / "live.db"
    conn = db.connect(str(path))
    db.init_db(conn)
    db.insert_label_events(conn, _synthetic_rows())
    retention.set_retention_floor(conn, OLD_FLOOR, "archive://gen1")
    db.set_meta(conn, retention.RETENTION_HISTORY_START_KEY, OLD_FLOOR)
    conn.commit()
    yield conn, path
    conn.close()


def _count_below(conn, floor):
    return conn.execute("SELECT COUNT(*) FROM label_events WHERE ts < ?", (floor,)).fetchone()[0]


def _export(path, tmp_path, floor=NEW_FLOOR):
    return trim.export_partition(str(path), floor, str(tmp_path / "archive"), batch_rows=97)


def test_export_binds_exact_rows(live, tmp_path):
    conn, path = live
    expected = conn.execute(
        "SELECT * FROM label_events WHERE ts < ? ORDER BY id", (NEW_FLOOR,)).fetchall()
    assert len(expected) == 7 * 120
    manifest_path = _export(path, tmp_path)
    manifest = trim.verify_partition(manifest_path)
    rows = manifest["rows"]
    assert rows["count"] == 840
    assert rows["id_min"] == expected[0]["id"] and rows["id_max"] == expected[-1]["id"]
    assert manifest["selection"]["watermark"] == conn.execute(
        "SELECT MAX(id) FROM label_events").fetchone()[0]
    assert manifest["previous_floor"] == OLD_FLOOR
    assert rows["below_previous_floor"] == 0
    assert set(rows["per_day"]) == {f"2026-08-{d}" for d in range(14, 21)}
    assert sum(rows["per_day"].values()) == 840
    part = sqlite3.connect(manifest_path.parent / manifest["partition"]["file"])
    got = part.execute(f"SELECT {','.join(trim.COLUMNS)} FROM label_events ORDER BY id").fetchall()
    assert got == [tuple(r) for r in expected]
    # The source was only read.
    assert _count_below(conn, NEW_FLOOR) == 840


def test_export_refuses_to_overwrite(live, tmp_path):
    _, path = live
    _export(path, tmp_path)
    with pytest.raises(FileExistsError):
        _export(path, tmp_path)


def test_export_rejects_non_midnight_floor(live, tmp_path):
    _, path = live
    with pytest.raises(trim.TrimRefused):
        _export(path, tmp_path, floor="2026-08-21T06:00:00Z")


def test_verify_detects_tampering(live, tmp_path):
    _, path = live
    manifest_path = _export(path, tmp_path)
    manifest = json.loads(manifest_path.read_text())
    manifest["rows"]["count"] += 1
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(trim.TrimRefused, match="metadata"):
        trim.verify_partition(manifest_path)

    manifest_path2 = trim.export_partition(str(path), NEW_FLOOR, str(tmp_path / "b"))
    part = manifest_path2.parent / json.loads(manifest_path2.read_text())["partition"]["file"]
    with part.open("r+b") as handle:
        handle.seek(4096)
        handle.write(b"\x00\x01")
    with pytest.raises(trim.TrimRefused, match="checksum"):
        trim.verify_partition(manifest_path2)


def test_trim_without_catalog(live, tmp_path):
    conn, path = live
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    manifest_path = _export(path, tmp_path)
    batches = []
    receipt = trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, batch_ids=100,
                            on_batch=batches.append)
    assert receipt["deleted"] == 840
    assert receipt["catalog"] == {"installed": False}
    assert len(batches) == receipt["batches"] > 1
    assert _count_below(conn, NEW_FLOOR) == 0
    assert conn.execute("SELECT COUNT(*) FROM label_events").fetchone()[0] == 21 * 120
    assert retention.live_floor(conn) == NEW_FLOOR
    assert retention.archive_ref(conn).endswith("#sha256=" + receipt["manifest_sha256"])
    assert db.get_meta(conn, trim.TRIM_PENDING_KEY) is None
    assert json.loads(db.get_meta(conn, trim.TRIM_LAST_KEY))["deleted"] == 840
    # Never VACUUMed: the file did not shrink.
    assert conn.execute("PRAGMA page_count").fetchone()[0] == page_count
    assert conn.execute("PRAGMA freelist_count").fetchone()[0] > 0


def test_trim_with_complete_catalog(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    watermark = json.loads(manifest_path.read_text())["selection"]["watermark"]
    rows = [(r["labeler_did"], r["uri"], r["val"], r["neg"], r["ts"], r["target_did"])
            for r in conn.execute("SELECT * FROM label_events WHERE ts < ?", (NEW_FLOOR,))]
    cat_manifest = build_test_catalog(tmp_path / "cat", rows, start_day="2026-08-14",
                                      end_day_exclusive="2026-08-21", watermark=watermark)
    custody = retention.load_catalog_custody(str(cat_manifest), filesystem_type="ext4")
    # Before the trim the catalog runs ahead of the floor: not served.
    assert retention.catalog_coverage(conn, custody)["status"] == "stale"
    receipt = trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, catalog_custody=custody)
    assert receipt["catalog"]["installed"] is True
    assert retention.catalog_coverage(conn, custody)["status"] == "complete"


def test_trim_refuses_stale_catalog(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    watermark = json.loads(manifest_path.read_text())["selection"]["watermark"]
    stale = build_test_catalog(tmp_path / "stale", [], start_day="2026-08-10",
                               end_day_exclusive="2026-08-14", watermark=watermark)
    custody = retention.load_catalog_custody(str(stale), filesystem_type="ext4")
    with pytest.raises(trim.TrimRefused, match="ends at 2026-08-14"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, catalog_custody=custody)
    assert retention.live_floor(conn) == OLD_FLOOR
    assert _count_below(conn, NEW_FLOOR) == 840


def test_trim_refuses_catalog_watermark_mismatch(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    watermark = json.loads(manifest_path.read_text())["selection"]["watermark"]
    other = build_test_catalog(tmp_path / "w", [], start_day="2026-08-14",
                               end_day_exclusive="2026-08-21", watermark=watermark - 1)
    custody = retention.load_catalog_custody(str(other), filesystem_type="ext4")
    with pytest.raises(trim.TrimRefused, match="watermark"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, catalog_custody=custody)


def test_trim_refuses_unusable_configured_catalog(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    custody = retention.load_catalog_custody(str(tmp_path / "missing.json"))
    with pytest.raises(trim.TrimRefused, match="not usable"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, catalog_custody=custody)


def test_trim_refuses_count_mismatch(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    conn.execute("DELETE FROM label_events WHERE id = (SELECT MIN(id) + 5 FROM label_events)")
    conn.commit()
    with pytest.raises(trim.TrimRefused, match="do not match the manifest"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW)
    assert retention.live_floor(conn) == OLD_FLOOR


@pytest.mark.parametrize("floor,match", [
    ("2026-08-28T00:00:00Z", "manifest floor"),
    ("2026-08-21T00:00:01Z", "UTC midnight"),
])
def test_trim_refuses_floor_mismatch(live, tmp_path, floor, match):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    with pytest.raises(trim.TrimRefused, match=match):
        trim.run_trim(conn, floor, manifest_path, now=NOW)


def test_trim_refuses_recent_floor(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    with pytest.raises(trim.TrimRefused, match="31 days"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path,
                      now=datetime(2026, 9, 15, tzinfo=timezone.utc))


def test_trim_refuses_non_advancing_floor(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    retention.set_retention_floor(conn, "2026-08-25T00:00:00Z", None)
    conn.commit()
    with pytest.raises(trim.TrimRefused, match="does not advance"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW)


def test_trim_refuses_env_floor_override(live, tmp_path, monkeypatch):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    monkeypatch.setenv("LABELWATCH_RETENTION_FLOOR", OLD_FLOOR)
    with pytest.raises(trim.TrimRefused, match="overrides"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW)


def test_trim_refuses_rows_below_previous_floor(live, tmp_path):
    conn, path = live
    db.insert_label_events(conn, [("did:plc:l0", None, "did:plc:x", None, "spam", 0, None,
                                   None, "2026-08-01T00:00:00Z", "old", None)])
    conn.commit()
    manifest_path = _export(path, tmp_path)
    with pytest.raises(trim.TrimRefused, match="below the previous floor"):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW)


def test_dry_run_changes_nothing(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    receipt = trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, dry_run=True)
    assert receipt["dry_run"] is True
    assert retention.live_floor(conn) == OLD_FLOOR
    assert _count_below(conn, NEW_FLOOR) == 840


def test_interrupted_trim_resumes(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)

    def boom(n):
        if n == 3:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, batch_ids=100, on_batch=boom)
    assert retention.live_floor(conn) == NEW_FLOOR
    assert db.get_meta(conn, trim.TRIM_PENDING_KEY) is not None
    assert 0 < _count_below(conn, NEW_FLOOR) < 840
    receipt = trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW, batch_ids=100)
    assert receipt["resumed"] is True
    assert _count_below(conn, NEW_FLOOR) == 0
    assert db.get_meta(conn, trim.TRIM_PENDING_KEY) is None

    other = trim.export_partition(str(path), "2026-08-28T00:00:00Z", str(tmp_path / "next"))
    db.set_meta(conn, trim.TRIM_PENDING_KEY, json.dumps({"manifest_sha256": "x"}))
    conn.commit()
    with pytest.raises(trim.TrimRefused, match="another trim is pending"):
        trim.run_trim(conn, "2026-08-28T00:00:00Z", other, now=NOW)


def test_late_rows_below_new_floor_are_quarantined(live, tmp_path):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    # Arrives after the export snapshot with an authored ts below the new floor.
    db.insert_label_events(conn, [("did:plc:l0", None, "did:plc:late", None, "spam", 0, None,
                                   None, "2026-08-19T00:00:00Z", "late", None)])
    conn.commit()
    receipt = trim.run_trim(conn, NEW_FLOOR, manifest_path, now=NOW)
    assert receipt["deleted"] == 840
    assert receipt["late_rows_quarantined"] == 1
    assert _count_below(conn, NEW_FLOOR) == 0
    q = conn.execute("SELECT event_hash, reason FROM quarantined_events").fetchall()
    assert [tuple(r) for r in q] == [("late", "below_retention_floor")]


def test_cli_retention_trim(live, tmp_path, capsys, monkeypatch):
    conn, path = live
    manifest_path = _export(path, tmp_path)
    conn.close()
    monkeypatch.setattr(trim, "datetime", type("D", (datetime,), {
        "now": staticmethod(lambda tz=None: NOW)}))
    with pytest.raises(SystemExit) as exc:
        cli.main(["--db", str(path), "retention-trim", "--floor", "2026-08-28T00:00:00Z",
                  "--manifest", str(manifest_path)])
    assert exc.value.code == 2
    assert "refused" in json.loads(capsys.readouterr().out)
    cli.main(["--db", str(path), "retention-trim", "--floor", NEW_FLOOR,
              "--manifest", str(manifest_path), "--batch-ids", "250"])
    out = json.loads(capsys.readouterr().out)
    assert out["deleted"] == 840
    check = db.connect(str(path))
    assert retention.live_floor(check) == NEW_FLOOR
    check.close()


def test_export_tool_script(live, tmp_path):
    _, path = live
    result = subprocess.run(
        [sys.executable, str(REPO / "tools" / "label_events_trim_export.py"),
         "--db", str(path), "--floor", NEW_FLOOR, "--out-dir", str(tmp_path / "t"), "--verify"],
        capture_output=True, text=True, check=True)
    out = json.loads(result.stdout)
    assert out["rows"] == 840
