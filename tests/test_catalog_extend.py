"""Extending a cold catalog with a weekly trim partition (pyarrow-free)."""
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pytest

from labelwatch import db, retention, trim
from labelwatch.frontdoor import lookup_subject
from tools.label_events_catalog_extend import (
    ExtendRefused,
    extend_catalog,
    summary_name,
)
from tools.label_events_frontdoor_archive import build_catalog, write_summary, SummaryRef

RECEIPT = {"overall_verdict": "admissible", "generated_at": "2026-09-03T00:00:00Z"}
NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)
SUBJECTS = [f"did:plc:s{i}" for i in range(6)]


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for name in ("LABELWATCH_RETENTION_FLOOR", "LABELWATCH_FRONTDOOR_COLD_CATALOG",
                 "LABELWATCH_BELOW_FLOOR_POLICY"):
        monkeypatch.delenv(name, raising=False)


def _rows(start_day, days, per_day=60, prefix="r"):
    out = []
    start = datetime.fromisoformat(start_day).replace(tzinfo=timezone.utc)
    for d in range(days):
        for i in range(per_day):
            ts = (start + timedelta(days=d, seconds=i * 1400)).strftime("%Y-%m-%dT%H:%M:%SZ")
            subject = SUBJECTS[i % len(SUBJECTS)]
            uri = subject if i % 4 == 0 else f"at://{subject}/app.bsky.feed.post/{(d * i) % 70}"
            out.append((f"did:plc:l{i % 3}", None, uri, None, ["spam", "warn"][i % 2],
                        int(i % 9 == 0), None, None, ts, f"{prefix}-{d}-{i}", subject))
    return out


def _base_catalog(tmp_path, rows, start_day, days, watermark):
    summaries = tmp_path / "base-summaries"
    summaries.mkdir()
    refs = []
    for d in range(days):
        day = (datetime.fromisoformat(start_day) + timedelta(days=d)).date().isoformat()
        day_rows = [{"labeler_did": r[0], "uri": r[2], "val": r[4], "neg": r[5],
                     "ts": r[8], "target_did": r[10]} for r in rows if r[8][:10] == day]
        receipt = write_summary(iter(day_rows), summaries / summary_name(day), {
            "format": "labelwatch.frontdoor-cold-summary", "format_version": "2",
            "day": day, "manifest_sha256": "a" * 64, "partition_sha256": "b" * 64,
            "source_database_sha256": "c" * 64})
        refs.append(SummaryRef(summaries / summary_name(day), receipt["summary_sha256"]))
    (tmp_path / "base").mkdir()
    manifest = build_catalog(refs, tmp_path / "base" / "frontdoor-cold-catalog.sqlite",
                             source_event_id_upper_bound=watermark, uri_top_n=50)
    return manifest, summaries


@pytest.fixture
def world(tmp_path):
    cold = _rows("2026-08-07", 7, prefix="cold")          # [08-07, 08-14), archived
    live_rows = _rows("2026-08-14", 14, prefix="live")    # [08-14, 08-28), live
    full = db.connect(str(tmp_path / "full.db"))
    db.init_db(full)
    db.insert_label_events(full, cold + live_rows)
    full.commit()
    watermark0 = len(cold)
    base_manifest, summaries = _base_catalog(tmp_path, cold, "2026-08-07", 7, watermark0)

    live_path = tmp_path / "live.db"
    live = db.connect(str(live_path))
    db.init_db(live)
    db.insert_label_events(live, cold)          # same ids as the full history
    live.execute("DELETE FROM label_events")    # generation-1: cold rows gone
    db.insert_label_events(live, live_rows)
    retention.set_retention_floor(live, "2026-08-14T00:00:00Z", "archive://gen1")
    db.set_meta(live, retention.RETENTION_HISTORY_START_KEY, "2026-08-07T00:00:00Z")
    live.commit()
    yield {"full": full, "live": live, "live_path": live_path, "base": base_manifest,
           "summaries": summaries, "tmp": tmp_path}
    full.close()
    live.close()


def _lookup(conn, subject, catalog=None):
    result = asdict(lookup_subject(conn, subject, audit_receipt=RECEIPT, cold_catalog=catalog))
    result.pop("generated_at")
    return result


def test_base_catalog_serves_complete_history(world):
    custody = retention.load_catalog_custody(str(world["base"]))
    assert retention.catalog_coverage(world["live"], custody)["status"] == "complete"
    for subject in SUBJECTS:
        observed = _lookup(world["live"], subject, custody)
        assert observed.pop("cold_history")["status"] == "catalog"
        expected = _lookup(world["full"], subject)
        expected.pop("cold_history")
        assert observed == expected


def test_extend_install_trim_keeps_answers_exact(world):
    tmp = world["tmp"]
    part = trim.export_partition(str(world["live_path"]), "2026-08-21T00:00:00Z",
                                 str(tmp / "nfs"))
    receipt = extend_catalog(world["base"], world["summaries"], part, tmp / "v-next")
    assert receipt["start_day"] == "2026-08-07"
    assert receipt["end_day_exclusive"] == "2026-08-21"
    assert len(receipt["new_days"]) == 7
    custody = retention.load_catalog_custody(receipt["manifest"])
    assert custody.load_status == "loaded"
    # Installed but floor not yet moved: catalog runs ahead, refuse.
    assert retention.catalog_coverage(world["live"], custody)["status"] == "stale"
    trim.run_trim(world["live"], "2026-08-21T00:00:00Z", part, now=NOW,
                  catalog_custody=custody)
    assert retention.catalog_coverage(world["live"], custody)["status"] == "complete"
    for subject in SUBJECTS:
        observed = _lookup(world["live"], subject, custody)
        assert observed.pop("cold_history")["status"] == "catalog"
        expected = _lookup(world["full"], subject)
        expected.pop("cold_history")
        assert observed == expected


def test_extend_refuses_discontinuous_partition(world):
    tmp = world["tmp"]
    live = world["live"]
    retention.set_retention_floor(live, "2026-08-15T00:00:00Z", None)
    live.execute("DELETE FROM label_events WHERE ts < '2026-08-15T00:00:00Z'")
    live.commit()
    part = trim.export_partition(str(world["live_path"]), "2026-08-21T00:00:00Z",
                                 str(tmp / "nfs"))
    with pytest.raises(ExtendRefused, match="catalog ends at 2026-08-14"):
        extend_catalog(world["base"], world["summaries"], part, tmp / "out")


def test_extend_refuses_tampered_base_summary(world):
    tmp = world["tmp"]
    part = trim.export_partition(str(world["live_path"]), "2026-08-21T00:00:00Z",
                                 str(tmp / "nfs"))
    victim = world["summaries"] / summary_name("2026-08-09")
    with victim.open("ab") as handle:
        handle.write(b"x")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        extend_catalog(world["base"], world["summaries"], part, tmp / "out")


def test_extend_refuses_rows_outside_new_days(world):
    tmp = world["tmp"]
    live = world["live"]
    # A space-separated ts sorts below the floor string but in no new day.
    db.insert_label_events(live, [("did:plc:l0", None, SUBJECTS[0], None, "spam", 0, None,
                                   None, "2026-08-21 00:00:01Z", "odd", SUBJECTS[0])])
    live.commit()
    part = trim.export_partition(str(world["live_path"]), "2026-08-21T00:00:00Z",
                                 str(tmp / "nfs"))
    with pytest.raises(ExtendRefused, match="outside the days"):
        extend_catalog(world["base"], world["summaries"], part, tmp / "out")
