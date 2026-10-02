"""Coverage guard between the live retention floor and the cold catalog."""
import json
import os
from dataclasses import asdict

import pytest

from labelwatch import db, retention
from labelwatch.frontdoor import lookup_subject, render_result_body_html

from _cold_catalog_fixture import build_test_catalog

FLOOR = "2026-08-14T00:00:00Z"
START = "2026-08-10"
END = "2026-08-14"
WATERMARK = 1000
SUBJECT = "did:plc:subject"
LABELER = "did:plc:labeler"
RECEIPT = {"overall_verdict": "admissible", "generated_at": "2026-09-03T00:00:00Z"}

COLD = [
    (LABELER, f"at://{SUBJECT}/app.bsky.feed.post/a", "spam", 0, "2026-08-11T01:00:00Z", SUBJECT),
    (LABELER, SUBJECT, "warn", 0, "2026-08-12T01:00:00Z", SUBJECT),
    (LABELER, "https://example.invalid", "other", 0, "2026-08-12T02:00:00Z", None),
]
WARM = [
    (LABELER, f"at://{SUBJECT}/app.bsky.feed.post/a", "spam", 1, "2026-08-20T01:00:00Z", SUBJECT),
]


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for name in ("LABELWATCH_RETENTION_FLOOR", "LABELWATCH_RETENTION_ARCHIVE_REF",
                 "LABELWATCH_FRONTDOOR_COLD_CATALOG"):
        monkeypatch.delenv(name, raising=False)


def _rows(items, prefix):
    return [(lab, tgt or uri, uri, None, val, neg, None, None, ts, f"{prefix}-{i}", tgt)
            for i, (lab, uri, val, neg, ts, tgt) in enumerate(items)]


def _db(path, items, prefix):
    conn = db.connect(str(path))
    db.init_db(conn)
    db.insert_label_events(conn, _rows(items, prefix))
    conn.commit()
    return conn


@pytest.fixture
def catalog_manifest(tmp_path):
    return build_test_catalog(tmp_path / "catalog", COLD, start_day=START,
                              end_day_exclusive=END, watermark=WATERMARK)


@pytest.fixture
def custody(catalog_manifest):
    c = retention.load_catalog_custody(str(catalog_manifest), filesystem_type="ext4")
    assert c.load_status == "loaded", c.reason
    return c


@pytest.fixture
def live(tmp_path):
    conn = _db(tmp_path / "live.db", WARM, "warm")
    retention.set_retention_floor(conn, FLOOR, "archive://gen1")
    db.set_meta(conn, retention.RETENTION_HISTORY_START_KEY, START + "T00:00:00Z")
    conn.commit()
    yield conn
    conn.close()


def test_floor_equal_catalog_end_is_complete(live, custody):
    cov = retention.catalog_coverage(live, custody)
    assert cov["status"] == "complete"
    assert cov["catalog"]["end_day_exclusive"] == END
    catalog, _ = retention.usable_catalog(live, custody)
    assert catalog is custody.catalog
    block = retention.cold_history_block(live, None, cov)
    assert block["status"] == "catalog"
    assert not retention.is_unavailable(block)
    # A caller that does not serve catalog aggregates still refuses.
    assert retention.is_unavailable(retention.cold_history_block(live, None))


def test_complete_coverage_frontdoor_matches_full_history(tmp_path, live, custody):
    full = _db(tmp_path / "full.db", COLD + WARM, "x")
    try:
        expected = asdict(lookup_subject(full, SUBJECT, audit_receipt=RECEIPT))
    finally:
        full.close()
    result = lookup_subject(live, SUBJECT, audit_receipt=RECEIPT, cold_catalog=custody)
    observed = asdict(result)
    for value in (expected, observed):
        value.pop("generated_at")
    assert observed.pop("cold_history")["status"] == "catalog"
    expected.pop("cold_history")
    assert observed == expected
    assert result.refusal is None
    assert "earlier history unavailable" not in render_result_body_html(result)


def test_complete_coverage_cold_only_subject_is_served(tmp_path, custody):
    conn = _db(tmp_path / "empty.db", [], "none")
    retention.set_retention_floor(conn, FLOOR, None)
    db.set_meta(conn, retention.RETENTION_HISTORY_START_KEY, "2026-08-11T00:00:00Z")
    conn.commit()
    result = lookup_subject(conn, SUBJECT, audit_receipt=RECEIPT, cold_catalog=custody)
    assert result.refusal is None
    assert {lab.labeler_did for lab in result.labelers} == {LABELER}


def _assert_frontdoor_refuses(conn, custody):
    # The subject's only live row is gone: absence must not be inferred.
    conn.execute("DELETE FROM label_events")
    conn.commit()
    result = lookup_subject(conn, SUBJECT, audit_receipt=RECEIPT, cold_catalog=custody)
    assert result.refusal == "cold_history_unavailable"
    assert result.cold_history["status"] == "unavailable"


def test_history_start_unknown_is_gap(live, custody):
    db.set_meta(live, retention.RETENTION_HISTORY_START_KEY, "")
    live.commit()
    cov = retention.catalog_coverage(live, custody)
    assert (cov["status"], cov["reason"]) == ("gap", "history_start_unknown")
    assert retention.usable_catalog(live, custody)[0] is None
    _assert_frontdoor_refuses(live, custody)


def test_history_start_before_catalog_start_is_gap(live, custody):
    db.set_meta(live, retention.RETENTION_HISTORY_START_KEY, "2026-02-24T00:00:00Z")
    live.commit()
    cov = retention.catalog_coverage(live, custody)
    assert (cov["status"], cov["reason"]) == ("gap", "catalog_starts_after_history_start")
    _assert_frontdoor_refuses(live, custody)


def test_invalid_history_start_is_gap(live, custody):
    db.set_meta(live, retention.RETENTION_HISTORY_START_KEY, "2026-08-10T00:00:00")
    live.commit()
    assert retention.catalog_coverage(live, custody)["status"] == "gap"


def test_floor_after_catalog_end_is_stale(live, custody):
    retention.set_retention_floor(live, "2026-08-21T00:00:00Z", None)
    live.commit()
    cov = retention.catalog_coverage(live, custody)
    assert (cov["status"], cov["reason"]) == ("stale", "catalog_ends_before_live_floor")
    _assert_frontdoor_refuses(live, custody)


def test_floor_before_catalog_end_is_stale(live, custody):
    retention.set_retention_floor(live, "2026-08-13T00:00:00Z", None)
    live.commit()
    cov = retention.catalog_coverage(live, custody)
    assert (cov["status"], cov["reason"]) == ("stale", "catalog_ends_after_live_floor")


def test_env_floor_override_participates(live, custody, monkeypatch):
    monkeypatch.setenv("LABELWATCH_RETENTION_FLOOR", "2026-08-15T00:00:00Z")
    assert retention.catalog_coverage(live, custody)["status"] == "stale"


def test_no_floor_with_catalog_is_not_used(tmp_path, custody):
    conn = _db(tmp_path / "nofloor.db", COLD + WARM, "x")
    cov = retention.catalog_coverage(conn, custody)
    assert (cov["status"], cov["reason"]) == ("stale", "no_live_floor")
    result = lookup_subject(conn, SUBJECT, audit_receipt=RECEIPT, cold_catalog=custody)
    assert result.cold_history is None and result.refusal is None


def test_absent_catalog(live, tmp_path):
    assert retention.catalog_coverage(live, None)["status"] == "absent"
    missing = retention.load_catalog_custody(str(tmp_path / "nope.manifest.json"))
    cov = retention.catalog_coverage(live, missing)
    assert (cov["status"], cov["reason"]) == ("absent", "manifest_missing")
    unconfigured = retention.load_catalog_custody()
    assert retention.catalog_coverage(live, unconfigured)["reason"] == "not_configured"
    _assert_frontdoor_refuses(live, missing)


def test_env_configures_catalog(monkeypatch, catalog_manifest, live):
    monkeypatch.setenv("LABELWATCH_FRONTDOOR_COLD_CATALOG", str(catalog_manifest))
    c = retention.load_catalog_custody(filesystem_type="ext4")
    assert retention.catalog_coverage(live, c)["status"] == "complete"


def test_tampered_catalog_refuses_at_load(live, catalog_manifest):
    database = catalog_manifest.parent / "catalog.sqlite"
    with database.open("r+b") as handle:
        handle.seek(200)
        handle.write(b"\x01")
    c = retention.load_catalog_custody(str(catalog_manifest), filesystem_type="ext4")
    assert c.load_status == "tampered"
    assert "checksum" in c.reason
    assert retention.catalog_coverage(live, c)["status"] == "tampered"
    _assert_frontdoor_refuses(live, c)


def test_tampered_manifest_refuses(live, catalog_manifest):
    manifest = json.loads(catalog_manifest.read_text())
    manifest["coverage"]["start_day"] = "2026-08-01"
    catalog_manifest.write_text(json.dumps(manifest))
    c = retention.load_catalog_custody(str(catalog_manifest), filesystem_type="ext4")
    assert retention.catalog_coverage(live, c)["status"] == "tampered"


def test_catalog_changed_after_verification_is_tampered(live, custody):
    path = custody.catalog.database_path
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    cov = retention.catalog_coverage(live, custody)
    assert (cov["status"], cov["reason"]) == (
        "tampered", "catalog_file_changed_after_verification")
    _assert_frontdoor_refuses(live, custody)


def test_nfs_hosted_catalog_refuses(live, catalog_manifest):
    c = retention.load_catalog_custody(str(catalog_manifest), filesystem_type="nfs4")
    assert c.load_status == "not_local"
    cov = retention.catalog_coverage(live, c)
    assert (cov["status"], cov["reason"]) == ("absent", "catalog_not_on_local_filesystem")
    _assert_frontdoor_refuses(live, c)


def test_invalid_floor_fails_closed(live, custody, monkeypatch):
    monkeypatch.setenv("LABELWATCH_RETENTION_FLOOR", "yesterday")
    with pytest.raises(ValueError):
        retention.catalog_coverage(live, custody)


def test_health_block_consistency(live, custody, tmp_path):
    block = retention.health_block(live, custody)
    assert block["consistent"] is True
    assert block["coverage"] == "complete"
    assert block["live_floor"] == FLOOR
    assert block["catalog"]["start_day"] == START
    assert block["catalog"]["end_day_exclusive"] == END

    assert retention.health_block(live, None)["consistent"] is False
    bare = _db(tmp_path / "bare.db", [], "b")
    assert retention.health_block(bare, None)["consistent"] is True
