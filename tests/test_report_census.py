"""Tests for report Phase 5: census page, visibility counts, evidence expander,
slug URLs, alert rollups, warmup banner, scope statement, per-labeler pages."""

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

from labelwatch import db
from labelwatch.report import (
    _alert_rollups,
    _census_counts,
    _did_slug,
    _evidence_expander,
    _visibility_badge,
    generate_report,
)
from labelwatch.utils import format_ts


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _insert_labeler(conn, did, **kwargs):
    defaults = {
        "handle": None,
        "display_name": None,
        "description": None,
        "service_endpoint": None,
        "labeler_class": "third_party",
        "is_reference": 0,
        "endpoint_status": "unknown",
        "last_probed": None,
        "first_seen": format_ts(datetime.now(timezone.utc)),
        "last_seen": format_ts(datetime.now(timezone.utc)),
        "visibility_class": "declared",
        "reachability_state": "accessible",
        "classification_confidence": "high",
        "classification_reason": "declared+probe_accessible",
        "classification_version": "v1",
        "classified_at": format_ts(datetime.now(timezone.utc)),
        "auditability": "high",
        "observed_as_src": 0,
        "has_labeler_service": 1,
        "has_label_key": 0,
        "declared_record": 1,
        "likely_test_dev": 0,
        "scan_count": 5,
    }
    defaults.update(kwargs)
    cols = ["labeler_did"] + list(defaults.keys())
    vals = [did] + list(defaults.values())
    placeholders = ", ".join(["?"] * len(vals))
    col_names = ", ".join(cols)
    conn.execute(f"INSERT INTO labelers({col_names}) VALUES({placeholders})", vals)
    conn.commit()


def _insert_event(conn, did, ts, uri="at://u/post/1", val="label", event_hash=None):
    if event_hash is None:
        import hashlib
        event_hash = hashlib.sha256(f"{did}{ts}{uri}".encode()).hexdigest()[:16]
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, ts, event_hash) VALUES(?,?,?,?,?)",
        (did, uri, val, ts, event_hash),
    )
    conn.commit()


def _insert_alert(conn, rule_id, did, ts, inputs=None, evidence=None):
    if inputs is None:
        inputs = {"confidence": "high"}
    if evidence is None:
        evidence = []
    conn.execute(
        "INSERT INTO alerts(rule_id, labeler_did, ts, inputs_json, evidence_hashes_json, config_hash, receipt_hash) "
        "VALUES(?,?,?,?,?,?,?)",
        (rule_id, did, ts, json.dumps(inputs), json.dumps(evidence), "cfghash", "rcpthash"),
    )
    conn.commit()


# --- _did_slug ---

def test_did_slug_plc():
    assert _did_slug("did:plc:abc123") == "did-plc-abc123"


def test_did_slug_web():
    assert _did_slug("did:web:example.com") == "did-web-example.com"


def test_did_slug_roundtrip():
    """Slug should be reversible by replacing - back to :."""
    did = "did:plc:ar7c4by46qjdydhdevvrndac"
    slug = _did_slug(did)
    assert ":" not in slug
    assert slug == "did-plc-ar7c4by46qjdydhdevvrndac"


# --- _visibility_badge ---

def test_visibility_badge_declared():
    html = _visibility_badge("declared")
    assert "Declared" in html
    assert "badge-stable" in html


def test_visibility_badge_protocol_public():
    html = _visibility_badge("protocol_public")
    assert "Protocol" in html
    assert "badge-burst" in html


def test_visibility_badge_observed_only():
    html = _visibility_badge("observed_only")
    assert "Observed" in html
    assert "badge-fixated" in html


def test_visibility_badge_unresolved():
    html = _visibility_badge("unresolved")
    assert "Unresolved" in html
    assert "badge-low-conf" in html


def test_visibility_badge_none_defaults_unresolved():
    html = _visibility_badge(None)
    assert "Unresolved" in html


# --- _census_counts ---

def test_census_counts_basic():
    conn = _make_db()
    _insert_labeler(conn, "did:plc:a", visibility_class="declared", reachability_state="accessible",
                    classification_confidence="high", auditability="high")
    _insert_labeler(conn, "did:plc:b", visibility_class="declared", reachability_state="down",
                    classification_confidence="medium", auditability="medium")
    _insert_labeler(conn, "did:plc:c", visibility_class="observed_only", reachability_state="unknown",
                    classification_confidence="low", auditability="low")

    census = _census_counts(conn)
    assert census["visibility_class"]["declared"] == 2
    assert census["visibility_class"]["observed_only"] == 1
    assert census["reachability_state"]["accessible"] == 1
    assert census["reachability_state"]["down"] == 1
    assert census["reachability_state"]["unknown"] == 1
    assert census["classification_confidence"]["high"] == 1
    assert census["classification_confidence"]["medium"] == 1
    assert census["classification_confidence"]["low"] == 1
    assert census["auditability"]["high"] == 1
    assert census["auditability"]["medium"] == 1
    assert census["auditability"]["low"] == 1


def test_census_counts_empty_db():
    conn = _make_db()
    census = _census_counts(conn)
    for field in census:
        assert census[field] == {}


# --- _evidence_expander ---

def test_evidence_expander_with_evidence():
    conn = _make_db()
    did = "did:plc:evtest"
    _insert_labeler(conn, did, classification_reason="declared+probe_accessible",
                    classification_version="v1", classified_at="2025-01-01T00:00:00Z")
    db.insert_evidence(conn, did, "declared_record", "true", "2025-01-01T00:00:00Z", "discovery")
    db.insert_evidence(conn, did, "probe_result", "accessible", "2025-01-01T00:00:00Z", "discovery")
    conn.commit()

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did=?", (did,)).fetchone()
    html = _evidence_expander(conn, did, row)
    assert "Why classified this way" in html
    assert "declared+probe_accessible" in html
    assert "declared_record" in html
    assert "probe_result" in html
    assert "v1" in html


def test_evidence_expander_no_evidence():
    conn = _make_db()
    did = "did:plc:noev"
    _insert_labeler(conn, did, classification_reason=None, classification_version=None, classified_at=None)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did=?", (did,)).fetchone()
    html = _evidence_expander(conn, did, row)
    assert "No evidence records yet" in html
    assert "No classification yet" in html


# --- _alert_rollups ---

def test_alert_rollups_standalone_high_confidence():
    """High-confidence alerts should render as standalone rows."""
    alerts = [
        {"id": 1, "rule_id": "label_rate_spike", "labeler_did": "did:plc:a", "ts": "2025-01-01T00:00:00Z",
         "inputs_json": json.dumps({"confidence": "high"})},
    ]
    html = _alert_rollups(alerts, {"did:plc:a": "alice.bsky.social"}, {})
    assert "label_rate_spike" in html
    assert "alice.bsky.social" in html
    # Should NOT be in a rollup
    assert "labelers</summary>" not in html


def test_alert_rollups_groups_low_confidence():
    """3+ low-confidence alerts with same rule_id+ts should form a rollup."""
    ts = "2025-01-01T00:00:00Z"
    alerts = [
        {"id": i, "rule_id": "label_rate_spike", "labeler_did": f"did:plc:{i}", "ts": ts,
         "inputs_json": json.dumps({"confidence": "low"})}
        for i in range(5)
    ]
    html = _alert_rollups(alerts, {}, {})
    assert "Low confidence" in html
    assert "5 labelers" in html
    assert "<details" in html


def test_alert_rollups_small_low_confidence_not_grouped():
    """2 or fewer low-confidence alerts should NOT be rolled up."""
    ts = "2025-01-01T00:00:00Z"
    alerts = [
        {"id": i, "rule_id": "label_rate_spike", "labeler_did": f"did:plc:{i}", "ts": ts,
         "inputs_json": json.dumps({"confidence": "low"})}
        for i in range(2)
    ]
    html = _alert_rollups(alerts, {}, {})
    assert "labelers</summary>" not in html


# --- generate_report integration ---

def test_generate_report_creates_census_page():
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:r1", visibility_class="declared", reachability_state="accessible")
    _insert_labeler(conn, "did:plc:r2", visibility_class="observed_only", reachability_state="unknown")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        census_path = os.path.join(out, "census.html")
        assert os.path.exists(census_path)
        content = open(census_path).read()
        assert "Discovery Census" in content
        assert "Visibility Class" in content
        assert "Reachability State" in content
        assert "Classification Confidence" in content
        assert "Auditability" in content


def test_generate_report_slug_urls():
    """Per-labeler files should use slug URLs (did-plc-xxx.html)."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:slugtest")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        slug_path = os.path.join(out, "labeler", "did-plc-slugtest.html")
        assert os.path.exists(slug_path)
        # Old colon-based path should NOT exist
        assert not os.path.exists(os.path.join(out, "labeler", "did:plc:slugtest.html"))


def test_generate_report_warmup_banner():
    """Report should show warm-up banner when labelers have low scan_count."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:warmup1", scan_count=0)
    _insert_labeler(conn, "did:plc:warmup2", scan_count=1)
    _insert_labeler(conn, "did:plc:mature", scan_count=10)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert "Baselines forming" in content
        assert "warm-up period" in content


def test_generate_report_no_warmup_banner_when_all_mature():
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:m1", scan_count=10)
    _insert_labeler(conn, "did:plc:m2", scan_count=5)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert "Baselines forming" not in content


def test_generate_report_scope_statement():
    """Report should include the expanded scope/methods statement."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:scope")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert "observes labeler behavior only" in content
        assert "No content analysis" in content
        assert "What is a labeler?" in content


def test_generate_report_cache_headers():
    """Report HTML should include cache-control meta tag."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:cache")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert 'Cache-Control' in content
        assert 'no-cache' in content


def test_generate_report_triage_tabs():
    """Index should include triage tab bar with Active/Alerts/New/Opaque/All."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:tab")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert 'data-view="active"' in content
        assert 'data-view="alerts"' in content
        assert 'data-view="new"' in content
        assert 'data-view="opaque"' in content
        assert 'data-view="all"' in content


def test_generate_report_per_labeler_evidence():
    """Per-labeler page should include evidence expander."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    did = "did:plc:evpage"
    _insert_labeler(conn, did, classification_reason="declared+probe_accessible")
    db.insert_evidence(conn, did, "declared_record", "true", format_ts(now), "discovery")
    conn.commit()

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-evpage.html")).read()
        assert "Why classified this way" in page
        assert "declared_record" in page
        assert "declared+probe_accessible" in page


def test_generate_report_per_labeler_probe_history():
    """Per-labeler page should show probe history table."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    did = "did:plc:probepage"
    _insert_labeler(conn, did)
    db.insert_probe_history(conn, did, format_ts(now), "https://example.com/xrpc",
                            200, "accessible", 150, None, None)
    conn.commit()

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-probepage.html")).read()
        assert "Probe history" in page
        assert "accessible" in page
        assert "150ms" in page


def test_generate_report_per_labeler_no_probe_history():
    """Per-labeler page should show placeholder when no probe history."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:noprobe")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-noprobe.html")).read()
        assert "No probe history recorded yet" in page


def test_generate_report_per_labeler_profile_link():
    """Per-labeler page should include Bluesky profile link when handle exists."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:profile", handle="alice.bsky.social")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-profile.html")).read()
        assert "bsky.app/profile/alice.bsky.social" in page
        assert "Open on Bluesky" in page


def test_generate_report_per_labeler_warmup_indicator():
    """Per-labeler page should show warmup indicator for low scan_count."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:warmind", scan_count=1)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-warmind.html")).read()
        assert "warm-up period" in page


def test_generate_report_per_labeler_visibility_badge():
    """Per-labeler page should show visibility badge."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:visbadge", visibility_class="observed_only")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-visbadge.html")).read()
        assert "Observed" in page
        assert "badge-fixated" in page


def test_generate_report_sparse_labeler_page():
    """Sparse labeler (no events) should still get a useful page."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:sparse", visibility_class="declared",
                    reachability_state="accessible", scan_count=5)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page_path = os.path.join(out, "labeler", "did-plc-sparse.html")
        assert os.path.exists(page_path)
        page = open(page_path).read()
        # Should have basic info even without events
        assert "did:plc:sparse" in page
        assert "Declared" in page
        assert "accessible" in page
        assert "Why classified this way" in page
        assert "Probe history" in page


def test_generate_report_overview_json_has_census():
    """overview.json should include census counts."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:ov1", visibility_class="declared")
    _insert_labeler(conn, "did:plc:ov2", visibility_class="observed_only")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        overview = json.load(open(os.path.join(out, "overview.json")))
        assert "census" in overview
        assert "visibility_class" in overview["census"]
        assert overview["census"]["visibility_class"]["declared"] == 1
        assert overview["census"]["visibility_class"]["observed_only"] == 1
        assert "test_dev_count" in overview


def test_generate_report_labelers_json_has_v4_fields():
    """labelers.json should include new v4 fields."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:v4", visibility_class="protocol_public",
                    reachability_state="auth_required", auditability="medium",
                    classification_confidence="medium")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        labelers = json.load(open(os.path.join(out, "labelers.json")))
        assert len(labelers) == 1
        lb = labelers[0]
        assert lb["visibility_class"] == "protocol_public"
        assert lb["reachability_state"] == "auth_required"
        assert lb["auditability"] == "medium"
        assert lb["classification_confidence"] == "medium"


def test_generate_report_staleness_indicators():
    """Index page should show staleness cards for generated_at, last_ingest, etc."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:stale")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert "Generated" in content
        assert "Last ingest" in content
        assert "Last scan" in content
        assert "Last discovery" in content


def test_generate_report_alert_rollups_in_index():
    """Index with low-confidence alerts should render rollups."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    ts = format_ts(now)

    for i in range(5):
        did = f"did:plc:roll{i}"
        _insert_labeler(conn, did)
        _insert_alert(conn, "label_rate_spike", did, ts,
                       inputs={"confidence": "low"})

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert "Low confidence" in content
        assert "5 labelers" in content


def test_generate_report_test_dev_toggle():
    """Index should show test/dev toggle with count."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:real1", likely_test_dev=0)
    _insert_labeler(conn, "did:plc:test1", likely_test_dev=1)
    _insert_labeler(conn, "did:plc:test2", likely_test_dev=1)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert "toggle-test-dev" in content
        assert "Show test/dev (2)" in content


def test_generate_report_data_attributes():
    """Labeler rows should have data-* attributes for JS filtering."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:dataattr", visibility_class="observed_only",
                    reachability_state="down", likely_test_dev=1)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        assert 'data-opaque="1"' in content
        assert 'data-test-dev="1"' in content


def test_generate_report_warmup_suppresses_scores_card():
    """Labeler in warming_up regime should NOT show derived scores card, even if scores exist."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:warmscore",
                    regime_state="warming_up",
                    auditability_risk=15,
                    inference_risk=56,
                    temporal_coherence=28)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-warmscore.html")).read()
        assert "Derived scores" not in page  # scores card is suppressed


def test_generate_report_graduated_shows_scores_card():
    """Labeler with a non-warmup regime should show derived scores card."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:graduated",
                    regime_state="stable",
                    auditability_risk=10,
                    inference_risk=0,
                    temporal_coherence=80,
                    scan_count=10)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-graduated.html")).read()
        assert "Derived scores" in page
        assert "Regime" in page
