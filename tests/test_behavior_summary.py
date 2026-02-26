"""Tests for behavior summary, data maturity line, and badge explainers."""

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from labelwatch.report import (
    _badge_explainer_html,
    _behavior_summary,
    _data_maturity_line,
    generate_report,
)
from labelwatch.utils import format_ts
from labelwatch import db


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


# --- _behavior_summary ---

def test_behavior_summary_flappy_when_regime_flapping():
    """Stability should be 'flappy' when regime_state == 'flapping'."""
    result = _behavior_summary("flapping", set(), [1, 2, 3])
    assert result["stability"] == "flappy"
    assert result["stability_css"] == "badge-churn"


def test_behavior_summary_reversal_heavy_when_flip_flop_fired():
    """Stability should be 'reversal-heavy' when flip_flop in rules_fired."""
    result = _behavior_summary("stable", {"flip_flop"}, [1, 2, 3])
    assert result["stability"] == "reversal-heavy"
    assert result["stability_css"] == "badge-flipflop"


def test_behavior_summary_stable_default_with_events():
    """Stability should be 'stable' when event data exists and no flip_flop/flapping."""
    result = _behavior_summary("stable", set(), [0, 5, 3, 0, 1])
    assert result["stability"] == "stable"
    assert result["stability_css"] == "badge-stable"


def test_behavior_summary_unknown_stability_no_events():
    """Stability should be 'unknown' when no event data."""
    result = _behavior_summary("stable", set(), [0, 0, 0])
    assert result["stability"] == "unknown"
    assert result["stability_css"] == "badge-low-conf"


def test_behavior_summary_burst_prone_high_burstiness():
    """Tempo should be 'burst-prone' when burstiness_index >= 65."""
    # Create very bursty data: one huge spike, rest zeros
    counts = [0] * 167 + [500]
    result = _behavior_summary("stable", set(), counts)
    assert result["tempo"] == "burst-prone"
    assert result["tempo_css"] == "badge-burst"
    assert result["burstiness"] >= 65


def test_behavior_summary_steady_low_burstiness():
    """Tempo should be 'steady' when burstiness_index < 65 and has data."""
    # Uniform data = low burstiness
    counts = [10] * 168
    result = _behavior_summary("stable", set(), counts)
    assert result["tempo"] == "steady"
    assert result["tempo_css"] == "badge-stable"
    assert result["burstiness"] < 65


def test_behavior_summary_unknown_tempo_empty_counts():
    """Tempo should be 'unknown' for empty hourly counts."""
    result = _behavior_summary("stable", set(), [])
    assert result["tempo"] == "unknown"
    assert result["tempo_css"] == "badge-low-conf"


def test_behavior_summary_warmup_override():
    """Warming_up regime suppresses axes entirely."""
    result = _behavior_summary("warming_up", {"flip_flop"}, [1, 2, 3])
    assert result["warmup"] is True
    assert result["stability"] == "warming_up"
    assert result["tempo"] == "warming_up"
    assert "Baselines forming" in result["one_liner"]
    assert result["stability_css"] == "badge-low-conf"


# --- _data_maturity_line ---

def test_data_maturity_line_new_labeler():
    """Should show '<24h' for very new labelers."""
    row = {
        "first_seen": format_ts(datetime.now(timezone.utc) - timedelta(hours=2)),
        "scan_count": 1,
        "coverage_ratio": None,
        "regime_state": "warming_up",
    }
    html = _data_maturity_line(row)
    assert "&lt;24h" in html
    assert "1 scan" in html
    assert "Warmup" in html


def test_data_maturity_line_pluralizes_scans():
    """Should pluralize '47 scans' vs '1 scan'."""
    row_single = {
        "first_seen": format_ts(datetime.now(timezone.utc) - timedelta(days=5)),
        "scan_count": 1,
        "coverage_ratio": None,
        "regime_state": "warming_up",
    }
    row_multi = {
        "first_seen": format_ts(datetime.now(timezone.utc) - timedelta(days=5)),
        "scan_count": 47,
        "coverage_ratio": None,
        "regime_state": "stable",
    }
    html_single = _data_maturity_line(row_single)
    html_multi = _data_maturity_line(row_multi)
    assert "1 scan" in html_single
    assert "1 scans" not in html_single
    assert "47 scans" in html_multi


def test_data_maturity_line_omits_coverage_when_none():
    """Coverage should be omitted entirely when NULL."""
    row = {
        "first_seen": format_ts(datetime.now(timezone.utc) - timedelta(days=14)),
        "scan_count": 10,
        "coverage_ratio": None,
        "regime_state": "stable",
    }
    html = _data_maturity_line(row)
    assert "Coverage" not in html
    assert "Ready" in html


def test_data_maturity_line_shows_coverage():
    """Coverage should appear when ratio is set."""
    row = {
        "first_seen": format_ts(datetime.now(timezone.utc) - timedelta(days=14)),
        "scan_count": 10,
        "coverage_ratio": 0.95,
        "regime_state": "stable",
    }
    html = _data_maturity_line(row)
    assert "Coverage 95%" in html


def test_data_maturity_line_warmup_state():
    """Should show 'Warmup' when regime is warming_up or scan_count < 3."""
    row = {
        "first_seen": format_ts(datetime.now(timezone.utc) - timedelta(days=14)),
        "scan_count": 2,
        "coverage_ratio": None,
        "regime_state": "stable",
    }
    html = _data_maturity_line(row)
    assert "Warmup" in html


# --- One-liner ---

def test_one_liner_mixed_unknown_omits_unknown():
    """When one axis is unknown, omit that part."""
    result = _behavior_summary("stable", set(), [])  # unknown tempo, stable = no events so unknown too
    # Actually this gives unknown stability too (no events). Let's test with specific inputs.
    # unknown tempo + known stability: pass hourly_counts with data but empty for tempo
    result = _behavior_summary("stable", set(), [0, 0, 0])  # no events = unknown both
    assert result["one_liner"] == ""

    # Use flapping regime (known stability) + no data (unknown tempo)
    result = _behavior_summary("flapping", set(), [0, 0, 0])
    assert result["stability"] == "flappy"
    assert result["tempo"] == "unknown"
    assert result["one_liner"] == "Flapping regime."


def test_one_liner_full_combo():
    """When both axes known, show full one-liner."""
    counts = [10] * 168  # steady + stable
    result = _behavior_summary("stable", set(), counts)
    assert result["one_liner"] == "Steady cadence, stable labeling."


def test_one_liner_burst_reversal():
    """Burst-prone + reversal-heavy combo."""
    counts = [0] * 167 + [500]  # bursty
    result = _behavior_summary("stable", {"flip_flop"}, counts)
    assert result["stability"] == "reversal-heavy"
    assert result["tempo"] == "burst-prone"
    assert result["one_liner"] == "Burst-prone with elevated reversals."


# --- _badge_explainer_html ---

def test_badge_explainer_warmup_returns_empty():
    """Warmup summary should produce no explainer."""
    summary = _behavior_summary("warming_up", set(), [1, 2])
    html = _badge_explainer_html(summary)
    assert html == ""


def test_badge_explainer_stable_steady():
    """Should explain both tags."""
    counts = [10] * 168
    summary = _behavior_summary("stable", set(), counts)
    html = _badge_explainer_html(summary)
    assert "Why these tags?" in html
    assert "No flip-flop sequences" in html
    assert "Burstiness index" in html
    assert "/100" in html


def test_badge_explainer_reversal_with_inputs():
    """Should use flip_flop_count from alert inputs when available."""
    counts = [10] * 168
    summary = _behavior_summary("stable", {"flip_flop"}, counts)
    latest = {"flip_flop": {"inputs_json": json.dumps({"flip_flop_count": 3})}}
    html = _badge_explainer_html(summary, latest)
    assert "3 apply" in html
    assert "reapply sequences" in html


# --- Integration: generate_report with behavior summary ---

def test_generate_report_per_labeler_has_maturity_line():
    """Per-labeler page should include data maturity line."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:matline", scan_count=10)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-matline.html")).read()
        assert "data-maturity" in page
        assert "Observed" in page
        assert "Ready" in page


def test_generate_report_per_labeler_has_behavior_summary():
    """Per-labeler page should include behavior summary badges."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:behav", scan_count=10)
    # Add some events so stability/tempo are not unknown
    for i in range(20):
        ts = format_ts(now - timedelta(hours=i * 8))
        _insert_event(conn, "did:plc:behav", ts, uri=f"at://u/post/{i}")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-behav.html")).read()
        assert "behavior-one-liner" in page
        assert "badge-explainer" in page
        assert "Why these tags?" in page


def test_generate_report_warmup_labeler_shows_warming_up():
    """Warmup labeler should show 'Warming up' badge, not axes."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:warmbs", scan_count=1, regime_state="warming_up")

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        page = open(os.path.join(out, "labeler", "did-plc-warmbs.html")).read()
        assert "Warming up" in page
        assert "Baselines forming" in page


def test_generate_report_triage_table_has_behavior_badges():
    """Triage table should show behavior badges (not old flat list)."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    _insert_labeler(conn, "did:plc:triage1", scan_count=10)
    # Add events and a flip_flop alert
    for i in range(10):
        ts = format_ts(now - timedelta(hours=i * 8))
        _insert_event(conn, "did:plc:triage1", ts, uri=f"at://u/post/{i}")
    _insert_alert(conn, "flip_flop", "did:plc:triage1", format_ts(now - timedelta(hours=1)))

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "report")
        generate_report(conn, out, now=now)
        content = open(os.path.join(out, "index.html")).read()
        # Should have reversal-heavy badge (from flip_flop alert)
        assert "Reversal-heavy" in content
        assert "badge-flipflop" in content
