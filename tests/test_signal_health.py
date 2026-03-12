"""Tests for per-labeler signal health."""
from labelwatch import db
from labelwatch.signal_health import classify_labeler_signal, signal_health_snapshot


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _seed(conn, labelers):
    for lab in labelers:
        conn.execute(
            "INSERT OR REPLACE INTO labelers "
            "(labeler_did, handle, events_7d, events_30d, observed_as_src, "
            " is_reference, regime_state) "
            "VALUES (?, ?, ?, ?, 1, ?, ?)",
            (lab["did"], lab.get("handle"), lab.get("ev7", 0), lab.get("ev30", 0),
             lab.get("is_ref", 0), lab.get("regime", "stable")),
        )
    conn.commit()


# --- classify_labeler_signal ---

def test_classify_active():
    # 7d/30d ≈ 0.23, steady
    assert classify_labeler_signal(230, 1000) == "active"


def test_classify_gone_dark():
    assert classify_labeler_signal(0, 5000) == "gone_dark"


def test_classify_degrading():
    # ratio < 0.10
    assert classify_labeler_signal(5, 1000) == "degrading"


def test_classify_surging():
    # ratio > 0.50
    assert classify_labeler_signal(600, 1000) == "surging"


def test_classify_never():
    assert classify_labeler_signal(0, 0) == "never"


def test_classify_new():
    assert classify_labeler_signal(10, 20) == "new"


def test_classify_quiet():
    assert classify_labeler_signal(0, 20) == "quiet"


# --- signal_health_snapshot ---

def test_snapshot_ok():
    conn = _make_db()
    _seed(conn, [
        {"did": "did:plc:a", "handle": "a.test", "ev7": 100, "ev30": 400},
        {"did": "did:plc:b", "handle": "b.test", "ev7": 50, "ev30": 200},
    ])
    snap = signal_health_snapshot(conn)
    assert snap["verdict"] == "OK"
    assert snap["classifications"]["active"] == 2
    assert snap["gone_dark"] == []


def test_snapshot_warn_gone_dark():
    conn = _make_db()
    _seed(conn, [
        {"did": "did:plc:a", "handle": "a.test", "ev7": 100, "ev30": 400},
        {"did": "did:plc:b", "handle": "b.test", "ev7": 0, "ev30": 500},
    ])
    snap = signal_health_snapshot(conn)
    assert snap["verdict"] == "WARN"
    assert len(snap["gone_dark"]) == 1
    assert snap["gone_dark"][0]["handle"] == "b.test"


def test_snapshot_degraded_many_dark():
    conn = _make_db()
    _seed(conn, [
        {"did": "did:plc:a", "ev7": 100, "ev30": 400},
        {"did": "did:plc:b", "ev7": 0, "ev30": 500},
        {"did": "did:plc:c", "ev7": 0, "ev30": 300},
        {"did": "did:plc:d", "ev7": 0, "ev30": 200},
    ])
    snap = signal_health_snapshot(conn)
    assert snap["verdict"] == "DEGRADED"


def test_snapshot_critical_reference_dark():
    conn = _make_db()
    _seed(conn, [
        {"did": "did:plc:a", "handle": "ref.test", "ev7": 0, "ev30": 10000,
         "is_ref": 1},
        {"did": "did:plc:b", "handle": "b.test", "ev7": 100, "ev30": 400},
    ])
    snap = signal_health_snapshot(conn)
    assert snap["verdict"] == "CRITICAL"
    assert len(snap["reference_issues"]) == 1


def test_snapshot_empty():
    conn = _make_db()
    snap = signal_health_snapshot(conn)
    assert snap["verdict"] == "OK"
    assert snap["total_observed"] == 0


def test_snapshot_surging_detected():
    conn = _make_db()
    _seed(conn, [
        {"did": "did:plc:a", "handle": "surge.test", "ev7": 800, "ev30": 1000},
    ])
    snap = signal_health_snapshot(conn)
    assert len(snap["surging"]) == 1
    assert snap["surging"][0]["handle"] == "surge.test"
