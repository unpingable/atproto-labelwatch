"""Tests for the coverage watermark feature (ingest_outcomes + coverage gating)."""

import socket
import urllib.error
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from labelwatch import db
from labelwatch.config import Config
from labelwatch.ingest import _classify_exception
from labelwatch.rules import (
    _build_coverage_cache,
    data_gap,
    label_rate_spike,
    run_rules,
)
from labelwatch.scan import _cleanup_ingest_outcomes, _update_coverage_columns
from labelwatch.utils import format_ts


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _insert_outcome(conn, labeler_did, ts, attempt_id, outcome, events_fetched=0,
                    source="service"):
    db.insert_ingest_outcome(
        conn, labeler_did, ts, attempt_id, outcome, events_fetched,
        None, 100, None, None, source,
    )


# --- 1. Outcome recording ---

def test_outcome_recording():
    """Success, empty, partial, timeout, error outcomes all insert correctly."""
    conn = _make_db()
    ts = "2025-01-01T00:00:00Z"
    aid = uuid4().hex

    for outcome in ("success", "empty", "partial", "timeout", "error"):
        _insert_outcome(conn, "did:plc:a", ts, aid, outcome)

    rows = conn.execute("SELECT * FROM ingest_outcomes").fetchall()
    assert len(rows) == 5
    outcomes = {r["outcome"] for r in rows}
    assert outcomes == {"success", "empty", "partial", "timeout", "error"}


# --- 2. Coverage ratio: success + empty count ---

def test_coverage_ratio_success_and_empty():
    """Coverage ratio counts success and empty as good coverage."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    aid = uuid4().hex
    did = "did:plc:a"

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?,?,?)",
        (did, ts, ts),
    )
    _insert_outcome(conn, did, ts, aid, "success", events_fetched=10)
    _insert_outcome(conn, did, ts, aid, "empty")
    _insert_outcome(conn, did, ts, aid, "error")
    _insert_outcome(conn, did, ts, aid, "timeout")
    conn.commit()

    cfg = Config(coverage_window_minutes=30, coverage_threshold=0.5)
    cache = _build_coverage_cache(conn, now, cfg)

    assert did in cache
    assert cache[did]["attempts"] == 4
    assert cache[did]["successes"] == 2
    assert cache[did]["ratio"] == 0.5
    assert cache[did]["sufficient"] is True


# --- 3. Partial does not boost coverage ---

def test_partial_does_not_count_as_success():
    """Partial outcomes from service ingest do not count as good coverage."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    aid = uuid4().hex
    did = "did:plc:a"

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?,?,?)",
        (did, ts, ts),
    )
    # 1 success, 3 partial
    _insert_outcome(conn, did, ts, aid, "success", events_fetched=10)
    _insert_outcome(conn, did, ts, aid, "partial")
    _insert_outcome(conn, did, ts, aid, "partial")
    _insert_outcome(conn, did, ts, aid, "partial")
    conn.commit()

    cfg = Config(coverage_window_minutes=30, coverage_threshold=0.5)
    cache = _build_coverage_cache(conn, now, cfg)

    assert cache[did]["successes"] == 1
    assert cache[did]["attempts"] == 4
    assert cache[did]["ratio"] == 0.25
    assert cache[did]["sufficient"] is False


# --- 4. Empty from multi counts as good coverage ---

def test_empty_from_multi_counts_as_good():
    """Empty outcome from per-labeler (multi) ingest is confirmed quiet — counts as good."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    aid = uuid4().hex
    did = "did:plc:a"

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?,?,?)",
        (did, ts, ts),
    )
    _insert_outcome(conn, did, ts, aid, "empty", source="multi")
    conn.commit()

    cfg = Config(coverage_window_minutes=30, coverage_threshold=0.5)
    cache = _build_coverage_cache(conn, now, cfg)

    assert cache[did]["successes"] == 1
    assert cache[did]["sufficient"] is True


# --- 5. Coverage gates anomaly rules ---

def test_coverage_gates_anomaly_rules():
    """Low coverage suppresses label_rate_spike — the rule is skipped entirely."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    did = "did:plc:spiker"

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen, is_reference, scan_count) VALUES(?,?,?,0,10)",
        (did, format_ts(now - timedelta(days=10)), ts),
    )

    # Insert events that would trigger a spike (50 events, no baseline)
    for i in range(50):
        offset = timedelta(minutes=1 + (i % 14))
        ev_ts = format_ts(now - offset)
        conn.execute(
            "INSERT OR IGNORE INTO label_events(labeler_did, uri, val, ts, event_hash) VALUES(?,?,?,?,?)",
            (did, f"at://{did}/post/{i}", "test", ev_ts, f"hash_{i}"),
        )

    # Insert coverage outcomes: all errors -> low coverage
    aid = uuid4().hex
    for _ in range(10):
        _insert_outcome(conn, did, ts, aid, "error")
    conn.commit()

    cfg = Config(
        spike_min_count_default=5,
        spike_k=10.0,
        warmup_enabled=False,
        coverage_window_minutes=30,
        coverage_threshold=0.5,
    )
    cov_cache = _build_coverage_cache(conn, now, cfg)
    alerts = label_rate_spike(conn, cfg, now, _cov_cache=cov_cache)

    # Rule should be suppressed — no alerts
    assert len(alerts) == 0


# --- 6. DATA_GAP alert fires ---

def test_data_gap_alert_fires():
    """data_gap alert fires for labelers with low coverage, includes rich inputs."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    did = "did:plc:gappy"

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen, scan_count) VALUES(?,?,?,10)",
        (did, format_ts(now - timedelta(days=10)), ts),
    )

    # All errors -> low coverage
    aid = uuid4().hex
    _insert_outcome(conn, did, ts, aid, "error")
    _insert_outcome(conn, did, ts, aid, "error")
    _insert_outcome(conn, did, ts, aid, "error")
    # One success (older, but within window)
    success_ts = format_ts(now - timedelta(minutes=10))
    _insert_outcome(conn, did, success_ts, aid, "success", events_fetched=5)
    conn.commit()

    cfg = Config(
        warmup_enabled=False,
        coverage_window_minutes=30,
        coverage_threshold=0.5,
    )
    cov_cache = _build_coverage_cache(conn, now, cfg)
    alerts = data_gap(conn, cfg, now, cov_cache)

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["rule_id"] == "data_gap"
    assert alert["labeler_did"] == did
    assert alert["inputs"]["coverage_ratio"] == 0.25
    assert alert["inputs"]["coverage_attempts"] == 4
    assert alert["inputs"]["coverage_successes"] == 1
    assert alert["inputs"]["coverage_threshold"] == 0.5
    assert alert["inputs"]["last_success_ts"] is not None
    assert alert["inputs"]["last_attempt_ts"] is not None
    assert alert["evidence_hashes"] == []


# --- 7. No outcomes → rules fire normally (graceful degradation) ---

def test_no_outcomes_rules_fire_normally():
    """When no ingest_outcomes rows exist, rules fire normally (pre-migration behavior)."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    did = "did:plc:legacy"

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen, is_reference, scan_count) VALUES(?,?,?,0,10)",
        (did, format_ts(now - timedelta(days=10)), ts),
    )

    # Insert events that would trigger a spike (50 events, no baseline)
    for i in range(50):
        offset = timedelta(minutes=1 + (i % 14))
        ev_ts = format_ts(now - offset)
        conn.execute(
            "INSERT OR IGNORE INTO label_events(labeler_did, uri, val, ts, event_hash) VALUES(?,?,?,?,?)",
            (did, f"at://{did}/post/{i}", "test", ev_ts, f"hash_{i}"),
        )
    conn.commit()

    # No ingest_outcomes at all -> coverage cache is empty -> rules fire normally
    cfg = Config(
        spike_min_count_default=5,
        spike_k=10.0,
        warmup_enabled=False,
        coverage_window_minutes=30,
        coverage_threshold=0.5,
    )
    cov_cache = _build_coverage_cache(conn, now, cfg)
    alerts = label_rate_spike(conn, cfg, now, _cov_cache=cov_cache)

    # The DID is not in the cache, so it defaults to sufficient=True
    assert len(alerts) == 1
    assert alerts[0]["labeler_did"] == did


# --- 8. Schema v9 migration ---

def test_schema_v9_migration():
    """Schema v9 migration creates ingest_outcomes table and coverage columns."""
    conn = db.connect(":memory:")
    db.init_db(conn)

    # Verify table exists
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_outcomes'"
    ).fetchall()
    assert len(tables) == 1

    # Verify columns on labelers
    cols = {r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()}
    assert "coverage_ratio" in cols
    assert "coverage_window_successes" in cols
    assert "coverage_window_attempts" in cols
    assert "last_ingest_success_ts" in cols
    assert "last_ingest_attempt_ts" in cols

    # Verify schema version
    assert db.get_schema_version(conn) == 9


# --- 9. Batch coverage cache ---

def test_batch_coverage_cache():
    """_build_coverage_cache returns correct per-labeler stats."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    aid = uuid4().hex

    did_a = "did:plc:a"
    did_b = "did:plc:b"
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?,?,?)",
        (did_a, ts, ts),
    )
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?,?,?)",
        (did_b, ts, ts),
    )

    # DID A: 3 success, 1 error
    _insert_outcome(conn, did_a, ts, aid, "success")
    _insert_outcome(conn, did_a, ts, aid, "success")
    _insert_outcome(conn, did_a, ts, aid, "success")
    _insert_outcome(conn, did_a, ts, aid, "error")

    # DID B: 1 empty, 1 timeout
    _insert_outcome(conn, did_b, ts, aid, "empty")
    _insert_outcome(conn, did_b, ts, aid, "timeout")
    conn.commit()

    cfg = Config(coverage_window_minutes=30, coverage_threshold=0.5)
    cache = _build_coverage_cache(conn, now, cfg)

    assert cache[did_a]["successes"] == 3
    assert cache[did_a]["attempts"] == 4
    assert cache[did_a]["ratio"] == 0.75
    assert cache[did_a]["sufficient"] is True

    assert cache[did_b]["successes"] == 1
    assert cache[did_b]["attempts"] == 2
    assert cache[did_b]["ratio"] == 0.5
    assert cache[did_b]["sufficient"] is True


# --- 10. Retention cleanup ---

def test_retention_cleanup():
    """Retention cleanup removes rows older than 7 days."""
    conn = _make_db()
    now = datetime(2025, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    aid = uuid4().hex

    old_ts = format_ts(now - timedelta(days=8))
    recent_ts = format_ts(now - timedelta(days=1))

    _insert_outcome(conn, "did:plc:a", old_ts, aid, "success")
    _insert_outcome(conn, "did:plc:a", recent_ts, aid, "success")
    conn.commit()

    assert conn.execute("SELECT COUNT(*) AS c FROM ingest_outcomes").fetchone()["c"] == 2

    _cleanup_ingest_outcomes(conn, now)
    conn.commit()

    rows = conn.execute("SELECT * FROM ingest_outcomes").fetchall()
    assert len(rows) == 1
    assert rows[0]["ts"] == recent_ts


# --- 11. Exception type classification ---

def test_classify_socket_timeout():
    """socket.timeout classifies as timeout."""
    outcome, http_status = _classify_exception(socket.timeout("timed out"))
    assert outcome == "timeout"
    assert http_status is None


def test_classify_urlerror_with_timeout_reason():
    """URLError wrapping a timeout classifies as timeout."""
    exc = urllib.error.URLError(socket.timeout("timed out"))
    outcome, http_status = _classify_exception(exc)
    assert outcome == "timeout"
    assert http_status is None


def test_classify_urlerror_with_other_reason():
    """URLError with non-timeout reason classifies as error."""
    exc = urllib.error.URLError("connection refused")
    outcome, http_status = _classify_exception(exc)
    assert outcome == "error"
    assert http_status is None


def test_classify_httperror():
    """HTTPError classifies as error with http_status."""
    exc = urllib.error.HTTPError("http://example.com", 503, "Service Unavailable", {}, None)
    outcome, http_status = _classify_exception(exc)
    assert outcome == "error"
    assert http_status == 503


def test_classify_generic_exception():
    """Generic exception classifies as error."""
    outcome, http_status = _classify_exception(ValueError("bad value"))
    assert outcome == "error"
    assert http_status is None


def test_classify_timeout_error():
    """TimeoutError classifies as timeout."""
    outcome, http_status = _classify_exception(TimeoutError("timed out"))
    assert outcome == "timeout"
    assert http_status is None


# --- Coverage column updates ---

def test_update_coverage_columns():
    """_update_coverage_columns writes correct values to labelers table."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    did = "did:plc:cov"
    aid = uuid4().hex

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?,?,?)",
        (did, ts, ts),
    )
    _insert_outcome(conn, did, ts, aid, "success", events_fetched=10)
    _insert_outcome(conn, did, ts, aid, "error")
    conn.commit()

    cfg = Config(coverage_window_minutes=30, coverage_threshold=0.5)
    _update_coverage_columns(conn, cfg, now)
    conn.commit()

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did=?", (did,)).fetchone()
    assert row["coverage_ratio"] == 0.5
    assert row["coverage_window_successes"] == 1
    assert row["coverage_window_attempts"] == 2
    assert row["last_ingest_success_ts"] == ts
    assert row["last_ingest_attempt_ts"] == ts


# --- data_gap skips warmup labelers ---

def test_data_gap_skips_warmup():
    """data_gap does not fire for labelers still in warmup."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    did = "did:plc:newbie"

    # Labeler just created (0 scan_count, very recent)
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen, scan_count) VALUES(?,?,?,0)",
        (did, ts, ts),
    )

    # All errors -> low coverage
    aid = uuid4().hex
    _insert_outcome(conn, did, ts, aid, "error")
    _insert_outcome(conn, did, ts, aid, "error")
    conn.commit()

    cfg = Config(
        warmup_enabled=True,
        warmup_min_age_hours=48,
        warmup_min_scans=3,
        coverage_window_minutes=30,
        coverage_threshold=0.5,
    )
    cov_cache = _build_coverage_cache(conn, now, cfg)
    alerts = data_gap(conn, cfg, now, cov_cache)

    # Should not fire because labeler is in warmup
    assert len(alerts) == 0


# --- run_rules integration ---

def test_run_rules_includes_data_gap():
    """run_rules includes data_gap alerts alongside other rules."""
    conn = _make_db()
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    did = "did:plc:gappy2"

    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen, scan_count) VALUES(?,?,?,10)",
        (did, format_ts(now - timedelta(days=10)), ts),
    )

    aid = uuid4().hex
    _insert_outcome(conn, did, ts, aid, "error")
    _insert_outcome(conn, did, ts, aid, "error")
    _insert_outcome(conn, did, ts, aid, "error")
    conn.commit()

    cfg = Config(
        warmup_enabled=False,
        coverage_window_minutes=30,
        coverage_threshold=0.5,
    )
    alerts = run_rules(conn, cfg, now)
    data_gap_alerts = [a for a in alerts if a["rule_id"] == "data_gap"]
    assert len(data_gap_alerts) == 1
    assert data_gap_alerts[0]["labeler_did"] == did
