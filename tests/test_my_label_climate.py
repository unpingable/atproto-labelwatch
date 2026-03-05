"""Tests for My Label Climate Phase 1: target_did, rollup tables, migration."""
import sqlite3
import time

import pytest

from labelwatch import db
from labelwatch.db import parse_target_did
from labelwatch.scan import _update_author_day, _update_author_labeler_day


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


# --- parse_target_did ---


def test_parse_target_did_valid():
    assert parse_target_did("at://did:plc:abc123/app.bsky.feed.post/xyz") == "did:plc:abc123"
    assert parse_target_did("at://did:web:example.com/app.bsky.feed.post/1") == "did:web:example.com"
    assert parse_target_did("at://did:plc:abc123/app.bsky.feed.like/xyz") == "did:plc:abc123"
    # Whitespace stripped
    assert parse_target_did("  at://did:plc:abc123/post/1  ") == "did:plc:abc123"


def test_parse_target_did_invalid():
    assert parse_target_did(None) is None
    assert parse_target_did("") is None
    assert parse_target_did("   ") is None
    assert parse_target_did("https://bsky.app/profile/foo") is None
    assert parse_target_did("did:plc:abc123") is None  # bare DID, no at://
    assert parse_target_did("at://handle.bsky.social/post/1") is None  # not a DID
    assert parse_target_did("at://") is None


# --- v15→v16 migration + backfill ---


def _make_v15_db():
    """Create a DB at schema v15 with label_events but no target_did column."""
    conn = db.connect(":memory:")
    # Build a minimal v15-shaped schema
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS label_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            labeler_did TEXT NOT NULL, src TEXT, uri TEXT NOT NULL, cid TEXT,
            val TEXT NOT NULL, neg INTEGER DEFAULT 0, exp TEXT, sig TEXT,
            ts TEXT NOT NULL, event_hash TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS labelers (
            labeler_did TEXT PRIMARY KEY,
            handle TEXT, display_name TEXT, description TEXT,
            service_endpoint TEXT,
            labeler_class TEXT DEFAULT 'third_party',
            is_reference INTEGER DEFAULT 0,
            endpoint_status TEXT DEFAULT 'unknown',
            last_probed TEXT, first_seen TEXT, last_seen TEXT,
            visibility_class TEXT DEFAULT 'unresolved',
            reachability_state TEXT DEFAULT 'unknown',
            classification_confidence TEXT DEFAULT 'low',
            classification_reason TEXT,
            classification_version TEXT DEFAULT 'v1',
            classified_at TEXT,
            auditability TEXT DEFAULT 'low',
            observed_as_src INTEGER DEFAULT 0,
            has_labeler_service INTEGER DEFAULT 0,
            has_label_key INTEGER DEFAULT 0,
            declared_record INTEGER DEFAULT 0,
            likely_test_dev INTEGER DEFAULT 0,
            scan_count INTEGER DEFAULT 0,
            regime_state TEXT, regime_reason_codes TEXT,
            auditability_risk INTEGER, auditability_risk_band TEXT, auditability_risk_reasons TEXT,
            inference_risk INTEGER, inference_risk_band TEXT, inference_risk_reasons TEXT,
            temporal_coherence INTEGER, temporal_coherence_band TEXT, temporal_coherence_reasons TEXT,
            derive_version TEXT, derived_at TEXT,
            regime_pending TEXT, regime_pending_count INTEGER DEFAULT 0,
            auditability_risk_prev INTEGER, inference_risk_prev INTEGER, temporal_coherence_prev INTEGER,
            coverage_ratio REAL,
            coverage_window_successes INTEGER DEFAULT 0,
            coverage_window_attempts INTEGER DEFAULT 0,
            last_ingest_success_ts TEXT,
            last_ingest_attempt_ts TEXT
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id TEXT NOT NULL,
            labeler_did TEXT NOT NULL, ts TEXT NOT NULL, inputs_json TEXT NOT NULL,
            evidence_hashes_json TEXT NOT NULL, config_hash TEXT NOT NULL,
            receipt_hash TEXT NOT NULL, warmup_alert INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS labeler_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT, labeler_did TEXT NOT NULL,
            evidence_type TEXT NOT NULL, evidence_value TEXT, ts TEXT NOT NULL, source TEXT
        );
        CREATE TABLE IF NOT EXISTS labeler_probe_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, labeler_did TEXT NOT NULL,
            ts TEXT NOT NULL, endpoint TEXT NOT NULL, http_status INTEGER,
            normalized_status TEXT NOT NULL, latency_ms INTEGER,
            failure_type TEXT, error TEXT
        );
        CREATE TABLE IF NOT EXISTS derived_receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, labeler_did TEXT NOT NULL,
            receipt_type TEXT NOT NULL, derivation_version TEXT NOT NULL,
            trigger TEXT NOT NULL, ts TEXT NOT NULL, input_hash TEXT NOT NULL,
            previous_value_json TEXT NOT NULL, new_value_json TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingest_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, labeler_did TEXT NOT NULL,
            ts TEXT NOT NULL, attempt_id TEXT NOT NULL, outcome TEXT NOT NULL,
            events_fetched INTEGER, http_status INTEGER, latency_ms INTEGER,
            error_type TEXT, error_summary TEXT, source TEXT
        );
        CREATE TABLE IF NOT EXISTS derived_label_fp (
            label_event_id INTEGER PRIMARY KEY, labeler_did TEXT NOT NULL,
            uri TEXT NOT NULL, label_ts TEXT NOT NULL,
            claim_fingerprint TEXT, post_created_ts TEXT, lag_sec_claimed INTEGER
        );
        CREATE TABLE IF NOT EXISTS derived_labeler_lag_7d (
            labeler_did TEXT PRIMARY KEY, n_total INTEGER NOT NULL,
            null_rate REAL NOT NULL, p50_lag INTEGER, p90_lag INTEGER,
            p95_lag INTEGER, p99_lag INTEGER, p90_p50_ratio REAL,
            neg_rate REAL NOT NULL, updated_epoch INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS derived_labeler_reversal_7d (
            labeler_did TEXT PRIMARY KEY, n_apply_events INTEGER NOT NULL,
            n_apply_groups INTEGER NOT NULL, n_reversals INTEGER NOT NULL,
            pct_reversed REAL NOT NULL, p50_dwell INTEGER, p90_dwell INTEGER,
            p95_dwell INTEGER, p99_dwell INTEGER, top_val TEXT, top_val_pct REAL,
            truncated INTEGER NOT NULL DEFAULT 0, updated_epoch INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS derived_labeler_boundary_load_7d (
            labeler_did TEXT PRIMARY KEY, n_matched INTEGER NOT NULL,
            n_negative INTEGER NOT NULL, n_sub_1s INTEGER NOT NULL,
            n_sub_5s INTEGER NOT NULL, n_sub_30s INTEGER NOT NULL,
            n_sub_60s INTEGER NOT NULL, p5_lag INTEGER, p10_lag INTEGER,
            updated_epoch INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS derived_val_dist_day (
            labeler_did TEXT NOT NULL, day_epoch INTEGER NOT NULL,
            val TEXT NOT NULL, n INTEGER NOT NULL,
            PRIMARY KEY (labeler_did, day_epoch, val)
        );
        CREATE TABLE IF NOT EXISTS derived_labeler_entropy_7d (
            labeler_did TEXT PRIMARY KEY, n_events_7d INTEGER NOT NULL,
            k_vals_7d INTEGER NOT NULL, entropy_7d REAL, h_norm_7d REAL,
            n_eff_7d REAL, top1_val TEXT, top1_share REAL, top2_share REAL,
            n_events_30d INTEGER NOT NULL, k_vals_30d INTEGER NOT NULL,
            entropy_30d REAL, h_norm_30d REAL, n_eff_30d REAL,
            delta_h_norm REAL, updated_epoch INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_label_events_labeler_ts ON label_events(labeler_did, ts);
        CREATE INDEX IF NOT EXISTS idx_label_events_uri_ts ON label_events(uri, ts);
        CREATE INDEX IF NOT EXISTS idx_alerts_rule_ts ON alerts(rule_id, ts);
        CREATE INDEX IF NOT EXISTS idx_labeler_evidence_did ON labeler_evidence(labeler_did, evidence_type);
        CREATE INDEX IF NOT EXISTS idx_probe_history_did_ts ON labeler_probe_history(labeler_did, ts);
        CREATE INDEX IF NOT EXISTS idx_derived_receipts_did_type ON derived_receipts(labeler_did, receipt_type, ts);
        CREATE INDEX IF NOT EXISTS idx_ingest_outcomes_did_ts ON ingest_outcomes(labeler_did, ts);
        CREATE INDEX IF NOT EXISTS idx_derived_label_fp_labeler ON derived_label_fp(labeler_did);
        CREATE INDEX IF NOT EXISTS idx_derived_label_fp_fp ON derived_label_fp(claim_fingerprint);
    """)
    db.set_meta(conn, "schema_version", "15")
    conn.commit()
    return conn


def test_backfill_from_v15():
    conn = _make_v15_db()
    # Insert events WITHOUT target_did
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, ts, event_hash) "
        "VALUES('did:plc:labeler1', 'at://did:plc:user1/app.bsky.feed.post/1', 'spam', "
        "'2025-06-01T00:00:00Z', 'hash1')"
    )
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, ts, event_hash) "
        "VALUES('did:plc:labeler1', 'at://did:web:example.com/app.bsky.feed.post/2', 'spam', "
        "'2025-06-01T00:00:00Z', 'hash2')"
    )
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, ts, event_hash) "
        "VALUES('did:plc:labeler1', 'https://not-at-uri.example.com', 'spam', "
        "'2025-06-01T00:00:00Z', 'hash3')"
    )
    conn.commit()

    db.init_db(conn)

    assert db.get_schema_version(conn) == db.SCHEMA_VERSION

    # Check backfill
    row1 = conn.execute("SELECT target_did FROM label_events WHERE event_hash='hash1'").fetchone()
    assert row1["target_did"] == "did:plc:user1"

    row2 = conn.execute("SELECT target_did FROM label_events WHERE event_hash='hash2'").fetchone()
    assert row2["target_did"] == "did:web:example.com"

    # Non-AT URI should still be NULL
    row3 = conn.execute("SELECT target_did FROM label_events WHERE event_hash='hash3'").fetchone()
    assert row3["target_did"] is None


def test_migration_idempotent():
    conn = _make_v15_db()
    db.init_db(conn)
    assert db.get_schema_version(conn) == db.SCHEMA_VERSION

    # Run again — should not error
    db.init_db(conn)
    assert db.get_schema_version(conn) == db.SCHEMA_VERSION


def test_insert_with_target_did():
    conn = _make_db()
    rows = [(
        "did:plc:labeler1", None, "at://did:plc:user1/app.bsky.feed.post/1",
        None, "spam", 0, None, None, "2025-06-01T00:00:00Z", "hash_new",
        "did:plc:user1",
    )]
    count = db.insert_label_events(conn, rows)
    assert count == 1

    row = conn.execute("SELECT target_did FROM label_events WHERE event_hash='hash_new'").fetchone()
    assert row["target_did"] == "did:plc:user1"


# --- Rollup tests ---


def _insert_test_events(conn, n=5):
    """Insert synthetic label events for rollup testing."""
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for i in range(n):
        neg = 1 if i == 0 else 0
        labeler = "did:plc:labeler1" if i < 3 else "did:plc:labeler2"
        conn.execute(
            "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (labeler, f"at://did:plc:author1/app.bsky.feed.post/{i}",
             "spam", neg, now_iso, f"rollup_hash_{i}", "did:plc:author1"),
        )
    conn.commit()


def test_author_day_rollup():
    conn = _make_db()
    _insert_test_events(conn)

    _update_author_day(conn)
    conn.commit()

    rows = conn.execute("SELECT * FROM derived_author_day").fetchall()
    assert len(rows) == 1

    row = rows[0]
    assert row["author_did"] == "did:plc:author1"
    assert row["day_epoch"] is not None
    assert row["events"] == 5
    assert row["applies"] == 4  # 4 non-neg
    assert row["removes"] == 1  # 1 neg
    assert row["labelers"] == 2  # labeler1 + labeler2
    assert row["targets"] == 5  # 5 distinct URIs
    assert row["vals"] == 1  # all "spam"


def test_author_labeler_day_rollup():
    conn = _make_db()
    _insert_test_events(conn)

    _update_author_labeler_day(conn)
    conn.commit()

    rows = conn.execute(
        "SELECT * FROM derived_author_labeler_day ORDER BY labeler_did"
    ).fetchall()
    assert len(rows) == 2

    # labeler1 has 3 events (1 neg + 2 apply)
    r1 = [r for r in rows if r["labeler_did"] == "did:plc:labeler1"][0]
    assert r1["events"] == 3
    assert r1["applies"] == 2
    assert r1["removes"] == 1

    # labeler2 has 2 events (both apply)
    r2 = [r for r in rows if r["labeler_did"] == "did:plc:labeler2"][0]
    assert r2["events"] == 2
    assert r2["applies"] == 2
    assert r2["removes"] == 0


# --- Index usage tests ---


def test_index_usage_target_did_ts():
    conn = _make_db()
    plan = conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM label_events WHERE target_did = ? AND ts >= ?",
        ("did:plc:test", "2025-01-01"),
    ).fetchall()
    plan_text = " ".join(str(r["detail"]) for r in plan)
    assert "idx_label_events_target_did_ts" in plan_text


def test_ts_index_for_rollups():
    conn = _make_db()
    plan = conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM label_events WHERE ts >= ?",
        ("2025-01-01",),
    ).fetchall()
    plan_text = " ".join(str(r["detail"]) for r in plan)
    assert "idx_label_events_ts" in plan_text
