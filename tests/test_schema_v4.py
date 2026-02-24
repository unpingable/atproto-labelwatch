"""Tests for schema v4: evidence tables, labeler column extensions, migration."""
import sqlite3

from labelwatch import db


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _make_v3_db():
    """Create a DB at schema v3 with some labeler data for migration testing."""
    conn = db.connect(":memory:")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS labelers (
            labeler_did TEXT PRIMARY KEY,
            handle TEXT,
            display_name TEXT,
            description TEXT,
            service_endpoint TEXT,
            labeler_class TEXT DEFAULT 'third_party',
            is_reference INTEGER DEFAULT 0,
            endpoint_status TEXT DEFAULT 'unknown',
            last_probed TEXT,
            first_seen TEXT,
            last_seen TEXT
        );
        CREATE TABLE IF NOT EXISTS label_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            labeler_did TEXT NOT NULL, src TEXT, uri TEXT NOT NULL, cid TEXT,
            val TEXT NOT NULL, neg INTEGER DEFAULT 0, exp TEXT, sig TEXT,
            ts TEXT NOT NULL, event_hash TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id TEXT NOT NULL,
            labeler_did TEXT NOT NULL, ts TEXT NOT NULL, inputs_json TEXT NOT NULL,
            evidence_hashes_json TEXT NOT NULL, config_hash TEXT NOT NULL, receipt_hash TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_label_events_labeler_ts ON label_events(labeler_did, ts);
        CREATE INDEX IF NOT EXISTS idx_label_events_uri_ts ON label_events(uri, ts);
        CREATE INDEX IF NOT EXISTS idx_alerts_rule_ts ON alerts(rule_id, ts);
    """)
    db.set_meta(conn, "schema_version", "3")
    conn.commit()
    return conn


# --- Fresh DB tests ---

def test_fresh_db_has_v4_columns():
    conn = _make_db()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
    for col in [
        "visibility_class", "reachability_state", "classification_confidence",
        "classification_reason", "classification_version", "classified_at",
        "auditability", "observed_as_src", "has_labeler_service", "has_label_key",
        "declared_record", "likely_test_dev", "scan_count",
    ]:
        assert col in cols, f"Missing column: {col}"


def test_fresh_db_has_evidence_table():
    conn = _make_db()
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "labeler_evidence" in tables


def test_fresh_db_has_probe_history_table():
    conn = _make_db()
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "labeler_probe_history" in tables


def test_fresh_db_schema_version_is_4():
    conn = _make_db()
    assert db.get_schema_version(conn) == 4


def test_fresh_db_labeler_defaults():
    conn = _make_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES('did:plc:test', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:test'").fetchone()
    assert row["visibility_class"] == "unresolved"
    assert row["reachability_state"] == "unknown"
    assert row["classification_confidence"] == "low"
    assert row["classification_reason"] is None
    assert row["classification_version"] == "v1"
    assert row["classified_at"] is None
    assert row["auditability"] == "low"
    assert row["observed_as_src"] == 0
    assert row["has_labeler_service"] == 0
    assert row["has_label_key"] == 0
    assert row["declared_record"] == 0
    assert row["likely_test_dev"] == 0
    assert row["scan_count"] == 0


# --- Migration v3→v4 tests ---

def test_migrate_v3_to_v4_adds_columns():
    conn = _make_v3_db()

    # Insert labeler data that should be preserved
    conn.execute(
        "INSERT INTO labelers(labeler_did, handle, service_endpoint, endpoint_status, first_seen, last_seen) "
        "VALUES('did:plc:test', 'test.bsky.social', 'https://test.example.com', 'accessible', '2025-01-01T00:00:00Z', '2025-06-01T00:00:00Z')"
    )
    conn.commit()

    db.init_db(conn)

    assert db.get_schema_version(conn) == 4

    cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
    for col in ["visibility_class", "reachability_state", "auditability", "declared_record", "scan_count"]:
        assert col in cols, f"Missing column after migration: {col}"


def test_migrate_v3_to_v4_backfill_declared_record():
    conn = _make_v3_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES('did:plc:a', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES('did:plc:b', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    db.init_db(conn)

    for did in ["did:plc:a", "did:plc:b"]:
        row = conn.execute("SELECT declared_record FROM labelers WHERE labeler_did=?", (did,)).fetchone()
        assert row["declared_record"] == 1


def test_migrate_v3_to_v4_backfill_has_labeler_service():
    conn = _make_v3_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, service_endpoint, first_seen, last_seen) "
        "VALUES('did:plc:withep', 'https://ep.example.com', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) "
        "VALUES('did:plc:noep', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    db.init_db(conn)

    row_with = conn.execute("SELECT has_labeler_service FROM labelers WHERE labeler_did='did:plc:withep'").fetchone()
    assert row_with["has_labeler_service"] == 1

    row_without = conn.execute("SELECT has_labeler_service FROM labelers WHERE labeler_did='did:plc:noep'").fetchone()
    assert row_without["has_labeler_service"] == 0


def test_migrate_v3_to_v4_backfill_visibility_class():
    conn = _make_v3_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES('did:plc:x', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    db.init_db(conn)

    row = conn.execute("SELECT visibility_class FROM labelers WHERE labeler_did='did:plc:x'").fetchone()
    assert row["visibility_class"] == "declared"


def test_migrate_v3_to_v4_backfill_reachability():
    conn = _make_v3_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, endpoint_status, first_seen, last_seen) "
        "VALUES('did:plc:up', 'accessible', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO labelers(labeler_did, endpoint_status, first_seen, last_seen) "
        "VALUES('did:plc:dn', 'down', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    db.init_db(conn)

    assert conn.execute("SELECT reachability_state FROM labelers WHERE labeler_did='did:plc:up'").fetchone()["reachability_state"] == "accessible"
    assert conn.execute("SELECT reachability_state FROM labelers WHERE labeler_did='did:plc:dn'").fetchone()["reachability_state"] == "down"


def test_migrate_v3_to_v4_backfill_classification_reason():
    conn = _make_v3_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES('did:plc:m', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    db.init_db(conn)

    row = conn.execute("SELECT classification_reason FROM labelers WHERE labeler_did='did:plc:m'").fetchone()
    assert row["classification_reason"] == "migrated_from_v3"


def test_migrate_v3_to_v4_creates_evidence_table():
    conn = _make_v3_db()
    db.init_db(conn)

    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "labeler_evidence" in tables
    assert "labeler_probe_history" in tables


def test_migrate_v3_to_v4_preserves_existing_data():
    conn = _make_v3_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, handle, display_name, first_seen, last_seen) "
        "VALUES('did:plc:keep', 'keep.bsky.social', 'Keeper', '2025-01-01T00:00:00Z', '2025-06-01T00:00:00Z')"
    )
    conn.commit()

    db.init_db(conn)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:keep'").fetchone()
    assert row["handle"] == "keep.bsky.social"
    assert row["display_name"] == "Keeper"
    assert row["first_seen"] == "2025-01-01T00:00:00Z"


# --- Evidence helpers ---

def test_insert_and_get_evidence():
    conn = _make_db()
    db.insert_evidence(conn, "did:plc:a", "declared_record", "true", "2025-06-01T00:00:00Z", "discovery")
    db.insert_evidence(conn, "did:plc:a", "probe_result", "accessible", "2025-06-01T00:01:00Z", "discovery")
    conn.commit()

    evidence = db.get_evidence(conn, "did:plc:a")
    assert len(evidence) == 2
    types = {e["evidence_type"] for e in evidence}
    assert "declared_record" in types
    assert "probe_result" in types


def test_get_evidence_empty():
    conn = _make_db()
    evidence = db.get_evidence(conn, "did:plc:nonexistent")
    assert evidence == []


def test_evidence_separate_by_did():
    conn = _make_db()
    db.insert_evidence(conn, "did:plc:a", "probe_result", "accessible", "2025-06-01T00:00:00Z")
    db.insert_evidence(conn, "did:plc:b", "probe_result", "down", "2025-06-01T00:00:00Z")
    conn.commit()

    a_evidence = db.get_evidence(conn, "did:plc:a")
    b_evidence = db.get_evidence(conn, "did:plc:b")
    assert len(a_evidence) == 1
    assert len(b_evidence) == 1
    assert a_evidence[0]["evidence_value"] == "accessible"
    assert b_evidence[0]["evidence_value"] == "down"


# --- Probe history helpers ---

def test_insert_and_get_probe_history():
    conn = _make_db()
    db.insert_probe_history(
        conn, "did:plc:a", "2025-06-01T00:00:00Z",
        "https://labeler.example.com", 200, "accessible", 150,
    )
    db.insert_probe_history(
        conn, "did:plc:a", "2025-06-01T01:00:00Z",
        "https://labeler.example.com", None, "down", None,
        failure_type="timeout", error="Connection timed out",
    )
    conn.commit()

    history = db.get_probe_history(conn, "did:plc:a")
    assert len(history) == 2
    # Ordered by ts DESC
    assert history[0]["normalized_status"] == "down"
    assert history[0]["failure_type"] == "timeout"
    assert history[1]["normalized_status"] == "accessible"
    assert history[1]["http_status"] == 200
    assert history[1]["latency_ms"] == 150


def test_probe_history_limit():
    conn = _make_db()
    for i in range(10):
        db.insert_probe_history(
            conn, "did:plc:a", f"2025-06-01T{i:02d}:00:00Z",
            "https://labeler.example.com", 200, "accessible", 100,
        )
    conn.commit()

    history = db.get_probe_history(conn, "did:plc:a", limit=3)
    assert len(history) == 3


def test_probe_history_empty():
    conn = _make_db()
    history = db.get_probe_history(conn, "did:plc:nonexistent")
    assert history == []


# --- Sticky field semantics ---

def test_sticky_fields_only_upgrade():
    """Sticky fields (observed_as_src, etc.) should only be set to 1, never back to 0."""
    conn = _make_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, observed_as_src, has_labeler_service, first_seen, last_seen) "
        "VALUES('did:plc:sticky', 1, 1, '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    row = conn.execute("SELECT observed_as_src, has_labeler_service FROM labelers WHERE labeler_did='did:plc:sticky'").fetchone()
    assert row["observed_as_src"] == 1
    assert row["has_labeler_service"] == 1

    # Demonstrate the sticky pattern: use MAX to prevent downgrade
    conn.execute(
        """
        UPDATE labelers SET observed_as_src = MAX(observed_as_src, 0)
        WHERE labeler_did = 'did:plc:sticky'
        """
    )
    row = conn.execute("SELECT observed_as_src FROM labelers WHERE labeler_did='did:plc:sticky'").fetchone()
    assert row["observed_as_src"] == 1  # Not downgraded


# --- scan_count ---

def test_increment_scan_count():
    conn = _make_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) "
        "VALUES('did:plc:cnt', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    assert conn.execute("SELECT scan_count FROM labelers WHERE labeler_did='did:plc:cnt'").fetchone()["scan_count"] == 0

    db.increment_scan_count(conn, "did:plc:cnt")
    assert conn.execute("SELECT scan_count FROM labelers WHERE labeler_did='did:plc:cnt'").fetchone()["scan_count"] == 1

    db.increment_scan_count(conn, "did:plc:cnt")
    db.increment_scan_count(conn, "did:plc:cnt")
    assert conn.execute("SELECT scan_count FROM labelers WHERE labeler_did='did:plc:cnt'").fetchone()["scan_count"] == 3
