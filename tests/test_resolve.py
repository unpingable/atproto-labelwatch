"""Tests for DID handle resolution and schema migration."""
from unittest.mock import patch, MagicMock
import json

from labelwatch import db
from labelwatch.resolve import resolve_handle, resolve_handles_for_labelers, resolve_label_key


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def test_schema_v2_has_handle_column():
    conn = _make_db()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
    assert "handle" in cols


def test_migrate_v1_to_v2():
    """Simulate a v1 DB missing the handle column, then migrate."""
    conn = db.connect(":memory:")
    # Create v1 schema manually (no handle column)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS labelers (
            labeler_did TEXT PRIMARY KEY,
            description TEXT,
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
    """)
    db.set_meta(conn, "schema_version", "1")
    conn.commit()
    # Now init_db should migrate
    db.init_db(conn)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
    assert "handle" in cols
    assert db.get_schema_version(conn) == db.SCHEMA_VERSION


def _mock_plc_response(did, handle):
    resp = MagicMock()
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    resp.read.return_value = json.dumps({
        "id": did,
        "alsoKnownAs": [f"at://{handle}"],
    }).encode("utf-8")
    return resp


def test_resolve_handle_success():
    did = "did:plc:abc123"
    with patch("labelwatch.resolve.urllib.request.urlopen") as mock_open:
        mock_open.return_value = _mock_plc_response(did, "example.bsky.social")
        result = resolve_handle(did)
    assert result == "example.bsky.social"


def test_resolve_handle_failure():
    with patch("labelwatch.resolve.urllib.request.urlopen", side_effect=Exception("timeout")):
        result = resolve_handle("did:plc:bad")
    assert result is None


def test_resolve_handles_for_labelers():
    conn = _make_db()
    db.upsert_labeler(conn, "did:plc:abc123", "2025-01-01T00:00:00Z")
    db.upsert_labeler(conn, "did:plc:xyz789", "2025-01-01T00:00:00Z")
    conn.commit()

    def fake_open(req, timeout=10):
        did = req.full_url.split("/")[-1]
        handles = {
            "did:plc:abc123": "alice.bsky.social",
            "did:plc:xyz789": "bob.bsky.social",
        }
        return _mock_plc_response(did, handles[did])

    with patch("labelwatch.resolve.urllib.request.urlopen", side_effect=fake_open):
        count = resolve_handles_for_labelers(conn)

    assert count == 2
    assert db.get_handle(conn, "did:plc:abc123") == "alice.bsky.social"
    assert db.get_handle(conn, "did:plc:xyz789") == "bob.bsky.social"


def test_resolve_skips_already_resolved():
    conn = _make_db()
    db.upsert_labeler(conn, "did:plc:abc123", "2025-01-01T00:00:00Z")
    conn.execute("UPDATE labelers SET handle='alice.bsky.social' WHERE labeler_did='did:plc:abc123'")
    conn.commit()

    with patch("labelwatch.resolve.urllib.request.urlopen") as mock_open:
        count = resolve_handles_for_labelers(conn)

    assert count == 0
    mock_open.assert_not_called()


# --- resolve_label_key ---

def test_resolve_label_key_present():
    doc = {
        "verificationMethod": [
            {"id": "#atproto_label", "type": "Multikey", "publicKeyMultibase": "zDna..."},
        ]
    }
    assert resolve_label_key(doc) is True


def test_resolve_label_key_absent():
    doc = {
        "verificationMethod": [
            {"id": "#atproto", "type": "Multikey", "publicKeyMultibase": "zDna..."},
        ]
    }
    assert resolve_label_key(doc) is False


def test_resolve_label_key_empty_doc():
    assert resolve_label_key({}) is False
    assert resolve_label_key({"verificationMethod": []}) is False


def test_resolve_label_key_no_verification_methods():
    doc = {"service": [{"id": "#atproto_labeler"}]}
    assert resolve_label_key(doc) is False
