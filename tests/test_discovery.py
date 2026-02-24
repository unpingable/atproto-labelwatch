"""Tests for labeler discovery pipeline."""
from unittest.mock import patch, MagicMock
import json

from labelwatch import db
from labelwatch.config import Config
from labelwatch.discover import (
    ProbeResult,
    list_labeler_dids,
    hydrate_labelers,
    probe_endpoint,
    run_discovery,
)
from labelwatch.resolve import fetch_did_doc, resolve_service_endpoint


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _mock_http_response(data):
    resp = MagicMock()
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    resp.read.return_value = json.dumps(data).encode("utf-8")
    resp.status = 200
    return resp


# --- Schema tests ---

def test_schema_v3_has_new_columns():
    conn = _make_db()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
    for col in ["display_name", "service_endpoint", "labeler_class", "is_reference",
                "endpoint_status", "last_probed"]:
        assert col in cols, f"Missing column: {col}"


def test_migrate_v2_to_v3():
    """Simulate a v2 DB and verify v3 migration adds new columns."""
    conn = db.connect(":memory:")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS labelers (
            labeler_did TEXT PRIMARY KEY,
            handle TEXT,
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
    db.set_meta(conn, "schema_version", "2")
    conn.commit()

    # Insert a labeler to verify data preservation
    conn.execute(
        "INSERT INTO labelers(labeler_did, handle, first_seen, last_seen) VALUES(?, ?, ?, ?)",
        ("did:plc:test", "test.bsky.social", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
    )
    conn.commit()

    db.init_db(conn)

    assert db.get_schema_version(conn) == db.SCHEMA_VERSION
    cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
    assert "display_name" in cols
    assert "service_endpoint" in cols
    assert "labeler_class" in cols
    assert "is_reference" in cols
    assert "endpoint_status" in cols
    assert "last_probed" in cols

    # Data preserved
    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:test'").fetchone()
    assert row["handle"] == "test.bsky.social"


# --- resolve.py tests ---

def test_fetch_did_doc():
    did = "did:plc:abc123"
    doc = {"id": did, "alsoKnownAs": ["at://alice.bsky.social"], "service": []}
    with patch("labelwatch.resolve.urllib.request.urlopen", return_value=_mock_http_response(doc)):
        result = fetch_did_doc(did)
    assert result is not None
    assert result["id"] == did


def test_fetch_did_doc_failure():
    with patch("labelwatch.resolve.urllib.request.urlopen", side_effect=Exception("timeout")):
        result = fetch_did_doc("did:plc:bad")
    assert result is None


def test_resolve_service_endpoint():
    doc = {
        "service": [
            {"id": "#atproto_pds", "type": "AtprotoPds", "serviceEndpoint": "https://pds.example.com"},
            {"id": "#atproto_labeler", "type": "AtprotoLabeler", "serviceEndpoint": "https://labeler.example.com"},
        ]
    }
    assert resolve_service_endpoint(doc) == "https://labeler.example.com"


def test_resolve_service_endpoint_missing():
    doc = {"service": [{"id": "#atproto_pds", "type": "AtprotoPds", "serviceEndpoint": "https://pds.example.com"}]}
    assert resolve_service_endpoint(doc) is None


def test_resolve_service_endpoint_empty():
    assert resolve_service_endpoint({}) is None
    assert resolve_service_endpoint({"service": []}) is None


# --- list_labeler_dids ---

def test_list_labeler_dids_single_page():
    page = {"repos": [{"did": "did:plc:a"}, {"did": "did:plc:b"}], "cursor": None}
    with patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response(page)):
        dids = list_labeler_dids(max_pages=1)
    assert dids == ["did:plc:a", "did:plc:b"]


def test_list_labeler_dids_pagination():
    page1 = {"repos": [{"did": "did:plc:a"}], "cursor": "c1"}
    page2 = {"repos": [{"did": "did:plc:b"}], "cursor": None}

    call_count = 0
    def fake_open(req, timeout=30):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _mock_http_response(page1)
        return _mock_http_response(page2)

    with patch("labelwatch.discover.urllib.request.urlopen", side_effect=fake_open):
        dids = list_labeler_dids(max_pages=5)
    assert dids == ["did:plc:a", "did:plc:b"]
    assert call_count == 2


def test_list_labeler_dids_network_error():
    with patch("labelwatch.discover.urllib.request.urlopen", side_effect=Exception("network")):
        dids = list_labeler_dids(max_pages=1)
    assert dids == []


# --- hydrate_labelers ---

def test_hydrate_labelers():
    response = {
        "views": [
            {"creator": {"did": "did:plc:a", "displayName": "Alice Labeler"}},
            {"creator": {"did": "did:plc:b", "displayName": None}},
        ]
    }
    with patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response(response)):
        result = hydrate_labelers(["did:plc:a", "did:plc:b"])
    assert result["did:plc:a"]["display_name"] == "Alice Labeler"
    assert result["did:plc:b"]["display_name"] is None


def test_hydrate_labelers_network_error():
    with patch("labelwatch.discover.urllib.request.urlopen", side_effect=Exception("fail")):
        result = hydrate_labelers(["did:plc:a"])
    assert result["did:plc:a"]["display_name"] is None


# --- probe_endpoint ---

def test_probe_endpoint_accessible():
    resp = _mock_http_response({"labels": []})
    with patch("labelwatch.discover.urllib.request.urlopen", return_value=resp):
        result = probe_endpoint("https://labeler.example.com", "did:plc:a")
    assert result.normalized_status == "accessible"
    assert result.http_status == 200
    assert result.latency_ms is not None


def test_probe_endpoint_auth_required():
    import urllib.error
    err = urllib.error.HTTPError("url", 401, "Unauthorized", {}, None)
    with patch("labelwatch.discover.urllib.request.urlopen", side_effect=err):
        result = probe_endpoint("https://labeler.example.com", "did:plc:a")
    assert result.normalized_status == "auth_required"
    assert result.http_status == 401


def test_probe_endpoint_down():
    with patch("labelwatch.discover.urllib.request.urlopen", side_effect=Exception("timeout")):
        result = probe_endpoint("https://labeler.example.com", "did:plc:a")
    assert result.normalized_status == "down"
    assert result.failure_type is not None


# --- run_discovery integration ---

def test_run_discovery_populates_db():
    conn = _make_db()
    cfg = Config(reference_dids=["did:plc:ref"])

    # Mock list_labeler_dids
    mock_dids = ["did:plc:ref", "did:plc:community1"]

    # Mock DID docs
    did_docs = {
        "did:plc:ref": {
            "id": "did:plc:ref",
            "alsoKnownAs": ["at://mod.bsky.app"],
            "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler",
                         "serviceEndpoint": "https://mod.bsky.app"}],
            "verificationMethod": [],
        },
        "did:plc:community1": {
            "id": "did:plc:community1",
            "alsoKnownAs": ["at://community.example.com"],
            "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler",
                         "serviceEndpoint": "https://community.example.com"}],
            "verificationMethod": [],
        },
    }

    hydration_response = {
        "views": [
            {"creator": {"did": "did:plc:ref", "displayName": "Bluesky Moderation"}},
            {"creator": {"did": "did:plc:community1", "displayName": "Community Labels"}},
        ]
    }

    probe_result = ProbeResult("accessible", http_status=200, latency_ms=50)

    with patch("labelwatch.discover.list_labeler_dids", return_value=mock_dids), \
         patch("labelwatch.discover.fetch_did_doc", side_effect=lambda did, timeout=10: did_docs.get(did)), \
         patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response(hydration_response)), \
         patch("labelwatch.discover.probe_endpoint", return_value=probe_result), \
         patch("labelwatch.discover.time.sleep"):
        summary = run_discovery(conn, cfg)

    assert summary["discovered"] == 2
    assert summary["accessible"] == 2

    # Verify DB population
    rows = conn.execute("SELECT * FROM labelers ORDER BY labeler_did").fetchall()
    assert len(rows) == 2

    ref_row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:ref'").fetchone()
    assert ref_row["is_reference"] == 1
    assert ref_row["labeler_class"] == "official_platform"
    assert ref_row["endpoint_status"] == "accessible"
    assert ref_row["display_name"] == "Bluesky Moderation"
    assert ref_row["handle"] == "mod.bsky.app"
    assert ref_row["service_endpoint"] == "https://mod.bsky.app"

    # New v4 fields
    assert ref_row["visibility_class"] == "declared"
    assert ref_row["reachability_state"] == "accessible"
    assert ref_row["declared_record"] == 1
    assert ref_row["has_labeler_service"] == 1
    assert ref_row["classification_reason"] is not None

    community_row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:community1'").fetchone()
    assert community_row["is_reference"] == 0
    assert community_row["labeler_class"] == "third_party"
    assert community_row["display_name"] == "Community Labels"
    assert community_row["visibility_class"] == "declared"


def test_run_discovery_writes_evidence():
    conn = _make_db()
    cfg = Config(reference_dids=[])

    mock_dids = ["did:plc:a"]
    did_doc = {
        "id": "did:plc:a",
        "alsoKnownAs": ["at://a.example.com"],
        "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler",
                     "serviceEndpoint": "https://a.example.com"}],
        "verificationMethod": [],
    }

    probe_result = ProbeResult("accessible", http_status=200, latency_ms=42)

    with patch("labelwatch.discover.list_labeler_dids", return_value=mock_dids), \
         patch("labelwatch.discover.fetch_did_doc", return_value=did_doc), \
         patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response({"views": []})), \
         patch("labelwatch.discover.probe_endpoint", return_value=probe_result), \
         patch("labelwatch.discover.time.sleep"):
        run_discovery(conn, cfg)

    evidence = db.get_evidence(conn, "did:plc:a")
    types = {e["evidence_type"] for e in evidence}
    assert "declared_record" in types
    assert "did_doc_labeler_service" in types
    assert "probe_result" in types

    # Probe history written
    history = db.get_probe_history(conn, "did:plc:a")
    assert len(history) == 1
    assert history[0]["normalized_status"] == "accessible"
    assert history[0]["http_status"] == 200
    assert history[0]["latency_ms"] == 42


def test_run_discovery_handles_probe_failures():
    conn = _make_db()
    cfg = Config(reference_dids=[])

    mock_dids = ["did:plc:a"]
    did_doc = {
        "id": "did:plc:a",
        "alsoKnownAs": ["at://a.example.com"],
        "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler",
                     "serviceEndpoint": "https://a.example.com"}],
        "verificationMethod": [],
    }

    probe_result = ProbeResult("down", failure_type="timeout", error="Connection timed out")

    with patch("labelwatch.discover.list_labeler_dids", return_value=mock_dids), \
         patch("labelwatch.discover.fetch_did_doc", return_value=did_doc), \
         patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response({"views": []})), \
         patch("labelwatch.discover.probe_endpoint", return_value=probe_result), \
         patch("labelwatch.discover.time.sleep"):
        summary = run_discovery(conn, cfg)

    assert summary["down"] == 1
    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:a'").fetchone()
    assert row["endpoint_status"] == "down"
    assert row["reachability_state"] == "down"


def test_run_discovery_no_endpoint():
    """Labeler with no service endpoint in DID doc."""
    conn = _make_db()
    cfg = Config(reference_dids=[])

    mock_dids = ["did:plc:noep"]
    did_doc = {
        "id": "did:plc:noep",
        "alsoKnownAs": ["at://noep.example.com"],
        "service": [],
        "verificationMethod": [],
    }

    with patch("labelwatch.discover.list_labeler_dids", return_value=mock_dids), \
         patch("labelwatch.discover.fetch_did_doc", return_value=did_doc), \
         patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response({"views": []})), \
         patch("labelwatch.discover.time.sleep"):
        summary = run_discovery(conn, cfg)

    assert summary["no_endpoint"] == 1
    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:noep'").fetchone()
    assert row["endpoint_status"] == "unknown"
    assert row["service_endpoint"] is None


def test_run_discovery_sticky_observed_src():
    """If a labeler was already marked as observed_as_src, discovery doesn't reset it."""
    conn = _make_db()
    cfg = Config(reference_dids=[])

    # Pre-insert labeler with observed_as_src=1
    conn.execute(
        "INSERT INTO labelers(labeler_did, observed_as_src, first_seen, last_seen) "
        "VALUES('did:plc:sticky', 1, '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    mock_dids = ["did:plc:sticky"]
    did_doc = {
        "id": "did:plc:sticky",
        "alsoKnownAs": ["at://sticky.example.com"],
        "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler",
                     "serviceEndpoint": "https://sticky.example.com"}],
        "verificationMethod": [],
    }

    probe_result = ProbeResult("accessible", http_status=200, latency_ms=50)

    with patch("labelwatch.discover.list_labeler_dids", return_value=mock_dids), \
         patch("labelwatch.discover.fetch_did_doc", return_value=did_doc), \
         patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response({"views": []})), \
         patch("labelwatch.discover.probe_endpoint", return_value=probe_result), \
         patch("labelwatch.discover.time.sleep"):
        run_discovery(conn, cfg)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:sticky'").fetchone()
    assert row["observed_as_src"] == 1  # Not reset by discovery
    assert row["declared_record"] == 1
    assert row["visibility_class"] == "declared"


def test_run_discovery_test_dev_detection():
    """Test/dev labelers are flagged when noise_policy_enabled."""
    conn = _make_db()
    cfg = Config(reference_dids=[], noise_policy_enabled=True)

    mock_dids = ["did:plc:testlab"]
    did_doc = {
        "id": "did:plc:testlab",
        "alsoKnownAs": ["at://test-labeler.bsky.social"],
        "service": [],
        "verificationMethod": [],
    }

    hydration_response = {
        "views": [
            {"creator": {"did": "did:plc:testlab", "displayName": "Test Labeler"}},
        ]
    }

    with patch("labelwatch.discover.list_labeler_dids", return_value=mock_dids), \
         patch("labelwatch.discover.fetch_did_doc", return_value=did_doc), \
         patch("labelwatch.discover.urllib.request.urlopen", return_value=_mock_http_response(hydration_response)), \
         patch("labelwatch.discover.time.sleep"):
        run_discovery(conn, cfg)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:testlab'").fetchone()
    assert row["likely_test_dev"] == 1
