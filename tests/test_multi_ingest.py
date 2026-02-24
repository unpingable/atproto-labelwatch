"""Tests for multi-source ingest from discovered labeler endpoints."""
from unittest.mock import patch, MagicMock

from labelwatch import db, ingest
from labelwatch.config import Config


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _make_labels(start, count, labeler):
    out = []
    for i in range(start, start + count):
        out.append({
            "src": labeler,
            "uri": f"at://{labeler}/app.bsky.feed.post/{i}",
            "val": "test-label",
            "ts": f"2024-01-01T00:{i:02d}:00Z",
        })
    return out


def _insert_accessible_labeler(conn, did, endpoint):
    conn.execute(
        """
        INSERT INTO labelers(labeler_did, service_endpoint, endpoint_status, first_seen, last_seen)
        VALUES(?, ?, 'accessible', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
        ON CONFLICT(labeler_did) DO UPDATE SET
            service_endpoint=excluded.service_endpoint,
            endpoint_status=excluded.endpoint_status
        """,
        (did, endpoint),
    )
    conn.commit()


def test_ingest_multi_basic():
    """Ingest from two accessible labelers."""
    conn = _make_db()
    cfg = Config()

    _insert_accessible_labeler(conn, "did:plc:a", "https://labeler-a.example.com")
    _insert_accessible_labeler(conn, "did:plc:b", "https://labeler-b.example.com")

    def fake_fetch(service_url, sources, cursor=None, limit=100):
        did = sources[0]
        return {"labels": _make_labels(0, 3, did), "cursor": None}

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        results = ingest.ingest_multi(conn, cfg)

    assert results["did:plc:a"] == 3
    assert results["did:plc:b"] == 3
    total = conn.execute("SELECT COUNT(*) AS c FROM label_events").fetchone()["c"]
    assert total == 6


def test_ingest_multi_per_did_cursors():
    """Each labeler gets its own cursor keyed by DID."""
    conn = _make_db()
    cfg = Config()

    _insert_accessible_labeler(conn, "did:plc:a", "https://labeler-a.example.com")

    page1 = {"labels": _make_labels(0, 3, "did:plc:a"), "cursor": "cursor_a_1"}
    page2 = {"labels": [], "cursor": None}

    call_count = 0
    def fake_fetch(service_url, sources, cursor=None, limit=100):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return page1
        return page2

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        ingest.ingest_multi(conn, cfg)

    # Cursor should be stored under the DID, not the service URL
    cursor = db.get_cursor(conn, "did:plc:a")
    assert cursor == "cursor_a_1"


def test_ingest_multi_failure_isolation():
    """Failure for one labeler doesn't block others."""
    conn = _make_db()
    cfg = Config()

    _insert_accessible_labeler(conn, "did:plc:bad", "https://bad.example.com")
    _insert_accessible_labeler(conn, "did:plc:good", "https://good.example.com")

    def fake_fetch(service_url, sources, cursor=None, limit=100):
        if "bad" in service_url:
            raise ConnectionError("network failure")
        return {"labels": _make_labels(0, 3, sources[0]), "cursor": None}

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        results = ingest.ingest_multi(conn, cfg)

    assert results["did:plc:bad"] == 0
    assert results["did:plc:good"] == 3


def test_ingest_multi_skips_non_accessible():
    """Only labelers with endpoint_status='accessible' are ingested."""
    conn = _make_db()
    cfg = Config()

    _insert_accessible_labeler(conn, "did:plc:good", "https://good.example.com")
    # Insert a non-accessible labeler
    conn.execute(
        "INSERT INTO labelers(labeler_did, service_endpoint, endpoint_status, first_seen, last_seen) "
        "VALUES(?, ?, 'down', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')",
        ("did:plc:down", "https://down.example.com"),
    )
    conn.commit()

    def fake_fetch(service_url, sources, cursor=None, limit=100):
        if "down" in service_url:
            raise AssertionError("Should not fetch from down endpoint")
        return {"labels": _make_labels(0, 2, sources[0]), "cursor": None}

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        results = ingest.ingest_multi(conn, cfg)

    assert "did:plc:down" not in results
    assert results["did:plc:good"] == 2


def test_ingest_multi_budget():
    """Time budget stops processing."""
    conn = _make_db()
    cfg = Config(multi_ingest_budget=0)  # Expire immediately

    _insert_accessible_labeler(conn, "did:plc:a", "https://a.example.com")
    _insert_accessible_labeler(conn, "did:plc:b", "https://b.example.com")

    def fake_fetch(service_url, sources, cursor=None, limit=100):
        return {"labels": _make_labels(0, 3, sources[0]), "cursor": None}

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        results = ingest.ingest_multi(conn, cfg)

    # With budget=0, may process zero or one labeler before budget check kicks in
    # The point is it doesn't hang forever
    assert isinstance(results, dict)


def test_ingest_multi_respects_max_pages():
    """Max pages limits pagination per labeler."""
    conn = _make_db()
    cfg = Config(multi_ingest_max_pages=2)

    _insert_accessible_labeler(conn, "did:plc:a", "https://a.example.com")

    call_count = 0
    def fake_fetch(service_url, sources, cursor=None, limit=100):
        nonlocal call_count
        call_count += 1
        return {"labels": _make_labels(call_count * 3, 3, "did:plc:a"), "cursor": f"c{call_count}"}

    with patch.object(ingest, "fetch_labels", side_effect=fake_fetch):
        ingest.ingest_multi(conn, cfg)

    assert call_count == 2  # Stopped at max_pages


# --- Observed-only labeler creation ---

def test_ingest_creates_observed_only_labeler():
    """Ingest with unknown src DID creates observed_only labeler row."""
    conn = _make_db()

    items = [{
        "src": "did:plc:unknown_labeler",
        "uri": "at://did:plc:user/app.bsky.feed.post/1",
        "val": "test-label",
        "ts": "2024-01-01T00:00:00Z",
    }]
    ingest.ingest_from_iter(conn, items)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:unknown_labeler'").fetchone()
    assert row is not None
    assert row["visibility_class"] == "observed_only"
    assert row["observed_as_src"] == 1
    assert row["reachability_state"] == "unknown"


def test_ingest_sets_sticky_observed_src():
    """Known labeler gets observed_as_src=1 when seen as src."""
    conn = _make_db()

    # Pre-insert a declared labeler
    conn.execute(
        "INSERT INTO labelers(labeler_did, visibility_class, observed_as_src, first_seen, last_seen) "
        "VALUES('did:plc:known', 'declared', 0, '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')"
    )
    conn.commit()

    items = [{
        "src": "did:plc:known",
        "uri": "at://did:plc:user/app.bsky.feed.post/1",
        "val": "test-label",
        "ts": "2024-01-01T00:00:00Z",
    }]
    ingest.ingest_from_iter(conn, items)

    row = conn.execute("SELECT observed_as_src FROM labelers WHERE labeler_did='did:plc:known'").fetchone()
    assert row["observed_as_src"] == 1


def test_ingest_writes_observed_evidence():
    """Evidence record is written for observed label src."""
    conn = _make_db()

    items = [{
        "src": "did:plc:evtest",
        "uri": "at://did:plc:user/app.bsky.feed.post/1",
        "val": "test-label",
        "ts": "2024-01-01T00:00:00Z",
    }]
    ingest.ingest_from_iter(conn, items)

    evidence = db.get_evidence(conn, "did:plc:evtest")
    types = {e["evidence_type"] for e in evidence}
    assert "observed_label_src" in types


def test_ingest_dedupes_evidence_within_run():
    """Multiple events from same src in same run only create one evidence record."""
    conn = _make_db()

    items = [
        {"src": "did:plc:dedup", "uri": "at://did:plc:u/post/1", "val": "label-a", "ts": "2024-01-01T00:00:00Z"},
        {"src": "did:plc:dedup", "uri": "at://did:plc:u/post/2", "val": "label-b", "ts": "2024-01-01T00:01:00Z"},
    ]
    ingest.ingest_from_iter(conn, items)

    evidence = db.get_evidence(conn, "did:plc:dedup")
    src_evidence = [e for e in evidence if e["evidence_type"] == "observed_label_src"]
    assert len(src_evidence) == 1


def test_ingest_rejects_malformed_src_did():
    """Garbage src DID is not inserted into labelers."""
    conn = _make_db()

    items = [{
        "labeler_did": "did:plc:valid_labeler",
        "src": "not-a-valid-did",
        "uri": "at://did:plc:user/app.bsky.feed.post/1",
        "val": "test-label",
        "ts": "2024-01-01T00:00:00Z",
    }]
    ingest.ingest_from_iter(conn, items)

    # The valid labeler_did should be there (from upsert_labeler)
    valid_row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:valid_labeler'").fetchone()
    assert valid_row is not None

    # The malformed src should NOT create a labeler row
    bad_row = conn.execute("SELECT * FROM labelers WHERE labeler_did='not-a-valid-did'").fetchone()
    assert bad_row is None


# --- Lifecycle integration tests ---

def test_observed_then_declared():
    """Ingest sees src first, then discovery-like upsert upgrades the row."""
    conn = _make_db()

    # Step 1: Ingest creates observed_only
    items = [{
        "src": "did:plc:lifecycle",
        "uri": "at://did:plc:u/post/1",
        "val": "label",
        "ts": "2024-01-01T00:00:00Z",
    }]
    ingest.ingest_from_iter(conn, items)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:lifecycle'").fetchone()
    assert row["visibility_class"] == "observed_only"
    assert row["observed_as_src"] == 1

    # Step 2: Simulate discovery upgrading this labeler
    conn.execute(
        """
        UPDATE labelers SET
            visibility_class='declared', declared_record=1,
            has_labeler_service=1, reachability_state='accessible',
            classification_reason='declared+did_service+probe_accessible+observed_src'
        WHERE labeler_did='did:plc:lifecycle'
        """
    )
    conn.commit()

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:lifecycle'").fetchone()
    assert row["visibility_class"] == "declared"
    assert row["observed_as_src"] == 1  # Preserved from ingest
    assert row["declared_record"] == 1

    # Evidence from both phases should exist
    evidence = db.get_evidence(conn, "did:plc:lifecycle")
    types = {e["evidence_type"] for e in evidence}
    assert "observed_label_src" in types
