"""Tests for Jetstream discovery stream."""
import json
from unittest.mock import patch, MagicMock, AsyncMock

from labelwatch import db
from labelwatch.discover import upsert_discovered_labeler, backstop_from_lists
from labelwatch.discovery_stream import (
    _build_ws_url,
    _load_known_labelers,
    _process_commit,
    _resolve_did_sync,
    _handle_discovery,
    _handle_identity_refresh,
    _worker,
    _Stats,
)
from labelwatch.utils import format_ts, hash_sha256, now_utc, stable_json

import asyncio
import pytest


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


# --- Schema v17 ---

def test_schema_v17_discovery_events_table():
    conn = _make_db()
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "discovery_events" in tables


def test_schema_v17_discovery_events_columns():
    conn = _make_db()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(discovery_events)").fetchall()]
    expected = ["id", "labeler_did", "operation", "source", "time_us",
                "commit_cid", "commit_rev", "record_json", "record_sha256",
                "resolved_endpoint", "discovered_at"]
    for col in expected:
        assert col in cols, f"Missing column: {col}"


def test_schema_v17_indexes():
    conn = _make_db()
    indexes = {r[1] for r in conn.execute("PRAGMA index_list(discovery_events)").fetchall()}
    assert "idx_discovery_events_did" in indexes
    assert "idx_discovery_events_ts" in indexes


# --- process_commit filter ---

def test_process_commit_labeler_self():
    msg = {"commit": {"collection": "app.bsky.labeler.service", "rkey": "self", "operation": "create"}}
    assert _process_commit(msg) is not None


def test_ignore_non_self():
    msg = {"commit": {"collection": "app.bsky.labeler.service", "rkey": "other", "operation": "create"}}
    assert _process_commit(msg) is None


def test_ignore_non_labeler_collection():
    msg = {"commit": {"collection": "app.bsky.feed.post", "rkey": "self", "operation": "create"}}
    assert _process_commit(msg) is None


def test_ignore_non_commit():
    msg = {"kind": "identity", "did": "did:plc:test123"}
    assert _process_commit(msg) is None


def test_ignore_missing_commit():
    msg = {"kind": "commit"}
    assert _process_commit(msg) is None


# --- Discovery event processing ---

DID_DOC = {
    "id": "did:plc:testlabeler1",
    "alsoKnownAs": ["at://test.labeler.example"],
    "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler",
                 "serviceEndpoint": "https://labeler.example.com"}],
    "verificationMethod": [{"id": "#atproto_label", "type": "Multikey"}],
}


@pytest.mark.asyncio
async def test_process_create():
    conn = _make_db()
    known = set()
    stats = _Stats()
    record = {"$type": "app.bsky.labeler.service", "policies": {"labelValues": ["spam"]}}

    item = {
        "kind": "discovery",
        "did": "did:plc:testlabeler1",
        "operation": "create",
        "commit": {
            "collection": "app.bsky.labeler.service",
            "rkey": "self",
            "operation": "create",
            "cid": "bafyreiabc123",
            "rev": "rev123",
            "record": record,
        },
        "time_us": 1700000000000000,
    }

    with patch("labelwatch.discovery_stream._resolve_did_sync") as mock_resolve:
        mock_resolve.return_value = {
            "did": "did:plc:testlabeler1",
            "handle": "test.labeler.example",
            "endpoint": "https://labeler.example.com",
            "has_label_key": True,
            "did_doc": DID_DOC,
        }
        await _handle_discovery(conn, item, known, stats)

    # Check labeler row created
    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:testlabeler1'").fetchone()
    assert row is not None
    assert row["handle"] == "test.labeler.example"
    assert row["service_endpoint"] == "https://labeler.example.com"
    assert row["declared_record"] == 1

    # Check discovery_event logged
    ev = conn.execute("SELECT * FROM discovery_events WHERE labeler_did='did:plc:testlabeler1'").fetchone()
    assert ev is not None
    assert ev["operation"] == "create"
    assert ev["source"] == "jetstream"
    assert ev["commit_cid"] == "bafyreiabc123"
    assert ev["commit_rev"] == "rev123"
    assert ev["record_json"] is not None
    assert ev["record_sha256"] is not None

    # Check in-memory set updated
    assert "did:plc:testlabeler1" in known
    assert stats.discoveries == 1


@pytest.mark.asyncio
async def test_process_update():
    conn = _make_db()
    known = set()
    stats = _Stats()

    # Pre-insert labeler
    ts = format_ts(now_utc())
    upsert_discovered_labeler(
        conn, "did:plc:testlabeler1",
        handle="old.handle",
        endpoint="https://old.example.com",
        has_service=True,
        declared_record=True,
        seen_ts=ts,
    )
    conn.commit()

    record = {"$type": "app.bsky.labeler.service", "policies": {"labelValues": ["spam", "nsfw"]}}
    item = {
        "kind": "discovery",
        "did": "did:plc:testlabeler1",
        "operation": "update",
        "commit": {
            "collection": "app.bsky.labeler.service",
            "rkey": "self",
            "operation": "update",
            "cid": "bafyreidef456",
            "rev": "rev456",
            "record": record,
        },
        "time_us": 1700000001000000,
    }

    with patch("labelwatch.discovery_stream._resolve_did_sync") as mock_resolve:
        mock_resolve.return_value = {
            "did": "did:plc:testlabeler1",
            "handle": "new.handle",
            "endpoint": "https://new.example.com",
            "has_label_key": True,
            "did_doc": DID_DOC,
        }
        await _handle_discovery(conn, item, known, stats)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:testlabeler1'").fetchone()
    assert row["handle"] == "new.handle"
    assert row["service_endpoint"] == "https://new.example.com"

    evs = conn.execute("SELECT * FROM discovery_events WHERE labeler_did='did:plc:testlabeler1'").fetchall()
    assert len(evs) == 1
    assert evs[0]["operation"] == "update"


@pytest.mark.asyncio
async def test_process_delete():
    conn = _make_db()
    known = set()
    stats = _Stats()

    # Pre-insert
    ts = format_ts(now_utc())
    upsert_discovered_labeler(
        conn, "did:plc:testlabeler1", declared_record=True, seen_ts=ts)
    conn.commit()

    item = {
        "kind": "discovery",
        "did": "did:plc:testlabeler1",
        "operation": "delete",
        "commit": {
            "collection": "app.bsky.labeler.service",
            "rkey": "self",
            "operation": "delete",
            "cid": "bafyreighi789",
            "rev": "rev789",
        },
        "time_us": 1700000002000000,
    }

    await _handle_discovery(conn, item, known, stats)

    # Labeler NOT deleted (sticky)
    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:testlabeler1'").fetchone()
    assert row is not None

    # Discovery event logged
    ev = conn.execute("SELECT * FROM discovery_events WHERE labeler_did='did:plc:testlabeler1'").fetchone()
    assert ev["operation"] == "delete"
    assert stats.deletes == 1


# --- Cursor ---

def test_cursor_save():
    conn = _make_db()
    db.set_meta(conn, "jetstream_discovery_cursor", "1700000000000000")
    conn.commit()
    val = db.get_meta(conn, "jetstream_discovery_cursor")
    assert val == "1700000000000000"


def test_cursor_resume():
    conn = _make_db()
    db.set_meta(conn, "jetstream_discovery_cursor", "1700000000000000")
    conn.commit()
    cursor = int(db.get_meta(conn, "jetstream_discovery_cursor"))
    url = _build_ws_url(cursor)
    # Should include rewound cursor
    expected_cursor = 1700000000000000 - 3_000_000
    assert f"cursor={expected_cursor}" in url


def test_cursor_no_resume():
    url = _build_ws_url(None)
    assert "cursor=" not in url


# --- Sticky fields ---

def test_sticky_fields():
    conn = _make_db()
    ts = format_ts(now_utc())

    # First upsert: observed_as_src=1 set via ingest path
    conn.execute(
        "INSERT INTO labelers(labeler_did, observed_as_src, has_labeler_service, first_seen, last_seen) "
        "VALUES(?, 1, 1, ?, ?)",
        ("did:plc:sticky1", ts, ts),
    )
    conn.commit()

    # Stream discovery upsert with has_service=False should NOT downgrade
    upsert_discovered_labeler(
        conn, "did:plc:sticky1",
        has_service=False,
        has_label_key=False,
        declared_record=True,
        seen_ts=ts,
    )
    conn.commit()

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:sticky1'").fetchone()
    assert row["observed_as_src"] == 1  # preserved
    assert row["has_labeler_service"] == 1  # preserved (sticky)
    assert row["declared_record"] == 1


# --- Unknown DID count ---

def test_unknown_did_count():
    conn = _make_db()
    ts = format_ts(now_utc())

    # observed_only + declared_record=0
    conn.execute(
        "INSERT INTO labelers(labeler_did, visibility_class, declared_record, first_seen, last_seen) "
        "VALUES(?, 'observed_only', 0, ?, ?)",
        ("did:plc:unknown1", ts, ts),
    )
    # declared + declared_record=1 (should NOT count)
    conn.execute(
        "INSERT INTO labelers(labeler_did, visibility_class, declared_record, first_seen, last_seen) "
        "VALUES(?, 'declared', 1, ?, ?)",
        ("did:plc:known1", ts, ts),
    )
    conn.commit()

    row = conn.execute(
        "SELECT COUNT(*) AS c FROM labelers WHERE visibility_class='observed_only' AND declared_record=0"
    ).fetchone()
    assert row["c"] == 1


# --- Replay dedupe ---

def test_replay_dedupe():
    conn = _make_db()
    ts = format_ts(now_utc())

    db.insert_discovery_event(
        conn, "did:plc:dedup1", "create", "jetstream",
        discovered_at=ts, commit_rev="rev_aaa",
    )
    conn.commit()

    # Same (did, rev, op) — should be ignored via UNIQUE constraint
    db.insert_discovery_event(
        conn, "did:plc:dedup1", "create", "jetstream",
        discovered_at=ts, commit_rev="rev_aaa",
    )
    conn.commit()

    rows = conn.execute(
        "SELECT * FROM discovery_events WHERE labeler_did='did:plc:dedup1'"
    ).fetchall()
    assert len(rows) == 1


# --- Identity event refresh ---

@pytest.mark.asyncio
async def test_identity_event_refresh():
    conn = _make_db()
    stats = _Stats()
    ts = format_ts(now_utc())

    # Pre-insert known labeler
    upsert_discovered_labeler(
        conn, "did:plc:knownlabeler",
        handle="old.handle",
        endpoint="https://old.example.com",
        has_service=True,
        declared_record=True,
        seen_ts=ts,
    )
    conn.commit()

    item = {"kind": "identity_refresh", "did": "did:plc:knownlabeler"}

    with patch("labelwatch.discovery_stream._resolve_did_sync") as mock_resolve:
        mock_resolve.return_value = {
            "did": "did:plc:knownlabeler",
            "handle": "new.handle",
            "endpoint": "https://new.endpoint.com",
            "has_label_key": True,
            "did_doc": DID_DOC,
        }
        await _handle_identity_refresh(conn, item, stats)

    row = conn.execute("SELECT * FROM labelers WHERE labeler_did='did:plc:knownlabeler'").fetchone()
    assert row["handle"] == "new.handle"
    assert row["service_endpoint"] == "https://new.endpoint.com"
    assert stats.identity_refreshes == 1


# --- Record SHA256 ---

def test_record_sha256():
    record1 = {"$type": "app.bsky.labeler.service", "policies": {"labelValues": ["spam"]}}
    record2 = {"$type": "app.bsky.labeler.service", "policies": {"labelValues": ["spam", "nsfw"]}}

    json1 = stable_json(record1)
    json2 = stable_json(record2)
    sha1 = hash_sha256(json1)
    sha2 = hash_sha256(json2)

    assert sha1 != sha2
    # Same input = same hash
    assert hash_sha256(stable_json(record1)) == sha1


# --- _build_ws_url ---

def test_build_ws_url_no_cursor():
    url = _build_ws_url()
    assert "wantedCollections=app.bsky.labeler.service" in url
    assert "cursor=" not in url


def test_build_ws_url_with_cursor():
    url = _build_ws_url(cursor=1700000000000000)
    assert "wantedCollections=app.bsky.labeler.service" in url
    assert "cursor=" in url


# --- load_known_labelers ---

def test_load_known_labelers():
    conn = _make_db()
    ts = format_ts(now_utc())
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?, ?, ?)",
        ("did:plc:lab1", ts, ts),
    )
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen) VALUES(?, ?, ?)",
        ("did:plc:lab2", ts, ts),
    )
    conn.commit()

    known = _load_known_labelers(conn)
    assert known == {"did:plc:lab1", "did:plc:lab2"}


# --- Fatal error on DB write failure ---

@pytest.mark.asyncio
async def test_worker_db_error_sets_fatal():
    """DB write errors in the worker must set fatal_error, not silently drop."""
    conn = _make_db()
    known = set()
    stats = _Stats()
    fatal_error = asyncio.Event()
    queue: asyncio.Queue = asyncio.Queue(maxsize=10)

    # Drop the discovery_events table to force a DB error on insert
    conn.execute("DROP TABLE discovery_events")
    conn.commit()

    queue.put_nowait({
        "kind": "discovery",
        "did": "did:plc:boom",
        "operation": "delete",
        "commit": {
            "collection": "app.bsky.labeler.service",
            "rkey": "self",
            "operation": "delete",
            "cid": "bafyreiboom",
            "rev": "revboom",
        },
        "time_us": 1700000000000000,
    })

    task = asyncio.create_task(_worker(conn, queue, known, stats, fatal_error))
    # Wait for queue to drain
    await queue.join()
    # Worker should have set fatal_error and returned
    assert fatal_error.is_set()
    task.cancel()
