"""Tests for labeler registry page."""
import hashlib

from labelwatch import db
from labelwatch.registry import _query_hide_stats, generate_registry, render_registry_html


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _seed_labelers(conn, labelers):
    for lab in labelers:
        conn.execute(
            "INSERT OR REPLACE INTO labelers "
            "(labeler_did, handle, display_name, description, "
            " visibility_class, observed_as_src, has_labeler_service, "
            " declared_record, likely_test_dev, endpoint_status, "
            " regime_state, events_7d, events_30d, unique_targets_7d) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                lab["did"], lab.get("handle"), lab.get("display_name"),
                lab.get("description"), lab.get("visibility_class", "declared"),
                lab.get("observed_as_src", 0), lab.get("has_labeler_service", 0),
                lab.get("declared_record", 0), lab.get("likely_test_dev", 0),
                lab.get("endpoint_status", "unknown"),
                lab.get("regime_state"), lab.get("events_7d", 0),
                lab.get("events_30d", 0), lab.get("unique_targets_7d", 0),
            ),
        )
    conn.commit()


def test_generate_registry_empty():
    conn = _make_db()
    payload = generate_registry(conn)
    assert payload["summary"]["total_labelers"] == 0
    assert payload["labelers"] == []
    assert "generated_at" in payload


def test_generate_registry_with_labelers():
    conn = _make_db()
    _seed_labelers(conn, [
        {"did": "did:plc:a", "handle": "alpha.test", "display_name": "Alpha",
         "description": "A labeler", "observed_as_src": 1,
         "has_labeler_service": 1, "declared_record": 1,
         "events_7d": 100, "events_30d": 500, "unique_targets_7d": 50,
         "regime_state": "stable"},
        {"did": "did:plc:b", "handle": "beta.test",
         "observed_as_src": 0, "has_labeler_service": 1,
         "events_7d": 0, "events_30d": 10},
        {"did": "did:plc:c", "handle": "gamma.test",
         "likely_test_dev": 1, "events_7d": 5},
    ])
    payload = generate_registry(conn)

    assert payload["summary"]["total_labelers"] == 3
    assert payload["summary"]["active_labelers"] == 1
    assert payload["summary"]["with_service"] == 2
    assert payload["summary"]["test_dev"] == 1
    assert payload["summary"]["total_events_7d"] == 105
    assert payload["summary"]["total_events_30d"] == 510

    # Ordered by events_7d desc
    assert payload["labelers"][0]["labeler_did"] == "did:plc:a"
    assert payload["labelers"][0]["handle"] == "alpha.test"


def test_render_registry_html():
    conn = _make_db()
    _seed_labelers(conn, [
        {"did": "did:plc:a", "handle": "alpha.test", "display_name": "Alpha",
         "description": "Moderation service",
         "observed_as_src": 1, "events_7d": 100, "regime_state": "stable"},
    ])
    payload = generate_registry(conn)
    html = render_registry_html(payload)

    assert "Labeler Registry" in html
    assert "alpha.test" in html
    assert "Alpha" in html
    assert "Moderation service" in html


def test_render_registry_html_empty():
    conn = _make_db()
    payload = generate_registry(conn)
    html = render_registry_html(payload)

    assert "Labeler Registry" in html
    assert "Total Labelers" in html


def test_registry_summary_counts():
    conn = _make_db()
    _seed_labelers(conn, [
        {"did": f"did:plc:{i}", "handle": f"lab{i}.test",
         "observed_as_src": 1 if i < 3 else 0,
         "has_labeler_service": 1 if i < 5 else 0,
         "declared_record": 1 if i < 4 else 0,
         "likely_test_dev": 1 if i == 6 else 0,
         "events_7d": i * 10, "events_30d": i * 50}
        for i in range(7)
    ])
    payload = generate_registry(conn)

    s = payload["summary"]
    assert s["total_labelers"] == 7
    assert s["active_labelers"] == 3
    assert s["declared_labelers"] == 4
    assert s["with_service"] == 5
    assert s["test_dev"] == 1
    assert s["total_events_7d"] == sum(i * 10 for i in range(7))


def _insert_event(conn, src, uri, val, ts, neg=0):
    h = hashlib.sha256(f"{src}{uri}{val}{ts}{neg}".encode()).hexdigest()
    conn.execute("""
        INSERT OR IGNORE INTO label_events
            (labeler_did, src, uri, cid, val, neg, ts, event_hash)
        VALUES (?, ?, ?, 'cid', ?, ?, ?, ?)
    """, (src, src, uri, val, neg, ts, h))


def test_hide_stats_basic():
    conn = _make_db()
    did = "did:plc:hidetest"
    _seed_labelers(conn, [{"did": did, "handle": "hidetest.lab"}])

    _insert_event(conn, did, "at://did:a/post/1", "!hide", "2026-03-01T00:00:00Z")
    _insert_event(conn, did, "at://did:b/post/2", "!hide", "2026-03-02T00:00:00Z")
    _insert_event(conn, did, "at://did:a/post/1", "!hide", "2026-03-03T00:00:00Z")  # same target
    _insert_event(conn, did, "at://did:c/post/3", "spam", "2026-03-01T00:00:00Z")  # not !hide
    _insert_event(conn, did, "at://did:d/post/4", "!hide", "2026-03-01T00:00:00Z", neg=1)  # unhide
    conn.commit()

    stats = _query_hide_stats(conn, "2025-03-14T00:00:00Z")
    assert did in stats
    s = stats[did]
    assert s["hide_total"] == 3
    assert s["hide_subjects_total"] == 2


def test_hide_stats_windowed():
    conn = _make_db()
    did = "did:plc:hidewindow"
    _seed_labelers(conn, [{"did": did, "handle": "hidewindow.lab"}])

    _insert_event(conn, did, "at://did:a/post/1", "!hide", "2024-01-01T00:00:00Z")  # old
    _insert_event(conn, did, "at://did:b/post/2", "!hide", "2026-03-01T00:00:00Z")  # recent
    conn.commit()

    stats = _query_hide_stats(conn, "2025-03-14T00:00:00Z")
    s = stats[did]
    assert s["hide_total"] == 2
    assert s["hide_365d"] == 1


def test_hide_stats_in_registry_payload():
    conn = _make_db()
    did = "did:plc:hidepayload"
    _seed_labelers(conn, [{"did": did, "handle": "hidepayload.lab"}])

    _insert_event(conn, did, "at://did:a/post/1", "!hide", "2026-03-01T00:00:00Z")
    _insert_event(conn, did, "at://did:b/post/2", "!hide", "2026-03-02T00:00:00Z")
    conn.commit()

    payload = generate_registry(conn)
    lab = next(l for l in payload["labelers"] if l["labeler_did"] == did)
    assert lab["hide_total"] == 2
    assert lab["hide_subjects_total"] == 2


def test_hide_stats_zero_for_labeler_without_hides():
    conn = _make_db()
    did = "did:plc:nohides"
    _seed_labelers(conn, [{"did": did, "handle": "nohides.lab"}])
    conn.commit()

    payload = generate_registry(conn)
    lab = next(l for l in payload["labelers"] if l["labeler_did"] == did)
    assert lab["hide_total"] == 0
    assert lab["hide_365d"] == 0
