"""Tests for My Label Climate Phase 3: queries, JSON assembly, HTML rendering."""
import json
import os
import time

import pytest

from labelwatch import db
from labelwatch.climate import (
    _at_uri_to_bsky_link,
    _query_daily_series,
    _query_recent_receipts,
    _query_summary,
    _query_top_labelers,
    _query_top_values,
    _query_week_deltas,
    generate_climate,
)
from labelwatch.scan import _update_author_day, _update_author_labeler_day


TARGET = "did:plc:author1"
LABELER1 = "did:plc:labeler1"
LABELER2 = "did:plc:labeler2"


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _day_iso(days_ago: int = 0):
    """ISO timestamp for N days ago at noon UTC."""
    epoch = int(time.time()) - days_ago * 86400
    return time.strftime("%Y-%m-%dT12:00:00Z", time.gmtime(epoch))


def _seed_events(conn, events):
    """Insert label events from list of dicts.

    Each dict: labeler_did, uri, val, neg (0/1), ts, target_did.
    event_hash is auto-generated.
    """
    for i, e in enumerate(events):
        conn.execute(
            "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                e["labeler_did"],
                e["uri"],
                e["val"],
                e.get("neg", 0),
                e["ts"],
                e.get("event_hash", f"hash_{i}_{time.monotonic_ns()}"),
                e["target_did"],
            ),
        )
    conn.commit()


def _seed_labeler(conn, labeler_did, handle=None, regime_state=None):
    """Insert or update a labeler row."""
    conn.execute(
        "INSERT OR REPLACE INTO labelers(labeler_did, handle, regime_state) VALUES(?, ?, ?)",
        (labeler_did, handle, regime_state),
    )
    conn.commit()


def _seed_and_rollup(conn, events, labelers=None):
    """Seed events + labelers, then run rollups."""
    _seed_events(conn, events)
    if labelers:
        for l in labelers:
            _seed_labeler(conn, **l)
    _update_author_day(conn)
    _update_author_labeler_day(conn)
    conn.commit()


def _make_basic_events(n=5):
    """Standard set: 5 events, 2 labelers, 1 remove."""
    ts = _now_iso()
    return [
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/{i}",
         "val": "spam", "neg": 1 if i == 0 else 0, "ts": ts, "target_did": TARGET}
        for i in range(3)
    ] + [
        {"labeler_did": LABELER2, "uri": f"at://{TARGET}/app.bsky.feed.post/{i}",
         "val": "porn" if i == 4 else "spam", "neg": 0, "ts": ts, "target_did": TARGET}
        for i in range(3, 5)
    ]


# ---------------------------------------------------------------------------
# JSON shape
# ---------------------------------------------------------------------------


def test_climate_json_shape(tmp_path):
    conn = _make_db()
    _seed_and_rollup(conn, _make_basic_events(), [
        {"labeler_did": LABELER1, "handle": "lab1.test", "regime_state": "stable"},
        {"labeler_did": LABELER2, "handle": "lab2.test"},
    ])

    payload = generate_climate(conn, TARGET, window_days=30, out_dir=str(tmp_path), fmt="both")

    assert payload["empty"] is False
    assert payload["target_did"] == TARGET
    assert "summary" in payload
    assert "week_deltas" in payload
    assert "top_labelers" in payload
    assert "top_values" in payload
    assert "daily_series" in payload
    assert "recent_receipts" in payload
    assert "generated_at" in payload

    # Files written
    assert os.path.exists(tmp_path / "climate.json")
    assert os.path.exists(tmp_path / "climate.html")

    # JSON is valid
    with open(tmp_path / "climate.json") as f:
        loaded = json.load(f)
    assert loaded["target_did"] == TARGET


def test_climate_empty_state(tmp_path):
    conn = _make_db()
    payload = generate_climate(conn, "did:plc:nobody", window_days=30,
                               out_dir=str(tmp_path), fmt="json")
    assert payload["empty"] is True
    assert "message" in payload
    assert os.path.exists(tmp_path / "climate.json")


# ---------------------------------------------------------------------------
# Summary counts
# ---------------------------------------------------------------------------


def test_climate_summary_counts():
    conn = _make_db()
    _seed_and_rollup(conn, _make_basic_events())

    now_epoch = int(time.time())
    now_day = (now_epoch // 86400) * 86400
    start_day = ((now_epoch // 86400) - 29) * 86400
    end_day = now_day + 86400
    start_iso = _day_iso(30)

    s = _query_summary(conn, TARGET, start_day, end_day, start_iso)
    assert s["label_actions"] == 5
    assert s["applies"] == 4
    assert s["removes"] == 1
    assert s["unique_posts"] == 5
    assert s["labelers"] == 2
    assert s["label_values"] == 2  # spam + porn


# ---------------------------------------------------------------------------
# Distinct-count accuracy
# ---------------------------------------------------------------------------


def test_climate_labelers_not_overcounted():
    """Labeler active on 3 days = 1 distinct, not 3."""
    conn = _make_db()
    events = []
    for day_offset in range(3):
        events.append({
            "labeler_did": LABELER1,
            "uri": f"at://{TARGET}/app.bsky.feed.post/day{day_offset}",
            "val": "spam", "neg": 0, "ts": _day_iso(day_offset),
            "target_did": TARGET,
        })
    _seed_and_rollup(conn, events)

    now_epoch = int(time.time())
    now_day = (now_epoch // 86400) * 86400
    start_day = ((now_epoch // 86400) - 29) * 86400
    end_day = now_day + 86400
    start_iso = _day_iso(30)

    s = _query_summary(conn, TARGET, start_day, end_day, start_iso)
    assert s["labelers"] == 1


def test_climate_unique_posts_not_overcounted():
    """Same post touched on 2 days = 1 unique post."""
    conn = _make_db()
    uri = f"at://{TARGET}/app.bsky.feed.post/samepost"
    events = [
        {"labeler_did": LABELER1, "uri": uri, "val": "spam", "neg": 0,
         "ts": _day_iso(0), "target_did": TARGET},
        {"labeler_did": LABELER1, "uri": uri, "val": "spam", "neg": 1,
         "ts": _day_iso(1), "target_did": TARGET},
    ]
    _seed_and_rollup(conn, events)

    now_epoch = int(time.time())
    now_day = (now_epoch // 86400) * 86400
    start_day = ((now_epoch // 86400) - 29) * 86400
    end_day = now_day + 86400
    start_iso = _day_iso(30)

    s = _query_summary(conn, TARGET, start_day, end_day, start_iso)
    assert s["unique_posts"] == 1


# ---------------------------------------------------------------------------
# Top labelers
# ---------------------------------------------------------------------------


def test_climate_top_labelers_ordering():
    """Ordered by events DESC, labeler_did ASC."""
    conn = _make_db()
    events = [
        {"labeler_did": LABELER2, "uri": f"at://{TARGET}/app.bsky.feed.post/a",
         "val": "spam", "neg": 0, "ts": _now_iso(), "target_did": TARGET},
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/b",
         "val": "spam", "neg": 0, "ts": _now_iso(), "target_did": TARGET},
    ]
    _seed_and_rollup(conn, events)

    now_epoch = int(time.time())
    now_day = (now_epoch // 86400) * 86400
    start_day = ((now_epoch // 86400) - 29) * 86400
    end_day = now_day + 86400

    result = _query_top_labelers(conn, TARGET, start_day, end_day)
    assert len(result) == 2
    # Same event count → ordered by labeler_did ASC
    assert result[0]["labeler_did"] == LABELER1
    assert result[1]["labeler_did"] == LABELER2


def test_climate_top_labelers_enriched():
    conn = _make_db()
    _seed_and_rollup(
        conn,
        [{"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/x",
          "val": "spam", "neg": 0, "ts": _now_iso(), "target_did": TARGET}],
        [{"labeler_did": LABELER1, "handle": "lab1.test", "regime_state": "stable"}],
    )

    now_epoch = int(time.time())
    now_day = (now_epoch // 86400) * 86400
    start_day = ((now_epoch // 86400) - 29) * 86400
    end_day = now_day + 86400

    result = _query_top_labelers(conn, TARGET, start_day, end_day)
    assert result[0]["handle"] == "lab1.test"
    assert result[0]["regime_state"] == "stable"


# ---------------------------------------------------------------------------
# Top values
# ---------------------------------------------------------------------------


def test_climate_top_values():
    conn = _make_db()
    events = [
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/{i}",
         "val": "spam" if i < 3 else "porn", "neg": 0, "ts": _now_iso(),
         "target_did": TARGET}
        for i in range(5)
    ]
    _seed_events(conn, events)

    start_iso = _day_iso(30)
    result = _query_top_values(conn, TARGET, start_iso)
    assert len(result) == 2
    assert result[0]["val"] == "spam"
    assert result[0]["applies"] == 3
    assert result[1]["val"] == "porn"
    assert result[1]["applies"] == 2


def test_climate_top_values_deterministic():
    """Same count → ordered by val ASC."""
    conn = _make_db()
    events = [
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/a",
         "val": "zzz", "neg": 0, "ts": _now_iso(), "target_did": TARGET},
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/b",
         "val": "aaa", "neg": 0, "ts": _now_iso(), "target_did": TARGET},
    ]
    _seed_events(conn, events)

    result = _query_top_values(conn, TARGET, _day_iso(30))
    assert result[0]["val"] == "aaa"
    assert result[1]["val"] == "zzz"


# ---------------------------------------------------------------------------
# Daily series
# ---------------------------------------------------------------------------


def test_climate_daily_series():
    conn = _make_db()
    events = [
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/d{d}",
         "val": "spam", "neg": 0, "ts": _day_iso(d), "target_did": TARGET}
        for d in range(3)
    ]
    _seed_and_rollup(conn, events)

    now_epoch = int(time.time())
    now_day = (now_epoch // 86400) * 86400
    start_day = ((now_epoch // 86400) - 29) * 86400
    end_day = now_day + 86400

    result = _query_daily_series(conn, TARGET, start_day, end_day)
    assert len(result) >= 1  # at least some days present
    # Ordered by date
    dates = [r["date"] for r in result]
    assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# Recent receipts
# ---------------------------------------------------------------------------


def test_climate_recent_receipts_limit():
    """20 events → max 15 returned."""
    conn = _make_db()
    ts = _now_iso()
    events = [
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/{i}",
         "val": "spam", "neg": 0, "ts": ts, "target_did": TARGET}
        for i in range(20)
    ]
    _seed_events(conn, events)

    result = _query_recent_receipts(conn, TARGET, _day_iso(30), [LABELER1])
    assert len(result) == 15


def test_climate_recent_receipts_scoped():
    """Only returns events for target_did, not other authors."""
    conn = _make_db()
    ts = _now_iso()
    other_did = "did:plc:other"
    events = [
        {"labeler_did": LABELER1, "uri": f"at://{TARGET}/app.bsky.feed.post/mine",
         "val": "spam", "neg": 0, "ts": ts, "target_did": TARGET},
        {"labeler_did": LABELER1, "uri": f"at://{other_did}/app.bsky.feed.post/theirs",
         "val": "spam", "neg": 0, "ts": ts, "target_did": other_did},
    ]
    _seed_events(conn, events)

    result = _query_recent_receipts(conn, TARGET, _day_iso(30), [LABELER1])
    assert len(result) == 1
    assert result[0]["uri"].startswith(f"at://{TARGET}/")


# ---------------------------------------------------------------------------
# Week deltas
# ---------------------------------------------------------------------------


def test_climate_week_deltas():
    """Test week-over-week deltas using direct rollup inserts.

    The rollup functions only recompute the last 7 days, so we insert
    rollup rows directly to test the delta logic for both weeks.
    """
    conn = _make_db()
    now_epoch = int(time.time())
    now_day = (now_epoch // 86400) * 86400

    # This week: 3 events across days 0-2
    for d in range(3):
        day_epoch = ((now_epoch // 86400) - d) * 86400
        conn.execute(
            "INSERT INTO derived_author_day(author_did, day_epoch, events, applies, removes, labelers, targets, vals) "
            "VALUES(?, ?, 1, 1, 0, 1, 1, 1)",
            (TARGET, day_epoch),
        )
        conn.execute(
            "INSERT INTO derived_author_labeler_day(author_did, day_epoch, labeler_did, events, applies, removes, targets) "
            "VALUES(?, ?, ?, 1, 1, 0, 1)",
            (TARGET, day_epoch, LABELER1),
        )

    # Previous week: 5 events across days 7-11
    for d in range(7, 12):
        day_epoch = ((now_epoch // 86400) - d) * 86400
        conn.execute(
            "INSERT INTO derived_author_day(author_did, day_epoch, events, applies, removes, labelers, targets, vals) "
            "VALUES(?, ?, 1, 1, 0, 1, 1, 1)",
            (TARGET, day_epoch),
        )
        conn.execute(
            "INSERT INTO derived_author_labeler_day(author_did, day_epoch, labeler_did, events, applies, removes, targets) "
            "VALUES(?, ?, ?, 1, 1, 0, 1)",
            (TARGET, day_epoch, LABELER1),
        )
    conn.commit()

    result = _query_week_deltas(conn, TARGET, now_day)

    assert result["events_this_week"] == 3
    assert result["events_prev_week"] == 5
    assert result["events_delta"] == -2


# ---------------------------------------------------------------------------
# Window clamping
# ---------------------------------------------------------------------------


def test_climate_window_clamped(tmp_path):
    conn = _make_db()
    payload = generate_climate(conn, TARGET, window_days=90,
                               out_dir=str(tmp_path), fmt="json")
    # Window should be clamped to 60
    assert payload["window_days"] == 60


# ---------------------------------------------------------------------------
# URI → bsky.app link
# ---------------------------------------------------------------------------


def test_at_uri_to_bsky_link():
    uri = "at://did:plc:abc123/app.bsky.feed.post/xyz789"
    link = _at_uri_to_bsky_link(uri)
    assert "bsky.app/profile/did:plc:abc123/post/xyz789" in link
    assert "<a " in link

    # Non-post collection → escaped text
    uri2 = "at://did:plc:abc/app.bsky.feed.like/xyz"
    result = _at_uri_to_bsky_link(uri2)
    assert "<a " not in result

    # Empty/None
    assert _at_uri_to_bsky_link("") == ""
    assert _at_uri_to_bsky_link(None) == ""


# ---------------------------------------------------------------------------
# HTML escaping
# ---------------------------------------------------------------------------


def test_climate_html_escaping(tmp_path):
    """Malicious val/handle strings must be escaped in HTML output."""
    conn = _make_db()
    xss_val = '<script>alert("xss")</script>'
    xss_handle = '"><img src=x onerror=alert(1)>'
    events = [
        {"labeler_did": LABELER1,
         "uri": f"at://{TARGET}/app.bsky.feed.post/xss",
         "val": xss_val, "neg": 0, "ts": _now_iso(), "target_did": TARGET},
    ]
    _seed_and_rollup(
        conn, events,
        [{"labeler_did": LABELER1, "handle": xss_handle, "regime_state": "stable"}],
    )

    payload = generate_climate(conn, TARGET, window_days=30,
                               out_dir=str(tmp_path), fmt="html")
    html_content = (tmp_path / "climate.html").read_text()

    # Raw XSS strings must NOT appear in HTML
    assert '<script>alert("xss")</script>' not in html_content
    assert 'onerror=alert(1)>' not in html_content
    # But escaped versions should be present
    assert "&lt;script&gt;" in html_content


# ---------------------------------------------------------------------------
# Index usage (EXPLAIN QUERY PLAN)
# ---------------------------------------------------------------------------


def test_index_usage_top_values():
    """top_values query uses idx_label_events_target_did_ts."""
    conn = _make_db()
    plan = conn.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT val, SUM(CASE WHEN neg=0 THEN 1 ELSE 0 END), "
        "       SUM(CASE WHEN neg=1 THEN 1 ELSE 0 END) "
        "FROM label_events "
        "WHERE target_did = ? AND ts >= ? "
        "  AND uri LIKE 'at://%/app.bsky.feed.post/%' "
        "GROUP BY val",
        ("did:plc:test", "2025-01-01"),
    ).fetchall()
    plan_text = " ".join(str(r["detail"]) for r in plan)
    assert "idx_label_events_target_did_ts" in plan_text


def test_index_usage_recent_receipts():
    """recent_receipts query uses idx_label_events_target_did_ts."""
    conn = _make_db()
    plan = conn.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT labeler_did, uri, val, neg, ts "
        "FROM label_events "
        "WHERE target_did = ? AND ts >= ? "
        "  AND labeler_did IN (?, ?) "
        "ORDER BY ts DESC LIMIT 15",
        ("did:plc:test", "2025-01-01", "did:plc:lab1", "did:plc:lab2"),
    ).fetchall()
    plan_text = " ".join(str(r["detail"]) for r in plan)
    assert "idx_label_events_target_did_ts" in plan_text
