import json
from datetime import datetime, timezone

from labelwatch import db, ingest, scan
from labelwatch.config import Config
from labelwatch.rules import label_rate_spike
from labelwatch.utils import format_ts


FIXTURE = "tests/fixtures/synthetic_streams.jsonl"


def test_label_rate_spike_triggers():
    conn = db.connect(":memory:")
    db.init_db(conn)
    ingest.ingest_from_fixture(conn, FIXTURE)

    cfg = Config(
        window_minutes=15,
        baseline_hours=24,
        spike_k=5.0,
        min_current_count=10,
        warmup_enabled=False,
    )
    now = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    total = scan.run_scan(conn, cfg, now=now)

    rows = conn.execute("SELECT * FROM alerts WHERE rule_id='label_rate_spike'").fetchall()
    assert total >= 1
    assert len(rows) == 1
    assert rows[0]["labeler_did"] == "did:plc:labelerA"


def _make_spike_db(labeler_did, is_reference=False, event_count=10):
    """Create a DB with a labeler that has events only in the current window (no baseline)."""
    from datetime import timedelta
    conn = db.connect(":memory:")
    db.init_db(conn)
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    now_ts = format_ts(now)

    # Insert labeler with is_reference flag
    conn.execute(
        "INSERT INTO labelers(labeler_did, is_reference, first_seen, last_seen) VALUES(?, ?, ?, ?)",
        (labeler_did, 1 if is_reference else 0, now_ts, now_ts),
    )
    # Insert events in the current 15-minute window (before now, exclusive of now)
    for i in range(event_count):
        # Spread events across minutes 1-14 within the window
        offset_min = 1 + (i % 14)
        ts = format_ts(now - timedelta(minutes=offset_min))
        eh = f"hash_{labeler_did}_{i}"
        conn.execute(
            "INSERT OR IGNORE INTO label_events(labeler_did, uri, val, ts, event_hash) VALUES(?, ?, ?, ?, ?)",
            (labeler_did, f"at://{labeler_did}/post/{i}", "test", ts, eh),
        )
    conn.commit()
    return conn, now


def test_spike_two_tier_reference_needs_50():
    """Reference labeler with zero baseline needs spike_min_count_reference (50) events to trigger."""
    conn, now = _make_spike_db("did:plc:ref", is_reference=True, event_count=10)
    cfg = Config(
        spike_min_count_reference=50,
        spike_min_count_default=5,
        spike_k=10.0,
        warmup_enabled=False,
    )
    alerts = label_rate_spike(conn, cfg, now)
    # Only 10 events, reference threshold is 50 -> should NOT trigger
    assert len(alerts) == 0


def test_spike_two_tier_reference_triggers_at_threshold():
    """Reference labeler triggers when count >= spike_min_count_reference."""
    conn, now = _make_spike_db("did:plc:ref", is_reference=True, event_count=50)
    cfg = Config(
        spike_min_count_reference=50,
        spike_min_count_default=5,
        spike_k=10.0,
        warmup_enabled=False,
    )
    alerts = label_rate_spike(conn, cfg, now)
    assert len(alerts) == 1
    assert alerts[0]["inputs"]["is_reference"] is True
    assert alerts[0]["inputs"]["min_current_count_used"] == 50


def test_spike_two_tier_default_triggers_at_5():
    """Non-reference labeler triggers at spike_min_count_default (5)."""
    conn, now = _make_spike_db("did:plc:community", is_reference=False, event_count=10)
    cfg = Config(
        spike_min_count_reference=50,
        spike_min_count_default=5,
        spike_k=10.0,
        warmup_enabled=False,
    )
    alerts = label_rate_spike(conn, cfg, now)
    assert len(alerts) == 1
    assert alerts[0]["inputs"]["is_reference"] is False
    assert alerts[0]["inputs"]["min_current_count_used"] == 5


def test_spike_confidence_tag_present():
    """Alert inputs include a confidence tag."""
    conn, now = _make_spike_db("did:plc:new", is_reference=False, event_count=10)
    cfg = Config(
        spike_min_count_default=5,
        spike_k=10.0,
        confidence_min_events=100,
        confidence_min_age_hours=168,
        warmup_enabled=False,
    )
    alerts = label_rate_spike(conn, cfg, now)
    assert len(alerts) == 1
    # Only 10 events, just created -> low confidence
    assert alerts[0]["inputs"]["confidence"] == "low"
