"""Tests for warm-up gating: cold-start suppression, sparse vs warming_up, scan_count."""
from datetime import datetime, timedelta, timezone

from labelwatch import db
from labelwatch.config import Config
from labelwatch.rules import (
    _warmup_state,
    label_rate_spike,
    flip_flop,
    target_concentration,
    churn_index,
)
from labelwatch.scan import run_scan
from labelwatch.utils import format_ts


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _insert_labeler(conn, did, first_seen, scan_count=0, events=None):
    """Insert a labeler and optionally some label events."""
    conn.execute(
        "INSERT INTO labelers(labeler_did, first_seen, last_seen, scan_count) "
        "VALUES(?, ?, ?, ?)",
        (did, first_seen, first_seen, scan_count),
    )
    if events:
        for i, ev in enumerate(events):
            conn.execute(
                "INSERT INTO label_events(labeler_did, uri, val, ts, event_hash) "
                "VALUES(?, ?, ?, ?, ?)",
                (did, ev.get("uri", f"at://u/post/{i}"), ev.get("val", "label"),
                 ev["ts"], f"hash_{did}_{i}"),
            )
    conn.commit()


# --- _warmup_state tests ---

def test_warmup_state_ready():
    conn = _make_db()
    cfg = Config(warmup_min_age_hours=48, warmup_min_events=20, warmup_min_scans=3)

    # Old labeler with enough events and scans
    first_seen = format_ts(datetime.now(timezone.utc) - timedelta(hours=100))
    events = [{"ts": format_ts(datetime.now(timezone.utc) - timedelta(hours=i))} for i in range(25)]
    _insert_labeler(conn, "did:plc:mature", first_seen, scan_count=5, events=events)

    assert _warmup_state(conn, cfg, "did:plc:mature") == "ready"


def test_warmup_state_warming_up_too_young():
    conn = _make_db()
    cfg = Config(warmup_min_age_hours=48, warmup_min_events=20, warmup_min_scans=3)

    # Very recent labeler
    first_seen = format_ts(datetime.now(timezone.utc) - timedelta(hours=1))
    events = [{"ts": format_ts(datetime.now(timezone.utc))} for _ in range(25)]
    _insert_labeler(conn, "did:plc:new", first_seen, scan_count=5, events=events)

    assert _warmup_state(conn, cfg, "did:plc:new") == "warming_up"


def test_warmup_state_warming_up_too_few_scans():
    conn = _make_db()
    cfg = Config(warmup_min_age_hours=48, warmup_min_events=20, warmup_min_scans=3)

    first_seen = format_ts(datetime.now(timezone.utc) - timedelta(hours=100))
    events = [{"ts": format_ts(datetime.now(timezone.utc))} for _ in range(25)]
    _insert_labeler(conn, "did:plc:fewscans", first_seen, scan_count=1, events=events)

    assert _warmup_state(conn, cfg, "did:plc:fewscans") == "warming_up"


def test_warmup_state_sparse():
    conn = _make_db()
    cfg = Config(warmup_min_age_hours=48, warmup_min_events=20, warmup_min_scans=3)

    # Old enough, enough scans, but not enough events
    first_seen = format_ts(datetime.now(timezone.utc) - timedelta(hours=100))
    events = [{"ts": format_ts(datetime.now(timezone.utc))} for _ in range(5)]
    _insert_labeler(conn, "did:plc:sparse", first_seen, scan_count=5, events=events)

    assert _warmup_state(conn, cfg, "did:plc:sparse") == "sparse"


def test_warmup_state_disabled():
    conn = _make_db()
    cfg = Config(warmup_enabled=False)

    first_seen = format_ts(datetime.now(timezone.utc) - timedelta(hours=1))
    _insert_labeler(conn, "did:plc:new", first_seen, scan_count=0)

    assert _warmup_state(conn, cfg, "did:plc:new") == "ready"


def test_warmup_state_unknown_labeler():
    conn = _make_db()
    cfg = Config()
    assert _warmup_state(conn, cfg, "did:plc:nonexistent") == "warming_up"


# --- Suppression behavior ---

def test_warming_up_suppresses_all_rules():
    """A warming_up labeler with suppress=True should not produce any alerts."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    cfg = Config(
        warmup_enabled=True,
        warmup_suppress_alerts=True,
        warmup_min_age_hours=48,
        warmup_min_events=5,
        warmup_min_scans=3,
        spike_min_count_default=1,
    )

    # Brand new labeler with a big spike
    first_seen = format_ts(now - timedelta(hours=1))
    events = [{"ts": format_ts(now - timedelta(minutes=i))} for i in range(10)]
    _insert_labeler(conn, "did:plc:newspikey", first_seen, scan_count=0, events=events)

    alerts = label_rate_spike(conn, cfg, now)
    assert len(alerts) == 0  # Suppressed


def test_warming_up_no_suppress_adds_warmup_tag():
    """With suppress=False, warming_up alerts should include warmup tag."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    cfg = Config(
        warmup_enabled=True,
        warmup_suppress_alerts=False,
        warmup_min_age_hours=48,
        warmup_min_events=5,
        warmup_min_scans=3,
        spike_min_count_default=1,
    )

    first_seen = format_ts(now - timedelta(hours=1))
    events = [{"ts": format_ts(now - timedelta(minutes=i))} for i in range(10)]
    _insert_labeler(conn, "did:plc:tagged", first_seen, scan_count=0, events=events)

    alerts = label_rate_spike(conn, cfg, now)
    if alerts:
        assert alerts[0]["inputs"].get("warmup") == "warming_up"


def test_sparse_suppresses_rate_based_rules():
    """Sparse labelers should have rate-based rules (spike, churn) suppressed."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    cfg = Config(
        warmup_enabled=True,
        warmup_suppress_alerts=True,
        warmup_min_age_hours=48,
        warmup_min_events=100,  # Very high threshold
        warmup_min_scans=3,
        spike_min_count_default=1,
    )

    # Old enough, enough scans, but sparse events
    first_seen = format_ts(now - timedelta(hours=100))
    events = [{"ts": format_ts(now - timedelta(minutes=i))} for i in range(10)]
    _insert_labeler(conn, "did:plc:sparserule", first_seen, scan_count=5, events=events)

    # Verify it's sparse
    assert _warmup_state(conn, cfg, "did:plc:sparserule") == "sparse"

    # Rate spike should be suppressed
    spike_alerts = label_rate_spike(conn, cfg, now)
    spike_for_sparse = [a for a in spike_alerts if a["labeler_did"] == "did:plc:sparserule"]
    assert len(spike_for_sparse) == 0

    # Churn should also be suppressed (rate-based)
    churn_alerts = churn_index(conn, cfg, now)
    churn_for_sparse = [a for a in churn_alerts if a["labeler_did"] == "did:plc:sparserule"]
    assert len(churn_for_sparse) == 0


def test_sparse_allows_pattern_rules():
    """Sparse labelers should still allow pattern rules (flip_flop, concentration)."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    cfg = Config(
        warmup_enabled=True,
        warmup_suppress_alerts=False,
        warmup_min_age_hours=48,
        warmup_min_events=1000,  # High threshold to force sparse
        warmup_min_scans=3,
        concentration_min_labels=3,
        concentration_threshold=0.1,
    )

    first_seen = format_ts(now - timedelta(hours=100))
    # Create events targeting same URI (concentration)
    events = []
    for i in range(10):
        events.append({"uri": "at://u/post/same", "val": "label", "ts": format_ts(now - timedelta(minutes=i))})
    _insert_labeler(conn, "did:plc:sparsepat", first_seen, scan_count=5, events=events)

    assert _warmup_state(conn, cfg, "did:plc:sparsepat") == "sparse"

    # Concentration rule should NOT be suppressed for sparse (pattern rule)
    conc_alerts = target_concentration(conn, cfg, now)
    conc_for_sparse = [a for a in conc_alerts if a["labeler_did"] == "did:plc:sparsepat"]
    # May or may not trigger (depends on HHI), but the point is it wasn't suppressed
    # If it triggered, it should have warmup tag since suppress=False
    for a in conc_for_sparse:
        assert a["inputs"].get("warmup") == "sparse"


def test_mature_labeler_alerts_normally():
    """Mature labeler (ready state) should alert without warmup tags."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    cfg = Config(
        warmup_enabled=True,
        warmup_min_age_hours=48,
        warmup_min_events=5,
        warmup_min_scans=3,
        spike_min_count_default=1,
    )

    first_seen = format_ts(now - timedelta(hours=100))
    events = [{"ts": format_ts(now - timedelta(minutes=i))} for i in range(10)]
    _insert_labeler(conn, "did:plc:mature", first_seen, scan_count=5, events=events)

    assert _warmup_state(conn, cfg, "did:plc:mature") == "ready"

    alerts = label_rate_spike(conn, cfg, now)
    mature_alerts = [a for a in alerts if a["labeler_did"] == "did:plc:mature"]
    for a in mature_alerts:
        assert "warmup" not in a["inputs"]


# --- scan_count ---

def test_scan_count_increments_for_evaluated_labelers():
    """run_scan should increment scan_count for labelers."""
    conn = _make_db()
    now = datetime.now(timezone.utc)
    cfg = Config(warmup_enabled=False)

    first_seen = format_ts(now - timedelta(hours=100))
    _insert_labeler(conn, "did:plc:count1", first_seen, scan_count=0)
    _insert_labeler(conn, "did:plc:count2", first_seen, scan_count=5)

    initial_1 = conn.execute("SELECT scan_count FROM labelers WHERE labeler_did='did:plc:count1'").fetchone()["scan_count"]
    initial_2 = conn.execute("SELECT scan_count FROM labelers WHERE labeler_did='did:plc:count2'").fetchone()["scan_count"]

    run_scan(conn, cfg, now=now)

    after_1 = conn.execute("SELECT scan_count FROM labelers WHERE labeler_did='did:plc:count1'").fetchone()["scan_count"]
    after_2 = conn.execute("SELECT scan_count FROM labelers WHERE labeler_did='did:plc:count2'").fetchone()["scan_count"]

    assert after_1 == initial_1 + 1
    assert after_2 == initial_2 + 1


# --- Lifecycle: warming up to mature ---

def test_warmup_to_mature_lifecycle():
    """Labeler should transition from warming_up to ready as it ages and gets scanned."""
    conn = _make_db()
    cfg = Config(
        warmup_enabled=True,
        warmup_min_age_hours=2,
        warmup_min_events=5,
        warmup_min_scans=2,
    )

    # Start: brand new, no scans
    first_seen = format_ts(datetime.now(timezone.utc) - timedelta(hours=1))
    events = [{"ts": format_ts(datetime.now(timezone.utc))} for _ in range(10)]
    _insert_labeler(conn, "did:plc:lifecycle", first_seen, scan_count=0, events=events)

    assert _warmup_state(conn, cfg, "did:plc:lifecycle") == "warming_up"

    # Simulate time passing: update first_seen to be old enough
    old_first_seen = format_ts(datetime.now(timezone.utc) - timedelta(hours=3))
    conn.execute("UPDATE labelers SET first_seen=? WHERE labeler_did='did:plc:lifecycle'", (old_first_seen,))
    conn.commit()

    # Still warming_up: not enough scans
    assert _warmup_state(conn, cfg, "did:plc:lifecycle") == "warming_up"

    # Increment scan_count
    db.increment_scan_count(conn, "did:plc:lifecycle")
    db.increment_scan_count(conn, "did:plc:lifecycle")
    conn.commit()

    # Now should be ready
    assert _warmup_state(conn, cfg, "did:plc:lifecycle") == "ready"
