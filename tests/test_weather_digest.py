"""Tests for labelwatch.weather_digest.v0 — weekly network-weather digest."""

from __future__ import annotations

import json
import sqlite3

import pytest

from labelwatch import db, weather_digest


def _seed_db(path: str):
    """Seed: 1 new labeler (this week), 1 went-dark, 1 high-volume current.
    Tweaked so each section of the digest has data to render."""
    conn = db.connect(path)
    db.init_db(conn)
    # New labeler: first_seen yesterday, has 7d events.
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, display_name, description, "
        "first_seen, last_seen, events_7d, events_30d, regime_state, "
        "endpoint_status) VALUES (?,?,?,?,?,?,?,?,?,?)",
        ("did:plc:newone", "newcomer.test", "Newcomer Labeler",
         "An emerging labeler that just declared itself.",
         "2026-06-09T00:00:00Z", "2026-06-09T12:00:00Z",
         42, 42, "warming_up", "ok"),
    )
    # Went-dark labeler: 30d events but zero 7d, last seen 10 days ago.
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, first_seen, last_seen, "
        "events_7d, events_30d, regime_state, endpoint_status) "
        "VALUES (?,?,?,?,?,?,?,?)",
        ("did:plc:darkone", "going-dark.test",
         "2026-04-01T00:00:00Z", "2026-05-30T00:00:00Z",
         0, 500, "dark_operational", "down"),
    )
    # High-volume current labeler: lots of 7d events, with description.
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, description, first_seen, "
        "last_seen, events_7d, events_30d, regime_state, endpoint_status) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        ("did:plc:loud", "loud-labeler.test",
         "A labeler that emits a lot of labels every week.",
         "2026-01-01T00:00:00Z", "2026-06-10T00:00:00Z",
         50000, 200000, "stable", "ok"),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Section queries
# ---------------------------------------------------------------------------

def test_new_labelers_picks_recent(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p)
    conn = db.connect(p, readonly=True)
    try:
        nl = weather_digest.new_labelers(conn, "2026-06-08T00:00:00Z")
    finally:
        conn.close()
    handles = [n["handle"] for n in nl]
    assert "newcomer.test" in handles
    # Others first_seen earlier, should not appear.
    assert "going-dark.test" not in handles
    assert "loud-labeler.test" not in handles


def test_went_dark_finds_dark_labelers(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p)
    conn = db.connect(p, readonly=True)
    try:
        dark = weather_digest.went_dark(conn)
    finally:
        conn.close()
    handles = [d["handle"] for d in dark]
    assert "going-dark.test" in handles
    # newcomer has events_7d > 0, loud has events_7d > 0 → not dark.
    assert "newcomer.test" not in handles
    assert "loud-labeler.test" not in handles


def test_notable_concentrations_picks_high_volume(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p)
    conn = db.connect(p, readonly=True)
    try:
        nc = weather_digest.notable_concentrations(conn)
    finally:
        conn.close()
    handles = [c["handle"] for c in nc]
    # Loud labeler has events_7d=50000 — should appear.
    assert "loud-labeler.test" in handles
    # Newcomer events_7d=42 — below the 1000 threshold.
    assert "newcomer.test" not in handles


# ---------------------------------------------------------------------------
# Digest assembly
# ---------------------------------------------------------------------------

def test_build_digest_full_shape(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p)
    conn = db.connect(p, readonly=True)
    try:
        digest = weather_digest.build_digest(conn)
    finally:
        conn.close()
    required = {
        "receipt_kind",
        "receipt_schema_version",
        "generated_at",
        "window_days",
        "window_start",
        "weather",
        "new_labelers",
        "went_dark",
        "notable_concentrations",
        "receipt_hash",
    }
    assert required.issubset(set(digest.keys()))
    assert digest["receipt_kind"] == "labelwatch.weather_digest.v0"
    # Weather has the small-table summary.
    for k in ("signals", "total_labelers", "emitting_this_week",
              "events_7d_total", "unreachable"):
        assert k in digest["weather"]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def test_render_text_runs(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p)
    conn = db.connect(p, readonly=True)
    try:
        digest = weather_digest.build_digest(conn)
    finally:
        conn.close()
    text = weather_digest.render_text(digest)
    assert "Labelwatch network weather" in text
    assert "New labelers" in text
    assert "Went dark" in text
    assert "Notable concentrations" in text
    # The seeded description is quoted.
    assert "An emerging labeler" in text
    # Locus-honest framing: descriptive language only.
    forbidden = ("trust score", "risk score", "moderation recommendation",
                 "should be moderated")
    for phrase in forbidden:
        assert phrase not in text.lower()


def test_render_bluesky_under_300_chars(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p)
    conn = db.connect(p, readonly=True)
    try:
        digest = weather_digest.build_digest(conn)
    finally:
        conn.close()
    post = weather_digest.render_bluesky(digest)
    assert len(post) <= 300, f"bluesky post too long: {len(post)} chars"
    # Must contain the network-weather signal vocabulary.
    assert "Labelwatch weather:" in post
    # Must NOT contain adjudication vocabulary.
    for phrase in ("trust score", "risk score", "recommended action"):
        assert phrase not in post.lower()


def test_render_json_round_trips(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p)
    conn = db.connect(p, readonly=True)
    try:
        digest = weather_digest.build_digest(conn)
    finally:
        conn.close()
    rendered = weather_digest.render_json(digest)
    parsed = json.loads(rendered)
    assert parsed["receipt_kind"] == "labelwatch.weather_digest.v0"
    assert parsed["receipt_hash"] == digest["receipt_hash"]


def test_description_trim_caps_quote_length():
    long_text = "A" * 500
    trimmed = weather_digest._trim_description(long_text)
    assert trimmed is not None
    assert len(trimmed) <= weather_digest.MAX_DESCRIPTION_QUOTE_CHARS
    assert trimmed.endswith("…")


def test_description_trim_handles_empty_inputs():
    assert weather_digest._trim_description(None) is None
    assert weather_digest._trim_description("") is None
    assert weather_digest._trim_description("short text") == "short text"
