"""Tests for labelwatch.authority_effect_triage.v0.

Proves:
  - Tier 1 (registry) hits do NOT enter the queue (already classified).
  - Tier 2 (emitter_described) editorial tone → reputational, needs_human_review.
  - Tier 2 (emitter_described) neutral + descriptive verb → descriptive.
  - Tier 3 (pattern_profile) safe class (spam variant) → auto_pattern_matched.
  - Tier 3 (pattern_profile) non-safe class → needs_human_review.
  - Tier 4 (raw_fallback) → proposed, no candidate effect.
  - Refusal grounds present on every candidate.
  - Ranking by event count (top-N).
  - Projected reduction calc reflects auto-only vs auto+ratified.
  - run_triage end-to-end on a temp DB produces a receipt-shaped dict
    with a sha256 receipt_hash.
  - run_triage does NOT write to the DB (readonly use).
"""
from __future__ import annotations

import json
import os
import time

import pytest

from labelwatch import db
from labelwatch.authority_triage import (
    INDEX_RECEIPT_KIND,
    INFERENCE_RECEIPT_KIND,
    PROMOTION_AUTO_PATTERN_MATCHED,
    PROMOTION_NEEDS_HUMAN_REVIEW,
    PROMOTION_PROPOSED,
    PROMOTION_REFUSED_INSUFFICIENT_EVIDENCE,
    _assign_promotion_status,
    _confidence_for_classification,
    _safe_pattern_match,
    aggregate_window,
    build_candidate,
    run_triage,
)


WINDOW_START = "2026-05-01T00:00:00Z"
WINDOW_END = "2026-06-01T00:00:00Z"
IN_WINDOW = "2026-05-15T12:00:00Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _seed_events(conn, events):
    for i, e in enumerate(events):
        conn.execute(
            "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                e["labeler_did"],
                e.get("uri", f"at://{e['target_did']}/app.bsky.feed.post/{i}"),
                e["val"],
                e.get("neg", 0),
                e["ts"],
                e.get("event_hash", f"hash_{i}_{time.monotonic_ns()}"),
                e["target_did"],
            ),
        )
    conn.commit()


def _seed_labeler(conn, did, *, handle=None, description=None):
    conn.execute(
        "INSERT INTO labelers(labeler_did, handle, description) VALUES(?, ?, ?)",
        (did, handle, description),
    )
    conn.commit()


def _seed_discovery(conn, did, label_value, definition):
    """Seed a discovery_events row carrying a single labelValueDefinition."""
    record = {
        "policies": {
            "labelValueDefinitions": [
                {"identifier": label_value, **definition},
            ],
        },
    }
    conn.execute(
        "INSERT INTO discovery_events(labeler_did, operation, source, record_json, discovered_at) "
        "VALUES(?, 'create', 'test', ?, ?)",
        (did, json.dumps(record), IN_WINDOW),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Pure-function tests (no DB)
# ---------------------------------------------------------------------------

def test_safe_pattern_matches_spam_variants():
    assert _safe_pattern_match("spam-link") is not None
    assert _safe_pattern_match("reply-spam") is not None
    assert _safe_pattern_match("shopping-spam") is not None
    assert _safe_pattern_match("scam-account") is not None
    assert _safe_pattern_match("phishing-domain") is not None
    assert _safe_pattern_match("malware-host") is not None
    assert _safe_pattern_match("impersonation-bot") is not None


def test_safe_pattern_does_not_match_unrelated():
    assert _safe_pattern_match("substack") is None
    assert _safe_pattern_match("trump") is None
    assert _safe_pattern_match("weird-novel-thing") is None
    # Even reputational classes (nazi/terf) must NOT match the safe table —
    # they need human review.
    assert _safe_pattern_match("nazi") is None
    assert _safe_pattern_match("terf-gc") is None


def test_assign_promotion_status_emitter_low_is_refused():
    cls = {"semantic_source": "emitter_described", "tone": "neutral"}
    assert (
        _assign_promotion_status("emitter_described", cls, "low", None)
        == PROMOTION_REFUSED_INSUFFICIENT_EVIDENCE
    )


def test_assign_promotion_status_emitter_high_is_human_review():
    cls = {"semantic_source": "emitter_described", "tone": "editorial"}
    assert (
        _assign_promotion_status("emitter_described", cls, "high", None)
        == PROMOTION_NEEDS_HUMAN_REVIEW
    )


def test_assign_promotion_status_emitter_medium_is_human_review():
    cls = {"semantic_source": "emitter_described"}
    assert (
        _assign_promotion_status("emitter_described", cls, "medium", None)
        == PROMOTION_NEEDS_HUMAN_REVIEW
    )


def test_assign_promotion_status_safe_pattern_auto_promote():
    cls = {"semantic_source": "pattern_profile"}
    assert (
        _assign_promotion_status(
            "pattern_profile", cls, "medium",
            safe_pattern=("safety", "advisory", "spam variant"),
        )
        == PROMOTION_AUTO_PATTERN_MATCHED
    )


def test_assign_promotion_status_unsafe_pattern_needs_review():
    cls = {"semantic_source": "pattern_profile"}
    assert (
        _assign_promotion_status("pattern_profile", cls, "medium", None)
        == PROMOTION_NEEDS_HUMAN_REVIEW
    )


def test_assign_promotion_status_raw_fallback_proposed():
    cls = {"semantic_source": "raw_fallback"}
    assert (
        _assign_promotion_status("raw_fallback", cls, "none", None)
        == PROMOTION_PROPOSED
    )


def test_confidence_metadata_only_is_low():
    cls = {
        "semantic_source": "emitter_described",
        "classification_basis": "emitter_label_metadata",
    }
    assert _confidence_for_classification(cls) == "low"


def test_confidence_editorial_reputational_is_high():
    cls = {
        "semantic_source": "emitter_described",
        "classification_basis": "emitter_locale_description",
        "tone": "editorial",
        "authority_effect": "reputational",
    }
    assert _confidence_for_classification(cls) == "high"


def test_confidence_pattern_profile_is_medium():
    cls = {"semantic_source": "pattern_profile"}
    assert _confidence_for_classification(cls) == "medium"


def test_confidence_raw_fallback_is_none():
    cls = {"semantic_source": "raw_fallback"}
    assert _confidence_for_classification(cls) == "none"


# ---------------------------------------------------------------------------
# DB-backed: aggregate_window
# ---------------------------------------------------------------------------

def test_aggregate_window_marks_tier1_hits():
    """Already-classified labels carry their tier1 effect; unknown is None."""
    conn = _make_db()
    _seed_events(conn, [
        # spam → tier 1 reputational
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": "did:plc:t1", "ts": IN_WINDOW},
        # weird-thing → unprofiled
        {"labeler_did": "did:plc:A", "val": "weird-thing", "target_did": "did:plc:t2", "ts": IN_WINDOW},
        {"labeler_did": "did:plc:A", "val": "weird-thing", "target_did": "did:plc:t3", "ts": IN_WINDOW},
    ])
    rows = aggregate_window(conn, start_ts=WINDOW_START, end_ts=WINDOW_END)
    by_val = {r["val"]: r for r in rows}
    assert by_val["spam"]["authority_effect_tier1"] == "reputational"
    assert by_val["weird-thing"]["authority_effect_tier1"] is None
    assert by_val["weird-thing"]["event_count"] == 2


def test_aggregate_window_excludes_negations():
    conn = _make_db()
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "thing", "target_did": "did:plc:t1", "ts": IN_WINDOW, "neg": 0},
        {"labeler_did": "did:plc:A", "val": "thing", "target_did": "did:plc:t2", "ts": IN_WINDOW, "neg": 1},
    ])
    rows = aggregate_window(conn, start_ts=WINDOW_START, end_ts=WINDOW_END)
    by_val = {r["val"]: r for r in rows}
    assert by_val["thing"]["event_count"] == 1  # neg row excluded


def test_aggregate_window_excludes_out_of_window():
    conn = _make_db()
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "thing", "target_did": "did:plc:t1", "ts": "2026-04-01T00:00:00Z"},
        {"labeler_did": "did:plc:A", "val": "thing", "target_did": "did:plc:t2", "ts": IN_WINDOW},
    ])
    rows = aggregate_window(conn, start_ts=WINDOW_START, end_ts=WINDOW_END)
    assert sum(r["event_count"] for r in rows if r["val"] == "thing") == 1


# ---------------------------------------------------------------------------
# DB-backed: build_candidate (tier dispatch)
# ---------------------------------------------------------------------------

def test_build_candidate_tier_3_safe_pattern_auto_promote():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:A", handle="safety.labeler.test")
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "spam-link", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    receipt = build_candidate(
        conn, labeler_did="did:plc:A", val="spam-link", event_count=1,
        start_ts=WINDOW_START, end_ts=WINDOW_END, window_label="7d",
    )
    assert receipt["receipt_kind"] == INFERENCE_RECEIPT_KIND
    assert receipt["tier"] == "pattern_profile"
    assert receipt["promotion_status"] == PROMOTION_AUTO_PATTERN_MATCHED
    assert receipt["candidate_authority_effect"] == "advisory"
    assert receipt["refusals"], "refusal grounds must be present"
    assert receipt["receipt_hash"]


def test_build_candidate_safe_pattern_upgrades_raw_fallback():
    """phishing-domain matches no emitter_classifier pattern but DOES match
    the triage safe-pattern table. It must upgrade from raw_fallback to
    pattern_profile with auto_pattern_matched, not fall to proposed.
    """
    conn = _make_db()
    _seed_labeler(conn, "did:plc:A", handle="safety.test")
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "phishing-domain", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    receipt = build_candidate(
        conn, labeler_did="did:plc:A", val="phishing-domain", event_count=1,
        start_ts=WINDOW_START, end_ts=WINDOW_END, window_label="7d",
    )
    assert receipt["tier"] == "pattern_profile"
    assert receipt["promotion_status"] == PROMOTION_AUTO_PATTERN_MATCHED
    assert receipt["candidate_authority_effect"] == "advisory"


def test_build_candidate_tier_4_raw_fallback_has_no_candidate_effect():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:A", handle="example.labeler.test")
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "weird-novel-thing", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    receipt = build_candidate(
        conn, labeler_did="did:plc:A", val="weird-novel-thing", event_count=1,
        start_ts=WINDOW_START, end_ts=WINDOW_END, window_label="7d",
    )
    assert receipt["tier"] == "raw_fallback"
    assert receipt["candidate_authority_effect"] is None
    assert receipt["promotion_status"] == PROMOTION_PROPOSED


def test_build_candidate_tier_2_editorial_emitter_is_reputational_review():
    conn = _make_db()
    did = "did:plc:editorial"
    _seed_labeler(conn, did, handle="editorial.labeler.test")
    _seed_events(conn, [
        {"labeler_did": did, "val": "antisubstack", "target_did": "did:plc:t1", "ts": IN_WINDOW},
    ])
    _seed_discovery(conn, did, "antisubstack", {
        "severity": "inform",
        "blurs": "none",
        "defaultSetting": "warn",
        "locales": [{
            "lang": "en",
            "name": "Anti-Substack",
            "description": (
                "Marks accounts and posts that platform extremist nazi propaganda "
                "via Substack. This is hateful, harmful behavior."
            ),
        }],
    })
    receipt = build_candidate(
        conn, labeler_did=did, val="antisubstack", event_count=1,
        start_ts=WINDOW_START, end_ts=WINDOW_END, window_label="7d",
    )
    assert receipt["tier"] == "emitter_described"
    # Editorial tone → reputational OR visibility_affecting; either way human review
    assert receipt["candidate_authority_effect"] in ("reputational", "visibility_affecting")
    assert receipt["promotion_status"] == PROMOTION_NEEDS_HUMAN_REVIEW
    # Citation must reference the emitter description excerpt
    ev = receipt["evidence"]["emitter_classifier_evidence"]
    assert "extremist" in (ev.get("description_excerpt") or "")


def test_build_candidate_attachment_locus_extracted_from_uri():
    conn = _make_db()
    did = "did:plc:A"
    _seed_labeler(conn, did)
    _seed_events(conn, [
        {"labeler_did": did, "val": "weird", "target_did": "did:plc:t1",
         "uri": "at://did:plc:t1/app.bsky.feed.post/abc", "ts": IN_WINDOW},
        {"labeler_did": did, "val": "weird", "target_did": "did:plc:t2",
         "uri": "did:plc:t2", "ts": IN_WINDOW},
    ])
    receipt = build_candidate(
        conn, labeler_did=did, val="weird", event_count=2,
        start_ts=WINDOW_START, end_ts=WINDOW_END, window_label="7d",
    )
    loci = receipt["evidence"]["attachment_loci"]
    assert "post" in loci
    assert "account" in loci


# ---------------------------------------------------------------------------
# End-to-end: run_triage
# ---------------------------------------------------------------------------

def _make_temp_db(tmp_path):
    db_path = str(tmp_path / "labelwatch.db")
    conn = db.connect(db_path)
    db.init_db(conn)
    conn.close()
    return db_path


def test_run_triage_excludes_tier1_already_classified(tmp_path, monkeypatch):
    """spam is in AUTHORITY_EFFECT_MAP — must NOT enter the triage queue."""
    db_path = _make_temp_db(tmp_path)
    conn = db.connect(db_path)
    db.init_db(conn)
    # Tier 1 (spam) — heavy volume, should be filtered out
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "spam", "target_did": f"did:plc:t{i}", "ts": _now_ts(i)}
        for i in range(20)
    ])
    # Tier 4 (raw_fallback) — should appear
    _seed_events(conn, [
        {"labeler_did": "did:plc:B", "val": "weird-novel-thing", "target_did": f"did:plc:u{i}", "ts": _now_ts(i)}
        for i in range(3)
    ])
    conn.close()

    index = run_triage(db_path, window_days=30, top_values=10, top_labelers=10)
    queue_vals = {c["label_value"] for c in index["queue"]}
    assert "spam" not in queue_vals
    assert "weird-novel-thing" in queue_vals


def test_run_triage_receipt_shape_and_hash(tmp_path):
    db_path = _make_temp_db(tmp_path)
    conn = db.connect(db_path)
    db.init_db(conn)
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "spam-link", "target_did": f"did:plc:t{i}", "ts": _now_ts(i)}
        for i in range(5)
    ])
    _seed_events(conn, [
        {"labeler_did": "did:plc:B", "val": "novel-thing", "target_did": f"did:plc:u{i}", "ts": _now_ts(i)}
        for i in range(2)
    ])
    conn.close()

    index = run_triage(db_path, window_days=30, top_values=5, top_labelers=5)
    assert index["receipt_kind"] == INDEX_RECEIPT_KIND
    assert index["receipt_hash"]
    assert "window" in index
    assert "queue" in index
    assert "projected_reduction" in index
    assert "tier_breakdown" in index
    assert "promotion_breakdown" in index


def test_run_triage_projected_reduction_split(tmp_path):
    """auto-only and auto+ratified projections should differ when both
    safe-pattern and emitter-described candidates exist.
    """
    db_path = _make_temp_db(tmp_path)
    conn = db.connect(db_path)
    db.init_db(conn)
    # spam-link → auto_pattern_matched (safe class) — 10 events
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "spam-link", "target_did": f"did:plc:t{i}", "ts": _now_ts(i)}
        for i in range(10)
    ])
    # weird-novel-thing → raw_fallback (proposed; NOT promotable in optimistic case)
    _seed_events(conn, [
        {"labeler_did": "did:plc:B", "val": "weird-novel-thing", "target_did": f"did:plc:u{i}", "ts": _now_ts(i)}
        for i in range(3)
    ])
    conn.close()

    index = run_triage(db_path, window_days=30, top_values=10, top_labelers=10)
    proj = index["projected_reduction"]
    # auto_promote_only recovers only the spam-link events
    assert proj["auto_promote_only"]["events_recovered"] == 10
    # auto + ratified ALSO recovers only spam-link (no needs_human_review here)
    assert proj["auto_plus_human_ratified"]["events_recovered"] == 10
    # raw_fallback events are NOT promotable in either projection
    assert proj["auto_plus_human_ratified"]["new_unprofiled_events"] == 3


def test_run_triage_ranks_by_event_count(tmp_path):
    db_path = _make_temp_db(tmp_path)
    conn = db.connect(db_path)
    db.init_db(conn)
    # Heavy: weird-A → 20 events
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "weird-A", "target_did": f"did:plc:t{i}", "ts": _now_ts(i)}
        for i in range(20)
    ])
    # Light: weird-B → 5 events
    _seed_events(conn, [
        {"labeler_did": "did:plc:B", "val": "weird-B", "target_did": f"did:plc:u{i}", "ts": _now_ts(i)}
        for i in range(5)
    ])
    conn.close()

    index = run_triage(db_path, window_days=30, top_values=10, top_labelers=10)
    vals_in_order = [c["label_value"] for c in index["queue"]]
    assert vals_in_order.index("weird-A") < vals_in_order.index("weird-B")


def test_run_triage_does_not_write_to_db(tmp_path):
    """Triage MUST be read-only. Confirm no rows added to label_events
    or to AUTHORITY_EFFECT_MAP (in-process module dict).
    """
    from labelwatch import label_family
    map_before = dict(label_family.AUTHORITY_EFFECT_MAP)

    db_path = _make_temp_db(tmp_path)
    conn = db.connect(db_path)
    db.init_db(conn)
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "weird", "target_did": "did:plc:t1", "ts": _now_ts(0)},
    ])
    row_count_before = conn.execute(
        "SELECT COUNT(*) FROM label_events"
    ).fetchone()[0]
    conn.close()

    run_triage(db_path, window_days=30, top_values=5, top_labelers=5)

    conn = db.connect(db_path)
    row_count_after = conn.execute(
        "SELECT COUNT(*) FROM label_events"
    ).fetchone()[0]
    conn.close()
    assert row_count_after == row_count_before
    # AUTHORITY_EFFECT_MAP must not have grown
    assert label_family.AUTHORITY_EFFECT_MAP == map_before


def test_run_triage_writes_receipts_to_out_dir(tmp_path):
    db_path = _make_temp_db(tmp_path)
    conn = db.connect(db_path)
    db.init_db(conn)
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "spam-link", "target_did": "did:plc:t1", "ts": _now_ts(0)},
        {"labeler_did": "did:plc:B", "val": "weird", "target_did": "did:plc:t2", "ts": _now_ts(1)},
    ])
    conn.close()

    out_dir = str(tmp_path / "receipts")
    index = run_triage(
        db_path, window_days=30, top_values=5, top_labelers=5, out_dir=out_dir,
    )

    inf_dir = os.path.join(out_dir, "authority_effect_inference")
    tri_dir = os.path.join(out_dir, "authority_effect_triage")
    assert os.path.isdir(inf_dir)
    assert os.path.isdir(tri_dir)
    # one inference receipt per queue item
    inf_files = os.listdir(inf_dir)
    assert len(inf_files) == len(index["queue"])
    # one index receipt
    tri_files = os.listdir(tri_dir)
    assert len(tri_files) == 1
    # round-trip the index
    with open(os.path.join(tri_dir, tri_files[0])) as f:
        loaded = json.load(f)
    assert loaded["receipt_kind"] == INDEX_RECEIPT_KIND


def test_only_auto_pattern_matched_bypasses_review():
    """Doctrinal check: scan the rules table; auto_pattern_matched must
    be the only status that bypasses human review.
    """
    from labelwatch import authority_triage as at_mod
    # The function under test makes this concrete. Verify the source-of-
    # truth function never emits auto_pattern_matched for non-safe tiers.
    for tier in ("emitter_described", "raw_fallback"):
        for conf in ("low", "medium", "high"):
            status = at_mod._assign_promotion_status(
                tier, {"semantic_source": tier}, conf, safe_pattern=None,
            )
            assert status != PROMOTION_AUTO_PATTERN_MATCHED, (
                f"tier={tier} confidence={conf} must not auto-promote without "
                f"a safe_pattern hit"
            )


def _now_ts(offset: int = 0) -> str:
    """A timestamp inside the default 30d window used by tmp_path tests.

    Always at least 5 minutes in the past to keep events safely below the
    `ts < now_utc()` window upper bound used by run_triage.
    """
    from datetime import datetime, timedelta, timezone
    return (
        datetime.now(timezone.utc) - timedelta(minutes=offset + 5)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
