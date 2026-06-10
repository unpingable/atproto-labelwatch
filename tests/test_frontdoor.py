"""Acceptance tests for whatsonme.frontdoor.v0 rendering slice.

Per chatty's spec (2026-06-10), eight acceptance tests:
1. Homepage renders lookup input.
2. Handle/DID lookup reaches subject result view.
3. Unknown subject renders subject_not_found / no_observed_labels (not 500).
4. Result cards include "Use this to see" and "Not for".
5. Result cards include generated plain-language sentence.
6. Result cards omit trust score / truth / moderation recommendation / unified score.
7. Methodology/dashboard content reachable on secondary route.
8. Frontdoor state derives from admissible audit receipt.

Plus a couple of structural tests for the pure-Python helpers.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import urllib.request
import urllib.error
from datetime import datetime, timezone

import pytest

from labelwatch import db, frontdoor


# ---------------------------------------------------------------------------
# DB fixture with a single labeled subject
# ---------------------------------------------------------------------------

SUBJECT_DID = "did:plc:subjecttestxxxxxxxxxxxx"
LABELER_A = "did:plc:labelaeritestxxxxxxxxxx"
LABELER_B = "did:plc:labelbtestxxxxxxxxxxxxx"


def _seed_db(path: str):
    conn = db.connect(path)
    db.init_db(conn)

    # Two labelers with different dial states.
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability, "
        "first_seen, last_seen, events_7d, events_30d) VALUES (?,?,?,?,?,?,?,?)",
        (LABELER_A, "labeler-a.test", "stable", "high",
         "2026-05-01T00:00:00Z", "2026-06-09T12:00:00Z", 100, 400),
    )
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability, "
        "first_seen, last_seen, events_7d, events_30d) VALUES (?,?,?,?,?,?,?,?)",
        (LABELER_B, "labeler-b.test", "flapping", "low",
         "2026-05-01T00:00:00Z", "2026-06-09T12:00:00Z", 5, 20),
    )

    # Labeler A: emits a single "porn" label on the subject.
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
        "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
        (LABELER_A, LABELER_A, SUBJECT_DID, "porn", 0,
         "2026-06-01T00:00:00Z", "hash-a-1", SUBJECT_DID),
    )
    # Labeler B: emits a "spam" label, then negates it (classification flip).
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
        "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
        (LABELER_B, LABELER_B, SUBJECT_DID, "spam", 0,
         "2026-06-02T00:00:00Z", "hash-b-1", SUBJECT_DID),
    )
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
        "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
        (LABELER_B, LABELER_B, SUBJECT_DID, "spam", 1,
         "2026-06-05T00:00:00Z", "hash-b-2", SUBJECT_DID),
    )
    conn.commit()
    conn.close()


def _admissible_receipt() -> dict:
    return {
        "receipt_kind": "labelwatch.index_audit.v1",
        "consumer_surface": "whatsonme.frontdoor.v0",
        "overall_verdict": "admissible",
        "generated_at": "2026-06-10T01:00:00Z",
        "_receipt_path": "<test>",
    }


def _refused_receipt() -> dict:
    return {
        "receipt_kind": "labelwatch.index_audit.v1",
        "consumer_surface": "whatsonme.frontdoor.v0",
        "overall_verdict": "refused_query_shape_unbounded",
        "generated_at": "2026-06-10T01:00:00Z",
        "_receipt_path": "<test>",
    }


@pytest.fixture
def seeded_db(tmp_path):
    p = str(tmp_path / "labelwatch.db")
    _seed_db(p)
    return p


# ---------------------------------------------------------------------------
# Test 1 — Homepage renders lookup input
# ---------------------------------------------------------------------------

def test_homepage_renders_lookup_input():
    html = frontdoor.render_homepage_html(audit_receipt=_admissible_receipt())
    assert "<form" in html
    assert 'action="/v1/frontdoor"' in html
    assert 'name="q"' in html
    assert "Paste a handle or DID" in html or "handle or DID" in html.lower()


# ---------------------------------------------------------------------------
# Test 2 — Handle/DID lookup reaches subject result view
# ---------------------------------------------------------------------------

def test_did_lookup_reaches_result_view(seeded_db):
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    assert result.refusal is None
    assert result.subject_did == SUBJECT_DID
    assert len(result.labelers) == 2
    handle_set = {c.handle for c in result.labelers}
    assert handle_set == {"labeler-a.test", "labeler-b.test"}


def test_handle_lookup_reaches_result_view(seeded_db):
    """When a handle is supplied, frontdoor resolves it (via injected resolver)
    and produces the result view."""
    def fake_resolver(handle: str):
        return SUBJECT_DID if handle == "alice.bsky.social" else None

    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "@alice.bsky.social",
            audit_receipt=_admissible_receipt(),
            handle_resolver=fake_resolver,
        )
    finally:
        conn.close()

    assert result.refusal is None
    assert result.subject_did == SUBJECT_DID
    assert result.subject_handle == "alice.bsky.social"
    assert len(result.labelers) == 2


# ---------------------------------------------------------------------------
# Test 3 — Unknown subject renders refusal, not a 500
# ---------------------------------------------------------------------------

def test_unknown_subject_renders_refusal(seeded_db):
    """A DID with no labels yields no_observed_labels (not crash)."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "did:plc:unobservedxxxxxxxxxxxxxx",
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    assert result.refusal == "no_observed_labels"
    assert result.labelers == []

    # Refusal renders cleanly to HTML (no crash, no truth-claim).
    html = frontdoor.render_result_page_html(result)
    assert "No labels observed" in html
    assert "<section class=\"refusal\">" in html


def test_subject_too_dense_refuses_cleanly(tmp_path, monkeypatch):
    """High-volume subjects refuse with subject_too_dense rather than
    chewing through Python aggregation. Load probe found p99 = 24s for
    subjects with 100k+ events; this circuit breaker keeps the surface
    bounded until SQL-side aggregation lands."""
    # Tiny cap so we can trigger with a small fixture.
    monkeypatch.setattr(frontdoor, "MAX_EVENTS_FOR_AGGREGATION", 3)

    p = str(tmp_path / "lw.db")
    conn = db.connect(p)
    db.init_db(conn)
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability) "
        "VALUES (?,?,?,?)",
        ("did:plc:densely", "dense-labeler.test", "stable", "high"),
    )
    # 5 events against one subject; cap is 3 → should refuse.
    for i in range(5):
        conn.execute(
            "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
            "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
            ("did:plc:densely", "did:plc:densely", "did:plc:densesubj",
             "spam", 0, f"2026-06-01T00:0{i}:00Z", f"h-dense-{i}",
             "did:plc:densesubj"),
        )
    conn.commit()
    conn.close()

    conn = db.connect(p, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "did:plc:densesubj",
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    assert result.refusal == "subject_too_dense"
    assert result.labelers == []
    # Detail should mention the actual count.
    assert "5" in result.refusal_detail

    # Refusal renders cleanly to HTML.
    html = frontdoor.render_result_page_html(result)
    assert "Subject too dense" in html
    # The forbidden phrases are still absent (no "trust score" etc).
    for phrase in ("trust score", "risk score", "moderation recommendation"):
        assert phrase not in html.lower()


def test_subject_under_cap_passes(tmp_path, monkeypatch):
    """Subjects under the cap still resolve normally."""
    monkeypatch.setattr(frontdoor, "MAX_EVENTS_FOR_AGGREGATION", 100)
    p = str(tmp_path / "lw.db")
    conn = db.connect(p)
    db.init_db(conn)
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability) "
        "VALUES (?,?,?,?)",
        ("did:plc:sparselbl", "sparse-labeler.test", "stable", "high"),
    )
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
        "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
        ("did:plc:sparselbl", "did:plc:sparselbl", "did:plc:sparsesubj",
         "spam", 0, "2026-06-01T00:00:00Z", "h-sparse", "did:plc:sparsesubj"),
    )
    conn.commit()
    conn.close()

    conn = db.connect(p, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "did:plc:sparsesubj",
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    assert result.refusal is None
    assert len(result.labelers) == 1


def test_unresolvable_handle_returns_subject_not_found(seeded_db):
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "@nobody.bsky.social",
            audit_receipt=_admissible_receipt(),
            handle_resolver=lambda h: None,
        )
    finally:
        conn.close()
    assert result.refusal == "subject_not_found"


# ---------------------------------------------------------------------------
# Test 4 — Result cards include "Use this to see" and "Not for"
# ---------------------------------------------------------------------------

def test_result_page_has_global_use_not_framing(seeded_db):
    """Per chatty 2026-06-10: drop per-card boilerplate, one global Use/Not
    block at top of the result. The framing remains; the location changes
    from per-card to per-page."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    html = frontdoor.render_result_page_html(result)
    # The global Use/Not block is what enforces the framing now.
    assert "<aside class=\"use-not\">" in html
    assert "<strong>Use:</strong>" in html
    assert "<strong>Not:</strong>" in html
    # Defensive: per-card Use/Not paragraphs should NOT reappear. Count
    # 'Use:' occurrences — there's exactly one (the global block).
    assert html.count("<strong>Use:</strong>") == 1
    assert html.count("<strong>Not:</strong>") == 1


# ---------------------------------------------------------------------------
# Test 5 — Result cards include generated plain-language sentence
# ---------------------------------------------------------------------------

def test_result_cards_include_plain_language_sentence(seeded_db):
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    # Every card has a non-empty sentence containing the template anchors.
    for card in result.labelers:
        s = card.plain_language_sentence
        assert s.startswith("This labeler"), s
        assert "appears" in s
        assert "to audit from available evidence" in s

    # Sentences differ between the two seeded labelers (different dials).
    sentences = {c.plain_language_sentence for c in result.labelers}
    assert len(sentences) == 2


def test_plain_language_sentence_rules():
    """Pure-function test of the sentence vocabulary."""
    # Stable + high-auditability labeler with mostly visibility labels.
    s = frontdoor.plain_language_sentence(
        {"regime_state": "stable", "auditability": "high"},
        {"visibility_affecting": 12, "reputational": 1},
    )
    assert "visibility_affecting" in s
    assert "appears stable" in s
    assert "easy to audit" in s

    # Flapping labeler with low auditability.
    s = frontdoor.plain_language_sentence(
        {"regime_state": "flapping", "auditability": "low"},
        {"reputational": 50},
    )
    assert "appears churny" in s
    assert "hard to audit" in s

    # Insufficient signal: warming_up + no auditability dial.
    s = frontdoor.plain_language_sentence(
        {"regime_state": "warming_up", "auditability": None},
        {},
    )
    assert "appears insufficient-history" in s
    assert "unclassified" in s
    assert "unknown to audit" in s

    # Sentence rules: derived classifications use "appears", and the
    # sentence never adjudicates subjects, trusts labelers, or names risk.
    # ("and is easy/limited/hard to audit" is mandated by chatty's template
    # for the auditability dial copula — that copula is fine; subject-
    # adjudicating "is" constructions are not.)
    s = frontdoor.plain_language_sentence(
        {"regime_state": "stable", "auditability": "high"},
        {"visibility_affecting": 5},
    )
    assert "appears " in s
    for forbidden in (
        "subject is",
        "this account is",
        "this user is",
        "trustworthy",
        "untrustworthy",
        "risk",
        "trust score",
    ):
        assert forbidden not in s.lower(), f"forbidden phrase '{forbidden}' in sentence"


# ---------------------------------------------------------------------------
# Test 6 — Result cards omit trust score / truth / moderation / unified score
# ---------------------------------------------------------------------------

def test_result_cards_omit_forbidden_fields(seeded_db):
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    payload = frontdoor.result_to_json(result)
    payload_str = json.dumps(payload).lower()

    # Forbidden field names: never present as JSON keys.
    for f in frontdoor._FORBIDDEN_FIELD_NAMES:
        for card in payload["labelers"]:
            assert f not in card, f"forbidden field {f} leaked into card"
    # Forbidden phrases: never present in JSON body.
    forbidden_in_json = (
        "trust_score",
        "risk_score",
        "moderation_recommendation",
    )
    for phrase in forbidden_in_json:
        assert phrase not in payload_str, f"forbidden phrase {phrase} in JSON"

    # HTML rendering: forbidden phrases (chatty's vocabulary) absent.
    html = frontdoor.render_result_page_html(result).lower()
    for phrase in (
        "trust score",
        "risk score",
        "moderation recommendation",
        "should be moderated",
        "this user is a",
        "this account is",
        "verdict on",
    ):
        assert phrase not in html, f"forbidden phrase '{phrase}' in HTML"


# ---------------------------------------------------------------------------
# Test 7 — Methodology content reachable on secondary route
# ---------------------------------------------------------------------------

def test_methodology_remains_reachable_via_report(tmp_path):
    """After generating the report, methodology lives at methodology.html
    and index.html is the lookup-first homepage."""
    from labelwatch import report
    p = str(tmp_path / "labelwatch.db")
    _seed_db(p)
    conn = db.connect(p)
    out = str(tmp_path / "report-out")
    report.generate_report(conn, out)
    conn.close()

    methodology_path = os.path.join(out, "methodology.html")
    index_path = os.path.join(out, "index.html")
    assert os.path.exists(methodology_path), "methodology.html not generated"
    assert os.path.exists(index_path), "index.html not generated"

    with open(methodology_path) as f:
        methodology = f.read()
    # Methodology should contain the old "Read spine" / structural nav anchors.
    assert "authority" in methodology.lower() or "labelers" in methodology.lower()

    with open(index_path) as f:
        homepage = f.read()
    # Homepage is the lookup-first page, not the methodology.
    assert "<form" in homepage
    assert 'action="/v1/frontdoor"' in homepage
    # Homepage links to methodology.
    assert "/methodology.html" in homepage


# ---------------------------------------------------------------------------
# Test 8 — Frontdoor state derives from admissible audit receipt
# ---------------------------------------------------------------------------

def test_frontdoor_refuses_when_audit_missing(seeded_db):
    """No audit receipt → every lookup refuses with index_audit_missing."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=None,
        )
    finally:
        conn.close()
    assert result.refusal == "index_audit_missing"
    assert result.labelers == []


def test_frontdoor_refuses_when_audit_refused(seeded_db):
    """Audit receipt with refused_* verdict → frontdoor refuses, does not query."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_refused_receipt(),
        )
    finally:
        conn.close()
    assert result.refusal == "query_shape_unbounded"
    assert result.labelers == []


def test_frontdoor_admissible_with_debt_still_serves(seeded_db):
    """admissible_with_debt is admissible — does not refuse."""
    receipt = _admissible_receipt()
    receipt["overall_verdict"] = "admissible_with_debt"

    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=receipt,
        )
    finally:
        conn.close()
    assert result.refusal is None
    assert len(result.labelers) == 2
    assert result.audit_verdict == "admissible_with_debt"


# ---------------------------------------------------------------------------
# Receipt-loader test
# ---------------------------------------------------------------------------

def test_find_latest_audit_receipt_picks_newest(tmp_path):
    d = tmp_path / "receipts"
    d.mkdir()

    earlier = d / "labelwatch.index_audit.whatsonme.frontdoor.v0.20260101T000000Z.json"
    earlier.write_text(json.dumps({
        "receipt_kind": "labelwatch.index_audit.v1",
        "consumer_surface": "whatsonme.frontdoor.v0",
        "overall_verdict": "admissible",
        "generated_at": "2026-01-01T00:00:00Z",
    }))
    later = d / "labelwatch.index_audit.whatsonme.frontdoor.v0.20260610T010000Z.json"
    later.write_text(json.dumps({
        "receipt_kind": "labelwatch.index_audit.v1",
        "consumer_surface": "whatsonme.frontdoor.v0",
        "overall_verdict": "admissible",
        "generated_at": "2026-06-10T01:00:00Z",
    }))

    receipt = frontdoor.find_latest_audit_receipt(str(d))
    assert receipt is not None
    assert receipt["generated_at"] == "2026-06-10T01:00:00Z"
    assert receipt["_receipt_path"].endswith(later.name)


def test_find_latest_audit_receipt_returns_none_when_empty(tmp_path):
    d = tmp_path / "receipts"
    d.mkdir()
    assert frontdoor.find_latest_audit_receipt(str(d)) is None


# ---------------------------------------------------------------------------
# Temporal coherence (classification flip detection)
# ---------------------------------------------------------------------------

def test_bsky_profile_url():
    f = frontdoor.bsky_profile_url
    assert f("did:plc:abc123") == "https://bsky.app/profile/did:plc:abc123"
    assert f("did:web:example.com") == "https://bsky.app/profile/did:web:example.com"
    assert f("") is None
    assert f(None) is None
    assert f("not-a-did") is None


def test_bsky_post_url():
    f = frontdoor.bsky_post_url
    # Bluesky posts → clickable
    assert f("at://did:plc:abc/app.bsky.feed.post/3xyz") == (
        "https://bsky.app/profile/did:plc:abc/post/3xyz"
    )
    # Non-post records → None (no link)
    assert f("at://did:plc:abc/app.bsky.actor.profile/self") is None
    assert f("at://did:plc:abc/app.bsky.graph.list/mylist") is None
    assert f("at://did:plc:abc/fm.plyr.track/abc") is None
    # Malformed URIs → None
    assert f(None) is None
    assert f("") is None
    assert f("did:plc:abc") is None
    assert f("https://example.com/post") is None
    assert f("at://did:plc:abc/app.bsky.feed.post") is None  # no rkey


def test_labeler_default_effect_applied_in_card(tmp_path):
    """If a labeler is in LABELER_DEFAULT_EFFECT, its 'unknown' family labels
    resolve to the labeler's default effect. antisubstack is the canonical
    case (added 2026-06-10)."""
    from labelwatch.label_family import LABELER_DEFAULT_EFFECT

    # Locate a real entry from the map to use in this test (avoids tying the
    # test to a single example that may rotate).
    decorative_labeler_did = next(
        d for d, eff in LABELER_DEFAULT_EFFECT.items() if eff == "decorative"
    )

    p = str(tmp_path / "lw.db")
    conn = db.connect(p)
    db.init_db(conn)
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability) "
        "VALUES (?,?,?,?)",
        (decorative_labeler_did, "decorative.test", "stable", "high"),
    )
    # Synthetic novel val that's NOT in AUTHORITY_EFFECT_MAP.
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
        "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
        (decorative_labeler_did, decorative_labeler_did,
         "did:plc:decosubj", "manner-of-death-improbable-cheese", 0,
         "2026-06-01T00:00:00Z", "h-deco", "did:plc:decosubj"),
    )
    conn.commit()
    conn.close()

    conn = db.connect(p, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "did:plc:decosubj",
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    card = result.labelers[0]
    # authority_effects should NOT contain 'unknown' — the labeler-default
    # fallback resolved it to 'decorative'.
    assert "unknown" not in card.authority_effects
    assert "decorative" in card.authority_effects


def test_card_links_to_labeler_profile(seeded_db):
    """Card header renders a clickable handle + 'View profile' link."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    html = frontdoor.render_result_page_html(result)
    # Both labelers' profile URLs should be present, with external-link attrs.
    for labeler_did in (LABELER_A, LABELER_B):
        url = f"https://bsky.app/profile/{labeler_did}"
        assert url in html, f"labeler profile URL missing for {labeler_did}"
    # External-link attributes applied.
    assert 'rel="noopener noreferrer"' in html
    assert 'target="_blank"' in html
    # "View profile" affordance appears (at least once per labeler).
    assert html.count("View profile") >= 2


def test_post_target_links_in_records_expander(tmp_path):
    """For at:// post URIs, the labeled-records table renders 'View post'
    plus the raw URI; non-post records stay raw-only."""
    p = str(tmp_path / "lw.db")
    conn = db.connect(p)
    db.init_db(conn)
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability) "
        "VALUES (?,?,?,?)",
        ("did:plc:linklabeler", "link-labeler.test", "stable", "high"),
    )
    # One post + one custom record
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
        "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
        ("did:plc:linklabeler", "did:plc:linklabeler",
         "at://did:plc:linksubj/app.bsky.feed.post/3abcxyz", "spam", 0,
         "2026-06-01T00:00:00Z", "h-link-post", "did:plc:linksubj"),
    )
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
        "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
        ("did:plc:linklabeler", "did:plc:linklabeler",
         "at://did:plc:linksubj/fm.plyr.track/customrkey", "copyright", 0,
         "2026-06-02T00:00:00Z", "h-link-track", "did:plc:linksubj"),
    )
    conn.commit()
    conn.close()

    conn = db.connect(p, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "did:plc:linksubj",
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    html = frontdoor.render_result_page_html(result)
    # Post URI → clickable
    assert "https://bsky.app/profile/did:plc:linksubj/post/3abcxyz" in html
    assert "View post" in html
    # Custom record type → raw URI visible, no link invented
    assert "fm.plyr.track/customrkey" in html
    # Make sure we did NOT invent a bsky URL for the custom record.
    assert "/post/customrkey" not in html


def test_subject_header_links_to_bsky_profile(seeded_db):
    """Subject handle/DID link out to bsky.app/profile/<did>."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    html = frontdoor.render_result_page_html(result)
    assert f"https://bsky.app/profile/{SUBJECT_DID}" in html


def test_result_page_uses_locus_honest_copy(seeded_db):
    """Per chatty 2026-06-10: avoid 'against this account' framing when
    most labels are post-level. Heading + subtitle make scope explicit."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    html = frontdoor.render_result_page_html(result)
    # Heading explicitly names the act of touching, not "against".
    assert "Observed labels touching" in html
    # Subtitle states scope: account labels + record labels.
    assert "attached directly to the account" in html
    assert "posts or records authored by it" in html
    # Forbidden adversarial copy.
    assert "against this account" not in html
    assert "against that account" not in html


def test_page_level_locus_rollup_sums_across_labelers(tmp_path):
    """Page-level rollup shows the locus mix at a glance — answers
    'is this mostly account-level or post-level?'"""
    p = str(tmp_path / "lw.db")
    conn = db.connect(p)
    db.init_db(conn)
    # Two labelers; mixed loci.
    for did, handle in (("did:plc:lpa", "lpa.test"), ("did:plc:lpb", "lpb.test")):
        conn.execute(
            "INSERT INTO labelers (labeler_did, handle, regime_state, auditability) "
            "VALUES (?,?,?,?)",
            (did, handle, "stable", "high"),
        )
    # 1 account-level (from A), 3 post-level (2 from A, 1 from B), 1 profile (B)
    rows = [
        ("did:plc:lpa", "did:plc:lpa", "did:plc:subjectp", "spam", 0,
         "2026-06-01T00:00:00Z", "h-a-acct", "did:plc:subjectp"),
        ("did:plc:lpa", "did:plc:lpa",
         "at://did:plc:subjectp/app.bsky.feed.post/p1", "spam", 0,
         "2026-06-02T00:00:00Z", "h-a-p1", "did:plc:subjectp"),
        ("did:plc:lpa", "did:plc:lpa",
         "at://did:plc:subjectp/app.bsky.feed.post/p2", "spam", 0,
         "2026-06-03T00:00:00Z", "h-a-p2", "did:plc:subjectp"),
        ("did:plc:lpb", "did:plc:lpb",
         "at://did:plc:subjectp/app.bsky.feed.post/p3", "spam", 0,
         "2026-06-04T00:00:00Z", "h-b-p3", "did:plc:subjectp"),
        ("did:plc:lpb", "did:plc:lpb",
         "at://did:plc:subjectp/app.bsky.actor.profile/self", "spam", 0,
         "2026-06-05T00:00:00Z", "h-b-prof", "did:plc:subjectp"),
    ]
    for r in rows:
        conn.execute(
            "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
            "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)", r,
        )
    conn.commit()
    conn.close()

    conn = db.connect(p, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "did:plc:subjectp",
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    rollup = frontdoor._page_level_locus_rollup(result.labelers)
    assert rollup == {"account": 1, "post": 3, "profile": 1}

    # Rendered page surfaces "Where attached (all labelers):" with totals.
    html = frontdoor.render_result_page_html(result)
    assert "Where attached (all labelers):" in html
    # All three loci visible in the rollup strip.
    for word in ("account-level", "post", "profile record"):
        assert word in html, f"locus label {word!r} missing from page rollup"


def test_attachment_locus_classification():
    """Pure-function: URI patterns map to the right locus."""
    f = frontdoor.attachment_locus
    # Account-level: bare DID or URI == target_did
    assert f("did:plc:abc", "did:plc:abc") == "account"
    assert f("did:plc:abc", "did:plc:xyz") == "account"  # any did: prefix
    # Profile record
    assert f("at://did:plc:abc/app.bsky.actor.profile/self", "did:plc:abc") == "profile"
    # Post
    assert f("at://did:plc:abc/app.bsky.feed.post/3abc", "did:plc:abc") == "post"
    # List
    assert f("at://did:plc:abc/app.bsky.graph.list/mylist", "did:plc:abc") == "list"
    # List item
    assert f("at://did:plc:abc/app.bsky.graph.listitem/item1", "did:plc:abc") == "list_item"
    # Feed generator
    assert f("at://did:plc:abc/app.bsky.feed.generator/genX", "did:plc:abc") == "feed_generator"
    # Unknown record collection
    assert f("at://did:plc:abc/com.custom.collection/foo", "did:plc:abc") == "record"
    # Unknown / non-at
    assert f(None, "did:plc:abc") == "unknown"
    assert f("", "did:plc:abc") == "unknown"
    assert f("https://example.com/foo", "did:plc:abc") == "unknown"


def test_attachment_locus_aggregation_in_card(seeded_db):
    """Seeded DB has account-level labels; cards should show locus_counts."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    for card in result.labelers:
        # Seeded labels have uri == SUBJECT_DID, so account locus only.
        assert "account" in card.locus_counts
        assert card.locus_counts["account"] >= 1
        # No post/profile loci in this seed.
        assert "post" not in card.locus_counts


def test_attachment_locus_with_record_level_labels(tmp_path):
    """A subject with a mix of account + post-level labels should produce
    a mixed locus_counts dict and a non-empty labeled_records list."""
    p = str(tmp_path / "lw.db")
    conn = db.connect(p)
    db.init_db(conn)
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability) "
        "VALUES (?,?,?,?)",
        ("did:plc:mixedlabeler", "mixed-labeler.test", "stable", "high"),
    )
    # 1 account-level + 3 post-level events, two distinct posts
    rows = [
        ("did:plc:mixedlabeler", "did:plc:mixedlabeler", "did:plc:mixedsubj", "spam", 0,
         "2026-06-01T00:00:00Z", "h-acct", "did:plc:mixedsubj"),
        ("did:plc:mixedlabeler", "did:plc:mixedlabeler",
         "at://did:plc:mixedsubj/app.bsky.feed.post/abc", "spam", 0,
         "2026-06-02T00:00:00Z", "h-p1a", "did:plc:mixedsubj"),
        ("did:plc:mixedlabeler", "did:plc:mixedlabeler",
         "at://did:plc:mixedsubj/app.bsky.feed.post/abc", "porn", 0,
         "2026-06-03T00:00:00Z", "h-p1b", "did:plc:mixedsubj"),
        ("did:plc:mixedlabeler", "did:plc:mixedlabeler",
         "at://did:plc:mixedsubj/app.bsky.feed.post/xyz", "spam", 0,
         "2026-06-04T00:00:00Z", "h-p2", "did:plc:mixedsubj"),
    ]
    for r in rows:
        conn.execute(
            "INSERT INTO label_events (labeler_did, src, uri, val, neg, ts, "
            "event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)", r,
        )
    conn.commit()
    conn.close()

    conn = db.connect(p, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            "did:plc:mixedsubj",
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    assert len(result.labelers) == 1
    card = result.labelers[0]
    assert card.locus_counts.get("account") == 1
    assert card.locus_counts.get("post") == 3
    # Two distinct post URIs surface in labeled_records.
    post_uris = {e["uri"] for e in card.labeled_records}
    assert len(post_uris) == 2
    assert all(e["locus"] == "post" for e in card.labeled_records)

    # HTML rendering surfaces the locus chips and the records expander.
    html = frontdoor.render_result_page_html(result)
    assert "Where attached:" in html
    assert "post <b>3</b>" in html or "post</span>" in html.lower() or "post" in html
    assert "Show labeled records" in html


def test_network_weather_payload(seeded_db):
    """network_weather() runs against the small dimension tables and produces
    the strip data structure."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        w = frontdoor.network_weather(conn)
    finally:
        conn.close()
    assert isinstance(w["total_labelers"], int)
    assert w["total_labelers"] >= 2  # seeded labelers
    assert isinstance(w["signals"], list)
    assert w["signals"]  # always non-empty (falls back to "calm")
    assert "computed_at" in w


def test_homepage_renders_weather_strip(seeded_db):
    """Homepage with weather shows the network strip + system-dashboard CTA."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        w = frontdoor.network_weather(conn)
    finally:
        conn.close()
    html = frontdoor.render_homepage_html(
        audit_receipt=_admissible_receipt(),
        weather=w,
    )
    assert "Network weather:" in html
    assert "system dashboard" in html  # CTA + nav link
    assert "Open system dashboard" in html  # button link
    assert "labelers" in html  # counts surfaced
    assert "events in 7d" in html


def test_methodology_link_renamed_to_system_dashboard():
    """The nav label is 'system dashboard & graphs', not 'methodology'."""
    html = frontdoor.render_homepage_html(audit_receipt=_admissible_receipt())
    # The visible nav text uses 'system dashboard', not the old 'methodology'.
    # (The target URL stays /methodology.html for v0; route rename is a
    # follow-up.)
    assert "system dashboard" in html
    assert ">methodology<" not in html  # the bare nav text is gone


def test_classification_changed_detected(seeded_db):
    """Labeler B's spam→spam+neg flip should be visible as classification_changed."""
    conn = db.connect(seeded_db, readonly=True)
    try:
        result = frontdoor.lookup_subject(
            conn,
            SUBJECT_DID,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()

    by_did = {c.labeler_did: c for c in result.labelers}
    assert by_did[LABELER_A].classification_changed is False
    assert by_did[LABELER_B].classification_changed is True


# ---------------------------------------------------------------------------
# End-to-end HTTP route test
# ---------------------------------------------------------------------------

def _start_test_server(db_path: str, audit_receipt: dict | None):
    """Start the labelwatch HTTP server on a random local port; return (port, shutdown_fn)."""
    from http.server import ThreadingHTTPServer
    from labelwatch import server as srv_mod

    # Monkey-patch the receipt loader to return our fixture, avoiding disk IO.
    original_loader = srv_mod.__dict__.get("_test_receipt_override")
    srv_mod._test_receipt_override = audit_receipt

    handler_cls = srv_mod.configure_handler(
        db_path=db_path,
        cache_dir=str(os.path.join(os.path.dirname(db_path), "cache")),
        max_concurrent=2,
        rate_limit=10000,  # effectively unbounded for tests
    )
    # Override the receipt on the configured handler class.
    handler_cls.audit_receipt = audit_receipt

    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    def shutdown():
        server.shutdown()
        server.server_close()

    return port, shutdown


def _http_get(port: int, path: str, *, expect_status: int | None = 200) -> tuple[int, str]:
    url = f"http://127.0.0.1:{port}{path}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read().decode("utf-8", errors="replace")
    if expect_status is not None:
        assert status == expect_status, f"GET {path} -> {status}: {body[:200]}"
    return status, body


def test_http_homepage_serves_lookup_form(seeded_db):
    port, shutdown = _start_test_server(seeded_db, _admissible_receipt())
    try:
        status, body = _http_get(port, "/")
        assert "<form" in body
        assert 'action="/v1/frontdoor"' in body
    finally:
        shutdown()


def test_http_frontdoor_lookup_returns_result_html(seeded_db):
    port, shutdown = _start_test_server(seeded_db, _admissible_receipt())
    try:
        status, body = _http_get(port, f"/v1/frontdoor/{SUBJECT_DID}")
        # Result page should mention each labeler's handle and the global
        # Use/Not framing block.
        assert "labeler-a.test" in body
        assert "labeler-b.test" in body
        assert "<aside class=\"use-not\">" in body
        # System-dashboard CTA points users back to the graphs surface.
        assert "system dashboard" in body
    finally:
        shutdown()


def test_http_frontdoor_lookup_returns_json(seeded_db):
    port, shutdown = _start_test_server(seeded_db, _admissible_receipt())
    try:
        status, body = _http_get(port, f"/v1/frontdoor/{SUBJECT_DID}?format=json")
        payload = json.loads(body)
        assert payload["surface"] == "whatsonme.frontdoor.v0"
        assert payload["subject_did"] == SUBJECT_DID
        assert len(payload["labelers"]) == 2
    finally:
        shutdown()


def test_http_frontdoor_refuses_without_audit(seeded_db):
    port, shutdown = _start_test_server(seeded_db, None)
    try:
        status, body = _http_get(port, f"/v1/frontdoor/{SUBJECT_DID}")
        # Refusal is a 200 OK with refusal copy, not an HTTP error.
        assert "Lookup paused" in body or "index_audit_missing" in body
    finally:
        shutdown()


def test_http_frontdoor_query_string_form(seeded_db):
    """GET /v1/frontdoor?q=did:... (homepage form shape) reaches result."""
    port, shutdown = _start_test_server(seeded_db, _admissible_receipt())
    try:
        # urlencode the DID via the path
        path = f"/v1/frontdoor?q={SUBJECT_DID}&format=json"
        status, body = _http_get(port, path)
        payload = json.loads(body)
        assert payload["subject_did"] == SUBJECT_DID
    finally:
        shutdown()
