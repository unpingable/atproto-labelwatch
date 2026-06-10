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

def test_result_cards_include_use_this_to_see_and_not_for(seeded_db):
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
    assert "Use this to see" in html
    assert "Not for" in html


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
        # Result page should mention each labeler's handle.
        assert "labeler-a.test" in body
        assert "labeler-b.test" in body
        assert "Use this to see" in body
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
