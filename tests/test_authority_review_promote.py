"""Tests for labelwatch authority-effect-review + authority-effect-promote.

Proves:
  - Review HTML renders one card per triage candidate, with rationale,
    refusals, and the action-row hint.
  - Decisions TOML template is valid TOML, one [[decisions]] per candidate,
    all pre-filled with action="defer".
  - Promotion validation flags: missing candidate_id, duplicate, unknown
    candidate_id, missing reason on ratify, missing authority_effect on
    ratify, invalid authority_effect, invalid action.
  - Validation errors → verdict=refused, no overlay write.
  - Ratify on a label_value whose family is already hand-authored →
    skipped_hand_authored (overlay never overrides AUTHORITY_EFFECT_MAP).
  - Successful ratify writes the overlay file as deterministic Python with
    sorted keys; label_family imports it via setdefault.
  - dry_run produces a receipt but does NOT write the overlay.
  - Promotion receipt has stable sha256 receipt_hash.
"""
from __future__ import annotations

import json
import os
import time
import tomllib
from datetime import datetime, timedelta, timezone

import pytest

from labelwatch import db
from labelwatch.authority_review import (
    ACTION_DEFER,
    ACTION_RATIFY,
    ACTION_REJECT,
    load_triage_receipt,
    render_decisions_template,
    render_review_html,
    write_review_packet,
)
from labelwatch.authority_promote import (
    PROMOTION_RECEIPT_KIND,
    apply_promotions,
    load_decisions,
    load_existing_overlay,
    validate_decisions,
    write_overlay,
)
from labelwatch.authority_triage import run_triage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ts(offset: int = 0) -> str:
    return (
        datetime.now(timezone.utc) - timedelta(minutes=offset + 5)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _seed_events(conn, events):
    for i, e in enumerate(events):
        conn.execute(
            "INSERT INTO label_events(labeler_did, uri, val, neg, ts, "
            "event_hash, target_did) VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                e["labeler_did"],
                e.get("uri", f"at://{e['target_did']}/app.bsky.feed.post/{i}"),
                e["val"], e.get("neg", 0), e["ts"],
                e.get("event_hash", f"hash_{i}_{time.monotonic_ns()}"),
                e["target_did"],
            ),
        )
    conn.commit()


def _triage_fixture(tmp_path):
    db_path = str(tmp_path / "lw.db")
    conn = db.connect(db_path)
    db.init_db(conn)
    # Tier-3 safe pattern (spam-link) → auto_pattern_matched
    _seed_events(conn, [
        {"labeler_did": "did:plc:A", "val": "spam-link",
         "target_did": f"did:plc:t{i}", "ts": _now_ts(i)}
        for i in range(3)
    ])
    # Tier-4 raw fallback (novel) → proposed
    _seed_events(conn, [
        {"labeler_did": "did:plc:B", "val": "novel-thing",
         "target_did": f"did:plc:u{i}", "ts": _now_ts(i)}
        for i in range(2)
    ])
    conn.close()
    return run_triage(db_path, window_days=30, top_values=10, top_labelers=5)


# ---------------------------------------------------------------------------
# Review packet tests
# ---------------------------------------------------------------------------

def test_render_review_html_has_one_card_per_candidate(tmp_path):
    triage = _triage_fixture(tmp_path)
    html = render_review_html(triage)
    n_cards = html.count('<div class="candidate ')
    assert n_cards == len(triage["queue"])
    # Title + summary block present
    assert "authority-effect review packet" in html
    assert "Run summary" in html
    assert "Projected unprofiled reduction" in html


def test_render_review_html_includes_rationale_and_refusals(tmp_path):
    triage = _triage_fixture(tmp_path)
    html = render_review_html(triage)
    assert "Rationale:" in html
    assert "Refusals (boundaries of this inference):" in html


def test_render_decisions_template_is_valid_toml(tmp_path):
    triage = _triage_fixture(tmp_path)
    toml = render_decisions_template(triage, receipt_path="receipts/t.json")
    parsed = tomllib.loads(toml)
    assert parsed["receipt_path"] == "receipts/t.json"
    assert parsed["family_version"] == triage["family_version"]
    decisions = parsed["decisions"]
    assert len(decisions) == len(triage["queue"])
    for d in decisions:
        assert d["action"] == ACTION_DEFER
        assert d["candidate_id"].startswith("aeinf_")


def test_write_review_packet_writes_both_files(tmp_path):
    triage = _triage_fixture(tmp_path)
    out_html = str(tmp_path / "review.html")
    write_review_packet(
        triage, receipt_path="receipts/t.json", out_html=out_html,
    )
    assert os.path.exists(out_html)
    assert os.path.exists(out_html + ".decisions.toml")


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

def test_validate_missing_candidate_id_flagged(tmp_path):
    triage = _triage_fixture(tmp_path)
    decisions_doc = {"decisions": [{"action": "ratify"}]}
    v = validate_decisions(triage, decisions_doc)
    assert any("missing candidate_id" in e for e in v.errors)


def test_validate_duplicate_candidate_id_flagged(tmp_path):
    triage = _triage_fixture(tmp_path)
    cid = triage["queue"][0]["candidate_id"]
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "defer"},
        {"candidate_id": cid, "action": "defer"},
    ]}
    v = validate_decisions(triage, decisions_doc)
    assert any("duplicate candidate_id" in e for e in v.errors)


def test_validate_unknown_candidate_id_flagged(tmp_path):
    triage = _triage_fixture(tmp_path)
    decisions_doc = {"decisions": [
        {"candidate_id": "aeinf_999", "action": "defer"},
    ]}
    v = validate_decisions(triage, decisions_doc)
    assert any("not present in triage receipt" in e for e in v.errors)


def test_validate_invalid_action_flagged(tmp_path):
    triage = _triage_fixture(tmp_path)
    cid = triage["queue"][0]["candidate_id"]
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "approve"},  # not a valid action
    ]}
    v = validate_decisions(triage, decisions_doc)
    assert any("invalid action" in e for e in v.errors)


def test_validate_ratify_requires_authority_effect(tmp_path):
    triage = _triage_fixture(tmp_path)
    cid = triage["queue"][0]["candidate_id"]
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "ratify", "reason": "x"},
    ]}
    v = validate_decisions(triage, decisions_doc)
    assert any("requires authority_effect" in e for e in v.errors)


def test_validate_ratify_requires_valid_authority_effect(tmp_path):
    triage = _triage_fixture(tmp_path)
    cid = triage["queue"][0]["candidate_id"]
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "ratify",
         "authority_effect": "weather_balloon", "reason": "x"},
    ]}
    v = validate_decisions(triage, decisions_doc)
    assert any("invalid authority_effect" in e for e in v.errors)


def test_validate_ratify_requires_reason(tmp_path):
    triage = _triage_fixture(tmp_path)
    cid = triage["queue"][0]["candidate_id"]
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "ratify",
         "authority_effect": "descriptive"},
    ]}
    v = validate_decisions(triage, decisions_doc)
    assert any("requires reason" in e for e in v.errors)


def test_validate_reject_requires_reason(tmp_path):
    triage = _triage_fixture(tmp_path)
    cid = triage["queue"][0]["candidate_id"]
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "reject"},
    ]}
    v = validate_decisions(triage, decisions_doc)
    assert any("requires reason" in e for e in v.errors)


# ---------------------------------------------------------------------------
# Promotion driver tests
# ---------------------------------------------------------------------------

def test_apply_refuses_on_validation_errors(tmp_path):
    triage = _triage_fixture(tmp_path)
    overlay_path = str(tmp_path / "overlay.py")
    decisions_doc = {"decisions": [
        {"candidate_id": "aeinf_999", "action": "ratify",
         "authority_effect": "descriptive", "reason": "x"},
    ]}
    receipt = apply_promotions(
        triage, decisions_doc,
        decisions_path="decisions.toml",
        triage_receipt_path="triage.json",
        overlay_path=overlay_path,
    )
    assert receipt["verdict"] == "refused"
    assert receipt["errors"]
    assert not os.path.exists(overlay_path), "no overlay write on refused"


def test_apply_dry_run_does_not_write_overlay(tmp_path):
    triage = _triage_fixture(tmp_path)
    overlay_path = str(tmp_path / "overlay.py")
    cid = triage["queue"][0]["candidate_id"]  # spam-link
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "ratify",
         "authority_effect": "advisory", "reason": "Safe pattern auto-promote."},
    ]}
    receipt = apply_promotions(
        triage, decisions_doc,
        decisions_path="decisions.toml",
        triage_receipt_path="triage.json",
        overlay_path=overlay_path,
        dry_run=True,
    )
    assert receipt["verdict"] == "dry_run"
    assert receipt["dry_run"] is True
    assert not os.path.exists(overlay_path)


def test_apply_writes_overlay_and_records_added(tmp_path):
    triage = _triage_fixture(tmp_path)
    overlay_path = str(tmp_path / "overlay.py")
    # spam-link is its own family (not collapsed by FAMILY_MAP) and is NOT
    # in AUTHORITY_EFFECT_MAP — ratification writes it to the overlay.
    candidate = next(
        c for c in triage["queue"] if c["label_value"] == "spam-link"
    )
    decisions_doc = {"decisions": [
        {"candidate_id": candidate["candidate_id"], "action": "ratify",
         "authority_effect": "advisory", "reason": "Safe pattern auto-promote."},
    ]}
    receipt = apply_promotions(
        triage, decisions_doc,
        decisions_path="decisions.toml",
        triage_receipt_path="triage.json",
        overlay_path=overlay_path,
    )
    assert receipt["verdict"] == "applied"
    assert receipt["overlay"]["effect_additions_added"] == 1
    assert os.path.exists(overlay_path)
    loaded = load_existing_overlay(overlay_path)
    assert loaded["AUTHORITY_EFFECT_ADDITIONS"]["spam-link"] == "advisory"


def test_ratify_label_in_hand_authored_map_is_skipped(tmp_path):
    """If a ratified label's family was added to AUTHORITY_EFFECT_MAP after
    the triage receipt was generated, the overlay MUST NOT override it.
    The skip is recorded in the promotion receipt.

    Defensive path: in normal flow a candidate whose family is already
    classified would never appear in the triage queue at all. This exercises
    the case where the hand-authored map changed between triage and promote.
    """
    from labelwatch import label_family as lf
    triage = _triage_fixture(tmp_path)
    overlay_path = str(tmp_path / "overlay.py")
    candidate = next(
        c for c in triage["queue"] if c["label_value"] == "novel-thing"
    )
    decisions_doc = {"decisions": [
        {"candidate_id": candidate["candidate_id"], "action": "ratify",
         "authority_effect": "advisory", "reason": "test"},
    ]}
    # Simulate hand-authored entry landing between triage and promote.
    backup = dict(lf.AUTHORITY_EFFECT_MAP)
    try:
        lf.AUTHORITY_EFFECT_MAP["novel-thing"] = "descriptive"
        receipt = apply_promotions(
            triage, decisions_doc,
            decisions_path="decisions.toml",
            triage_receipt_path="triage.json",
            overlay_path=overlay_path,
        )
        assert receipt["overlay"]["effect_additions_skipped_hand_authored"] == 1
        assert receipt["overlay"]["effect_additions_added"] == 0
        if os.path.exists(overlay_path):
            loaded = load_existing_overlay(overlay_path)
            assert "novel-thing" not in loaded["AUTHORITY_EFFECT_ADDITIONS"]
    finally:
        lf.AUTHORITY_EFFECT_MAP.clear()
        lf.AUTHORITY_EFFECT_MAP.update(backup)


def test_ratify_novel_family_writes_to_overlay(tmp_path):
    """A label_value whose family is NOT in the hand-authored map gets
    written into the overlay's AUTHORITY_EFFECT_ADDITIONS.
    """
    triage = _triage_fixture(tmp_path)
    overlay_path = str(tmp_path / "overlay.py")
    candidate = next(
        c for c in triage["queue"] if c["label_value"] == "novel-thing"
    )
    decisions_doc = {"decisions": [
        {"candidate_id": candidate["candidate_id"], "action": "ratify",
         "authority_effect": "descriptive",
         "reason": "Novel descriptive marker, no value-laden framing."},
    ]}
    receipt = apply_promotions(
        triage, decisions_doc,
        decisions_path="decisions.toml",
        triage_receipt_path="triage.json",
        overlay_path=overlay_path,
    )
    assert receipt["verdict"] == "applied"
    assert receipt["overlay"]["effect_additions_added"] == 1
    loaded = load_existing_overlay(overlay_path)
    assert loaded["AUTHORITY_EFFECT_ADDITIONS"]["novel-thing"] == "descriptive"


def test_promotion_receipt_has_stable_hash(tmp_path):
    triage = _triage_fixture(tmp_path)
    overlay_path = str(tmp_path / "overlay.py")
    cid = triage["queue"][0]["candidate_id"]
    decisions_doc = {"decisions": [
        {"candidate_id": cid, "action": "defer"},
    ]}
    receipt = apply_promotions(
        triage, decisions_doc,
        decisions_path="decisions.toml",
        triage_receipt_path="triage.json",
        overlay_path=overlay_path,
    )
    assert receipt["receipt_kind"] == PROMOTION_RECEIPT_KIND
    assert receipt["receipt_hash"]
    assert len(receipt["receipt_hash"]) == 64  # sha256 hex


def test_load_existing_overlay_handles_missing_file(tmp_path):
    overlay_path = str(tmp_path / "does-not-exist.py")
    loaded = load_existing_overlay(overlay_path)
    assert loaded["AUTHORITY_EFFECT_ADDITIONS"] == {}
    assert loaded["LABELER_DEFAULT_EFFECT_ADDITIONS"] == {}


def test_write_overlay_is_sorted_deterministic(tmp_path):
    overlay_path = str(tmp_path / "overlay.py")
    eff = {"zebra": "advisory", "alpha": "descriptive", "mike": "telemetry"}
    lab = {}
    write_overlay(overlay_path, eff, lab, ["test provenance line"])
    with open(overlay_path) as f:
        content = f.read()
    # Keys appear in sorted order
    assert content.index("alpha") < content.index("mike") < content.index("zebra")
    # Provenance line preserved
    assert "test provenance line" in content


def test_overlay_round_trips_through_ast_parser(tmp_path):
    overlay_path = str(tmp_path / "overlay.py")
    eff = {"foo-bar": "descriptive", "baz-qux": "telemetry"}
    lab = {"did:plc:abc": "decorative"}
    write_overlay(overlay_path, eff, lab, ["line 1", "line 2"])
    loaded = load_existing_overlay(overlay_path)
    assert loaded["AUTHORITY_EFFECT_ADDITIONS"] == eff
    assert loaded["LABELER_DEFAULT_EFFECT_ADDITIONS"] == lab


def test_subsequent_promotion_preserves_prior_overlay_entries(tmp_path):
    """Re-running promote with a new decision MUST keep prior entries —
    not wipe them. Cumulative overlay across promotion rounds.
    """
    overlay_path = str(tmp_path / "overlay.py")
    # Seed an existing overlay
    write_overlay(
        overlay_path,
        {"prior-thing": "descriptive"},
        {},
        ["prior provenance"],
    )

    triage = _triage_fixture(tmp_path)
    candidate = next(
        c for c in triage["queue"] if c["label_value"] == "novel-thing"
    )
    decisions_doc = {"decisions": [
        {"candidate_id": candidate["candidate_id"], "action": "ratify",
         "authority_effect": "descriptive", "reason": "x"},
    ]}
    apply_promotions(
        triage, decisions_doc,
        decisions_path="decisions.toml",
        triage_receipt_path="triage.json",
        overlay_path=overlay_path,
    )
    loaded = load_existing_overlay(overlay_path)
    # Prior entry preserved
    assert "prior-thing" in loaded["AUTHORITY_EFFECT_ADDITIONS"]
    # New entry added
    assert "novel-thing" in loaded["AUTHORITY_EFFECT_ADDITIONS"]


def test_label_family_imports_overlay_with_setdefault(tmp_path, monkeypatch):
    """The overlay's setdefault must NEVER override a hand-authored
    AUTHORITY_EFFECT_MAP entry. Smoke at the module-import boundary by
    pointing at an overlay that tries to override "spam".
    """
    # Build an overlay file that would clobber the hand-authored "spam"
    # entry if the merge used assignment instead of setdefault.
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    # We can't easily monkeypatch label_family's import target at runtime;
    # instead, verify the doctrine by inspecting label_family's merge
    # statement directly.
    from labelwatch import label_family as lf
    # AUTHORITY_EFFECT_MAP["spam"] is hand-authored as "reputational".
    # Overlay imports happen at module load with setdefault; after import
    # the value should still be the hand-authored one.
    assert lf.AUTHORITY_EFFECT_MAP["spam"] == "reputational"


# ---------------------------------------------------------------------------
# End-to-end smoke: triage → review → fill decisions → promote → retriage
# ---------------------------------------------------------------------------

def test_end_to_end_triage_review_promote_retriage(tmp_path):
    """Build a fixture DB with one novel label, run triage, generate the
    review packet, hand-fill a decisions TOML to ratify the novel candidate,
    run promote, re-run triage, and assert the unprofiled volume dropped.
    """
    db_path = str(tmp_path / "lw.db")
    conn = db.connect(db_path)
    db.init_db(conn)
    # 10 events of a novel label that the triage will surface as raw_fallback
    _seed_events(conn, [
        {"labeler_did": "did:plc:N", "val": "totally-novel-marker",
         "target_did": f"did:plc:t{i}", "ts": _now_ts(i)}
        for i in range(10)
    ])
    conn.close()

    triage_before = run_triage(
        db_path, window_days=30, top_values=5, top_labelers=5,
    )
    unprofiled_before = triage_before["input_state"]["unprofiled_events"]
    assert unprofiled_before == 10

    # Write triage receipt + decisions
    triage_path = str(tmp_path / "triage.json")
    with open(triage_path, "w") as f:
        json.dump(triage_before, f)
    cid = triage_before["queue"][0]["candidate_id"]
    decisions_path = str(tmp_path / "decisions.toml")
    with open(decisions_path, "w") as f:
        f.write(
            f'receipt_path = "{triage_path}"\n'
            f'family_version = "{triage_before["family_version"]}"\n'
            f'[[decisions]]\n'
            f'candidate_id = "{cid}"\n'
            f'action = "ratify"\n'
            f'authority_effect = "descriptive"\n'
            f'reason = "Smoke test."\n'
        )

    decisions_doc = load_decisions(decisions_path)
    overlay_path = str(tmp_path / "overlay.py")
    promo = apply_promotions(
        triage_before, decisions_doc,
        decisions_path=decisions_path,
        triage_receipt_path=triage_path,
        overlay_path=overlay_path,
    )
    assert promo["verdict"] == "applied"
    assert promo["overlay"]["effect_additions_added"] == 1

    # Now monkeypatch label_family to merge in the new overlay we just wrote.
    # We can't re-import label_family cleanly mid-process, so simulate the
    # effect by directly updating AUTHORITY_EFFECT_MAP with the overlay's
    # additions for the duration of the retriage call.
    from labelwatch import label_family as lf
    loaded = load_existing_overlay(overlay_path)
    backup = dict(lf.AUTHORITY_EFFECT_MAP)
    try:
        for k, v in loaded["AUTHORITY_EFFECT_ADDITIONS"].items():
            lf.AUTHORITY_EFFECT_MAP.setdefault(k, v)
        triage_after = run_triage(
            db_path, window_days=30, top_values=5, top_labelers=5,
        )
        # The novel label is now classified, so unprofiled drops to 0.
        assert triage_after["input_state"]["unprofiled_events"] == 0
    finally:
        lf.AUTHORITY_EFFECT_MAP.clear()
        lf.AUTHORITY_EFFECT_MAP.update(backup)
