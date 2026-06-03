"""Tests for the authority-posture aggregator.

Proves:
  - Population counts (observed / active / events) are correct.
  - Dial distributions count labelers by class and risk bands.
  - Volume share by class/auditability_risk sums to total events_7d-weighted.
  - Authority_effect × class crosstab routes each event to the right
    (effect, class) cell, including the labeler-default fallback.
  - Copy lines are descriptive; never "trusted" / "bad."
  - HTML renders dial counts and crosstabs and avoids loaded language.
"""
from __future__ import annotations

import time

from labelwatch import db
from labelwatch.authority_posture import (
    CLASS_BUCKETS,
    RISK_BANDS,
    build_authority_posture,
    render_authority_posture_html,
)
from labelwatch.label_family import (
    AUTHORITY_EFFECT_ORDER,
    LABELER_DEFAULT_EFFECT,
)


WINDOW_START = "2026-05-01T00:00:00Z"
WINDOW_END = "2026-06-01T00:00:00Z"
IN_WINDOW = "2026-05-15T12:00:00Z"


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _seed_labeler(
    conn,
    labeler_did,
    *,
    labeler_class="third_party",
    auditability_risk_band="medium",
    inference_risk_band="medium",
    temporal_coherence_band="medium",
    events_7d=0,
):
    conn.execute(
        """
        INSERT INTO labelers(
            labeler_did, labeler_class,
            auditability_risk_band, inference_risk_band, temporal_coherence_band,
            events_7d
        ) VALUES(?, ?, ?, ?, ?, ?)
        """,
        (
            labeler_did, labeler_class,
            auditability_risk_band, inference_risk_band, temporal_coherence_band,
            events_7d,
        ),
    )
    conn.commit()


def _seed_event(conn, labeler_did, val, target_did, ts=IN_WINDOW, neg=0, idx=0):
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (
            labeler_did,
            f"at://{target_did}/app.bsky.feed.post/{idx}",
            val,
            neg,
            ts,
            f"hash_{idx}_{time.monotonic_ns()}",
            target_did,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Population + dial distributions
# ---------------------------------------------------------------------------


def test_population_counts():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:a", events_7d=100)
    _seed_labeler(conn, "did:plc:b", events_7d=0)  # observed but inactive
    _seed_labeler(conn, "did:plc:c", events_7d=50)
    _seed_event(conn, "did:plc:a", "spam", "did:plc:t1", idx=1)

    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    assert p["population"]["labelers_observed"] == 3
    assert p["population"]["labelers_active_7d"] == 2
    assert p["population"]["events_in_window"] == 1


def test_dial_distributions():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:a",
                  labeler_class="official_platform",
                  auditability_risk_band="low",
                  inference_risk_band="low",
                  temporal_coherence_band="high")
    _seed_labeler(conn, "did:plc:b",
                  labeler_class="third_party",
                  auditability_risk_band="high",
                  inference_risk_band="high",
                  temporal_coherence_band="low")
    _seed_labeler(conn, "did:plc:c",
                  labeler_class="third_party",
                  auditability_risk_band="medium",
                  inference_risk_band="medium",
                  temporal_coherence_band="medium")

    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    assert p["dials"]["class"]["official_platform"] == 1
    assert p["dials"]["class"]["third_party"] == 2
    assert p["dials"]["auditability_risk"]["low"] == 1
    assert p["dials"]["auditability_risk"]["medium"] == 1
    assert p["dials"]["auditability_risk"]["high"] == 1
    assert p["dials"]["inference_risk"]["high"] == 1
    assert p["dials"]["temporal_coherence"]["high"] == 1
    # Every dial has every bucket key present, including zero buckets.
    for bucket in CLASS_BUCKETS:
        assert bucket in p["dials"]["class"]
    for band in RISK_BANDS:
        assert band in p["dials"]["auditability_risk"]


def test_unknown_dial_value_routes_to_unknown_bucket():
    conn = _make_db()
    conn.execute(
        "INSERT INTO labelers(labeler_did, labeler_class, auditability_risk_band) "
        "VALUES(?, NULL, ?)",
        ("did:plc:weird", "unmapped_value"),
    )
    conn.commit()
    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    assert p["dials"]["class"]["unknown"] == 1
    # auditability_risk_band="unmapped_value" is not in RISK_BANDS → unknown bucket.
    assert p["dials"]["auditability_risk"]["unknown"] == 1


# ---------------------------------------------------------------------------
# Volume share
# ---------------------------------------------------------------------------


def test_volume_share_uses_events_7d():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:a", labeler_class="official_platform",
                  auditability_risk_band="low", events_7d=400)
    _seed_labeler(conn, "did:plc:b", labeler_class="third_party",
                  auditability_risk_band="high", events_7d=600)

    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    assert p["volume_share"]["by_class"]["official_platform"] == 400
    assert p["volume_share"]["by_class"]["third_party"] == 600
    assert p["volume_share"]["by_auditability_risk"]["low"] == 400
    assert p["volume_share"]["by_auditability_risk"]["high"] == 600


# ---------------------------------------------------------------------------
# Authority-effect × class crosstab
# ---------------------------------------------------------------------------


def test_authority_effect_by_class_crosstab_routes_events_correctly():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:official",
                  labeler_class="official_platform",
                  auditability_risk_band="low", events_7d=2)
    _seed_labeler(conn, "did:plc:thirdparty",
                  labeler_class="third_party",
                  auditability_risk_band="high", events_7d=3)

    # spam (reputational) from both
    _seed_event(conn, "did:plc:official", "spam", "did:plc:t1", idx=1)
    _seed_event(conn, "did:plc:official", "spam", "did:plc:t2", idx=2)
    _seed_event(conn, "did:plc:thirdparty", "spam", "did:plc:t3", idx=3)
    # !hide (visibility_affecting) from third_party
    _seed_event(conn, "did:plc:thirdparty", "!hide", "did:plc:t4", idx=4)
    # !takedown (enforcement_instruction) from third_party
    _seed_event(conn, "did:plc:thirdparty", "!takedown", "did:plc:t5", idx=5)

    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    ae_class = p["authority_effect_by_class"]

    assert ae_class["reputational"]["official_platform"] == 2
    assert ae_class["reputational"]["third_party"] == 1
    assert ae_class["visibility_affecting"]["third_party"] == 1
    assert ae_class["enforcement_instruction"]["third_party"] == 1
    assert p["population"]["events_in_window"] == 5


def test_labeler_default_fallback_routes_to_correct_effect_in_crosstab():
    conn = _make_db()
    hinted_did = next(iter(LABELER_DEFAULT_EFFECT.keys()))
    hinted_effect = LABELER_DEFAULT_EFFECT[hinted_did]
    _seed_labeler(conn, hinted_did, labeler_class="third_party",
                  auditability_risk_band="medium", events_7d=2)
    _seed_event(conn, hinted_did, "crushed-piano", "did:plc:t1", idx=1)
    _seed_event(conn, hinted_did, "crushed-piano", "did:plc:t2", idx=2)

    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    # crushed-piano is not in AUTHORITY_EFFECT_MAP; labeler-default routes it
    # to the hinted_effect (decorative) for the posture aggregate.
    assert p["authority_effect_by_class"][hinted_effect]["third_party"] == 2


def test_authority_effect_by_audit_risk_crosstab():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:lowrisk",
                  labeler_class="third_party",
                  auditability_risk_band="low", events_7d=1)
    _seed_labeler(conn, "did:plc:highrisk",
                  labeler_class="third_party",
                  auditability_risk_band="high", events_7d=1)
    _seed_event(conn, "did:plc:lowrisk", "spam", "did:plc:t1", idx=1)
    _seed_event(conn, "did:plc:highrisk", "terf-gc", "did:plc:t2", idx=2)

    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    ae_audit = p["authority_effect_by_auditability_risk"]
    assert ae_audit["reputational"]["low"] == 1
    assert ae_audit["reputational"]["high"] == 1


# ---------------------------------------------------------------------------
# Copy discipline + HTML
# ---------------------------------------------------------------------------


def test_copy_lines_avoid_loaded_language():
    conn = _make_db()
    # 100 events of spam from a high-auditability-risk third-party labeler.
    _seed_labeler(conn, "did:plc:risky", labeler_class="third_party",
                  auditability_risk_band="high", events_7d=100)
    for i in range(100):
        _seed_event(conn, "did:plc:risky", "spam", f"did:plc:t{i}", idx=i)

    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    lines = " ".join(p["copy_disposition"])
    assert "trusted" not in lines.lower()
    assert "bad labeler" not in lines.lower()
    assert "moralizing" not in lines.lower()
    # Should use "high auditability risk" framing.
    assert "auditability risk" in lines.lower() or lines == ""


def test_html_renders_and_avoids_loaded_language():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:a", labeler_class="third_party",
                  auditability_risk_band="medium", events_7d=10)
    _seed_event(conn, "did:plc:a", "spam", "did:plc:t1", idx=1)
    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    html = render_authority_posture_html(p)
    assert "Authority surface" in html
    assert "third party" in html
    assert "spam" not in html  # crosstab is by effect, not by val
    assert "trusted" not in html.lower()
    assert "bad labeler" not in html.lower()
    # Descriptive copy must accompany the strip.
    assert "does not infer" in html.lower()


def test_html_dial_cells_disambiguate_polarity():
    """Risk and coherence dial cells must bake the dial name into each row
    so a scanner can't compose 'Auditability risk' + 'high' into 'high
    auditability' (which would imply trust)."""
    conn = _make_db()
    _seed_labeler(conn, "did:plc:a", labeler_class="third_party",
                  auditability_risk_band="high",
                  inference_risk_band="high",
                  temporal_coherence_band="high",
                  events_7d=10)
    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    html = render_authority_posture_html(p)
    # Each risk dial cell must use the full polar phrase, not bare "high".
    assert "high auditability risk" in html
    assert "high inference risk" in html
    # Temporal coherence reuses the same disambiguation pattern even though
    # the polarity is inverse — keeps reader scanning consistent.
    assert "high temporal coherence" in html


def test_html_audit_risk_crosstab_columns_disambiguate_polarity():
    """The auditability_risk crosstab column headers must read
    'high auditability risk' (etc.), not bare 'high'/'medium'/'low'."""
    conn = _make_db()
    _seed_labeler(conn, "did:plc:a", labeler_class="third_party",
                  auditability_risk_band="high", events_7d=10)
    _seed_event(conn, "did:plc:a", "spam", "did:plc:t1", idx=1)
    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    html = render_authority_posture_html(p)
    # Find the auditability_risk crosstab section by its summary.
    audit_section_start = html.find("Authority effect by auditability risk")
    assert audit_section_start > 0
    audit_section = html[audit_section_start:]
    # Audit-risk column header must spell out polarity.
    assert "high auditability risk" in audit_section


def test_empty_db_renders_empty_section_gracefully():
    conn = _make_db()
    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    assert p["population"]["labelers_observed"] == 0
    # HTML still renders without errors.
    html = render_authority_posture_html(p)
    assert "Authority surface" in html


def test_neg_events_excluded_from_crosstabs():
    conn = _make_db()
    _seed_labeler(conn, "did:plc:a", labeler_class="third_party",
                  auditability_risk_band="medium", events_7d=2)
    _seed_event(conn, "did:plc:a", "spam", "did:plc:t1", idx=1, neg=0)
    _seed_event(conn, "did:plc:a", "spam", "did:plc:t2", idx=2, neg=1)  # negation
    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    assert p["authority_effect_by_class"]["reputational"]["third_party"] == 1
    assert p["population"]["events_in_window"] == 1


def test_axis_order_preserved_in_crosstabs():
    conn = _make_db()
    p = build_authority_posture(conn, WINDOW_START, WINDOW_END)
    # All effects present as keys, even with no data.
    for effect in AUTHORITY_EFFECT_ORDER:
        assert effect in p["authority_effect_by_class"]
        assert effect in p["authority_effect_by_auditability_risk"]
