from __future__ import annotations

from dataclasses import replace
import pytest

from labelwatch.derive import (
    LabelerSignals,
    RegimeResult,
    classify_regime_state,
    score_auditability_risk,
    score_inference_risk,
    score_temporal_coherence,
)
import labelwatch.derive as derive


def _base_signals() -> LabelerSignals:
    """A mature, stable, well-observed labeler baseline."""
    return LabelerSignals(
        labeler_did="did:plc:testlabeler123",
        visibility_class="declared",
        auditability="high",
        classification_confidence="high",
        likely_test_dev=False,
        first_seen_hours_ago=24.0 * 14,
        scan_count=10,
        event_count_total=250,
        warmup_enabled=True,
        warmup_min_age_hours=48,
        warmup_min_events=20,
        warmup_min_scans=3,
        event_count_24h=4,
        event_count_7d=28,
        event_count_30d=80,
        hourly_counts_7d=[1] * 168,
        interarrival_secs_7d=[3600.0] * 100,
        dormancy_days=0.5,
        probe_count_30d=20,
        probe_success_ratio_30d=0.95,
        probe_transition_count_30d=1,
        probe_last_status="accessible",
        probe_statuses_7d=["accessible"] * 7,
        probe_recent_fail_streak=0,
        class_transition_count_30d=0,
        confidence_transition_count_30d=0,
        recent_class_change_hours_ago=None,
        declared_record=True,
        has_labeler_service=True,
        has_label_key=True,
        observed_as_src=True,
    )


def _reasons(obj) -> set[str]:
    return set(getattr(obj, "reason_codes", []))


def _assert_reasons(obj, *codes: str) -> None:
    got = _reasons(obj)
    for code in codes:
        assert code in got, f"Missing reason code {code!r}; got={sorted(got)}"


# ---------------------------------------------------------------------------
# Regime classification
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "overrides, expected_reason",
    [
        ({"first_seen_hours_ago": 12}, "warmup_age"),
        ({"event_count_total": 19}, "warmup_low_volume"),
        ({"scan_count": 2}, "warmup_low_scans"),
    ],
)
def test_regime_warmup_gate_reasons(overrides, expected_reason):
    s = replace(_base_signals(), **overrides)
    r = classify_regime_state(s)
    assert r.regime_state == "warming_up"
    _assert_reasons(r, "warmup_active", expected_reason)


def test_regime_inactive_edge_at_30_days():
    s_29 = replace(_base_signals(), dormancy_days=29.99, event_count_30d=0)
    r_29 = classify_regime_state(s_29)
    assert r_29.regime_state != "inactive"

    s_30 = replace(_base_signals(), dormancy_days=30.0, event_count_30d=0)
    r_30 = classify_regime_state(s_30)
    assert r_30.regime_state == "inactive"
    _assert_reasons(r_30, "dormant_30d", "declared_no_recent_activity")


def test_regime_flapping_requires_transitions_and_mixed_statuses():
    s_not = replace(
        _base_signals(),
        probe_transition_count_30d=6,
        probe_statuses_7d=["accessible"] * 7,
    )
    r_not = classify_regime_state(s_not)
    assert r_not.regime_state != "flapping"

    s_yes = replace(
        _base_signals(),
        probe_transition_count_30d=6,
        probe_statuses_7d=["accessible", "down", "accessible", "down"],
    )
    r_yes = classify_regime_state(s_yes)
    assert r_yes.regime_state == "flapping"
    _assert_reasons(r_yes, "probe_flapping_30d")
    assert any(code.startswith("probe_transitions_") for code in r_yes.reason_codes)


def test_regime_degraded_probe_success_threshold_edge():
    s_low = replace(
        _base_signals(),
        probe_count_30d=5,
        probe_success_ratio_30d=0.39,
        probe_recent_fail_streak=3,
    )
    r_low = classify_regime_state(s_low)
    assert r_low.regime_state == "degraded"
    _assert_reasons(r_low, "probe_success_low", "declared_or_service_present", "probe_fail_streak")

    s_edge = replace(
        _base_signals(),
        probe_count_30d=5,
        probe_success_ratio_30d=0.40,
        probe_recent_fail_streak=3,
    )
    r_edge = classify_regime_state(s_edge)
    assert r_edge.regime_state != "degraded"


def test_regime_ghost_declared_threshold_edge():
    s_ghost = replace(_base_signals(), event_count_30d=2)
    r_ghost = classify_regime_state(s_ghost)
    assert r_ghost.regime_state == "ghost_declared"
    _assert_reasons(r_ghost, "declared_low_activity")

    s_not = replace(_base_signals(), event_count_30d=3)
    r_not = classify_regime_state(s_not)
    assert r_not.regime_state != "ghost_declared"


def test_regime_dark_operational_requires_observed_without_declaration():
    s = replace(
        _base_signals(),
        declared_record=False,
        has_labeler_service=False,
        observed_as_src=True,
        event_count_7d=5,
        event_count_30d=10,
        visibility_class="observed_only",
        auditability="low",
        classification_confidence="low",
    )
    r = classify_regime_state(s)
    assert r.regime_state == "dark_operational"
    _assert_reasons(r, "observed_without_declaration", "no_labeler_service_in_did")


def test_regime_bursty_threshold(monkeypatch):
    monkeypatch.setattr(derive, "burstiness_index", lambda _: 65.0)
    s = replace(_base_signals(), event_count_7d=10, event_count_30d=25)
    r = classify_regime_state(s)
    assert r.regime_state == "bursty"
    _assert_reasons(r, "high_burstiness", "burstiness_65")


def test_regime_stable_threshold_edge_reason_codes():
    s_19 = replace(_base_signals(), event_count_30d=19)
    r_19 = classify_regime_state(s_19)
    assert r_19.regime_state == "stable"
    _assert_reasons(r_19, "active_no_strong_pattern")
    assert "sustained_activity" not in _reasons(r_19)

    s_20 = replace(_base_signals(), event_count_30d=20)
    r_20 = classify_regime_state(s_20)
    assert r_20.regime_state == "stable"
    _assert_reasons(r_20, "sustained_activity", "probe_consistent", "low_class_churn")


def test_regime_fallback_inactive_insufficient_signal():
    s = replace(
        _base_signals(),
        declared_record=False,
        has_labeler_service=False,
        observed_as_src=False,
        event_count_24h=0,
        event_count_7d=0,
        event_count_30d=0,
        dormancy_days=2.0,
    )
    r = classify_regime_state(s)
    assert r.regime_state == "inactive"
    _assert_reasons(r, "insufficient_signal")


# ---------------------------------------------------------------------------
# Auditability risk
# ---------------------------------------------------------------------------

def test_auditability_risk_declared_well_probed_is_low_exact():
    s = _base_signals()
    out = score_auditability_risk(s)
    assert out.score == 10
    assert out.band == "low"
    _assert_reasons(out, "visibility_declared", "auditability_high", "classification_confidence_high")


def test_auditability_risk_observed_only_active_clamps_high():
    s = replace(
        _base_signals(),
        visibility_class="observed_only",
        auditability="low",
        classification_confidence="low",
        declared_record=False,
        has_labeler_service=False,
        has_label_key=False,
        probe_count_30d=0,
        event_count_30d=5,
    )
    out = score_auditability_risk(s)
    assert out.score == 100
    assert out.band == "high"
    _assert_reasons(
        out,
        "visibility_observed_only",
        "auditability_low",
        "missing_declared_record",
        "missing_labeler_service",
        "missing_label_key",
        "no_probe_history",
        "active_observed_only",
        "classification_confidence_low",
    )


def test_auditability_risk_probe_success_edge_changes_penalty_and_reason():
    base = replace(_base_signals(), probe_count_30d=5)
    low = score_auditability_risk(replace(base, probe_success_ratio_30d=0.39))
    edge = score_auditability_risk(replace(base, probe_success_ratio_30d=0.40))
    assert low.score - edge.score == 7
    _assert_reasons(low, "probe_success_low")
    _assert_reasons(edge, "probe_success_mixed")
    assert "probe_success_mixed" not in _reasons(low)
    assert "probe_success_low" not in _reasons(edge)


def test_auditability_risk_warmup_adds_five():
    mature = score_auditability_risk(_base_signals())
    warm = score_auditability_risk(replace(_base_signals(), first_seen_hours_ago=1.0))
    assert warm.score - mature.score == 5
    _assert_reasons(warm, "warmup_active")


@pytest.mark.parametrize(
    "confidence, expected_delta, expected_reason",
    [
        ("high", 0, "classification_confidence_high"),
        ("medium", 4, "classification_confidence_medium"),
        ("low", 10, "classification_confidence_low"),
    ],
)
def test_auditability_risk_confidence_penalties(confidence, expected_delta, expected_reason):
    base = score_auditability_risk(_base_signals())
    out = score_auditability_risk(replace(_base_signals(), classification_confidence=confidence))
    assert out.score - base.score == expected_delta
    _assert_reasons(out, expected_reason)


# ---------------------------------------------------------------------------
# Inference risk
# ---------------------------------------------------------------------------

def test_inference_risk_stable_baseline_clamps_to_zero():
    s = _base_signals()
    regime = RegimeResult(regime_state="stable", reason_codes=["sustained_activity"])
    out = score_inference_risk(s, regime)
    assert out.score == 0
    assert out.band == "low"
    _assert_reasons(out, "regime_stable")


def test_inference_risk_regime_adjustment_delta_stable_vs_flapping():
    s = _base_signals()
    stable = score_inference_risk(s, RegimeResult("stable", []))
    flapping = score_inference_risk(s, RegimeResult("flapping", []))
    # stable=-8 (clamps to 0), flapping=+10 => visible delta=10
    assert flapping.score - stable.score == 10
    _assert_reasons(flapping, "regime_flapping")
    _assert_reasons(stable, "regime_stable")


@pytest.mark.parametrize(
    "events_30d, expected_score, expected_reason",
    [
        (0, 25, "no_events_30d"),
        (4, 18, "very_low_volume_30d"),
        (5, 10, "low_volume_30d"),
        (19, 10, "low_volume_30d"),
        (20, 0, None),
    ],
)
def test_inference_risk_volume_thresholds_exact(events_30d, expected_score, expected_reason):
    s = replace(_base_signals(), event_count_30d=events_30d)
    regime = RegimeResult("inactive", [])
    out = score_inference_risk(s, regime)
    assert out.score == expected_score
    if expected_reason:
        _assert_reasons(out, expected_reason)


def test_inference_risk_likely_test_dev_adds_reason_not_score():
    s_false = _base_signals()
    s_true = replace(_base_signals(), likely_test_dev=True)
    regime = RegimeResult("inactive", [])
    out_false = score_inference_risk(s_false, regime)
    out_true = score_inference_risk(s_true, regime)
    assert out_true.score == out_false.score
    assert "likely_test_dev" not in _reasons(out_false)
    _assert_reasons(out_true, "likely_test_dev")


@pytest.mark.parametrize(
    "irregularity, expected_delta, expected_reason",
    [
        (39.0, 0, None),
        (40.0, 6, "cadence_irregularity_medium"),
        (70.0, 12, "cadence_irregularity_high"),
    ],
)
def test_inference_risk_cadence_irregularity_thresholds(monkeypatch, irregularity, expected_delta, expected_reason):
    monkeypatch.setattr(derive, "cadence_irregularity", lambda _: irregularity)
    s = _base_signals()
    regime = RegimeResult("inactive", [])
    out = score_inference_risk(s, regime)
    assert out.score == expected_delta
    if expected_reason:
        _assert_reasons(out, expected_reason)


@pytest.mark.parametrize(
    "class_churn, conf_churn, expected_score, required_reasons",
    [
        (0, 0, 0, []),
        (1, 0, 10, ["recent_class_change"]),
        (3, 0, 20, ["high_class_churn"]),
        (0, 1, 5, ["confidence_changed"]),
        (0, 3, 10, ["confidence_churn"]),
        (3, 3, 30, ["high_class_churn", "confidence_churn"]),
    ],
)
def test_inference_risk_churn_thresholds_exact(class_churn, conf_churn, expected_score, required_reasons):
    s = replace(
        _base_signals(),
        class_transition_count_30d=class_churn,
        confidence_transition_count_30d=conf_churn,
    )
    regime = RegimeResult("inactive", [])
    out = score_inference_risk(s, regime)
    assert out.score == expected_score
    for code in required_reasons:
        _assert_reasons(out, code)


# ---------------------------------------------------------------------------
# Temporal coherence
# ---------------------------------------------------------------------------

def test_temporal_coherence_stable_high_volume_exact():
    s = _base_signals()
    regime = RegimeResult("stable", [])
    out = score_temporal_coherence(s, regime)
    assert out.score == 80
    assert out.band == "high"
    _assert_reasons(out, "volume_high_30d", "regime_stable")


def test_temporal_coherence_bad_case_clamps_low(monkeypatch):
    monkeypatch.setattr(derive, "cadence_irregularity", lambda _: 70.0)
    s = replace(
        _base_signals(),
        first_seen_hours_ago=1.0,
        event_count_30d=0,
        dormancy_days=30.0,
        probe_transition_count_30d=6,
        class_transition_count_30d=3,
    )
    regime = RegimeResult("flapping", [])
    out = score_temporal_coherence(s, regime)
    assert out.score == 0
    assert out.band == "low"
    _assert_reasons(
        out,
        "volume_low_30d",
        "dormant_30d",
        "probe_flapping_30d",
        "high_class_churn",
        "cadence_irregularity_high",
        "warmup_active",
        "regime_flapping",
    )


@pytest.mark.parametrize(
    "events_30d, expected_score, expected_reason",
    [
        (4, 35, "volume_low_30d"),
        (5, 50, None),
        (19, 50, None),
        (20, 60, "volume_good_30d"),
        (49, 60, "volume_good_30d"),
        (50, 70, "volume_high_30d"),
    ],
)
def test_temporal_coherence_volume_thresholds_exact(monkeypatch, events_30d, expected_score, expected_reason):
    monkeypatch.setattr(derive, "cadence_irregularity", lambda _: 0.0)
    s = replace(_base_signals(), event_count_30d=events_30d)
    regime = RegimeResult("inactive", [])
    out = score_temporal_coherence(s, regime)
    assert out.score == expected_score
    if expected_reason:
        _assert_reasons(out, expected_reason)


def test_temporal_coherence_regime_nudge_delta_stable_vs_bursty(monkeypatch):
    monkeypatch.setattr(derive, "cadence_irregularity", lambda _: 0.0)
    s = _base_signals()
    stable = score_temporal_coherence(s, RegimeResult("stable", []))
    bursty = score_temporal_coherence(s, RegimeResult("bursty", []))
    assert stable.score - bursty.score == 18
    _assert_reasons(stable, "regime_stable")
    _assert_reasons(bursty, "regime_bursty")


@pytest.mark.parametrize(
    "irregularity, expected_score, expected_reason",
    [
        (39.0, 80, None),
        (40.0, 72, "cadence_irregularity_medium"),
        (70.0, 65, "cadence_irregularity_high"),
    ],
)
def test_temporal_coherence_cadence_thresholds(monkeypatch, irregularity, expected_score, expected_reason):
    monkeypatch.setattr(derive, "cadence_irregularity", lambda _: irregularity)
    s = _base_signals()
    out = score_temporal_coherence(s, RegimeResult("stable", []))
    assert out.score == expected_score
    if expected_reason:
        _assert_reasons(out, expected_reason)
