"""Derive regime state, risk scores, and temporal coherence from labeler signals.

Pure functions — no DB, no network. Deterministic and testable.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LabelerSignals:
    labeler_did: str
    visibility_class: str
    auditability: str
    classification_confidence: str
    likely_test_dev: bool
    first_seen_hours_ago: float
    scan_count: int
    event_count_total: int
    warmup_enabled: bool
    warmup_min_age_hours: int
    warmup_min_events: int
    warmup_min_scans: int
    event_count_24h: int
    event_count_7d: int
    event_count_30d: int
    hourly_counts_7d: Sequence[int]
    interarrival_secs_7d: Sequence[float]
    dormancy_days: float
    probe_count_30d: int
    probe_success_ratio_30d: float
    probe_transition_count_30d: int
    probe_last_status: Optional[str]
    probe_statuses_7d: Sequence[str]
    probe_recent_fail_streak: int
    class_transition_count_30d: int
    confidence_transition_count_30d: int
    recent_class_change_hours_ago: Optional[float]
    declared_record: bool
    has_labeler_service: bool
    has_label_key: bool
    observed_as_src: bool


@dataclass(frozen=True)
class RegimeResult:
    regime_state: str
    reason_codes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ScoreResult:
    score: int
    band: str
    reason_codes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clamp(v: float) -> int:
    return max(0, min(100, int(round(v))))


def _band(score: int) -> str:
    if score < 34:
        return "low"
    if score < 67:
        return "medium"
    return "high"


def _is_warming_up(s: LabelerSignals) -> tuple[bool, list[str]]:
    if not s.warmup_enabled:
        return False, []
    reasons: list[str] = []
    if s.first_seen_hours_ago < s.warmup_min_age_hours:
        reasons.append("warmup_age")
    if s.event_count_total < s.warmup_min_events:
        reasons.append("warmup_low_volume")
    if s.scan_count < s.warmup_min_scans:
        reasons.append("warmup_low_scans")
    if reasons:
        return True, ["warmup_active", *reasons]
    return False, []


def _mixed_statuses(statuses: Sequence[str]) -> bool:
    return len({s for s in statuses if s}) >= 2


def burstiness_index(hourly_counts: Sequence[int]) -> float:
    if not hourly_counts:
        return 0.0
    mean = sum(hourly_counts) / len(hourly_counts)
    if mean <= 0:
        return 0.0
    var = sum((x - mean) ** 2 for x in hourly_counts) / len(hourly_counts)
    raw = (var / (mean * mean)) * 25.0
    return max(0.0, min(100.0, raw))


def cadence_irregularity(interarrival_secs: Sequence[float]) -> float:
    vals = [x for x in interarrival_secs if x and x > 0]
    if len(vals) < 2:
        return 50.0
    mean = sum(vals) / len(vals)
    if mean <= 0:
        return 50.0
    var = sum((x - mean) ** 2 for x in vals) / len(vals)
    cv = (var ** 0.5) / mean
    return max(0.0, min(100.0, cv * 25.0))


# ---------------------------------------------------------------------------
# Regime classification — priority cascade
# ---------------------------------------------------------------------------

def classify_regime_state(s: LabelerSignals) -> RegimeResult:
    # 1) Warm-up gate
    warming, warm_reasons = _is_warming_up(s)
    if warming:
        return RegimeResult("warming_up", warm_reasons)

    # 2) Inactive
    if s.dormancy_days >= 30 and s.event_count_30d == 0:
        reasons = ["dormant_30d"]
        if s.declared_record:
            reasons.append("declared_no_recent_activity")
        return RegimeResult("inactive", reasons)

    # 3) Flapping
    if s.probe_transition_count_30d >= 6 and _mixed_statuses(s.probe_statuses_7d):
        return RegimeResult("flapping", [
            "probe_flapping_30d",
            f"probe_transitions_{s.probe_transition_count_30d}",
        ])

    # 4) Degraded
    if (s.declared_record or s.has_labeler_service):
        if s.probe_count_30d >= 5 and s.probe_success_ratio_30d < 0.4:
            reasons = ["probe_success_low", "declared_or_service_present"]
            if s.probe_recent_fail_streak >= 3:
                reasons.append("probe_fail_streak")
            return RegimeResult("degraded", reasons)

    # 5) Ghost declared
    if s.declared_record and s.event_count_30d <= 2:
        reasons = ["declared_low_activity"]
        if s.probe_last_status in ("auth_required", "down", "timeout"):
            reasons.append(f"probe_{s.probe_last_status}")
        return RegimeResult("ghost_declared", reasons)

    # 6) Dark operational
    if s.observed_as_src and not s.declared_record and not s.has_labeler_service:
        if s.event_count_7d > 0:
            return RegimeResult("dark_operational", [
                "observed_without_declaration",
                "no_labeler_service_in_did",
            ])

    # 7) Bursty
    b = burstiness_index(s.hourly_counts_7d)
    if s.event_count_7d >= 10 and b >= 65:
        return RegimeResult("bursty", [
            "high_burstiness",
            f"burstiness_{int(b)}",
        ])

    # 8) Stable (strong case)
    if (
        s.event_count_30d >= 20
        and s.probe_success_ratio_30d >= 0.7
        and s.probe_transition_count_30d <= 2
        and s.class_transition_count_30d <= 1
        and s.dormancy_days < 7
    ):
        return RegimeResult("stable", [
            "sustained_activity", "probe_consistent", "low_class_churn",
        ])

    # 9) Fallback: active but no strong pattern
    if s.event_count_30d > 0:
        return RegimeResult("stable", ["active_no_strong_pattern"])

    # 10) Absolute fallback
    if not s.declared_record and not s.has_labeler_service and not s.observed_as_src:
        return RegimeResult("inactive", ["insufficient_signal"])

    return RegimeResult("inactive", ["insufficient_signal"])


# ---------------------------------------------------------------------------
# Auditability risk (0-100)
# ---------------------------------------------------------------------------

def score_auditability_risk(s: LabelerSignals) -> ScoreResult:
    score = 0.0
    reasons: list[str] = []

    # Visibility class baseline (matches classify.py output values)
    vis = {
        "declared": 10, "protocol_public": 25,
        "observed_only": 70, "unresolved": 80,
    }
    score += vis.get(s.visibility_class, 80)
    reasons.append(f"visibility_{s.visibility_class}")

    # Auditability
    score += {"high": 0, "medium": 10, "low": 20}.get(s.auditability, 20)
    reasons.append(f"auditability_{s.auditability}")

    # Missing surfaces
    if not s.declared_record:
        score += 8
        reasons.append("missing_declared_record")
    if not s.has_labeler_service:
        score += 10
        reasons.append("missing_labeler_service")
    if not s.has_label_key:
        score += 5
        reasons.append("missing_label_key")

    # Probe quality
    if s.probe_count_30d == 0:
        score += 20
        reasons.append("no_probe_history")
    else:
        if s.probe_success_ratio_30d < 0.4:
            score += 15
            reasons.append("probe_success_low")
        elif s.probe_success_ratio_30d < 0.7:
            score += 8
            reasons.append("probe_success_mixed")
        if s.probe_transition_count_30d >= 6:
            score += 12
            reasons.append("probe_flapping_30d")
        elif s.probe_transition_count_30d >= 3:
            score += 6
            reasons.append("probe_some_flapping")

    # Active observed-only
    if s.visibility_class == "observed_only" and s.event_count_30d > 0:
        score += 10
        reasons.append("active_observed_only")

    # Warmup
    warming, _ = _is_warming_up(s)
    if warming:
        score += 5
        reasons.append("warmup_active")

    # Confidence
    score += {"high": 0, "medium": 4, "low": 10}.get(s.classification_confidence, 10)
    reasons.append(f"classification_confidence_{s.classification_confidence}")

    final = _clamp(score)
    return ScoreResult(final, _band(final), reasons)


# ---------------------------------------------------------------------------
# Inference risk (0-100)
# ---------------------------------------------------------------------------

def score_inference_risk(s: LabelerSignals, regime: RegimeResult) -> ScoreResult:
    score = 0.0
    reasons: list[str] = []

    # Warmup
    warming, _ = _is_warming_up(s)
    if warming:
        score += 35
        reasons.append("warmup_active")

    # Volume
    if s.event_count_30d == 0:
        score += 25
        reasons.append("no_events_30d")
    elif s.event_count_30d < 5:
        score += 18
        reasons.append("very_low_volume_30d")
    elif s.event_count_30d < 20:
        score += 10
        reasons.append("low_volume_30d")

    # Probe sparsity
    if s.probe_count_30d == 0:
        score += 15
        reasons.append("no_probe_history")
    elif s.probe_count_30d < 5:
        score += 8
        reasons.append("sparse_probe_history")

    if s.probe_transition_count_30d >= 6:
        score += 15
        reasons.append("probe_flapping_30d")
    elif s.probe_transition_count_30d >= 3:
        score += 8
        reasons.append("probe_some_flapping")

    # Class/confidence churn
    if s.class_transition_count_30d >= 3:
        score += 20
        reasons.append("high_class_churn")
    elif s.class_transition_count_30d >= 1:
        score += 10
        reasons.append("recent_class_change")

    if s.confidence_transition_count_30d >= 3:
        score += 10
        reasons.append("confidence_churn")
    elif s.confidence_transition_count_30d >= 1:
        score += 5
        reasons.append("confidence_changed")

    # Classification confidence
    score += {"high": 0, "medium": 8, "low": 18}.get(s.classification_confidence, 18)
    reasons.append(f"classification_confidence_{s.classification_confidence}")

    # Cadence irregularity
    irr = cadence_irregularity(s.interarrival_secs_7d)
    if irr >= 70:
        score += 12
        reasons.append("cadence_irregularity_high")
    elif irr >= 40:
        score += 6
        reasons.append("cadence_irregularity_medium")

    # Regime adjustments
    regime_adj = {
        "stable": -8, "flapping": 10, "degraded": 10,
        "ghost_declared": 8, "dark_operational": 8,
        "warming_up": 0, "inactive": 0, "bursty": 0,
    }
    score += regime_adj.get(regime.regime_state, 0)
    reasons.append(f"regime_{regime.regime_state}")

    # Test/dev — reason only, no score
    if s.likely_test_dev:
        reasons.append("likely_test_dev")

    final = _clamp(score)
    return ScoreResult(final, _band(final), reasons)


# ---------------------------------------------------------------------------
# Temporal coherence (0-100, high = good)
# ---------------------------------------------------------------------------

def score_temporal_coherence(s: LabelerSignals, regime: RegimeResult) -> ScoreResult:
    score = 50.0
    reasons: list[str] = []

    # Volume
    if s.event_count_30d >= 50:
        score += 20
        reasons.append("volume_high_30d")
    elif s.event_count_30d >= 20:
        score += 10
        reasons.append("volume_good_30d")
    elif s.event_count_30d < 5:
        score -= 15
        reasons.append("volume_low_30d")

    # Dormancy
    if s.dormancy_days >= 30:
        score -= 25
        reasons.append("dormant_30d")
    elif s.dormancy_days >= 7:
        score -= 10
        reasons.append("dormant_7d")

    # Probe flapping
    if s.probe_transition_count_30d >= 6:
        score -= 20
        reasons.append("probe_flapping_30d")
    elif s.probe_transition_count_30d >= 3:
        score -= 10
        reasons.append("probe_some_flapping")

    # Class churn
    if s.class_transition_count_30d >= 3:
        score -= 15
        reasons.append("high_class_churn")
    elif s.class_transition_count_30d >= 1:
        score -= 8
        reasons.append("recent_class_change")

    # Cadence irregularity
    irr = cadence_irregularity(s.interarrival_secs_7d)
    if irr >= 70:
        score -= 15
        reasons.append("cadence_irregularity_high")
    elif irr >= 40:
        score -= 8
        reasons.append("cadence_irregularity_medium")

    # Warmup
    warming, _ = _is_warming_up(s)
    if warming:
        score -= 20
        reasons.append("warmup_active")

    # Regime
    regime_adj = {
        "stable": 10, "bursty": -8, "flapping": -8, "degraded": -8,
        "dark_operational": -8, "ghost_declared": -6,
        "warming_up": -6, "inactive": 0,
    }
    score += regime_adj.get(regime.regime_state, 0)
    reasons.append(f"regime_{regime.regime_state}")

    final = _clamp(score)
    return ScoreResult(final, _band(final), reasons)
