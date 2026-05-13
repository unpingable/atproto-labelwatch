"""Per-labeler signal health — detect "EPS steady but signal dead" blind spots.

Compares per-labeler 7d vs 30d event rates to detect labelers that have
gone dark, are degrading, or are new. Uses existing events_7d/events_30d
columns from the labelers table (populated by the derive loop).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

# Minimum 30d events to consider a labeler "was active"
MIN_30D_ACTIVE = 50

# 7d/30d ratio thresholds (7d is ~23% of 30d if steady)
# Below this ratio = degrading
DEGRADING_RATIO = 0.10

# Above this ratio = surging (unusual increase)
SURGING_RATIO = 0.50


# ---------------------------------------------------------------------------
# Signal classification
# ---------------------------------------------------------------------------

def classify_labeler_signal(
    events_7d: int,
    events_30d: int,
    is_reference: bool = False,
) -> str:
    """Classify a labeler's signal health.

    Returns one of: active, degrading, gone_dark, surging, quiet, new, never.
    """
    if events_30d == 0 and events_7d == 0:
        return "never"

    if events_30d < MIN_30D_ACTIVE:
        if events_7d > 0:
            return "new"
        return "quiet"

    if events_7d == 0:
        return "gone_dark"

    # Normalize: if rate were steady, 7d/30d ≈ 7/30 ≈ 0.233
    ratio = events_7d / events_30d if events_30d > 0 else 0

    if ratio < DEGRADING_RATIO:
        return "degrading"
    if ratio > SURGING_RATIO:
        return "surging"

    return "active"


# ---------------------------------------------------------------------------
# Signal health snapshot
# ---------------------------------------------------------------------------

def signal_health_snapshot(
    conn,
    flaky_reference_dids: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Compute signal health across all observed labelers.

    Returns a snapshot with per-labeler classifications, aggregate counts,
    and lists of gone-dark and degrading labelers for alerting.

    `flaky_reference_dids`: reference labelers with recurrent quiet behavior.
    Their gone_dark / degrading state routes to `flaky_reference_quiet`
    (advisory) rather than `reference_issues` (CRITICAL-driving). Popularity
    is not calibration reliability — a flaky popular reference should not
    take the whole verdict hostage.
    """
    flaky = set(flaky_reference_dids or ())

    rows = conn.execute(
        "SELECT labeler_did, handle, events_7d, events_30d, "
        "       is_reference, regime_state "
        "FROM labelers "
        "WHERE observed_as_src = 1 "
        "ORDER BY events_30d DESC",
    ).fetchall()

    classifications: Dict[str, int] = {
        "active": 0,
        "degrading": 0,
        "gone_dark": 0,
        "surging": 0,
        "quiet": 0,
        "new": 0,
        "never": 0,
    }

    gone_dark: List[Dict[str, Any]] = []
    degrading: List[Dict[str, Any]] = []
    surging: List[Dict[str, Any]] = []
    reference_issues: List[Dict[str, Any]] = []
    flaky_reference_quiet: List[Dict[str, Any]] = []

    total_7d = 0
    total_30d = 0

    for r in rows:
        ev7 = r["events_7d"] or 0
        ev30 = r["events_30d"] or 0
        is_ref = bool(r["is_reference"])
        is_flaky_ref = is_ref and r["labeler_did"] in flaky
        signal = classify_labeler_signal(ev7, ev30, is_ref)
        classifications[signal] = classifications.get(signal, 0) + 1
        total_7d += ev7
        total_30d += ev30

        info = {
            "labeler_did": r["labeler_did"],
            "handle": r["handle"],
            "events_7d": ev7,
            "events_30d": ev30,
            "regime_state": r["regime_state"],
            "is_reference": is_ref,
            "known_flaky": is_flaky_ref,
            "signal": signal,
        }

        if signal == "gone_dark":
            gone_dark.append(info)
            if is_ref:
                (flaky_reference_quiet if is_flaky_ref else reference_issues).append(info)
        elif signal == "degrading":
            degrading.append(info)
            if is_ref:
                (flaky_reference_quiet if is_flaky_ref else reference_issues).append(info)
        elif signal == "surging":
            surging.append(info)

    # Aggregate rate: is total 7d volume tracking 30d?
    overall_ratio = total_7d / total_30d if total_30d > 0 else None

    # Verdict — only non-flaky reference issues drive CRITICAL.
    if reference_issues:
        verdict = "CRITICAL"
    elif len(gone_dark) >= 3 or (gone_dark and overall_ratio and overall_ratio < DEGRADING_RATIO):
        verdict = "DEGRADED"
    elif gone_dark or degrading or flaky_reference_quiet:
        verdict = "WARN"
    else:
        verdict = "OK"

    return {
        "verdict": verdict,
        "classifications": classifications,
        "total_observed": len(rows),
        "total_events_7d": total_7d,
        "total_events_30d": total_30d,
        "overall_7d_30d_ratio": round(overall_ratio, 3) if overall_ratio else None,
        "gone_dark": gone_dark,
        "degrading": degrading,
        "surging": surging,
        "reference_issues": reference_issues,
        "flaky_reference_quiet": flaky_reference_quiet,
    }
