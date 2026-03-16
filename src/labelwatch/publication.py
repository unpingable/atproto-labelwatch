"""Publication readiness assessment for findings.

Decides whether a finding is ready to post, needs review, or should stay
internal. Rule-based v1 — conservative thresholds, no auto-posting.

Three tiers:
  - internal:   interesting signal, not postable yet
  - reviewable: probably worth posting, human should look
  - ready:      high confidence, draft copy attached

The caller (CLI or future automation) decides what to do with each tier.
This module never posts anything.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

from .boundary import classify_disagreement
from .label_family import (
    FAMILY_MAP,
    POLARITY_MAP,
    classify_domain,
    classify_kind,
    classify_polarity,
)
from .posting import FindingPost

log = logging.getLogger(__name__)


# ── Thresholds ──────────────────────────────────────────────────────────

# Minimum shared targets for each tier
MIN_TARGETS_REVIEWABLE = 15
MIN_TARGETS_READY = 25

# Minimum dominant-family share (0–1) for classification confidence
MIN_SHARE_REVIEWABLE = 0.4
MIN_SHARE_READY = 0.6

# Persistence: minimum distinct compute windows the fight must appear in
MIN_WINDOWS_REVIEWABLE = 2
MIN_WINDOWS_READY = 3

# JSD floor: median JSD across edges must exceed this
MIN_MEDIAN_JSD_REVIEWABLE = 0.3
MIN_MEDIAN_JSD_READY = 0.5


@dataclass(frozen=True)
class PublicationAssessment:
    """Result of assessing a finding's readiness for publication."""

    tier: str  # "internal" | "reviewable" | "ready"
    reasons: tuple[str, ...]  # why this tier (not a higher one)
    promotions: tuple[str, ...]  # what's good about this finding
    finding: FindingPost
    disagreement_type: str
    n_targets: int
    median_jsd: float
    top_share_a: float
    top_share_b: float
    n_windows: int
    previously_posted: bool


@dataclass
class _Scores:
    """Internal scoring workspace."""

    n_targets: int = 0
    median_jsd: float = 0.0
    top_share_a: float = 0.0
    top_share_b: float = 0.0
    n_windows: int = 0
    dtype: str = ""
    family_a_known: bool = False
    family_b_known: bool = False
    polarity_a_known: bool = False
    polarity_b_known: bool = False
    kind_a_known: bool = False
    kind_b_known: bool = False
    previously_posted: bool = False
    reasons: list[str] = field(default_factory=list)
    promotions: list[str] = field(default_factory=list)


def assess_finding(
    conn,
    labeler_a: str,
    labeler_b: str,
    edges: list[dict],
    finding: FindingPost,
    previously_posted: bool = False,
) -> PublicationAssessment:
    """Assess a fight-pair finding's readiness for publication.

    Args:
        conn: DB connection (for handle lookup, not used yet but reserved)
        labeler_a: DID of first labeler
        labeler_b: DID of second labeler
        edges: contradiction edge dicts for this pair
        finding: the formatted FindingPost
        previously_posted: whether this dedupe_key has been posted before

    Returns:
        PublicationAssessment with tier, reasons, and the original finding.
    """
    scores = _compute_scores(edges, previously_posted)
    tier = _determine_tier(scores)

    return PublicationAssessment(
        tier=tier,
        reasons=tuple(scores.reasons),
        promotions=tuple(scores.promotions),
        finding=finding,
        disagreement_type=scores.dtype,
        n_targets=scores.n_targets,
        median_jsd=scores.median_jsd,
        top_share_a=scores.top_share_a,
        top_share_b=scores.top_share_b,
        n_windows=scores.n_windows,
        previously_posted=previously_posted,
    )


def _compute_scores(edges: list[dict], previously_posted: bool) -> _Scores:
    """Extract scoring signals from edge data."""
    s = _Scores()
    s.previously_posted = previously_posted

    if not edges:
        s.reasons.append("no edges")
        return s

    # Volume: distinct targets
    targets = {e["target_uri"] for e in edges}
    s.n_targets = len(targets)

    # JSD: median across edges
    jsds = sorted(e["jsd"] for e in edges)
    mid = len(jsds) // 2
    s.median_jsd = jsds[mid] if len(jsds) % 2 else (jsds[mid - 1] + jsds[mid]) / 2

    # Dominant families and shares
    fam_counts_a: dict[str, int] = defaultdict(int)
    fam_counts_b: dict[str, int] = defaultdict(int)
    for e in edges:
        fam_counts_a[e["top_family_a"]] += 1
        fam_counts_b[e["top_family_b"]] += 1

    top_fam_a = max(fam_counts_a, key=fam_counts_a.get)  # type: ignore[arg-type]
    top_fam_b = max(fam_counts_b, key=fam_counts_b.get)  # type: ignore[arg-type]
    s.top_share_a = fam_counts_a[top_fam_a] / len(edges)
    s.top_share_b = fam_counts_b[top_fam_b] / len(edges)

    # Classification completeness
    s.family_a_known = top_fam_a in FAMILY_MAP.values() or top_fam_a in FAMILY_MAP
    s.family_b_known = top_fam_b in FAMILY_MAP.values() or top_fam_b in FAMILY_MAP
    s.polarity_a_known = classify_polarity(top_fam_a) != "unknown"
    s.polarity_b_known = classify_polarity(top_fam_b) != "unknown"
    s.kind_a_known = classify_kind(top_fam_a) != "unknown"
    s.kind_b_known = classify_kind(top_fam_b) != "unknown"

    # Disagreement type
    s.dtype = classify_disagreement(top_fam_a, top_fam_b)

    # Persistence: how many distinct compute windows do these edges span?
    windows = {e["computed_at"][:10] for e in edges if "computed_at" in e}
    s.n_windows = max(len(windows), 1)

    # Build reason/promotion lists
    if s.n_targets >= MIN_TARGETS_READY:
        s.promotions.append(f"high volume ({s.n_targets} targets)")
    elif s.n_targets >= MIN_TARGETS_REVIEWABLE:
        s.promotions.append(f"moderate volume ({s.n_targets} targets)")
    else:
        s.reasons.append(f"low volume ({s.n_targets} targets, need {MIN_TARGETS_REVIEWABLE})")

    if s.median_jsd >= MIN_MEDIAN_JSD_READY:
        s.promotions.append(f"strong divergence (JSD={s.median_jsd:.2f})")
    elif s.median_jsd >= MIN_MEDIAN_JSD_REVIEWABLE:
        s.promotions.append(f"moderate divergence (JSD={s.median_jsd:.2f})")
    else:
        s.reasons.append(f"weak divergence (JSD={s.median_jsd:.2f}, need {MIN_MEDIAN_JSD_REVIEWABLE})")

    min_share = min(s.top_share_a, s.top_share_b)
    if min_share >= MIN_SHARE_READY:
        s.promotions.append(f"clear dominant families ({min_share:.0%} min share)")
    elif min_share >= MIN_SHARE_REVIEWABLE:
        s.promotions.append(f"moderate family dominance ({min_share:.0%} min share)")
    else:
        s.reasons.append(f"weak family dominance ({min_share:.0%}, need {MIN_SHARE_REVIEWABLE:.0%})")

    if s.n_windows >= MIN_WINDOWS_READY:
        s.promotions.append(f"persistent ({s.n_windows} windows)")
    elif s.n_windows >= MIN_WINDOWS_REVIEWABLE:
        s.promotions.append(f"recurring ({s.n_windows} windows)")
    else:
        s.reasons.append(f"not yet persistent ({s.n_windows} window, need {MIN_WINDOWS_REVIEWABLE})")

    if not s.family_a_known or not s.family_b_known:
        s.reasons.append("unmapped family (classification incomplete)")

    if previously_posted:
        s.reasons.append("previously posted (cooldown)")

    return s


def _determine_tier(s: _Scores) -> str:
    """Map scores to a publication tier."""
    # Previously posted → internal (cooldown)
    if s.previously_posted:
        return "internal"

    # Hard gates: below these, always internal
    if s.n_targets < MIN_TARGETS_REVIEWABLE:
        return "internal"
    if s.median_jsd < MIN_MEDIAN_JSD_REVIEWABLE:
        return "internal"

    # Ready: all dimensions meet high thresholds
    if (
        s.n_targets >= MIN_TARGETS_READY
        and s.median_jsd >= MIN_MEDIAN_JSD_READY
        and min(s.top_share_a, s.top_share_b) >= MIN_SHARE_READY
        and s.n_windows >= MIN_WINDOWS_READY
        and s.family_a_known
        and s.family_b_known
    ):
        return "ready"

    # Reviewable: meets minimum thresholds
    if (
        min(s.top_share_a, s.top_share_b) >= MIN_SHARE_REVIEWABLE
        and s.family_a_known
        and s.family_b_known
    ):
        return "reviewable"

    return "internal"


def assess_all_fights(
    conn,
    findings: list[tuple[str, str, list[dict], FindingPost]],
    cooldown_days: int = 7,
) -> list[PublicationAssessment]:
    """Assess all fight-pair findings and return sorted by tier.

    Args:
        conn: DB connection
        findings: list of (labeler_a, labeler_b, edges, FindingPost) tuples
        cooldown_days: suppress previously-posted findings within this window

    Returns:
        List of PublicationAssessment, sorted: ready first, then reviewable,
        then internal. Within each tier, sorted by n_targets descending.
    """
    from .db import is_finding_posted

    assessments = []
    for la, lb, edges, finding in findings:
        posted = is_finding_posted(conn, finding.dedupe_key, cooldown_days)
        assessment = assess_finding(conn, la, lb, edges, finding, posted)
        assessments.append(assessment)

    tier_order = {"ready": 0, "reviewable": 1, "internal": 2}
    assessments.sort(key=lambda a: (tier_order.get(a.tier, 9), -a.n_targets))

    log.info(
        "assess_all_fights: %d findings — %d ready, %d reviewable, %d internal",
        len(assessments),
        sum(1 for a in assessments if a.tier == "ready"),
        sum(1 for a in assessments if a.tier == "reviewable"),
        sum(1 for a in assessments if a.tier == "internal"),
    )
    return assessments


def format_assessment(a: PublicationAssessment) -> str:
    """Human-readable summary of an assessment, for CLI output."""
    lines = [
        f"[{a.tier.upper()}] {a.finding.headline}",
        f"  Type: {a.disagreement_type.replace('_', ' ')}",
        f"  Targets: {a.n_targets}  JSD: {a.median_jsd:.2f}  "
        f"Shares: {a.top_share_a:.0%}/{a.top_share_b:.0%}  "
        f"Windows: {a.n_windows}",
    ]
    if a.promotions:
        lines.append(f"  + {', '.join(a.promotions)}")
    if a.reasons:
        lines.append(f"  - {', '.join(a.reasons)}")
    if a.previously_posted:
        lines.append("  (previously posted)")
    lines.append("")
    if a.tier in ("reviewable", "ready"):
        lines.append("  Draft:")
        for line in a.finding.render_text().splitlines():
            lines.append(f"    {line}")
        lines.append("")
    return "\n".join(lines)
