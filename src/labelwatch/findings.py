"""Format boundary fight pairs into postable findings.

This module is the transfer membrane between internal labelwatch signals
and externally legible posts. It is deliberately narrow: one finding class
(boundary disagreements), one output shape (FindingPost), one job.

Do not generalize this into a content framework.
"""
from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from .label_family import FAMILY_VERSION, classify_domain
from .posting import FindingPost
from .utils import format_ts

log = logging.getLogger(__name__)

# Public URL prefix for link cards
SITE_URL = "https://labelwatch.neutral.zone"


def _classify_disagreement(family_a: str, family_b: str) -> str:
    """Classify the type of disagreement between two families.

    Returns: 'taxonomy_shear' or 'substantive_disagreement'.
    (severity_difference requires polarity model — not yet implemented.)
    """
    domain_a = classify_domain(family_a)
    domain_b = classify_domain(family_b)
    if domain_a != domain_b:
        return "substantive_disagreement"
    return "taxonomy_shear"


def _dedupe_key(labeler_a: str, labeler_b: str, family_a: str,
                family_b: str) -> str:
    """Stable dedupe key for a fight-pair finding.

    Fight identity = sorted pair + sorted families. No date component —
    the same fight gets the same key forever. Cooldown logic (separate
    from identity) decides whether to repost if the fight persists.
    """
    pair = tuple(sorted([labeler_a, labeler_b]))
    families = tuple(sorted([family_a, family_b]))
    raw = f"{pair[0]}|{pair[1]}|{families[0]}|{families[1]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _handle_or_short_did(conn, did: str) -> str:
    """Look up handle for a DID, fall back to truncated DID."""
    row = conn.execute(
        "SELECT handle FROM labelers WHERE labeler_did = ?", (did,)
    ).fetchone()
    if row and row["handle"]:
        return row["handle"]
    return did[:32] + "..."


def _human_disagreement_type(dtype: str) -> str:
    """One-phrase explanation of disagreement type."""
    if dtype == "taxonomy_shear":
        return "Both agree the content is bad \u2014 they just categorize it differently."
    if dtype == "substantive_disagreement":
        return "They make different claims about what the content actually is."
    return ""


def format_fight_pair(
    conn,
    labeler_a: str,
    labeler_b: str,
    edges: list[dict],
) -> FindingPost | None:
    """Format a single fight pair into a FindingPost.

    Args:
        conn: DB connection (for handle lookup)
        labeler_a: DID of first labeler
        labeler_b: DID of second labeler
        edges: list of contradiction edge dicts for this pair

    Returns:
        FindingPost or None if the pair isn't interesting enough to post.
    """
    if not edges:
        return None

    # Aggregate: count shared targets, find dominant families
    targets = {e["target_uri"] for e in edges}
    n_targets = len(targets)

    # Count family occurrences per side
    family_counts_a: dict[str, int] = defaultdict(int)
    family_counts_b: dict[str, int] = defaultdict(int)
    for e in edges:
        family_counts_a[e["top_family_a"]] += 1
        family_counts_b[e["top_family_b"]] += 1

    top_family_a = max(family_counts_a, key=family_counts_a.get)  # type: ignore[arg-type]
    top_family_b = max(family_counts_b, key=family_counts_b.get)  # type: ignore[arg-type]

    handle_a = _handle_or_short_did(conn, labeler_a)
    handle_b = _handle_or_short_did(conn, labeler_b)

    dtype = _classify_disagreement(top_family_a, top_family_b)
    explanation = _human_disagreement_type(dtype)

    headline = f"Labeler disagreement: {handle_a} vs {handle_b}"

    summary = (
        f"{handle_a} calls it \u201c{top_family_a}\u201d; "
        f"{handle_b} calls it \u201c{top_family_b}\u201d. "
        f"{n_targets} shared target{'s' if n_targets != 1 else ''} "
        f"in the last 7 days."
    )
    if explanation:
        summary += f"\n\n{explanation}"

    return FindingPost(
        headline=headline,
        summary=summary,
        detail_url=f"{SITE_URL}/v1/registry",
        card_title=f"Labelwatch: {handle_a} vs {handle_b}",
        card_description=(
            f"Boundary disagreement: \u201c{top_family_a}\u201d vs "
            f"\u201c{top_family_b}\u201d on {n_targets} targets"
        ),
        dedupe_key=_dedupe_key(labeler_a, labeler_b, top_family_a,
                               top_family_b),
    )


def _is_protocol_action(family: str) -> bool:
    """Check if a family is an ATProto protocol action, not a policy claim.

    Families starting with ! are protocol/mechanical actions
    (e.g. !classification-forced, !hide, !warn). They're moderation-domain
    by routing but don't represent a labeler's policy stance on content.
    """
    return family.startswith("!")


def find_postable_fights(
    conn,
    now: datetime | None = None,
    min_targets: int = 10,
) -> list[FindingPost]:
    """Scan boundary edges for fight pairs worth posting about.

    Returns a list of FindingPost objects, one per qualifying fight pair.
    Caller is responsible for checking the sent-post ledger before posting.
    """
    if now is None:
        from .utils import now_utc
        now = now_utc()

    window_end = format_ts(now)
    window_start = format_ts(now - timedelta(days=7))

    rows = conn.execute("""
        SELECT target_uri, labeler_a, labeler_b,
               jsd, top_family_a, top_share_a, top_family_b, top_share_b,
               n_events_a, n_events_b
        FROM boundary_edges
        WHERE edge_type = 'contradiction'
          AND computed_at >= ? AND computed_at <= ?
          AND family_version = ?
        ORDER BY jsd DESC
    """, (window_start, window_end, FAMILY_VERSION)).fetchall()

    # Group by pair, filter moderation-only
    pair_edges: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        edge = dict(r)
        domain_a = classify_domain(edge["top_family_a"])
        domain_b = classify_domain(edge["top_family_b"])
        if domain_a != "moderation" or domain_b != "moderation":
            continue
        pair_key = (edge["labeler_a"], edge["labeler_b"])
        pair_edges[pair_key].append(edge)

    findings = []
    for (la, lb), edges in pair_edges.items():
        distinct_targets = len({e["target_uri"] for e in edges})
        if distinct_targets < min_targets:
            continue
        # Skip pairs where the dominant family on either side is a protocol
        # action (e.g. !classification-forced) — those are mechanical, not policy
        fam_counts_a: dict[str, int] = defaultdict(int)
        fam_counts_b: dict[str, int] = defaultdict(int)
        for e in edges:
            fam_counts_a[e["top_family_a"]] += 1
            fam_counts_b[e["top_family_b"]] += 1
        top_a = max(fam_counts_a, key=fam_counts_a.get)  # type: ignore[arg-type]
        top_b = max(fam_counts_b, key=fam_counts_b.get)  # type: ignore[arg-type]
        if _is_protocol_action(top_a) or _is_protocol_action(top_b):
            continue
        finding = format_fight_pair(conn, la, lb, edges)
        if finding is not None:
            findings.append(finding)

    log.info("find_postable_fights: %d pairs, %d postable", len(pair_edges), len(findings))
    return findings
