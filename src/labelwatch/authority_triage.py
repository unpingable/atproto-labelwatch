"""labelwatch.authority_effect_triage.v0 — rank unprofiled volume, propose
receipted candidates.

The dashboard already knows where the volume sits. This module writes the
queue and proposes effect classifications; humans ratify, the machine never
auto-promotes a reputational claim.

Composes with:
  - label_family.classify_authority_effect (tier 1, registry)
  - label_family.LABELER_DEFAULT_EFFECT (tier 1, bespoke-labeler fallback)
  - emitter_classifier.classify_one (tiers 2/3/4)

Output:
  - per-candidate `labelwatch.authority_effect_inference.v0` receipts
  - one `labelwatch.authority_effect_triage.v0` index receipt

Doctrine:
  - Detect-only structural: receipts written; no labels emitted.
  - Weather, never verdict: candidates are "we read this labeler's
    testimony as X", not subject adjudication.
  - Aggregate-first: scope is (labeler, value); targets appear only as
    citation samples.
  - Co-presence is not corroboration: co_occurring_labels is citation,
    NOT rationale support.
  - No LLM in v0.

The only promotion path that bypasses human review is the safe pattern
class (spam/scam/malware/phishing variants). Reputational, visibility,
and emitter-described inferences all require human ratification before
they enter AUTHORITY_EFFECT_MAP.

See specs/gaps/gap-spec-authority-effect-inference-v0.md and
gap-spec-authority-effect-triage-001.md.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from . import db as db_mod
from .emitter_classifier import (
    SEMANTIC_SOURCE_EMITTER_DESCRIBED,
    SEMANTIC_SOURCE_PATTERN_PROFILE,
    SEMANTIC_SOURCE_RAW_FALLBACK,
    _find_any_emitter_definition,
    classify_one,
)
from .label_family import (
    AUTHORITY_EFFECT_MAP,
    FAMILY_VERSION,
    LABELER_DEFAULT_EFFECT,
    classify_authority_effect,
    normalize_family,
)
from .utils import (
    format_ts,
    get_git_commit,
    hash_sha256,
    now_utc,
    stable_json,
)

log = logging.getLogger(__name__)


INDEX_RECEIPT_KIND = "labelwatch.authority_effect_triage.v0"
INFERENCE_RECEIPT_KIND = "labelwatch.authority_effect_inference.v0"
RECEIPT_SCHEMA_VERSION = 0

DEFAULT_TOP_VALUES = 20
DEFAULT_TOP_LABELERS = 10
DEFAULT_WINDOW_DAYS = 7
SAMPLE_TARGET_LIMIT = 5


# ---------------------------------------------------------------------------
# Safe-class pattern table (the ONLY auto_pattern_matched path in v0)
# ---------------------------------------------------------------------------
#
# These are the only patterns that bypass human review. Narrow on purpose.
# Decades of precedent + low political surface area: spam / scam / malware
# / phishing variants are reliably safety-adjacent regardless of the
# emitter's framing.
#
# Reputational tier-3 patterns (nazi/racist/terf/...) and visibility-class
# tier-3 patterns (hide/mute/adult/...) are HANDLED ELSEWHERE — they get
# classified but flagged needs_human_review. Auto-promotion is reserved
# for the safety class only.
_SAFE_PATTERN_PROFILES: List[Tuple[re.Pattern, str, str]] = [
    (re.compile(r"^(?:.+-)?spam(?:-.+)?$"),       "safety", "spam variant"),
    (re.compile(r"^(?:.+-)?scam(?:-.+)?$"),       "safety", "scam variant"),
    (re.compile(r"^(?:.+-)?phishing(?:-.+)?$"),   "safety", "phishing variant"),
    (re.compile(r"^(?:.+-)?phish(?:-.+)?$"),      "safety", "phish variant"),
    (re.compile(r"^(?:.+-)?malware(?:-.+)?$"),    "safety", "malware variant"),
    (re.compile(r"^(?:.+-)?impersonation(?:-.+)?$"), "safety", "impersonation variant"),
]

# Authority effect we propose for safe-class matches. "advisory" rather
# than "reputational" because the safety class describes a behavior/threat
# class rather than rendering a normative judgment about the actor.
_SAFE_PATTERN_AUTHORITY_EFFECT = "advisory"


def _safe_pattern_match(label_value: str) -> Optional[Tuple[str, str, str]]:
    """Return (pattern_class, effect, note) if label_value matches a safe
    auto-promote pattern, else None.

    Exact AUTHORITY_EFFECT_MAP membership is checked by the caller (tier 1);
    this only fires when tier 1 misses. Safe patterns are intentionally
    narrow — we'd rather miss a borderline match than auto-promote wrong.
    """
    for rx, cls, note in _SAFE_PATTERN_PROFILES:
        if rx.match(label_value):
            return (cls, _SAFE_PATTERN_AUTHORITY_EFFECT, note)
    return None


# ---------------------------------------------------------------------------
# Tier resolution wrapper
# ---------------------------------------------------------------------------

def _resolve_tier1(label_value: str, labeler_did: str) -> Optional[str]:
    """Tier 1: registry. Returns the effect string if already classified,
    else None. AUTHORITY_EFFECT_MAP keyed by family wins absolutely;
    LABELER_DEFAULT_EFFECT applies only when the family has no map entry.
    """
    family = normalize_family(label_value)
    direct = AUTHORITY_EFFECT_MAP.get(family)
    if direct:
        return direct
    # Labeler-level fallback: only when family is not in AUTHORITY_EFFECT_MAP
    if labeler_did in LABELER_DEFAULT_EFFECT:
        return LABELER_DEFAULT_EFFECT[labeler_did]
    return None


def _confidence_for_classification(cls: Dict[str, Any]) -> str:
    """Synthesize a confidence level from the emitter_classifier output."""
    source = cls.get("semantic_source")
    if source == SEMANTIC_SOURCE_EMITTER_DESCRIBED:
        basis = cls.get("classification_basis")
        if basis == "emitter_label_metadata":
            return "low"  # metadata-only; no locale text
        tone = cls.get("tone")
        effect = cls.get("authority_effect")
        # Strong signals: editorial tone + reputational/visibility, or
        # neutral tone + clear descriptive verb (descriptive effect)
        if tone == "editorial" and effect in ("reputational", "visibility_affecting"):
            return "high"
        if tone == "neutral" and effect == "descriptive":
            return "high"
        if effect == "ambiguous":
            return "low"
        return "medium"
    if source == SEMANTIC_SOURCE_PATTERN_PROFILE:
        return "medium"  # tier-3 ceiling
    return "none"


# ---------------------------------------------------------------------------
# Promotion rules (the heart of the spec table)
# ---------------------------------------------------------------------------

# Promotion statuses (spec §Promotion statuses)
PROMOTION_AUTO_PATTERN_MATCHED = "auto_pattern_matched"
PROMOTION_NEEDS_HUMAN_REVIEW = "needs_human_review"
PROMOTION_REFUSED_INSUFFICIENT_EVIDENCE = "refused_insufficient_evidence"
PROMOTION_PROPOSED = "proposed"


def _assign_promotion_status(
    tier: str,
    classification: Dict[str, Any],
    confidence: str,
    safe_pattern: Optional[Tuple[str, str, str]],
) -> str:
    """Map (tier, classification, confidence, safe_pattern hit) → promotion
    status. The ONLY auto_pattern_matched path is the safe pattern table
    above.
    """
    if tier == "emitter_described":
        if confidence == "low":
            return PROMOTION_REFUSED_INSUFFICIENT_EVIDENCE
        return PROMOTION_NEEDS_HUMAN_REVIEW
    if tier == "pattern_profile":
        if safe_pattern is not None:
            return PROMOTION_AUTO_PATTERN_MATCHED
        return PROMOTION_NEEDS_HUMAN_REVIEW
    if tier == "raw_fallback":
        return PROMOTION_PROPOSED
    # Defensive fall-through (tier1 hits never reach here)
    return PROMOTION_PROPOSED


def _refusal_grounds(tier: str, classification: Dict[str, Any]) -> List[str]:
    """Standard refusal grounds emitted on every inference receipt. These
    are not failure modes — they are the boundaries of what the inference
    is permitted to claim, made explicit so the receipt cannot be quietly
    over-read.
    """
    grounds = [
        "Cannot infer whether labeled content is true or false.",
        "Cannot infer downstream moderation behavior without direct observation.",
        "Cannot infer private labeler intent beyond public testimony.",
    ]
    if tier == "raw_fallback":
        grounds.append(
            "No emitter description and no pattern match; classification "
            "declined."
        )
    elif tier == "pattern_profile":
        grounds.append(
            "Pattern match is structural over the label string; the emitter's "
            "framing may diverge."
        )
    return grounds


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

_AGGREGATE_BY_LABELER_VAL_SQL = """
    SELECT labeler_did, val, COUNT(*) AS n
    FROM label_events
    WHERE ts >= ? AND ts < ?
      AND neg = 0
    GROUP BY labeler_did, val
"""


def aggregate_window(
    conn: sqlite3.Connection, *, start_ts: str, end_ts: str,
) -> List[Dict[str, Any]]:
    """Pull (labeler_did, val, count) for the window. Single SCAN over
    label_events.ts range — bounded by the window, not all-time.

    Returns rows annotated with `family` and `authority_effect` (tier 1
    resolution), so the caller can split profiled vs unprofiled in Python.
    """
    rows = conn.execute(
        _AGGREGATE_BY_LABELER_VAL_SQL, (start_ts, end_ts)
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        val = r["val"]
        labeler = r["labeler_did"]
        family = normalize_family(val)
        tier1 = _resolve_tier1(val, labeler)
        out.append({
            "labeler_did": labeler,
            "val": val,
            "family": family,
            "event_count": int(r["n"]),
            "authority_effect_tier1": tier1,  # None if unprofiled
        })
    return out


def _attachment_locus(uri: str) -> str:
    """Classify a label target URI into account/post/profile/list. Cheap
    string heuristic; no regex compile per call needed.
    """
    if not uri:
        return "unknown"
    if uri.startswith("did:"):
        return "account"
    if "/app.bsky.feed.post/" in uri:
        return "post"
    if "/app.bsky.actor.profile/" in uri:
        return "profile"
    if "/app.bsky.graph.list/" in uri:
        return "list"
    return "other"


_ATTACHMENT_SQL = """
    SELECT uri FROM label_events
    WHERE labeler_did = ? AND val = ?
      AND ts >= ? AND ts < ?
      AND neg = 0
    LIMIT ?
"""


def attachment_locus_split(
    conn: sqlite3.Connection,
    labeler_did: str,
    val: str,
    *,
    start_ts: str,
    end_ts: str,
    sample_cap: int = 500,
) -> Tuple[Dict[str, int], List[str]]:
    """Sample up to `sample_cap` URIs for (labeler, val) in window; return
    (locus → count map, top-K sample URIs).
    """
    rows = conn.execute(
        _ATTACHMENT_SQL, (labeler_did, val, start_ts, end_ts, sample_cap)
    ).fetchall()
    locus_counts: Dict[str, int] = {}
    sample: List[str] = []
    for r in rows:
        uri = r["uri"] or ""
        locus = _attachment_locus(uri)
        locus_counts[locus] = locus_counts.get(locus, 0) + 1
        if len(sample) < SAMPLE_TARGET_LIMIT:
            sample.append(uri)
    return locus_counts, sample


_LABELER_INFO_SQL = """
    SELECT labeler_did, handle, display_name, description
    FROM labelers
    WHERE labeler_did = ?
"""


def labeler_info(conn: sqlite3.Connection, did: str) -> Dict[str, Any]:
    row = conn.execute(_LABELER_INFO_SQL, (did,)).fetchone()
    if row is None:
        return {"labeler_did": did, "handle": None}
    return {
        "labeler_did": row["labeler_did"],
        "handle": row["handle"],
        "display_name": row["display_name"],
        "description": row["description"],
    }


# ---------------------------------------------------------------------------
# Candidate construction
# ---------------------------------------------------------------------------

def build_candidate(
    conn: sqlite3.Connection,
    *,
    labeler_did: str,
    val: str,
    event_count: int,
    start_ts: str,
    end_ts: str,
    window_label: str,
) -> Dict[str, Any]:
    """Generate one labelwatch.authority_effect_inference.v0 receipt for a
    single (labeler, val) candidate.
    """
    emitter_def = _find_any_emitter_definition(conn, val)
    classification = classify_one(val, emitter_def)

    tier_map = {
        SEMANTIC_SOURCE_EMITTER_DESCRIBED: "emitter_described",
        SEMANTIC_SOURCE_PATTERN_PROFILE: "pattern_profile",
        SEMANTIC_SOURCE_RAW_FALLBACK: "raw_fallback",
    }
    tier = tier_map.get(classification.get("semantic_source"), "raw_fallback")

    # Safe-pattern check: fires on pattern_profile (emitter_classifier
    # already matched a structural pattern but we override with the safe
    # class) AND on raw_fallback (emitter_classifier matched nothing, but
    # our narrower safe-pattern table catches spam/scam/phishing/malware
    # variants). NEVER overrides emitter_described — emitter testimony
    # always wins.
    safe_pattern = None
    if tier in ("pattern_profile", "raw_fallback"):
        safe_pattern = _safe_pattern_match(val)
        if safe_pattern is not None:
            pattern_class, effect, note = safe_pattern
            classification = dict(classification)
            classification["semantic_source"] = SEMANTIC_SOURCE_PATTERN_PROFILE
            classification["authority_effect"] = effect
            ev = dict(classification.get("evidence") or {})
            ev["safe_pattern_class"] = pattern_class
            ev["safe_pattern_note"] = note
            classification["evidence"] = ev
            tier = "pattern_profile"

    confidence = _confidence_for_classification(classification)
    promotion_status = _assign_promotion_status(
        tier, classification, confidence, safe_pattern,
    )

    # raw_fallback: do not propose an effect at all
    candidate_effect: Optional[str]
    if tier == "raw_fallback":
        candidate_effect = None
    else:
        candidate_effect = classification.get("authority_effect")

    locus_counts, sample_targets = attachment_locus_split(
        conn, labeler_did, val, start_ts=start_ts, end_ts=end_ts,
    )
    attachment_loci = sorted(locus_counts.keys()) if locus_counts else []

    info = labeler_info(conn, labeler_did)

    rationale = _build_rationale(tier, classification, candidate_effect)

    evidence_block = {
        "labeler_description": _truncate(info.get("description"), 500),
        "labeler_description_source": (
            "discovery_events.labelValueDefinition"
            if tier == "emitter_described"
            else "labelers.description"
        ),
        "observed_values": [val],
        "attachment_loci": attachment_loci,
        "attachment_locus_counts": locus_counts,
        "event_count": event_count,
        "sample_targets": sample_targets,
        "time_window": {"first_seen": start_ts, "last_seen": end_ts},
    }
    # Carry the emitter classifier's evidence excerpt verbatim — citation,
    # not derived prose. The receipt is auditable to its sources.
    cls_evidence = classification.get("evidence") or {}
    if cls_evidence:
        evidence_block["emitter_classifier_evidence"] = cls_evidence

    receipt = {
        "receipt_kind": INFERENCE_RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "labeler_did": labeler_did,
        "labeler_handle": info.get("handle"),
        "label_value": val,
        "scope": "labeler_value",
        "tier": tier,
        "candidate_authority_effect": candidate_effect,
        "confidence": confidence,
        "promotion_status": promotion_status,
        "evidence": evidence_block,
        "rationale": rationale,
        "refusals": _refusal_grounds(tier, classification),
        "generated_at": format_ts(now_utc()),
        "git_commit": get_git_commit(),
        "family_version": FAMILY_VERSION,
        "window": window_label,
    }
    receipt["receipt_hash"] = hash_sha256(
        stable_json({k: v for k, v in receipt.items() if k != "receipt_hash"})
    )
    return receipt


def _truncate(s: Optional[str], n: int) -> Optional[str]:
    if not s:
        return None
    return s if len(s) <= n else s[:n] + "…"


def _build_rationale(
    tier: str,
    classification: Dict[str, Any],
    candidate_effect: Optional[str],
) -> List[str]:
    """Cite the evidence the classifier rested on. Co-presence and target
    samples are NOT cited as rationale support (co-presence-is-not-
    corroboration doctrine).
    """
    lines: List[str] = []
    if tier == "emitter_described":
        basis = classification.get("classification_basis")
        tone = classification.get("tone")
        scope = classification.get("target_scope")
        excerpt = (classification.get("evidence") or {}).get("description_excerpt") or ""
        if basis == "emitter_locale_description":
            lines.append("Read from emitter's labelValueDefinition locale description.")
        else:
            lines.append("Read from emitter's labelValueDefinition metadata (no locale text).")
        if tone == "editorial":
            lines.append("Description language carries editorial/value-laden framing.")
        if tone == "neutral" and candidate_effect == "descriptive":
            lines.append("Description language is neutral and descriptive.")
        if scope and scope != "ambiguous":
            lines.append(f"Description target scope: {scope}.")
        if excerpt:
            lines.append(f'Source excerpt: "{excerpt[:200]}"')
    elif tier == "pattern_profile":
        ev = classification.get("evidence") or {}
        pat = ev.get("matched_pattern") or ev.get("safe_pattern_note") or "pattern"
        lines.append(f"Matched pattern: {pat}.")
        if ev.get("safe_pattern_class"):
            lines.append(
                f"Safe pattern class: {ev['safe_pattern_class']} — auto-promote "
                "eligible per the narrow safety-pattern exception."
            )
    elif tier == "raw_fallback":
        lines.append(
            "No emitter labelValueDefinition found and no pattern matched; "
            "label surfaces to the unknown-label watchlist for human review."
        )
    return lines


# ---------------------------------------------------------------------------
# Triage driver
# ---------------------------------------------------------------------------

def run_triage(
    db_path: str,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    top_values: int = DEFAULT_TOP_VALUES,
    top_labelers: int = DEFAULT_TOP_LABELERS,
    out_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the triage pass and return the index receipt.

    When out_dir is provided, also writes per-candidate inference receipts
    to `<out_dir>/authority_effect_inference/` and the index receipt to
    `<out_dir>/authority_effect_triage/`. Otherwise the index receipt
    carries the per-candidate receipts inline.
    """
    now = now_utc()
    start = now - timedelta(days=window_days)
    start_ts = format_ts(start)
    end_ts = format_ts(now)
    window_label = f"{window_days}d"

    conn = db_mod.connect(db_path, readonly=True)
    try:
        rows = aggregate_window(conn, start_ts=start_ts, end_ts=end_ts)
    finally:
        conn.close()

    total_events = sum(r["event_count"] for r in rows)
    unprofiled_rows = [r for r in rows if r["authority_effect_tier1"] is None]
    unprofiled_events = sum(r["event_count"] for r in unprofiled_rows)

    # Top-N values by event count among unprofiled (aggregated across labelers)
    by_val: Dict[str, Dict[str, Any]] = {}
    for r in unprofiled_rows:
        slot = by_val.setdefault(r["val"], {
            "val": r["val"],
            "family": r["family"],
            "total": 0,
            "by_labeler": [],
        })
        slot["total"] += r["event_count"]
        slot["by_labeler"].append((r["labeler_did"], r["event_count"]))
    val_ranked = sorted(by_val.values(), key=lambda x: x["total"], reverse=True)
    top_vals_share = (
        sum(v["total"] for v in val_ranked[:top_values]) / unprofiled_events
        if unprofiled_events > 0 else 0.0
    )

    # Top-M labelers by event count among unprofiled
    by_labeler: Dict[str, int] = {}
    for r in unprofiled_rows:
        by_labeler[r["labeler_did"]] = by_labeler.get(r["labeler_did"], 0) + r["event_count"]
    labeler_ranked = sorted(by_labeler.items(), key=lambda x: x[1], reverse=True)
    top_labelers_share = (
        sum(n for _, n in labeler_ranked[:top_labelers]) / unprofiled_events
        if unprofiled_events > 0 else 0.0
    )

    # Build candidates: one per top-N value, using its dominant emitter.
    # Re-open the connection for per-candidate evidence lookups.
    conn = db_mod.connect(db_path, readonly=True)
    queue: List[Dict[str, Any]] = []
    try:
        for v in val_ranked[:top_values]:
            # Dominant emitter for this val
            top_labeler, top_count = max(v["by_labeler"], key=lambda x: x[1])
            receipt = build_candidate(
                conn,
                labeler_did=top_labeler,
                val=v["val"],
                event_count=v["total"],  # total across all labelers, not just dominant
                start_ts=start_ts,
                end_ts=end_ts,
                window_label=window_label,
            )
            queue.append(receipt)
    finally:
        conn.close()

    # Projected reduction: sum events for queue items that would be promoted
    # (auto_pattern_matched + needs_human_review, optimistic case)
    promotable_events = sum(
        v["total"] for v, c in zip(val_ranked[:top_values], queue)
        if c["promotion_status"] in (
            PROMOTION_AUTO_PATTERN_MATCHED,
            PROMOTION_NEEDS_HUMAN_REVIEW,
        )
    )
    auto_promotable_events = sum(
        v["total"] for v, c in zip(val_ranked[:top_values], queue)
        if c["promotion_status"] == PROMOTION_AUTO_PATTERN_MATCHED
    )

    projected = {
        "auto_promote_only": {
            "events_recovered": auto_promotable_events,
            "events_recovered_share_of_total": (
                auto_promotable_events / total_events if total_events else 0.0
            ),
            "new_unprofiled_events": unprofiled_events - auto_promotable_events,
            "new_unprofiled_share": (
                (unprofiled_events - auto_promotable_events) / total_events
                if total_events else 0.0
            ),
        },
        "auto_plus_human_ratified": {
            "events_recovered": promotable_events,
            "events_recovered_share_of_total": (
                promotable_events / total_events if total_events else 0.0
            ),
            "new_unprofiled_events": unprofiled_events - promotable_events,
            "new_unprofiled_share": (
                (unprofiled_events - promotable_events) / total_events
                if total_events else 0.0
            ),
        },
    }

    tier_breakdown: Dict[str, int] = {}
    promotion_breakdown: Dict[str, int] = {}
    for c in queue:
        tier_breakdown[c["tier"]] = tier_breakdown.get(c["tier"], 0) + 1
        promotion_breakdown[c["promotion_status"]] = (
            promotion_breakdown.get(c["promotion_status"], 0) + 1
        )

    index = {
        "receipt_kind": INDEX_RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": format_ts(now),
        "git_commit": get_git_commit(),
        "window": window_label,
        "window_start": start_ts,
        "window_end": end_ts,
        "family_version": FAMILY_VERSION,
        "params": {
            "top_values": top_values,
            "top_labelers": top_labelers,
        },
        "input_state": {
            "total_events_in_window": total_events,
            "unprofiled_events": unprofiled_events,
            "unprofiled_share": (
                unprofiled_events / total_events if total_events else 0.0
            ),
            "distinct_unprofiled_values": len(by_val),
            "distinct_unprofiled_labelers": len(by_labeler),
            "top_values_share_of_unprofiled": top_vals_share,
            "top_labelers_share_of_unprofiled": top_labelers_share,
        },
        "tier_breakdown": tier_breakdown,
        "promotion_breakdown": promotion_breakdown,
        "projected_reduction": projected,
        "top_labelers_unprofiled": [
            {"labeler_did": did, "event_count": n}
            for did, n in labeler_ranked[:top_labelers]
        ],
        "queue": queue,
    }
    index["receipt_hash"] = hash_sha256(
        stable_json({k: v for k, v in index.items() if k != "receipt_hash"})
    )

    if out_dir:
        _write_receipts(out_dir, index, queue)

    return index


def _write_receipts(
    out_dir: str, index: Dict[str, Any], queue: List[Dict[str, Any]],
) -> None:
    inf_dir = os.path.join(out_dir, "authority_effect_inference")
    tri_dir = os.path.join(out_dir, "authority_effect_triage")
    os.makedirs(inf_dir, exist_ok=True)
    os.makedirs(tri_dir, exist_ok=True)

    gen_stamp = index["generated_at"].replace(":", "").replace("-", "")
    for c in queue:
        safe_val = re.sub(r"[^A-Za-z0-9._-]", "_", c["label_value"])
        safe_did = re.sub(r"[^A-Za-z0-9._:-]", "_", c["labeler_did"])
        fname = f"{safe_did}__{safe_val}__{gen_stamp}.json"
        with open(os.path.join(inf_dir, fname), "w", encoding="utf-8") as f:
            json.dump(c, f, indent=2)

    idx_fname = f"triage__{gen_stamp}.json"
    with open(os.path.join(tri_dir, idx_fname), "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)


# ---------------------------------------------------------------------------
# Human-readable rendering
# ---------------------------------------------------------------------------

def render_text(index: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"=== {index['receipt_kind']} ===")
    lines.append(f"generated_at     : {index['generated_at']}")
    git = index.get("git_commit")
    lines.append(f"git_commit       : {git[:12] if git else 'unknown'}")
    lines.append(f"window           : {index['window']}  "
                 f"({index['window_start']} → {index['window_end']})")
    lines.append(f"family_version   : {index['family_version']}")
    lines.append("")

    inp = index["input_state"]
    lines.append("input_state:")
    lines.append(f"  total_events_in_window  : {inp['total_events_in_window']:,}")
    lines.append(f"  unprofiled_events       : {inp['unprofiled_events']:,}")
    lines.append(f"  unprofiled_share        : {inp['unprofiled_share']:.1%}")
    lines.append(f"  distinct unprofiled vals: {inp['distinct_unprofiled_values']:,}")
    lines.append(f"  distinct unprofiled labelers: {inp['distinct_unprofiled_labelers']:,}")
    lines.append(f"  top-{index['params']['top_values']} values share of unprofiled : {inp['top_values_share_of_unprofiled']:.1%}")
    lines.append(f"  top-{index['params']['top_labelers']} labelers share of unprofiled : {inp['top_labelers_share_of_unprofiled']:.1%}")
    lines.append("")

    lines.append("tier_breakdown:")
    for tier, n in sorted(index["tier_breakdown"].items()):
        lines.append(f"  {tier:<22} {n}")
    lines.append("")

    lines.append("promotion_breakdown:")
    for status, n in sorted(index["promotion_breakdown"].items()):
        lines.append(f"  {status:<32} {n}")
    lines.append("")

    proj = index["projected_reduction"]
    lines.append("projected_reduction:")
    lines.append(
        "  auto_promote_only        : recover "
        f"{proj['auto_promote_only']['events_recovered']:,} events "
        f"({proj['auto_promote_only']['events_recovered_share_of_total']:.1%} of total); "
        f"new unprofiled = {proj['auto_promote_only']['new_unprofiled_share']:.1%}"
    )
    lines.append(
        "  auto + human ratified    : recover "
        f"{proj['auto_plus_human_ratified']['events_recovered']:,} events "
        f"({proj['auto_plus_human_ratified']['events_recovered_share_of_total']:.1%} of total); "
        f"new unprofiled = {proj['auto_plus_human_ratified']['new_unprofiled_share']:.1%}"
    )
    lines.append("")

    lines.append(f"queue ({len(index['queue'])} candidates):")
    lines.append("")
    for i, c in enumerate(index["queue"], 1):
        handle = c.get("labeler_handle") or c["labeler_did"]
        lines.append(f"{i}. {handle} / {c['label_value']}")
        ev = c.get("evidence") or {}
        lines.append(f"   events            : {ev.get('event_count', 0):,}")
        loci = ev.get("attachment_loci") or []
        lines.append(f"   attachment_loci   : {', '.join(loci) if loci else '-'}")
        lines.append(f"   tier              : {c['tier']}")
        lines.append(f"   candidate         : {c.get('candidate_authority_effect') or '(unclassified)'}")
        lines.append(f"   confidence        : {c['confidence']}")
        lines.append(f"   promotion_status  : {c['promotion_status']}")
        for r in c.get("rationale", []):
            lines.append(f"   rationale         · {r}")
        for ref in c.get("refusals", []):
            lines.append(f"   refusal           · {ref}")
        lines.append("")

    return "\n".join(lines)
