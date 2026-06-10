"""labelwatch authority-effect-promote: consume decisions TOML, apply to registry.

The machine proposes. The operator ratifies. The receipt remembers.

Reads:
  - a labelwatch.authority_effect_triage.v0 receipt (the triage queue)
  - a TOML decisions file the operator filled in via authority-effect-review

Writes:
  - src/labelwatch/label_family_overlay.py — auto-generated dict additions
    that label_family.py merges into AUTHORITY_EFFECT_MAP /
    LABELER_DEFAULT_EFFECT at module load (setdefault — hand-authored entries
    always win)
  - a labelwatch.authority_effect_promotion.v0 receipt summarizing what was
    ratified, deferred, rejected, or refused (with reasons)

Doctrine:
  - No runtime mutation. The overlay is a source file; promotions are git-
    reviewable diffs.
  - No fuzzy auto-approval. Every promotion comes from an explicit decisions
    file; the safe-pattern bypass that lives in triage is still subject to a
    `action = "ratify"` line here.
  - Hand-authored AUTHORITY_EFFECT_MAP entries always win on conflict. Overlay
    uses setdefault, not assignment — guarantees the curated registry is
    never silently overridden.
  - No registry mutation outside the overlay. Promotion never edits
    label_family.py directly.

Companion to: authority_triage (input), authority_review (decisions template).
"""
from __future__ import annotations

import json
import os
import sys
import tomllib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from . import authority_review
from .label_family import (
    AUTHORITY_EFFECT_MAP,
    FAMILY_VERSION,
    LABELER_DEFAULT_EFFECT,
)
from .utils import (
    format_ts,
    get_git_commit,
    hash_sha256,
    now_utc,
    stable_json,
)


PROMOTION_RECEIPT_KIND = "labelwatch.authority_effect_promotion.v0"
RECEIPT_SCHEMA_VERSION = 0

VALID_AUTHORITY_EFFECTS = {
    "enforcement_instruction",
    "visibility_affecting",
    "advisory",
    "reputational",
    "descriptive",
    "telemetry",
    "decorative",
}


# ---------------------------------------------------------------------------
# Decisions loading & validation
# ---------------------------------------------------------------------------

@dataclass
class DecisionsValidation:
    errors: List[str]
    decisions: List[Dict[str, Any]]
    family_version: str
    triage_generated_at: str
    receipt_path: str


def load_decisions(path: str) -> Dict[str, Any]:
    with open(path, "rb") as f:
        return tomllib.load(f)


def validate_decisions(
    triage_receipt: Dict[str, Any],
    decisions_doc: Dict[str, Any],
) -> DecisionsValidation:
    """Verify the decisions TOML matches the triage receipt and that each
    decision is structurally valid.

    Errors collected, not raised — caller decides whether to refuse promotion.
    """
    errors: List[str] = []
    decisions = decisions_doc.get("decisions") or []
    if not isinstance(decisions, list):
        errors.append("decisions: must be a list of [[decisions]] tables")
        decisions = []

    triage_cids = {c.get("candidate_id") for c in triage_receipt.get("queue", [])}
    seen_cids: set = set()

    for i, d in enumerate(decisions):
        if not isinstance(d, dict):
            errors.append(f"decisions[{i}]: not a table")
            continue
        cid = d.get("candidate_id")
        if not cid:
            errors.append(f"decisions[{i}]: missing candidate_id")
            continue
        if cid in seen_cids:
            errors.append(f"decisions[{i}] ({cid}): duplicate candidate_id")
        seen_cids.add(cid)
        if cid not in triage_cids:
            errors.append(
                f"decisions[{i}] ({cid}): candidate_id not present in triage receipt"
            )
        action = d.get("action") or ""
        if action not in authority_review.VALID_ACTIONS:
            errors.append(
                f"decisions[{i}] ({cid}): invalid action {action!r}; "
                f"must be one of {sorted(authority_review.VALID_ACTIONS)}"
            )
            continue
        if action == authority_review.ACTION_RATIFY:
            effect = (d.get("authority_effect") or "").strip()
            if not effect:
                errors.append(
                    f"decisions[{i}] ({cid}): action=ratify requires authority_effect"
                )
            elif effect not in VALID_AUTHORITY_EFFECTS:
                errors.append(
                    f"decisions[{i}] ({cid}): invalid authority_effect "
                    f"{effect!r}; must be one of {sorted(VALID_AUTHORITY_EFFECTS)}"
                )
            if not (d.get("reason") or "").strip():
                errors.append(
                    f"decisions[{i}] ({cid}): action=ratify requires reason"
                )
        if action == authority_review.ACTION_REJECT:
            if not (d.get("reason") or "").strip():
                errors.append(
                    f"decisions[{i}] ({cid}): action=reject requires reason"
                )

    # Cross-check: every triage candidate should appear in decisions (catch
    # operator omissions). Not an error — surfaced as a warning in the
    # promotion receipt instead.
    return DecisionsValidation(
        errors=errors,
        decisions=decisions,
        family_version=str(decisions_doc.get("family_version") or ""),
        triage_generated_at=str(decisions_doc.get("triage_generated_at") or ""),
        receipt_path=str(decisions_doc.get("receipt_path") or ""),
    )


# ---------------------------------------------------------------------------
# Overlay file: read existing → merge → write back
# ---------------------------------------------------------------------------

DEFAULT_OVERLAY_PATH = "src/labelwatch/label_family_overlay.py"


def load_existing_overlay(overlay_path: str) -> Dict[str, Dict[str, str]]:
    """Load existing overlay additions if any, else return empty dicts.

    Importing the module would mutate label_family at import time and tangle
    promotion logic with side effects; instead we parse the file's two
    top-level dict literals via the `ast` module. The overlay is auto-
    generated so its shape is predictable — fail loudly on parse error
    rather than silently lose entries.
    """
    if not os.path.exists(overlay_path):
        return {"AUTHORITY_EFFECT_ADDITIONS": {}, "LABELER_DEFAULT_EFFECT_ADDITIONS": {}}
    import ast
    with open(overlay_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=overlay_path)
    out: Dict[str, Dict[str, str]] = {
        "AUTHORITY_EFFECT_ADDITIONS": {},
        "LABELER_DEFAULT_EFFECT_ADDITIONS": {},
    }
    for node in tree.body:
        if not isinstance(node, ast.AnnAssign) and not isinstance(node, ast.Assign):
            continue
        target_name = _assign_target_name(node)
        if target_name not in out:
            continue
        value = _ast_value(node)
        if not isinstance(value, ast.Dict):
            raise ValueError(
                f"{overlay_path}: {target_name} must be a dict literal"
            )
        as_dict: Dict[str, str] = {}
        for k_node, v_node in zip(value.keys, value.values):
            if not isinstance(k_node, ast.Constant) or not isinstance(k_node.value, str):
                raise ValueError(
                    f"{overlay_path}: {target_name} keys must be string literals"
                )
            if not isinstance(v_node, ast.Constant) or not isinstance(v_node.value, str):
                raise ValueError(
                    f"{overlay_path}: {target_name} values must be string literals"
                )
            as_dict[k_node.value] = v_node.value
        out[target_name] = as_dict
    return out


def _assign_target_name(node):
    import ast
    if isinstance(node, ast.AnnAssign):
        return node.target.id if isinstance(node.target, ast.Name) else None
    targets = node.targets
    if len(targets) != 1 or not isinstance(targets[0], ast.Name):
        return None
    return targets[0].id


def _ast_value(node):
    import ast
    if isinstance(node, ast.AnnAssign):
        return node.value
    return node.value


_OVERLAY_HEADER = '''\
"""Ratified authority_effect additions — AUTO-GENERATED. Do not hand-edit.

Generated by `labelwatch authority-effect-promote`. Manual edits will be
overwritten on the next promotion run. To add an entry by hand, put it in
label_family.AUTHORITY_EFFECT_MAP directly; the hand-authored map always
wins on conflict (label_family imports this overlay with setdefault).

Provenance (cumulative across all promotion runs):
{provenance}
"""

AUTHORITY_EFFECT_ADDITIONS: dict[str, str] = {effect_dict}

LABELER_DEFAULT_EFFECT_ADDITIONS: dict[str, str] = {labeler_dict}
'''


def write_overlay(
    overlay_path: str,
    effect_additions: Dict[str, str],
    labeler_additions: Dict[str, str],
    provenance_lines: List[str],
) -> None:
    """Write the overlay file. Sorted-keys for deterministic diffs."""
    def render_dict(d: Dict[str, str]) -> str:
        if not d:
            return "{}"
        items = sorted(d.items())
        body = ",\n".join(
            f'    {json.dumps(k)}: {json.dumps(v)}' for k, v in items
        )
        return "{\n" + body + ",\n}"

    provenance = "\n".join(
        f"  - {line}" for line in provenance_lines
    ) or "  (none)"
    content = _OVERLAY_HEADER.format(
        provenance=provenance,
        effect_dict=render_dict(effect_additions),
        labeler_dict=render_dict(labeler_additions),
    )
    os.makedirs(os.path.dirname(os.path.abspath(overlay_path)) or ".", exist_ok=True)
    with open(overlay_path, "w", encoding="utf-8") as f:
        f.write(content)


def _extract_provenance_lines(overlay_path: str) -> List[str]:
    """Best-effort extract of prior provenance lines from the overlay's
    module docstring. Returns [] when absent."""
    if not os.path.exists(overlay_path):
        return []
    import ast
    with open(overlay_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=overlay_path)
    docstring = ast.get_docstring(tree) or ""
    lines: List[str] = []
    in_prov = False
    for raw in docstring.splitlines():
        s = raw.strip()
        if s.startswith("Provenance"):
            in_prov = True
            continue
        if in_prov:
            if s.startswith("- "):
                lines.append(s[2:])
            elif s == "" or s.startswith('"""'):
                continue
            else:
                # End of provenance block
                break
    return lines


# ---------------------------------------------------------------------------
# Promotion driver
# ---------------------------------------------------------------------------

def apply_promotions(
    triage_receipt: Dict[str, Any],
    decisions_doc: Dict[str, Any],
    *,
    decisions_path: str,
    triage_receipt_path: str,
    overlay_path: str = DEFAULT_OVERLAY_PATH,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Validate decisions, merge into overlay, emit promotion receipt.

    On validation errors the function refuses to write anything and returns
    a receipt with verdict=refused. dry_run also produces a receipt but
    skips overlay writes.
    """
    validation = validate_decisions(triage_receipt, decisions_doc)

    # Index triage candidates by candidate_id for lookup
    by_cid = {c.get("candidate_id"): c for c in triage_receipt.get("queue", [])}
    triage_cids = set(by_cid.keys())

    applied: List[Dict[str, Any]] = []
    deferred: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    if validation.errors:
        verdict = "refused"
    else:
        verdict = "applied" if not dry_run else "dry_run"
        for d in validation.decisions:
            cid = d.get("candidate_id")
            action = d.get("action")
            entry = {
                "candidate_id": cid,
                "labeler_did": d.get("labeler_did")
                    or (by_cid.get(cid, {}) or {}).get("labeler_did"),
                "label_value": d.get("label_value")
                    or (by_cid.get(cid, {}) or {}).get("label_value"),
                "reason": (d.get("reason") or "").strip() or None,
            }
            if action == authority_review.ACTION_RATIFY:
                entry["authority_effect"] = d.get("authority_effect")
                applied.append(entry)
            elif action == authority_review.ACTION_DEFER:
                deferred.append(entry)
            elif action == authority_review.ACTION_REJECT:
                rejected.append(entry)

    # Warn on triage candidates with no decision row (omitted from operator
    # review). Not an error — just visible.
    decided_cids = {d.get("candidate_id") for d in validation.decisions}
    omitted = sorted(triage_cids - decided_cids)

    overlay_summary = {
        "path": overlay_path,
        "effect_additions_added": 0,
        "effect_additions_skipped_hand_authored": 0,
        "effect_additions_skipped_existing_overlay": 0,
        "labeler_additions_added": 0,
    }

    if verdict == "applied" and applied:
        # Load existing overlay state
        existing = load_existing_overlay(overlay_path)
        eff = dict(existing["AUTHORITY_EFFECT_ADDITIONS"])
        lab = dict(existing["LABELER_DEFAULT_EFFECT_ADDITIONS"])
        prior_lines = _extract_provenance_lines(overlay_path)

        for a in applied:
            family = _family_for(a["label_value"])
            effect = a["authority_effect"]
            if family in AUTHORITY_EFFECT_MAP:
                # Hand-authored entry always wins; record the skip in the
                # promotion receipt so the operator can see what didn't apply.
                a["overlay_action"] = "skipped_hand_authored"
                overlay_summary["effect_additions_skipped_hand_authored"] += 1
                continue
            if family in eff and eff[family] == effect:
                a["overlay_action"] = "already_present"
                overlay_summary["effect_additions_skipped_existing_overlay"] += 1
                continue
            if family in eff and eff[family] != effect:
                a["overlay_action"] = "overrode_prior_overlay"
                a["prior_overlay_effect"] = eff[family]
            else:
                a["overlay_action"] = "added"
            eff[family] = effect
            overlay_summary["effect_additions_added"] += 1

        # Build a new provenance line + dedupe against priors
        prov_line = (
            f"{format_ts(now_utc())} — triage={os.path.basename(triage_receipt_path)} "
            f"decisions={os.path.basename(decisions_path)} "
            f"applied={overlay_summary['effect_additions_added']}"
        )
        new_lines = list(prior_lines) + [prov_line]

        if not dry_run:
            write_overlay(overlay_path, eff, lab, new_lines)

    receipt = {
        "receipt_kind": PROMOTION_RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "verdict": verdict,
        "dry_run": dry_run,
        "generated_at": format_ts(now_utc()),
        "git_commit": get_git_commit(),
        "family_version": FAMILY_VERSION,
        "sources": {
            "triage_receipt_path": triage_receipt_path,
            "triage_receipt_hash": triage_receipt.get("receipt_hash"),
            "triage_generated_at": triage_receipt.get("generated_at"),
            "decisions_path": decisions_path,
        },
        "overlay": overlay_summary,
        "errors": validation.errors,
        "summary": {
            "triage_candidates": len(triage_cids),
            "decided": len(decided_cids),
            "omitted": len(omitted),
            "ratified": len(applied),
            "deferred": len(deferred),
            "rejected": len(rejected),
        },
        "applied": applied,
        "deferred": deferred,
        "rejected": rejected,
        "omitted_candidate_ids": omitted,
    }
    receipt["receipt_hash"] = hash_sha256(
        stable_json({k: v for k, v in receipt.items() if k != "receipt_hash"})
    )
    return receipt


def _family_for(label_value: str) -> str:
    """Map a label_value to its family via label_family.normalize_family.
    Indirected to avoid an import cycle when label_family imports the overlay."""
    from .label_family import normalize_family
    return normalize_family(label_value)
