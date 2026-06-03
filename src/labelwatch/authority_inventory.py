"""Authority-effect inventory: group observed labels by what kind of authority
the label attempts to exercise, and surface the actual labels in each group.

This is an observational view, not a moderation judgment. It classifies the
LABEL — what role the string plays in the control/reputation surface — not the
labeler's intent and not the truth of the claim.

The valuable affordance is the per-group label list sorted by event_count:
operators need to see the actual namespace under each authority bucket so they
can tell whether a "label conflict" is governance, reputation, telemetry, or
decorative churn. Counts alone are bait.

Unknown labels are listed individually rather than dropped. Unknown is a valid
finding: it surfaces labels the namespace grew without the classifier guessing.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from collections import defaultdict

from .label_family import (
    AUTHORITY_EFFECT_COPY,
    AUTHORITY_EFFECT_ORDER,
    FAMILY_VERSION,
    LABELER_DEFAULT_EFFECT,
    classify_authority_effect,
    normalize_family,
)


def _resolve_val_effect(family: str, labeler_dids: set[str]) -> tuple[str, bool]:
    """Resolve a label's authority_effect from its family + the set of labelers
    that emit it. Returns (effect, used_labeler_fallback).

    Resolution order:
      1. AUTHORITY_EFFECT_MAP lookup (label-level mapping always wins).
      2. If every emitting labeler is in LABELER_DEFAULT_EFFECT AND they all
         agree on the same effect, use that. This catches bespoke-decorative
         namespaces (e.g. an oracle labeler emitting 200+ themed strings)
         without forcing per-val enumeration.
      3. unknown.
    """
    effect = classify_authority_effect(family)
    if effect != "unknown":
        return effect, False

    if not labeler_dids:
        return "unknown", False

    hints = {LABELER_DEFAULT_EFFECT.get(d) for d in labeler_dids}
    if None in hints or len(hints) != 1:
        return "unknown", False

    return hints.pop(), True


def build_authority_effect_inventory(
    conn,
    start_ts: str,
    end_ts: str,
) -> Dict[str, Any]:
    """Build a grouped inventory of observed labels by authority_effect.

    Aggregates label_events in [start_ts, end_ts) over neg=0 events
    (active label applications; negations excluded for interpretability).

    For each observed raw label value, computes:
      - event_count       (rows)
      - labeler_count     (DISTINCT labeler_did)
      - target_count      (DISTINCT target_did, may include NULL bucket)
      - family            (normalize_family(val))
      - authority_effect  (classify_authority_effect(family))

    Returns a dict keyed by group with:
      - label_count  (distinct raw label values in the group)
      - event_count  (sum of events across the group)
      - labels       (sorted by event_count desc)
      - description  (descriptive copy from AUTHORITY_EFFECT_COPY)

    The top-level total_label_count equals the sum across groups — used by tests
    to prove no labels are silently dropped.
    """
    # Group by (val, labeler_did) so we can apply the labeler-default fallback
    # when AUTHORITY_EFFECT_MAP has no mapping for the val. Per-val target sets
    # have to be unioned in Python because COUNT(DISTINCT target_did) at the
    # (val, labeler_did) level cannot be summed back up correctly.
    rows = conn.execute(
        """
        SELECT
            val,
            labeler_did,
            COUNT(*) AS event_count
        FROM label_events
        WHERE ts >= ? AND ts < ? AND neg = 0
        GROUP BY val, labeler_did
        """,
        (start_ts, end_ts),
    ).fetchall()

    # Per-val aggregates.
    per_val_event_count: Dict[str, int] = defaultdict(int)
    per_val_labelers: Dict[str, set[str]] = defaultdict(set)
    for r in rows:
        val = r["val"]
        per_val_event_count[val] += int(r["event_count"] or 0)
        per_val_labelers[val].add(r["labeler_did"])

    # Distinct target_did per val — single supplementary query rather than
    # passing target sets through the (val, labeler_did) grouping.
    target_rows = conn.execute(
        """
        SELECT val, COUNT(DISTINCT target_did) AS target_count
        FROM label_events
        WHERE ts >= ? AND ts < ? AND neg = 0
        GROUP BY val
        """,
        (start_ts, end_ts),
    ).fetchall()
    per_val_target_count = {r["val"]: int(r["target_count"] or 0) for r in target_rows}

    groups: Dict[str, Dict[str, Any]] = {
        g: {
            "description": AUTHORITY_EFFECT_COPY[g],
            "label_count": 0,
            "event_count": 0,
            "labels": [],
        }
        for g in AUTHORITY_EFFECT_ORDER
    }

    total_labels = 0
    for val, event_count in per_val_event_count.items():
        labelers = per_val_labelers[val]
        family = normalize_family(val)
        effect, used_labeler_fallback = _resolve_val_effect(family, labelers)
        # Defensive: an unrecognized effect would silently drop the label.
        if effect not in groups:
            effect = "unknown"
        groups[effect]["labels"].append(
            {
                "value": val,
                "family": family,
                "event_count": event_count,
                "labeler_count": len(labelers),
                "target_count": per_val_target_count.get(val, 0),
                # Surfaces the labeler-default fallback in the JSON for audit.
                # HTML does not render this; analysts inspecting the artifact can.
                "labeler_fallback": used_labeler_fallback,
            }
        )
        groups[effect]["label_count"] += 1
        groups[effect]["event_count"] += event_count
        total_labels += 1

    # Sort each group's labels by event_count desc, then value asc for stability.
    for g in groups.values():
        g["labels"].sort(key=lambda x: (-x["event_count"], x["value"]))

    return {
        "axis": "authority_effect",
        "axis_description": (
            "What kind of authority a label attempts to exercise in the "
            "control/reputation surface. Structural classification of the "
            "label string; not an inference about labeler intent."
        ),
        "window": {"start": start_ts, "end": end_ts},
        "family_version": FAMILY_VERSION,
        "total_label_count": total_labels,
        "total_event_count": sum(g["event_count"] for g in groups.values()),
        "groups": groups,
        "group_order": list(AUTHORITY_EFFECT_ORDER),
    }


# Groups whose <details> elements should render open by default in the HTML
# report. The operator most needs to see actuators, reach controls, claims that
# attach normative charge, and labels the classifier could not assign.
DEFAULT_OPEN_GROUPS: tuple[str, ...] = (
    "enforcement_instruction",
    "visibility_affecting",
    "reputational",
    "unknown",
)


def render_authority_effect_html(
    inventory: Dict[str, Any],
    max_labels_per_group: Optional[int] = None,
) -> str:
    """Render the inventory as a collapsible HTML section.

    `max_labels_per_group` truncates long groups (decorative/telemetry tend to
    sprawl); None shows all. The full namespace is always present in the JSON
    artifact regardless.
    """
    from html import escape  # local import keeps module import light

    parts: List[str] = []
    parts.append('<div class="authority-effect-section">')
    parts.append('<h2>Authority-effect inventory (7d)</h2>')
    parts.append(
        '<p class="labeler-context">'
        'Observed labels grouped by what kind of authority the label, as a '
        'string, attempts to exercise. Structural classification — does not '
        'infer labeler intent. Family version: '
        f'<code>{escape(inventory.get("family_version", "?"))}</code>.'
        '</p>'
    )
    parts.append(
        '<p class="small" style="opacity:0.7;margin-top:0;">'
        f'{inventory["total_label_count"]:,} distinct label values across '
        f'{inventory["total_event_count"]:,} active events in the window.'
        '</p>'
    )

    for group_name in inventory["group_order"]:
        group = inventory["groups"][group_name]
        if group["label_count"] == 0:
            continue
        open_attr = " open" if group_name in DEFAULT_OPEN_GROUPS else ""
        human_name = group_name.replace("_", " ")
        parts.append(f'<details{open_attr}>')
        parts.append(
            '<summary><strong>'
            f'{escape(human_name)}</strong> &mdash; '
            f'{group["label_count"]:,} labels, '
            f'{group["event_count"]:,} events'
            '</summary>'
        )
        parts.append(
            f'<p class="small" style="opacity:0.7;">'
            f'{escape(group["description"])}'
            '</p>'
        )

        labels = group["labels"]
        truncated = False
        if max_labels_per_group is not None and len(labels) > max_labels_per_group:
            labels = labels[:max_labels_per_group]
            truncated = True

        parts.append('<ul class="authority-effect-labels">')
        for lbl in labels:
            value = escape(lbl["value"])
            family = escape(lbl["family"])
            same_family = lbl["value"] == lbl["family"]
            family_note = "" if same_family else f' <span class="small">(family: <code>{family}</code>)</span>'
            parts.append(
                f'<li><code>{value}</code>{family_note} &mdash; '
                f'{lbl["event_count"]:,} events, '
                f'{lbl["labeler_count"]:,} labelers, '
                f'{lbl["target_count"]:,} targets</li>'
            )
        parts.append('</ul>')
        if truncated:
            remaining = group["label_count"] - max_labels_per_group
            parts.append(
                f'<p class="small" style="opacity:0.7;">'
                f'(+{remaining:,} more labels; full list in '
                f'<code>authority_effect_inventory.json</code>)'
                '</p>'
            )
        parts.append('</details>')

    parts.append('</div>')
    return "\n".join(parts)
