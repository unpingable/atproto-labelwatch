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
    labeler_did: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a grouped inventory of observed labels by authority_effect.

    Aggregates label_events in [start_ts, end_ts) over neg=0 events
    (active label applications; negations excluded for interpretability).

    If `labeler_did` is given, the inventory is scoped to that labeler only —
    used by the per-labeler authority profile. Without it, the inventory is
    network-wide. The labeler-default fallback still applies in both modes
    (a single emitting labeler that's in LABELER_DEFAULT_EFFECT can resolve
    an unknown family).

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
    if labeler_did is None:
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
    else:
        rows = conn.execute(
            """
            SELECT
                val,
                labeler_did,
                COUNT(*) AS event_count
            FROM label_events
            WHERE ts >= ? AND ts < ? AND neg = 0 AND labeler_did = ?
            GROUP BY val, labeler_did
            """,
            (start_ts, end_ts, labeler_did),
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
    if labeler_did is None:
        target_rows = conn.execute(
            """
            SELECT val, COUNT(DISTINCT target_did) AS target_count
            FROM label_events
            WHERE ts >= ? AND ts < ? AND neg = 0
            GROUP BY val
            """,
            (start_ts, end_ts),
        ).fetchall()
    else:
        target_rows = conn.execute(
            """
            SELECT val, COUNT(DISTINCT target_did) AS target_count
            FROM label_events
            WHERE ts >= ? AND ts < ? AND neg = 0 AND labeler_did = ?
            GROUP BY val
            """,
            (start_ts, end_ts, labeler_did),
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
        "scope": "labeler" if labeler_did is not None else "network",
        "labeler_did": labeler_did,
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


# --- Per-labeler authority profile ---------------------------------------------

# Significance threshold for surfacing an authority_effect in the per-labeler
# distribution copy. Below this, the effect is volume-dust — included in the
# distribution strip and the per-group breakdown, but not name-checked in the
# one-line summary. Tuned to avoid "primarily X (97%); also Y (0.4%)" noise.
_LABELER_PROFILE_SIGNIFICANT_PCT = 5.0


def _labeler_profile_distribution(inventory: Dict[str, Any]) -> List[Dict[str, Any]]:
    """List of {effect, event_count, pct} for groups with nonzero events,
    sorted by event_count desc. Used by both the distribution strip and the
    one-line summary.
    """
    total = inventory["total_event_count"]
    rows: List[Dict[str, Any]] = []
    for group_name in inventory["group_order"]:
        group = inventory["groups"][group_name]
        if group["event_count"] == 0:
            continue
        pct = (group["event_count"] / total * 100.0) if total else 0.0
        rows.append(
            {
                "effect": group_name,
                "event_count": group["event_count"],
                "pct": pct,
            }
        )
    rows.sort(key=lambda r: -r["event_count"])
    return rows


def _labeler_profile_summary_line(distribution: List[Dict[str, Any]]) -> str:
    """One descriptive sentence about the labeler's authority-effect mix.

    Stays clinical (percentages of observed events, not "this labeler is X").
    Only effects above the significance threshold are name-checked.
    """
    if not distribution:
        return "No active label events observed in the window."

    significant = [
        r for r in distribution if r["pct"] >= _LABELER_PROFILE_SIGNIFICANT_PCT
    ]
    if not significant:
        # All effects are tiny slivers of a tiny pie — describe the leader
        # plainly without "primarily" framing.
        top = distribution[0]
        return (
            f"Primary effect: {top['effect'].replace('_', ' ')} "
            f"({top['pct']:.0f}% of {top['event_count']:,} events; "
            f"all observed effects below {_LABELER_PROFILE_SIGNIFICANT_PCT:.0f}% individually)."
        )

    fmt = lambda r: f"{r['effect'].replace('_', ' ')} ({r['pct']:.0f}%)"
    if len(significant) == 1:
        return f"Primary effect: {fmt(significant[0])} of observed event volume."
    if len(significant) == 2:
        return (
            f"Primary effect: {fmt(significant[0])}; "
            f"secondary: {fmt(significant[1])}."
        )
    # Three or more — list them rather than picking a single "primary."
    listed = ", ".join(fmt(r) for r in significant)
    return f"Effects with significant volume share: {listed}."


# CSS color hint per effect for the distribution strip. Keeps the visual
# meaning structural — enforcement/visibility carry weight, decorative is
# light, unknown is muted-warning. Not a value judgment; the renderer doesn't
# infer good/bad.
_EFFECT_STRIP_COLOR: Dict[str, str] = {
    "descriptive": "#7aa2cc",
    "advisory": "#8bb38b",
    "reputational": "#c98a7a",
    "visibility_affecting": "#c47fb3",
    "enforcement_instruction": "#b35454",
    "decorative": "#cdc090",
    "telemetry": "#9090a8",
    "unknown": "#c9a55a",
}


def render_labeler_authority_profile_html(inventory: Dict[str, Any]) -> str:
    """Render a per-labeler authority_effect profile.

    Distinct from the network-wide inventory renderer:
      - Adds a one-line clinical distribution summary.
      - Adds a horizontal distribution strip so the mix is visible at a glance.
      - Skips the long axis description (the per-labeler page already framed
        the labeler).
      - Keeps the per-group <details> breakdown for the labels actually emitted.
    """
    from html import escape

    if inventory.get("scope") != "labeler":
        raise ValueError(
            "render_labeler_authority_profile_html requires an inventory built "
            "with labeler_did= set (scope='labeler')."
        )

    parts: List[str] = []
    parts.append('<div class="labeler-authority-profile">')
    parts.append('<h2>Authority profile (7d)</h2>')

    total_events = inventory["total_event_count"]
    if total_events == 0:
        parts.append(
            '<p class="labeler-context">'
            'No active label events observed in the window. '
            'Authority-effect distribution unavailable.'
            '</p>'
            '</div>'
        )
        return "\n".join(parts)

    parts.append(
        '<p class="labeler-context">'
        'How this labeler’s observed labels distribute across '
        'authority-effect classes. Structural classification of label strings; '
        'not an inference of intent. '
        f'Family version: <code>{escape(inventory.get("family_version", "?"))}</code>.'
        '</p>'
    )

    distribution = _labeler_profile_distribution(inventory)
    summary_line = _labeler_profile_summary_line(distribution)
    parts.append(
        '<p class="small" style="opacity:0.85;">'
        f'{escape(summary_line)}'
        '</p>'
    )
    parts.append(
        '<p class="small" style="opacity:0.7;margin-top:0;">'
        f'{inventory["total_label_count"]:,} distinct label values across '
        f'{total_events:,} active events.'
        '</p>'
    )

    # Distribution strip: percent-width segments, ordered by effect mix.
    if distribution:
        parts.append(
            '<div class="authority-profile-strip" '
            'style="display:flex;width:100%;height:1.4rem;border-radius:4px;'
            'overflow:hidden;margin:0.5rem 0 0.4rem 0;border:1px solid var(--border,#ccc);">'
        )
        for row in distribution:
            color = _EFFECT_STRIP_COLOR.get(row["effect"], "#888")
            title = (
                f"{row['effect'].replace('_', ' ')}: "
                f"{row['event_count']:,} events ({row['pct']:.1f}%)"
            )
            parts.append(
                f'<div style="flex:0 0 {row["pct"]:.3f}%;'
                f'background:{color};" title="{escape(title)}"></div>'
            )
        parts.append('</div>')
        # Legend
        legend_items = []
        for row in distribution:
            color = _EFFECT_STRIP_COLOR.get(row["effect"], "#888")
            legend_items.append(
                f'<span style="display:inline-block;width:0.7rem;height:0.7rem;'
                f'background:{color};margin-right:0.3rem;vertical-align:middle;'
                f'border:1px solid var(--border,#ccc);"></span>'
                f'{escape(row["effect"].replace("_", " "))} '
                f'<span class="small">({row["pct"]:.1f}%)</span>'
            )
        parts.append(
            '<p class="small" style="margin-top:0.2rem;line-height:1.8;">'
            + " &nbsp;&nbsp; ".join(legend_items)
            + '</p>'
        )

    # Per-group breakdown — only emit groups with events.
    for group_name in inventory["group_order"]:
        group = inventory["groups"][group_name]
        if group["event_count"] == 0:
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

        # Per-labeler view: only show this labeler's labels, no labeler_count.
        parts.append('<ul class="authority-effect-labels">')
        for lbl in group["labels"]:
            value = escape(lbl["value"])
            family = escape(lbl["family"])
            same_family = lbl["value"] == lbl["family"]
            family_note = (
                "" if same_family
                else f' <span class="small">(family: <code>{family}</code>)</span>'
            )
            parts.append(
                f'<li><code>{value}</code>{family_note} &mdash; '
                f'{lbl["event_count"]:,} events, '
                f'{lbl["target_count"]:,} targets</li>'
            )
        parts.append('</ul>')
        parts.append('</details>')

    parts.append('</div>')
    return "\n".join(parts)
