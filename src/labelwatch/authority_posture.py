"""Authority-posture aggregation: ecosystem-level view of what kind of
authority the observed labelers claim, how auditable that claim is, and how
that posture is distributed across emitted authority-effect classes.

Composes two axes already shipped:
  - Per-labeler classification (`labelers` table): labeler_class,
    auditability_risk_band, inference_risk_band, temporal_coherence_band,
    events_7d.
  - Per-label authority_effect classification (`label_family.py`,
    `authority_inventory.py`).

The aggregator is observation-only and aggregate-first:
  - Counts labelers and events, never names them at this layer.
  - Reports posture in descriptive bands ("high auditability", not "trusted";
    "low auditability / insufficient evidence", not "bad").
  - Does not infer labeler intent. Class and risk bands are the existing
    classifier's outputs; this module only redistributes them.

Output shape (JSON):
{
  "axis": "authority_posture",
  "window": {...},
  "population": {
      "labelers_observed": <int>,
      "labelers_active_7d": <int>,
      "events_in_window": <int>
  },
  "dials": {
      "class": {<bucket>: count, ...},
      "auditability_risk": {<band>: count, ...},
      "inference_risk":    {<band>: count, ...},
      "temporal_coherence":{<band>: count, ...}
  },
  "volume_share": {
      "by_class":              {<bucket>: events_7d_sum, ...},
      "by_auditability_risk":  {<band>: events_7d_sum, ...}
  },
  "authority_effect_by_class":           {<effect>: {<bucket>: events, ...}, ...},
  "authority_effect_by_auditability_risk":{<effect>: {<band>: events, ...}, ...},
  "copy_disposition": [...]   # safe narrative lines, descriptive only
}
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from .label_family import (
    AUTHORITY_EFFECT_ORDER,
    LABELER_DEFAULT_EFFECT,
    classify_authority_effect,
    normalize_family,
)


CLASS_BUCKETS: Tuple[str, ...] = (
    "official_platform",
    "first_party",
    "third_party",
    "unknown",
)

RISK_BANDS: Tuple[str, ...] = (
    "low",       # less concerning
    "medium",
    "high",      # most concerning
    "unknown",
)


def _bucket_or_unknown(value: Optional[str], allowed: Tuple[str, ...]) -> str:
    if value is None or value == "":
        return "unknown"
    return value if value in allowed else "unknown"


def _resolve_event_effect(val: str, labeler_did: str) -> str:
    """Per-event authority-effect resolution for the volume crosstab.

    Mirrors `_resolve_val_effect` in authority_inventory but operates on a
    single (val, labeler) pair rather than the global emitter set. This is
    correct here because we are summing events into a posture aggregate, not
    classifying a label namespace-wide. A val that is unmapped and emitted
    by a hinted labeler still routes to the labeler-default effect at the
    event-volume level — which is what the posture aggregate should reflect.
    """
    effect = classify_authority_effect(normalize_family(val))
    if effect != "unknown":
        return effect
    return LABELER_DEFAULT_EFFECT.get(labeler_did, "unknown")


def build_authority_posture(
    conn,
    start_ts: str,
    end_ts: str,
) -> Dict[str, Any]:
    """Build the ecosystem-level authority-posture aggregate over a window.

    Window applies to event-volume metrics. Labeler population counts are
    snapshot (whole `labelers` table), to match how the homepage census reads.
    """
    # ---- Labeler population + dial distributions ----
    labelers = conn.execute(
        """
        SELECT
            labeler_did,
            labeler_class,
            auditability_risk_band,
            inference_risk_band,
            temporal_coherence_band,
            events_7d
        FROM labelers
        """
    ).fetchall()

    labelers_observed = len(labelers)
    labelers_active_7d = sum(1 for r in labelers if (r["events_7d"] or 0) > 0)

    dial_class = {b: 0 for b in CLASS_BUCKETS}
    dial_audit = {b: 0 for b in RISK_BANDS}
    dial_inf = {b: 0 for b in RISK_BANDS}
    dial_temp = {b: 0 for b in RISK_BANDS}
    vol_by_class = {b: 0 for b in CLASS_BUCKETS}
    vol_by_audit = {b: 0 for b in RISK_BANDS}

    # Per-labeler lookups for the volume crosstab pass below.
    labeler_class_lookup: Dict[str, str] = {}
    labeler_audit_lookup: Dict[str, str] = {}

    for r in labelers:
        cls = _bucket_or_unknown(r["labeler_class"], CLASS_BUCKETS)
        audit = _bucket_or_unknown(r["auditability_risk_band"], RISK_BANDS)
        inf = _bucket_or_unknown(r["inference_risk_band"], RISK_BANDS)
        temp = _bucket_or_unknown(r["temporal_coherence_band"], RISK_BANDS)
        ev = int(r["events_7d"] or 0)

        dial_class[cls] += 1
        dial_audit[audit] += 1
        dial_inf[inf] += 1
        dial_temp[temp] += 1
        vol_by_class[cls] += ev
        vol_by_audit[audit] += ev

        labeler_class_lookup[r["labeler_did"]] = cls
        labeler_audit_lookup[r["labeler_did"]] = audit

    # ---- Authority-effect × dial volume crosstabs ----
    # GROUP BY (val, labeler_did) once; for each cell, resolve effect and
    # attribute the event_count to the right (effect, class) and
    # (effect, auditability_risk_band) buckets.
    events_in_window = 0
    ae_by_class: Dict[str, Dict[str, int]] = {
        eff: {b: 0 for b in CLASS_BUCKETS} for eff in AUTHORITY_EFFECT_ORDER
    }
    ae_by_audit: Dict[str, Dict[str, int]] = {
        eff: {b: 0 for b in RISK_BANDS} for eff in AUTHORITY_EFFECT_ORDER
    }

    rows = conn.execute(
        """
        SELECT val, labeler_did, COUNT(*) AS n
        FROM label_events
        WHERE ts >= ? AND ts < ? AND neg = 0
        GROUP BY val, labeler_did
        """,
        (start_ts, end_ts),
    ).fetchall()
    for r in rows:
        val = r["val"]
        did = r["labeler_did"]
        n = int(r["n"] or 0)
        events_in_window += n
        effect = _resolve_event_effect(val, did)
        cls = labeler_class_lookup.get(did, "unknown")
        audit = labeler_audit_lookup.get(did, "unknown")
        ae_by_class.setdefault(effect, {b: 0 for b in CLASS_BUCKETS})[cls] += n
        ae_by_audit.setdefault(effect, {b: 0 for b in RISK_BANDS})[audit] += n

    copy_disposition = _disposition_lines(
        ae_by_class=ae_by_class,
        ae_by_audit=ae_by_audit,
        events_in_window=events_in_window,
        vol_by_audit=vol_by_audit,
    )

    return {
        "axis": "authority_posture",
        "axis_description": (
            "Ecosystem-level distribution of labelers by class and dial "
            "(auditability risk, inference risk, temporal coherence), and "
            "of event volume by authority_effect crossed with class and "
            "auditability_risk. Composes two classifications already in the "
            "schema; does not infer intent or assert verdicts."
        ),
        "window": {"start": start_ts, "end": end_ts},
        "population": {
            "labelers_observed": labelers_observed,
            "labelers_active_7d": labelers_active_7d,
            "events_in_window": events_in_window,
        },
        "dials": {
            "class": dial_class,
            "auditability_risk": dial_audit,
            "inference_risk": dial_inf,
            "temporal_coherence": dial_temp,
        },
        "volume_share": {
            "by_class": vol_by_class,
            "by_auditability_risk": vol_by_audit,
        },
        "authority_effect_by_class": ae_by_class,
        "authority_effect_by_auditability_risk": ae_by_audit,
        "copy_disposition": copy_disposition,
    }


def _disposition_lines(
    *,
    ae_by_class: Dict[str, Dict[str, int]],
    ae_by_audit: Dict[str, Dict[str, int]],
    events_in_window: int,
    vol_by_audit: Dict[str, int],
) -> List[str]:
    """Generate a small set of descriptive narrative lines.

    Hard copy rules: "high auditability risk" / "low auditability risk", never
    "trusted" or "bad." Lines are skipped when supporting volume is zero —
    avoiding the "100% of 3 events" dashboard-numerology trap.
    """
    lines: List[str] = []

    def share(effect_dist: Dict[str, int], bucket: str) -> Optional[Tuple[int, float]]:
        total = sum(effect_dist.values())
        if total == 0:
            return None
        cell = effect_dist.get(bucket, 0)
        return cell, (cell / total) * 100.0

    # 1) Concentration of enforcement/visibility-affecting volume by class.
    for effect, human in (
        ("enforcement_instruction", "enforcement-instruction"),
        ("visibility_affecting", "visibility-affecting"),
    ):
        dist = ae_by_class.get(effect, {})
        third = share(dist, "third_party")
        official = share(dist, "official_platform")
        if third and third[0] >= 100:
            lines.append(
                f"{third[1]:.0f}% of {human} event volume in the window is "
                f"emitted by third_party labelers ({third[0]:,} events)."
            )
        if official and official[0] >= 100:
            lines.append(
                f"{official[1]:.0f}% of {human} event volume in the window is "
                f"emitted by official_platform labelers ({official[0]:,} events)."
            )

    # 2) Reputational volume by auditability_risk band.
    rep_audit = ae_by_audit.get("reputational", {})
    high_audit = share(rep_audit, "high")
    if high_audit and high_audit[0] >= 100:
        lines.append(
            f"{high_audit[1]:.0f}% of reputational event volume comes from "
            f"labelers with high auditability risk ({high_audit[0]:,} events)."
        )

    # 3) Active volume share emitted by high auditability-risk labelers.
    # When the high-risk share is zero, reframe as a visibility/coverage
    # statement instead of an "innocence finding" — high auditability risk
    # means we can't observe the labeler well enough to classify, so silence
    # in the classified volume is partly definitional. A naive "0% from
    # high-risk labelers" line reads as "high-risk labelers are quiet,"
    # which is the opposite of the intended dial semantics.
    total_active = sum(vol_by_audit.values())
    if total_active >= 1000:
        high_vol = vol_by_audit.get("high", 0)
        high_share = (high_vol / total_active) * 100.0
        if high_vol == 0:
            lines.append(
                "No classified event volume is attributed to high-auditability-risk "
                "labelers in this window. This is a visibility/coverage statement, "
                "not proof of inactivity or harmlessness — high auditability risk "
                "means insufficient observable surface to classify."
            )
        else:
            lines.append(
                f"{high_share:.0f}% of active 7d event volume is emitted by labelers "
                f"with high auditability risk."
            )

    return lines


# Default rendering order, low-to-high concern, on the dial summaries.
DIAL_ORDER: Tuple[str, ...] = (
    "class",
    "auditability_risk",
    "inference_risk",
    "temporal_coherence",
)

DIAL_HUMAN: Dict[str, str] = {
    "class": "Labeler class",
    "auditability_risk": "Auditability risk",
    "inference_risk": "Inference risk",
    "temporal_coherence": "Temporal coherence",
}

# Polarity disambiguation. Without these, a card titled "Auditability risk"
# above a row labeled "high" reads dangerously close to "high auditability"
# (which would imply *good*). The risk dials want "high = bad"; the coherence
# dial wants "high = good." Render every cell with the dial name baked in so
# no card-header + cell-row composition can mislead.
#
# Two risk dials (high = most concerning):
#   auditability_risk  — how *opaque* the labeler is to audit
#   inference_risk     — how much the labeler infers vs. observes
#
# One coherence dial (high = most coherent):
#   temporal_coherence — how stable the labeler's posture is over time
_BAND_LABEL: Dict[str, Dict[str, str]] = {
    "auditability_risk": {
        "low": "low auditability risk",
        "medium": "medium auditability risk",
        "high": "high auditability risk",
        "unknown": "unknown auditability risk",
    },
    "inference_risk": {
        "low": "low inference risk",
        "medium": "medium inference risk",
        "high": "high inference risk",
        "unknown": "unknown inference risk",
    },
    "temporal_coherence": {
        "low": "low temporal coherence",
        "medium": "medium temporal coherence",
        "high": "high temporal coherence",
        "unknown": "unknown temporal coherence",
    },
}


def _band_label(dial_key: str, bucket: str) -> str:
    """Polarity-safe display label for a dial bucket.

    Falls back to the raw bucket name for non-polar dials (class).
    """
    return _BAND_LABEL.get(dial_key, {}).get(bucket, bucket.replace("_", " "))


def render_authority_posture_html(posture: Dict[str, Any]) -> str:
    """Render the authority-posture aggregate as the homepage Authority Surface strip."""
    from html import escape

    parts: List[str] = []
    parts.append('<div class="authority-posture-section">')
    parts.append('<h2>Authority surface</h2>')

    pop = posture["population"]
    parts.append(
        '<p class="labeler-context" style="font-size:1.0rem;">'
        '<strong>Binding label authority remains platform-concentrated.</strong> '
        'Official-platform labelers emit nearly all enforcement-instruction, '
        'visibility-affecting, and advisory volume in the current 7d window; '
        'third-party labelers dominate reputational, telemetry, decorative, '
        'and unknown flow. Below: event volume crossed by authority effect, '
        'labeler class, and auditability risk. Descriptive — does not infer '
        'intent or assert verdicts.'
        '</p>'
    )
    parts.append(
        '<p class="small" style="opacity:0.7;margin-top:0;">'
        f'{pop["labelers_observed"]:,} labelers observed; '
        f'{pop["labelers_active_7d"]:,} emitting (7d); '
        f'{pop["events_in_window"]:,} active events in window.'
        '</p>'
    )
    parts.append(
        '<p class="small" style="opacity:0.8;margin-top:0.4rem;font-style:italic;">'
        '<strong>Unknown</strong> = not classified into an authority-effect '
        'bucket; may reflect unmapped labels, ambiguous authority posture, '
        'or insufficient observable surface. Treat as instrumentation debt '
        'unless separately classified.'
        '</p>'
    )

    # Narrative disposition lines.
    if posture["copy_disposition"]:
        parts.append('<ul class="authority-posture-findings">')
        for line in posture["copy_disposition"]:
            parts.append(f'<li>{escape(line)}</li>')
        parts.append('</ul>')

    # Dial counts. Cells carry the dial name baked in so a scanner can't
    # mentally compose "Auditability risk" + "high" → "high auditability."
    parts.append('<details open>')
    parts.append('<summary><strong>Dial counts</strong></summary>')
    parts.append('<div class="grid">')
    for dial_key in DIAL_ORDER:
        dial = posture["dials"][dial_key]
        parts.append('<div class="card">')
        parts.append(f'<strong>{escape(DIAL_HUMAN[dial_key])}</strong>')
        parts.append('<ul class="small">')
        for bucket, count in dial.items():
            if count == 0:
                continue
            parts.append(
                f'<li>{escape(_band_label(dial_key, bucket))}: {count:,}</li>'
            )
        parts.append('</ul>')
        parts.append('</div>')
    parts.append('</div>')
    parts.append('</details>')

    # Crosstab: authority_effect × class.
    parts.append('<details>')
    parts.append(
        '<summary><strong>Authority effect by labeler class</strong> '
        '(7d event volume)</summary>'
    )
    parts.append(_crosstab_table(
        posture["authority_effect_by_class"],
        CLASS_BUCKETS,
        column_label_fn=lambda c: c.replace("_", " "),
    ))
    parts.append('</details>')

    # Crosstab: authority_effect × auditability_risk.
    #
    # Ghost-column alibi: the crosstab suppresses columns whose 7d active
    # volume is zero, but the dial counts on the same page advertise a
    # nonzero population of medium/high-auditability-risk labelers. A bare
    # table then reads like the renderer swallowed columns. Compute which
    # bands exist in the labeler population but emitted no active volume
    # in this window, and call it out above the table.
    parts.append('<details>')
    parts.append(
        '<summary><strong>Authority effect by auditability risk</strong> '
        '(7d event volume)</summary>'
    )
    ae_audit = posture["authority_effect_by_auditability_risk"]
    audit_dial = posture["dials"].get("auditability_risk", {})
    present_in_crosstab = {
        band
        for band in RISK_BANDS
        if any(ae_audit.get(eff, {}).get(band, 0) > 0 for eff in AUTHORITY_EFFECT_ORDER)
    }
    absent_with_population = [
        band
        for band in RISK_BANDS
        if band not in present_in_crosstab and audit_dial.get(band, 0) > 0
    ]
    if absent_with_population:
        absent_label_pairs = [
            (
                _band_label("auditability_risk", b),
                audit_dial.get(b, 0),
            )
            for b in absent_with_population
        ]
        absent_phrase = "; ".join(
            f"{count:,} {label}" for label, count in absent_label_pairs
        )
        parts.append(
            '<p class="small" style="opacity:0.8;">'
            f'No classified event volume is attributed to {escape(absent_phrase)} '
            f'labelers in this window. This is a visibility/coverage statement, '
            f'not proof of inactivity or harmlessness — high auditability risk '
            f'means insufficient observable surface to classify.'
            '</p>'
        )
    parts.append(_crosstab_table(
        ae_audit,
        RISK_BANDS,
        column_label_fn=lambda b: _band_label("auditability_risk", b),
    ))
    parts.append('</details>')

    parts.append('</div>')
    return "\n".join(parts)


def _crosstab_table(
    crosstab: Dict[str, Dict[str, int]],
    column_order: Tuple[str, ...],
    column_label_fn=None,
) -> str:
    """Render a (authority_effect × dial-bucket) crosstab as a small HTML table.

    `column_label_fn` is a callable that maps a bucket key to its display label.
    Required for polarity-sensitive dials so column headers carry the dial name
    (e.g. "high auditability risk" rather than a bare "high" that could be
    read as "high auditability"). Defaults to underscore→space transform.

    Rows in AUTHORITY_EFFECT_ORDER; columns suppress all-zero buckets to keep
    the table narrow. Effect rows with zero total are also suppressed.
    """
    from html import escape

    if column_label_fn is None:
        column_label_fn = lambda c: c.replace("_", " ")

    # Which columns have any nonzero cell.
    column_totals = {col: 0 for col in column_order}
    for eff in AUTHORITY_EFFECT_ORDER:
        row = crosstab.get(eff, {})
        for col, val in row.items():
            if col in column_totals:
                column_totals[col] += val
    cols = [c for c in column_order if column_totals[c] > 0]
    if not cols:
        return '<p class="small">No events in window.</p>'

    parts: List[str] = []
    parts.append('<table class="authority-posture-crosstab">')
    parts.append('<thead><tr>')
    parts.append('<th>authority_effect</th>')
    for col in cols:
        parts.append(f'<th>{escape(column_label_fn(col))}</th>')
    parts.append('<th>row total</th>')
    parts.append('</tr></thead>')
    parts.append('<tbody>')
    for eff in AUTHORITY_EFFECT_ORDER:
        row = crosstab.get(eff, {})
        row_total = sum(row.values())
        if row_total == 0:
            continue
        parts.append('<tr>')
        parts.append(f'<td>{escape(eff.replace("_", " "))}</td>')
        for col in cols:
            cell = row.get(col, 0)
            pct = (cell / row_total * 100.0) if row_total else 0
            label = f"{cell:,}" if cell == 0 else f"{cell:,} ({pct:.0f}%)"
            parts.append(f'<td>{label}</td>')
        parts.append(f'<td><strong>{row_total:,}</strong></td>')
        parts.append('</tr>')
    parts.append('</tbody>')
    parts.append('</table>')
    return "\n".join(parts)
