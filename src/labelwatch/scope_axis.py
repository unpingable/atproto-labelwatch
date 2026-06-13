"""labelwatch.scope_presentation.v0 — population verdict-scope presentation of active labels.

Spec:     specs/gaps/gap-spec-scope-axis-v0.md
Doctrine: docs/evaluation-detachment-axes.md (the scope-axis) ·
          docs/labelers-as-testimony.md (anyone may testify; no labeler holds
          enforcement authority — the subscriber converts).

What this measures: labeler **self-presentation**, not legitimacy. The band is the
labeler's OWN declared ``labelValueDefinition.defaultSetting``, cited not adopted.
Weather-not-verdict is preserved structurally by *what the number is about* — a
labeler's declared default posture, never an adjudication of any labeled subject.
Aggregate-first: the output carries no ``target_did`` and no ranked per-labeler list.

The unit of measurement is the verdict-scope *presentation share*: the fraction of
active label volume whose declared default presentation is verdict-scope. It is
presentation pressure, not guilt.
"""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Dict, Optional

from .utils import format_ts, now_utc

RECEIPT_KIND = "labelwatch.scope_presentation.v0"

# --- bands ---------------------------------------------------------------
# The scope band is the defaultSetting projection of the labeler's own
# labelValueDefinition. It is finer than emitter_classifier.authority_effect
# (which folds hide+warn into a single visibility_affecting class); the
# hide/warn split is the whole point of this axis.
BAND_VERDICT = "verdict"    # defaultSetting == 'hide'  — mandatory-hide-by-default
BAND_NUDGE = "nudge"        # defaultSetting == 'warn'  — a default action, but soft
BAND_WEATHER = "weather"    # defaultSetting == 'ignore' / unset — opt-in, descriptive
BAND_UNGRADED = "ungraded"  # no published definition — WARRANT-GAP, never weather

GRADED_BANDS = (BAND_VERDICT, BAND_NUDGE, BAND_WEATHER)

# Coverage canary: below this graded/(graded+ungraded) emission coverage, the
# headline verdict-scope share is suppressed rather than published bare.
# Mirrors the report-side coverage-watermark discipline.
COVERAGE_FLOOR = 0.5


def scope_band(definition: Optional[Dict[str, Any]]) -> str:
    """Pure projection of a labelValueDefinition onto a scope band.

    A *missing* definition (None) is ``ungraded`` — a warrant-gap, NOT weather:
    a label with no published basis is unfalsifiable from outside, which is a
    different fact from a label the labeler explicitly declares as opt-in.

    NOTE (reviewable assumption): an explicit ``defaultSetting`` of ``ignore``
    AND an *omitted* ``defaultSetting`` both map to ``weather`` here. The ATProto
    lexicon arguably defaults an omitted ``defaultSetting`` to ``warn``; we follow
    the codebase's existing explicit-only convention (see
    ``emitter_classifier._authority_from_metadata_only``, which treats only an
    explicit hide/warn as action-bearing). Counting only explicitly-declared
    posture is the conservative reading and avoids inferring pressure the labeler
    did not write. Revisit if protocol-default semantics become load-bearing.
    """
    if not definition or not isinstance(definition, dict):
        return BAND_UNGRADED
    ds = definition.get("defaultSetting")
    if ds == "hide":
        return BAND_VERDICT
    if ds == "warn":
        return BAND_NUDGE
    # 'ignore', None, or any unrecognized/forward-compat value: weather.
    return BAND_WEATHER


def classify_cell(
    labeler_did: str,
    val: str,
    definition: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Classify one ``(labeler, val)`` cell, carrying the labeler's verbatim
    metadata as cited testimony (provenance, not adopted framing).

    Returns ``{band, evidence}``. ``evidence`` never contains a labeled subject —
    only the labeler DID, the value, and the labeler's own declared metadata.
    """
    band = scope_band(definition)
    evidence: Dict[str, Any] = {
        "labeler_did": labeler_did,
        "val": val,
        # Verbatim, cited-not-adopted. None when ungraded.
        "defaultSetting": (definition or {}).get("defaultSetting") if definition else None,
        "severity": (definition or {}).get("severity") if definition else None,
        "blurs": (definition or {}).get("blurs") if definition else None,
        "definition_present": definition is not None,
    }
    return {"band": band, "evidence": evidence}


def _load_definition_map(conn) -> Dict[tuple, Dict[str, Any]]:
    """Build ``{(labeler_did, identifier): definition}`` from the latest service
    record per labeler in ``discovery_events`` — one pass over the small table,
    so emission cells classify in Python without N round-trips.
    """
    rows = conn.execute(
        """
        WITH latest AS (
          SELECT
            labeler_did,
            record_json,
            ROW_NUMBER() OVER (
              PARTITION BY labeler_did
              ORDER BY discovered_at DESC
            ) AS rn
          FROM discovery_events
          WHERE operation IN ('create','update')
            AND json_extract(record_json,'$.policies.labelValueDefinitions') IS NOT NULL
        )
        SELECT labeler_did, record_json FROM latest WHERE rn = 1
        """
    ).fetchall()

    def_map: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        labeler_did = row["labeler_did"] if hasattr(row, "keys") else row[0]
        raw = row["record_json"] if hasattr(row, "keys") else row[1]
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError):
            continue
        defs = (rec.get("policies") or {}).get("labelValueDefinitions") or []
        for d in defs:
            ident = d.get("identifier")
            if ident:
                def_map[(labeler_did, ident)] = d
    return def_map


def compute_scope_presentation(
    conn,
    window_days: int = 7,
    now=None,
) -> Dict[str, Any]:
    """Population scope-presentation metric over active (neg=0) label events.

    Two cuts, both with explicit denominators:

    * **emission** (headline) — active label volume aggregated by ``(labeler, val)``,
      each cell graded by its band, event counts summed per band. Headline =
      verdict-scope presentation share of *graded* active volume.
    * **declaration** (companion) — distinct published ``(labeler, val)`` definitions,
      unweighted, per band.

    ``ungraded`` (no published definition) is reported alongside as the warrant-gap
    coverage shortfall and is NEVER summed into weather. Bang-labels (``!``-prefixed,
    protocol-reserved) are excluded from the band cuts and counted on their own
    deferred line (hosting-layer; see specimen 003).
    """
    now = now or now_utc()
    window_start = format_ts(now - timedelta(days=window_days))

    def_map = _load_definition_map(conn)

    # --- emission cut ---------------------------------------------------
    cells = conn.execute(
        """
        SELECT labeler_did, val, COUNT(*) AS n
        FROM label_events
        WHERE neg = 0 AND ts >= ?
        GROUP BY labeler_did, val
        """,
        (window_start,),
    ).fetchall()

    emission_by_band = {BAND_VERDICT: 0, BAND_NUDGE: 0, BAND_WEATHER: 0}
    ungraded_events = 0
    protocol_reserved_events = 0
    graded_cells = 0
    ungraded_cells = 0
    protocol_reserved_cells = 0
    # Sensitivity observable (NOT a band): weather volume that comes from defs
    # which OMIT defaultSetting. This is exactly the population that would
    # reclassify to nudge under a protocol-warn-fallback reading. Surface it so
    # the unset ambiguity is measurable before anyone decides to flip v0.
    weather_from_omitted_default_events = 0

    for row in cells:
        labeler_did = row["labeler_did"] if hasattr(row, "keys") else row[0]
        val = row["val"] if hasattr(row, "keys") else row[1]
        n = row["n"] if hasattr(row, "keys") else row[2]

        if val.startswith("!"):
            # Protocol-reserved enforcement label — purest verdict-scope, but
            # hosting-layer. Counted, deferred. Never folded into a band.
            protocol_reserved_events += n
            protocol_reserved_cells += 1
            continue

        definition = def_map.get((labeler_did, val))
        result = classify_cell(labeler_did, val, definition)
        band = result["band"]
        if band == BAND_UNGRADED:
            ungraded_events += n
            ungraded_cells += 1
        else:
            emission_by_band[band] += n
            graded_cells += 1
            if band == BAND_WEATHER and definition.get("defaultSetting") is None:
                weather_from_omitted_default_events += n

    graded_events = sum(emission_by_band.values())
    graded_coverage = (
        graded_events / (graded_events + ungraded_events)
        if (graded_events + ungraded_events) > 0
        else None
    )
    verdict_share_raw = (
        emission_by_band[BAND_VERDICT] / graded_events if graded_events > 0 else None
    )
    suppressed = (
        graded_coverage is not None and graded_coverage < COVERAGE_FLOOR
    )

    emission = {
        "active_label_events": graded_events + ungraded_events + protocol_reserved_events,
        "graded_events": graded_events,
        "ungraded_events": ungraded_events,  # warrant-gap, not weather
        "protocol_reserved_deferred_events": protocol_reserved_events,
        "by_band": dict(emission_by_band),
        "distinct_cells": {
            "graded": graded_cells,
            "ungraded": ungraded_cells,
            "protocol_reserved_deferred": protocol_reserved_cells,
        },
        "graded_coverage": graded_coverage,
        # Headline. Suppressed (None) when graded coverage < floor; raw kept for audit.
        "verdict_scope_share": None if suppressed else verdict_share_raw,
        "verdict_scope_share_raw": verdict_share_raw,
        "verdict_scope_share_suppressed": suppressed,
        # Sensitivity: weather volume from defs that omit defaultSetting (would
        # become nudge under a protocol-warn-fallback reading). See assumptions.
        "weather_from_omitted_default_events": weather_from_omitted_default_events,
    }

    # --- declaration cut ------------------------------------------------
    declaration_by_band = {BAND_VERDICT: 0, BAND_NUDGE: 0, BAND_WEATHER: 0}
    declaration_omitted_default = 0  # defs that publish but omit defaultSetting
    for (_labeler_did, _ident), definition in def_map.items():
        band = scope_band(definition)
        if band in declaration_by_band:
            declaration_by_band[band] += 1
        if definition.get("defaultSetting") is None:
            declaration_omitted_default += 1
    defined_values = sum(declaration_by_band.values())
    declaration = {
        "defined_label_values": defined_values,
        "by_band": dict(declaration_by_band),
        # Sensitivity observable: how many published defs omit defaultSetting
        # entirely (counted as weather under v0's explicit-only convention).
        "default_setting_omitted": declaration_omitted_default,
        "verdict_scope_share": (
            declaration_by_band[BAND_VERDICT] / defined_values
            if defined_values > 0
            else None
        ),
    }

    return {
        "receipt_kind": RECEIPT_KIND,
        "generated_at": format_ts(now),
        "window_days": window_days,
        "window_start": window_start,
        "doctrine": (
            "self-presentation, not legitimacy; weather-not-verdict; aggregate-first. "
            "Band = the labeler's own declared defaultSetting, cited not adopted."
        ),
        "assumptions": {
            # Obnoxiously visible on purpose: future-you should not have to
            # reverse-engineer why this number differs from client behavior.
            "default_setting_omitted": "weather_scope_explicit_only",
            "not_client_behavior_simulation": True,
            "note": (
                "v0 uses explicit-only declaration semantics: an omitted "
                "defaultSetting is treated as weather-scope, NOT as the "
                "protocol/client warn-fallback. The omitted population is "
                "surfaced (declaration.default_setting_omitted, "
                "emission.weather_from_omitted_default_events) so the ambiguity "
                "is measurable before it is ever made policy."
            ),
        },
        "emission": emission,
        "declaration": declaration,
        "deferred": {
            "protocol_reserved_labels": "bang-prefixed labels (!hide/!warn/!takedown): hosting-layer, see specimen 003",
            "negation_and_expiry": "neg/exp dynamics belong to the freshness-axis; this metric counts neg=0 active assertions only",
        },
    }


# --- renderers -----------------------------------------------------------

_BAND_LABEL = {
    BAND_VERDICT: "verdict-scope",
    BAND_NUDGE: "nudge",
    BAND_WEATHER: "weather",
}


def _pct(x: Optional[float]) -> str:
    return "—" if x is None else f"{100.0 * x:.1f}%"


def render_text(metric: Dict[str, Any]) -> str:
    em = metric["emission"]
    dec = metric["declaration"]
    lines = []
    lines.append(f"=== Scope-presentation ({metric['window_days']}d) — {metric['receipt_kind']} ===")
    lines.append("Measures labeler self-presentation, not legitimacy. Band = the labeler's")
    lines.append("own declared defaultSetting, cited not adopted.")
    lines.append("")
    lines.append("Emission (active label volume, neg=0):")
    lines.append(f"  active label events:        {em['active_label_events']:,}")
    lines.append(f"  graded (has definition):    {em['graded_events']:,}")
    lines.append(f"  ungraded (warrant-gap):     {em['ungraded_events']:,}")
    lines.append(f"  protocol-reserved (deferred): {em['protocol_reserved_deferred_events']:,}")
    lines.append(f"  graded coverage:            {_pct(em['graded_coverage'])}")
    lines.append("")
    lines.append(f"  {'band':14s} {'events':>14s} {'share of graded':>18s}")
    lines.append(f"  {'─'*14} {'─'*14} {'─'*18}")
    for band in GRADED_BANDS:
        n = em["by_band"][band]
        share = (n / em["graded_events"]) if em["graded_events"] else None
        lines.append(f"  {_BAND_LABEL[band]:14s} {n:>14,} {_pct(share):>18s}")
    lines.append("")
    if em["verdict_scope_share_suppressed"]:
        lines.append(
            f"  Headline verdict-scope presentation share: SUPPRESSED "
            f"(graded coverage {_pct(em['graded_coverage'])} < {int(COVERAGE_FLOOR*100)}%); "
            f"raw = {_pct(em['verdict_scope_share_raw'])}"
        )
    else:
        lines.append(
            f"  Headline verdict-scope presentation share: {_pct(em['verdict_scope_share'])}"
        )
    lines.append("")
    lines.append("Declaration (distinct published label values):")
    lines.append(f"  defined label values:       {dec['defined_label_values']:,}")
    for band in GRADED_BANDS:
        lines.append(f"  {_BAND_LABEL[band]:14s} {dec['by_band'][band]:>14,}")
    lines.append(
        f"  declared verdict-scope share: {_pct(dec['verdict_scope_share'])}"
    )
    lines.append("")
    lines.append("Assumption (explicit-only): omitted defaultSetting counts as weather,")
    lines.append("NOT protocol warn-fallback. Sensitivity-relevant population:")
    lines.append(
        f"  defs omitting defaultSetting:   {dec['default_setting_omitted']:,}"
    )
    lines.append(
        f"  weather events from omitted:    {em['weather_from_omitted_default_events']:,} "
        f"(would become nudge under warn-fallback)"
    )
    return "\n".join(lines)


def render_html_figure(metric: Dict[str, Any]) -> str:
    """Self-contained HTML fragment — the v0 report figure. No per-labeler rows
    (aggregate-first). Caption carries the weather-not-verdict framing."""
    em = metric["emission"]
    dec = metric["declaration"]

    def _row(band: str) -> str:
        n = em["by_band"][band]
        share = (n / em["graded_events"]) if em["graded_events"] else None
        return (
            f"<tr><td>{_BAND_LABEL[band]}</td>"
            f"<td style='text-align:right'>{n:,}</td>"
            f"<td style='text-align:right'>{_pct(share)}</td>"
            f"<td style='text-align:right'>{dec['by_band'][band]:,}</td></tr>"
        )

    if em["verdict_scope_share_suppressed"]:
        headline = (
            f"Verdict-scope presentation share: <em>suppressed</em> "
            f"(graded coverage {_pct(em['graded_coverage'])} below "
            f"{int(COVERAGE_FLOOR*100)}%)."
        )
    else:
        headline = (
            f"<strong>{_pct(em['verdict_scope_share'])}</strong> of graded active "
            f"label volume presents at verdict-scope."
        )

    return (
        "<section class='scope-presentation'>"
        "<h3>Scope presentation</h3>"
        "<p class='caption'>How labels present, not whether they are right. The band is the "
        "labeler's own declared <code>defaultSetting</code> &mdash; cited, not adopted. "
        "No labeler holds enforcement authority; the subscriber converts. "
        "This figure reports presentation pressure across the population, never a verdict "
        "on any labeled subject.</p>"
        f"<p>{headline}</p>"
        "<table>"
        "<thead><tr><th>band</th><th>active events</th><th>share of graded</th>"
        "<th>declared values</th></tr></thead>"
        "<tbody>"
        + "".join(_row(b) for b in GRADED_BANDS)
        + "</tbody></table>"
        f"<p class='caption'>Graded coverage {_pct(em['graded_coverage'])} "
        f"({em['graded_events']:,} graded / {em['ungraded_events']:,} ungraded "
        f"warrant-gap events). Protocol-reserved (bang) labels deferred: "
        f"{em['protocol_reserved_deferred_events']:,} events. Negation/expiry are "
        f"freshness-axis, excluded here.</p>"
        f"<p class='caption'>Explicit-only convention: omitted "
        f"<code>defaultSetting</code> counts as weather, not protocol warn-fallback. "
        f"{em['weather_from_omitted_default_events']:,} weather events come from defs "
        f"that omit <code>defaultSetting</code> ({dec['default_setting_omitted']:,} "
        f"such defs) &mdash; the population that would shift to nudge under a "
        f"warn-fallback reading.</p>"
        "</section>"
    )
