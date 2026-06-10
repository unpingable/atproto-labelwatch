"""labelwatch authority-effect-review: static review packet + decisions template.

The thinnest useful thing for human ratification: a static HTML packet
generated from a triage receipt, plus a TOML decisions template the
operator fills in. No server, no auth, no database writes, no React goblin.

Doctrine:

    The machine proposes. The operator ratifies. The receipt remembers.

Companion to:
  - authority_triage (input: labelwatch.authority_effect_triage.v0 receipt)
  - authority_promote (consumes the decisions TOML this module templates)

Output:
  - <out_prefix>.html       — review packet (open in a browser)
  - <out_prefix>.decisions.toml — decisions template (fill in `action` per
                                   candidate; consumed by authority_promote)
"""
from __future__ import annotations

import json
import os
from html import escape
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Decisions vocabulary
# ---------------------------------------------------------------------------

ACTION_RATIFY = "ratify"
ACTION_DEFER = "defer"
ACTION_REJECT = "reject"
VALID_ACTIONS = {ACTION_RATIFY, ACTION_DEFER, ACTION_REJECT}


# ---------------------------------------------------------------------------
# Decisions TOML template
# ---------------------------------------------------------------------------

def render_decisions_template(
    triage_receipt: Dict[str, Any],
    *,
    receipt_path: str,
) -> str:
    """Generate a TOML decisions template.

    The template lists one [[decisions]] table per triage candidate, each
    pre-filled with action="defer" so the operator must explicitly OK any
    promotion. Comments preserve the spec's three valid actions and the
    required-when-ratify rule.
    """
    family_version = triage_receipt.get("family_version", "unknown")
    generated_at = triage_receipt.get("generated_at", "")
    queue = triage_receipt.get("queue", [])

    out: List[str] = []
    out.append(
        "# Authority-effect promotion decisions\n"
        "#\n"
        "# Generated from a labelwatch.authority_effect_triage.v0 receipt.\n"
        "# Edit the `action` field per candidate. Valid actions:\n"
        "#\n"
        '#   ratify  - accept; promote into AUTHORITY_EFFECT_MAP overlay\n'
        '#   defer   - skip; revisit on next triage round (default)\n'
        '#   reject  - explicit no; tracked in promotion receipt\n'
        "#\n"
        "# When action = \"ratify\", you MUST set `authority_effect`. Valid values:\n"
        "#   enforcement_instruction, visibility_affecting, advisory,\n"
        "#   reputational, descriptive, telemetry, decorative\n"
        "#\n"
        "# `reason` is required for ratify and reject; defer can be blank.\n"
        "# Lines beginning with `proposed_*` are read-only and shown for context.\n"
    )
    out.append("")
    out.append(f'receipt_path = "{_toml_str(receipt_path)}"')
    out.append(f'family_version = "{_toml_str(family_version)}"')
    out.append(f'triage_generated_at = "{_toml_str(generated_at)}"')
    out.append("")

    for c in queue:
        cid = c.get("candidate_id", "?")
        proposed_effect = c.get("candidate_authority_effect") or ""
        out.append(f"[[decisions]]")
        out.append(f'candidate_id = "{_toml_str(cid)}"')
        out.append(f'labeler_handle = "{_toml_str(c.get("labeler_handle") or "")}"')
        out.append(f'labeler_did = "{_toml_str(c.get("labeler_did") or "")}"')
        out.append(f'label_value = "{_toml_str(c.get("label_value") or "")}"')
        out.append(f'proposed_authority_effect = "{_toml_str(proposed_effect)}"')
        out.append(f'proposed_tier = "{_toml_str(c.get("tier") or "")}"')
        out.append(f'proposed_confidence = "{_toml_str(c.get("confidence") or "")}"')
        ev = c.get("evidence") or {}
        out.append(f'event_count_window = {int(ev.get("event_count", 0))}')
        out.append(f'action = "{ACTION_DEFER}"')
        out.append(f'authority_effect = ""    # required when action="ratify"')
        out.append(f'reason = ""')
        out.append("")

    return "\n".join(out).rstrip() + "\n"


def _toml_str(s: Any) -> str:
    """Minimal TOML basic-string escape: backslash + double-quote.

    Triage strings are emitter descriptions / DIDs / handles — no control
    characters expected, but escape defensively. Newlines get spaced.
    """
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("\r", " ")
    return s


# ---------------------------------------------------------------------------
# HTML review packet
# ---------------------------------------------------------------------------

_REVIEW_CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       max-width: 64rem; margin: 1.5rem auto; padding: 0 1.25rem;
       background: #fafaf7; color: #222; line-height: 1.5; }
h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
h2 { font-size: 1.1rem; margin-top: 2rem; border-bottom: 1px solid #ddd; padding-bottom: 0.25rem; }
.lede { color: #555; max-width: 50rem; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(12rem, 1fr));
                gap: 0.75rem; margin: 1rem 0 1.5rem; }
.summary-card { background: #fff; border: 1px solid #e5e3dd; border-radius: 4px;
                padding: 0.75rem 1rem; }
.summary-card .label { color: #666; font-size: 0.85rem; }
.summary-card .value { font-size: 1.5rem; font-weight: 600; margin-top: 0.15rem; }
.summary-card .sub { color: #888; font-size: 0.8rem; margin-top: 0.15rem; }
.projection { background: #fffbe6; border: 1px solid #f0e0a0; border-radius: 4px;
              padding: 0.75rem 1rem; margin: 1rem 0; }
.candidate { background: #fff; border: 1px solid #ddd; border-radius: 6px;
             padding: 1rem 1.25rem; margin: 0.75rem 0; }
.candidate .id { color: #777; font-family: monospace; font-size: 0.85rem; }
.candidate h3 { margin: 0.1rem 0 0.4rem; font-size: 1.05rem; }
.candidate .meta { color: #555; font-size: 0.9rem; }
.tier-emitter_described { border-left: 4px solid #2766a8; }
.tier-pattern_profile { border-left: 4px solid #2a8454; }
.tier-raw_fallback { border-left: 4px solid #8a8a8a; }
.status-auto_pattern_matched { color: #2a8454; font-weight: 600; }
.status-needs_human_review { color: #b87900; font-weight: 600; }
.status-refused_insufficient_evidence { color: #777; font-weight: 600; }
.status-proposed { color: #555; font-weight: 600; }
.evidence-block { background: #f5f5f0; border-radius: 4px;
                  padding: 0.5rem 0.75rem; margin: 0.5rem 0;
                  font-family: ui-monospace, monospace; font-size: 0.85rem;
                  white-space: pre-wrap; word-break: break-word; }
.rationale, .refusals { margin: 0.4rem 0 0.4rem 0; padding-left: 1.25rem; }
.rationale li { color: #333; }
.refusals li { color: #777; font-size: 0.9rem; }
.action-row { background: #f5f7ff; border: 1px dashed #b8c3e0; border-radius: 4px;
              padding: 0.5rem 0.75rem; margin-top: 0.5rem;
              font-family: ui-monospace, monospace; font-size: 0.85rem; }
.sample-targets { font-size: 0.85rem; color: #555; }
.sample-targets a { color: #2766a8; text-decoration: none; word-break: break-all; }
.sample-targets a:hover { text-decoration: underline; }
.tag { display: inline-block; padding: 0.1rem 0.4rem; border-radius: 3px;
       background: #eee; color: #444; font-size: 0.75rem; margin-right: 0.4rem; }
"""


def render_review_html(
    triage_receipt: Dict[str, Any],
    *,
    decisions_path: Optional[str] = None,
) -> str:
    """Static HTML review packet for a triage receipt.

    Each candidate card surfaces enough to ratify (labeler context, evidence,
    candidate effect, confidence, rationale, refusals, sample targets) and
    points at the action row in the decisions TOML.
    """
    queue = triage_receipt.get("queue", [])
    input_state = triage_receipt.get("input_state", {}) or {}
    proj = triage_receipt.get("projected_reduction", {}) or {}
    tier_breakdown = triage_receipt.get("tier_breakdown", {}) or {}
    promotion_breakdown = triage_receipt.get("promotion_breakdown", {}) or {}

    title = "Labelwatch — authority-effect review packet"

    summary_cards = (
        _summary_card(
            "Total events", f"{input_state.get('total_events_in_window', 0):,}",
            sub=f"window {escape(triage_receipt.get('window', ''))}",
        )
        + _summary_card(
            "Unprofiled events", f"{input_state.get('unprofiled_events', 0):,}",
            sub=f"{input_state.get('unprofiled_share', 0):.1%} of total",
        )
        + _summary_card(
            "Candidates", f"{len(queue):,}",
            sub=f"top {triage_receipt.get('params', {}).get('top_values', '?')} values",
        )
        + _summary_card(
            "Family version", escape(triage_receipt.get("family_version", "?")),
        )
    )

    auto = proj.get("auto_promote_only", {}) or {}
    ratified = proj.get("auto_plus_human_ratified", {}) or {}
    projection_block = (
        f"<div class=\"projection\">"
        f"<strong>Projected unprofiled reduction.</strong> "
        f"Auto-promote only recovers "
        f"{int(auto.get('events_recovered', 0)):,} events "
        f"({auto.get('events_recovered_share_of_total', 0):.1%} of total); "
        f"new unprofiled = {auto.get('new_unprofiled_share', 0):.1%}. "
        f"Auto + human-ratified recovers "
        f"{int(ratified.get('events_recovered', 0)):,} events "
        f"({ratified.get('events_recovered_share_of_total', 0):.1%}); "
        f"new unprofiled = {ratified.get('new_unprofiled_share', 0):.1%}."
        f"</div>"
    )

    decisions_hint = ""
    if decisions_path:
        decisions_hint = (
            f"<p class=\"lede\">Decisions file: "
            f"<code>{escape(decisions_path)}</code>. "
            f"Edit `action` per candidate, then run "
            f"<code>labelwatch authority-effect-promote --from &lt;receipt&gt; "
            f"--decisions &lt;file&gt;</code>.</p>"
        )

    breakdown_block = (
        "<h2>Triage breakdown</h2>"
        "<div class=\"summary-grid\">"
        + _summary_card(
            "Tier mix",
            "<br>".join(f"{escape(k)}: {v}" for k, v in sorted(tier_breakdown.items()))
            or "—",
        )
        + _summary_card(
            "Promotion mix",
            "<br>".join(f"{escape(k)}: {v}" for k, v in sorted(promotion_breakdown.items()))
            or "—",
        )
        + "</div>"
    )

    cards = "\n".join(_candidate_card(c) for c in queue)

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{escape(title)}</title>
<style>{_REVIEW_CSS}</style>
</head><body>
<h1>{escape(title)}</h1>
<p class="lede">
Generated from a <code>labelwatch.authority_effect_triage.v0</code> receipt
({escape(triage_receipt.get('generated_at', ''))}).
The machine proposes; the operator ratifies; the receipt remembers.
Every candidate carries a refusal block: read it before ratifying.
</p>
{decisions_hint}

<h2>Run summary</h2>
<div class="summary-grid">{summary_cards}</div>
{projection_block}

{breakdown_block}

<h2>Candidates</h2>
{cards}

</body></html>
"""


def _summary_card(label: str, value: str, *, sub: str = "") -> str:
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return (
        f"<div class=\"summary-card\">"
        f"<div class=\"label\">{escape(label)}</div>"
        f"<div class=\"value\">{value}</div>"
        f"{sub_html}"
        f"</div>"
    )


def _candidate_card(c: Dict[str, Any]) -> str:
    cid = c.get("candidate_id", "?")
    tier = c.get("tier", "unknown")
    status = c.get("promotion_status", "unknown")
    handle = c.get("labeler_handle") or c.get("labeler_did") or "?"
    label_value = c.get("label_value") or "?"
    effect = c.get("candidate_authority_effect") or "(unclassified)"
    confidence = c.get("confidence", "?")
    ev = c.get("evidence") or {}
    cls_ev = ev.get("emitter_classifier_evidence") or {}
    description = ev.get("labeler_description") or ""
    excerpt = cls_ev.get("description_excerpt") or ""
    loci = ev.get("attachment_loci") or []
    event_count = ev.get("event_count", 0)
    samples = ev.get("sample_targets") or []

    rationale_items = "".join(
        f"<li>{escape(r)}</li>" for r in (c.get("rationale") or [])
    )
    refusal_items = "".join(
        f"<li>{escape(r)}</li>" for r in (c.get("refusals") or [])
    )

    # Build labeler links — Bluesky profile + raw DID
    labeler_did = c.get("labeler_did") or ""
    labeler_links = ""
    if c.get("labeler_handle"):
        labeler_links += (
            f' · <a href="https://bsky.app/profile/{escape(c["labeler_handle"])}" '
            f'rel="noopener" target="_blank">profile</a>'
        )
    if labeler_did:
        labeler_links += (
            f' · <a href="https://plc.directory/{escape(labeler_did)}" '
            f'rel="noopener" target="_blank">plc</a>'
        )

    sample_targets_html = ""
    if samples:
        items: List[str] = []
        for uri in samples[:5]:
            display = escape(uri)
            link = _bsky_link_for(uri)
            if link:
                items.append(
                    f'<li><a href="{escape(link)}" rel="noopener" '
                    f'target="_blank">{display}</a></li>'
                )
            else:
                items.append(f"<li>{display}</li>")
        sample_targets_html = (
            "<div class=\"sample-targets\"><strong>Sample targets:</strong>"
            "<ul>" + "".join(items) + "</ul></div>"
        )

    description_block = ""
    if description:
        description_block = (
            "<div class=\"evidence-block\"><strong>Labeler description:</strong>\n"
            + escape(description) + "</div>"
        )

    excerpt_block = ""
    if excerpt:
        excerpt_block = (
            "<div class=\"evidence-block\"><strong>Emitter excerpt cited:</strong>\n"
            + escape(excerpt) + "</div>"
        )

    locus_label = ", ".join(escape(x) for x in loci) if loci else "—"

    safe_pattern_note = ""
    if cls_ev.get("safe_pattern_note"):
        safe_pattern_note = (
            f'<div class="tag" title="Safe-class pattern match — '
            f'auto-promote eligible">'
            f'safe pattern: {escape(cls_ev["safe_pattern_note"])}</div>'
        )

    return f"""
<div class="candidate tier-{escape(tier)}">
  <div class="id">{escape(cid)}</div>
  <h3>{escape(handle)} / {escape(label_value)}</h3>
  <div class="meta">
    <span class="tag">tier: {escape(tier)}</span>
    <span class="tag">candidate: {escape(effect)}</span>
    <span class="tag">confidence: {escape(confidence)}</span>
    <span class="status-{escape(status)}">{escape(status)}</span>
    {safe_pattern_note}
  </div>
  <div class="meta">
    events_7d: {event_count:,} · attachment_loci: {locus_label}{labeler_links}
  </div>
  {description_block}
  {excerpt_block}
  <strong>Rationale:</strong>
  <ul class="rationale">{rationale_items}</ul>
  <strong>Refusals (boundaries of this inference):</strong>
  <ul class="refusals">{refusal_items}</ul>
  {sample_targets_html}
  <div class="action-row">
    [[decisions]]<br>
    candidate_id = "{escape(cid)}"<br>
    action = "defer"&nbsp;&nbsp;# ratify | defer | reject<br>
    authority_effect = ""&nbsp;&nbsp;# required when action="ratify"<br>
    reason = ""
  </div>
</div>"""


def _bsky_link_for(uri: str) -> Optional[str]:
    """Map an at:// URI or raw DID into a browser-clickable Bluesky URL.

    Best-effort; if no mapping is obvious, return None and let the card
    show the URI as plain text.
    """
    if not uri:
        return None
    if uri.startswith("did:"):
        return f"https://bsky.app/profile/{uri}"
    if uri.startswith("at://"):
        # at://did/app.bsky.feed.post/rkey  →  bsky.app/profile/did/post/rkey
        rest = uri[len("at://"):]
        if "/app.bsky.feed.post/" in rest:
            did, _, rkey = rest.partition("/app.bsky.feed.post/")
            return f"https://bsky.app/profile/{did}/post/{rkey}"
        if "/app.bsky.actor.profile/" in rest:
            did = rest.split("/")[0]
            return f"https://bsky.app/profile/{did}"
    return None


# ---------------------------------------------------------------------------
# Entry points (called by CLI)
# ---------------------------------------------------------------------------

def load_triage_receipt(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        receipt = json.load(f)
    if receipt.get("receipt_kind") != "labelwatch.authority_effect_triage.v0":
        raise ValueError(
            f"Not an authority-effect triage receipt: receipt_kind="
            f"{receipt.get('receipt_kind')!r}"
        )
    return receipt


def write_review_packet(
    triage_receipt: Dict[str, Any],
    *,
    receipt_path: str,
    out_html: str,
    out_decisions: Optional[str] = None,
) -> None:
    """Write the review HTML and (optionally) the decisions TOML.

    Default behavior writes both files alongside each other:
      <out_html>                  — HTML
      <out_html>.decisions.toml   — TOML template
    Explicit out_decisions overrides the TOML path.
    """
    os.makedirs(os.path.dirname(os.path.abspath(out_html)) or ".", exist_ok=True)
    html = render_review_html(
        triage_receipt,
        decisions_path=out_decisions or (out_html + ".decisions.toml"),
    )
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)

    decisions_target = out_decisions or (out_html + ".decisions.toml")
    os.makedirs(os.path.dirname(os.path.abspath(decisions_target)) or ".", exist_ok=True)
    toml = render_decisions_template(triage_receipt, receipt_path=receipt_path)
    with open(decisions_target, "w", encoding="utf-8") as f:
        f.write(toml)
