"""Labeler registry — browsable directory of all known labelers.

Queries the labelers table to produce a public directory page showing
who is labeling on ATProto, with what activity level, and how legible
their operation is.
"""
from __future__ import annotations

import html
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .report import (
    _endpoint_dot,
    _layout,
    _regime_badge,
    _table,
    _visibility_badge,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def _query_registry(conn) -> List[Dict[str, Any]]:
    """Fetch all labelers with relevant metadata."""
    rows = conn.execute(
        "SELECT labeler_did, handle, display_name, description, "
        "       visibility_class, labeler_class, "
        "       observed_as_src, has_labeler_service, declared_record, "
        "       likely_test_dev, endpoint_status, service_endpoint, "
        "       regime_state, "
        "       auditability_risk_band, inference_risk_band, temporal_coherence_band, "
        "       events_7d, events_30d, unique_targets_7d, unique_targets_30d, "
        "       first_seen, last_seen "
        "FROM labelers "
        "ORDER BY events_7d DESC, handle ASC",
    ).fetchall()
    return [dict(r) for r in rows]


def _query_summary(conn) -> Dict[str, Any]:
    """Aggregate registry statistics."""
    total = conn.execute("SELECT COUNT(*) FROM labelers").fetchone()[0]
    active = conn.execute(
        "SELECT COUNT(*) FROM labelers WHERE observed_as_src = 1",
    ).fetchone()[0]
    declared = conn.execute(
        "SELECT COUNT(*) FROM labelers WHERE declared_record = 1",
    ).fetchone()[0]
    with_service = conn.execute(
        "SELECT COUNT(*) FROM labelers WHERE has_labeler_service = 1",
    ).fetchone()[0]
    test_dev = conn.execute(
        "SELECT COUNT(*) FROM labelers WHERE likely_test_dev = 1",
    ).fetchone()[0]

    # Events totals
    row = conn.execute(
        "SELECT COALESCE(SUM(events_7d), 0), COALESCE(SUM(events_30d), 0) "
        "FROM labelers",
    ).fetchone()

    return {
        "total_labelers": total,
        "active_labelers": active,
        "declared_labelers": declared,
        "with_service": with_service,
        "test_dev": test_dev,
        "total_events_7d": row[0],
        "total_events_30d": row[1],
    }


# ---------------------------------------------------------------------------
# Payload assembly
# ---------------------------------------------------------------------------

def generate_registry(conn) -> Dict[str, Any]:
    """Generate registry payload from DB."""
    labelers = _query_registry(conn)
    summary = _query_summary(conn)

    return {
        "summary": summary,
        "labelers": labelers,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_REGISTRY_INTRO = """\
<div class="card" style="padding:1.2rem;margin-bottom:1.5rem">
<p>This is a directory of every labeler Labelwatch has discovered on the
AT Protocol network. Labelers are services that apply labels to accounts
and content &mdash; moderation flags, badges, community tags, and more.</p>
<p style="margin-top:0.5rem">The protocol lets anyone run a labeler, but
provides no built-in way to see who is labeling, what they claim to do,
or whether their behavior matches their claims. This registry exists to
make the labeling layer <strong>legible</strong>: who is operating, how
active they are, and what we can observe about their behavior.</p>
<p class="small" style="margin-top:0.5rem;opacity:0.7">
Data sourced from Labelwatch's local archive. Not guaranteed exhaustive.</p>
</div>
"""


def _risk_badge(band: Optional[str]) -> str:
    """Render a risk band as a colored badge."""
    if not band:
        return '<span class="badge badge-low-conf">?</span>'
    colors = {
        "low": "badge-stable",
        "medium": "badge-burst",
        "high": "badge-fixated",
        "critical": "badge-fixated",
    }
    cls = colors.get(band, "badge-low-conf")
    return f'<span class="badge {cls}">{html.escape(band)}</span>'


def _format_count(n: Optional[int]) -> str:
    if n is None or n == 0:
        return "\u2014"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def render_registry_html(payload: Dict[str, Any]) -> str:
    """Render the registry as a standalone HTML page."""
    summary = payload["summary"]
    labelers = payload["labelers"]
    gen_at = html.escape(payload.get("generated_at", ""))

    sections: List[str] = []

    # Nav
    sections.append(
        '<p class="small" style="margin-bottom:0.5rem;">'
        '<a href="/">&larr; Back to dashboard</a></p>'
    )

    sections.append(_REGISTRY_INTRO)

    # Summary cards
    sections.append(f"""
    <div class="grid">
      <div class="card health-metric">
        <div class="label">Total Labelers</div>
        <div class="value">{summary["total_labelers"]:,}</div>
      </div>
      <div class="card health-metric">
        <div class="label">Active (observed)</div>
        <div class="value">{summary["active_labelers"]:,}</div>
      </div>
      <div class="card health-metric">
        <div class="label">With Service</div>
        <div class="value">{summary["with_service"]:,}</div>
      </div>
      <div class="card health-metric">
        <div class="label">Events (7d)</div>
        <div class="value">{_format_count(summary["total_events_7d"])}</div>
      </div>
      <div class="card health-metric">
        <div class="label">Events (30d)</div>
        <div class="value">{_format_count(summary["total_events_30d"])}</div>
      </div>
    </div>
    """)

    # Search box
    sections.append("""
    <div style="margin-bottom:1rem">
      <input type="text" id="registry-search"
             placeholder="Search by handle, DID, or description..."
             style="width:100%;padding:0.5rem;border:1px solid var(--border);
                    border-radius:4px;font-size:0.95rem;
                    background:var(--bg);color:var(--fg)">
    </div>
    <div style="margin-bottom:0.8rem">
      <label style="font-size:0.85rem;margin-right:1rem">
        <input type="checkbox" id="hide-test-dev" checked> Hide test/dev
      </label>
      <label style="font-size:0.85rem;margin-right:1rem">
        <input type="checkbox" id="hide-inactive"> Hide inactive (0 events 7d)
      </label>
    </div>
    """)

    # Table
    def _clean_search(text: str) -> str:
        """Sanitize text for use in data-search attribute."""
        # Strip newlines, collapse whitespace, escape for attribute
        return html.escape(
            " ".join(text.lower().split()),
            quote=True,
        )

    rows = []
    for lab in labelers:
        did = lab["labeler_did"]
        h = lab.get("handle") or ""
        dn = lab.get("display_name") or ""
        safe_did = html.escape(did)
        if dn:
            name_display = f'<a href="https://bsky.app/profile/{safe_did}">{html.escape(dn)}</a>'
        elif h:
            name_display = f'<a href="https://bsky.app/profile/{safe_did}">@{html.escape(h)}</a>'
        else:
            name_display = f'<a href="https://bsky.app/profile/{safe_did}">{safe_did}</a>'
        if h and dn:
            name_display += f'<br><span class="small" style="opacity:0.6">@{html.escape(h)}</span>'

        desc = lab.get("description") or ""
        desc_short = html.escape(desc[:80].replace("\n", " ") + ("..." if len(desc) > 80 else ""))

        vis = _visibility_badge(lab.get("visibility_class"))
        endpoint = _endpoint_dot(lab.get("endpoint_status"))
        regime = _regime_badge(lab.get("regime_state"))
        ev7 = _format_count(lab.get("events_7d"))
        ev30 = _format_count(lab.get("events_30d"))
        tgt7 = _format_count(lab.get("unique_targets_7d"))

        audit = _risk_badge(lab.get("auditability_risk_band"))

        test_dev = lab.get("likely_test_dev", 0)
        inactive = 1 if (lab.get("events_7d") or 0) == 0 else 0

        search_text = _clean_search(f"{h} {dn} {did} {desc}")

        row_html = (
            f'<tr data-search="{search_text}" '
            f'data-test-dev="{test_dev}" data-inactive="{inactive}">'
            f'<td style="white-space:nowrap">{name_display}</td>'
            f'<td style="max-width:250px;overflow:hidden;text-overflow:ellipsis">{desc_short}</td>'
            f'<td>{vis}</td>'
            f'<td style="text-align:center">{endpoint}</td>'
            f'<td>{regime}</td>'
            f'<td style="text-align:right">{ev7}</td>'
            f'<td style="text-align:right">{ev30}</td>'
            f'<td style="text-align:right">{tgt7}</td>'
            f'<td>{audit}</td>'
            f'</tr>'
        )
        rows.append(row_html)

    headers = [
        "Labeler", "Description",
        '<span title="How this labeler was discovered">Visibility</span>',
        '<span title="Service endpoint reachability">EP</span>',
        '<span title="Behavioral regime: stable, bursty, warming up, etc.">Regime</span>',
        '<span title="Label events in last 7 days">7d</span>',
        '<span title="Label events in last 30 days">30d</span>',
        '<span title="Unique targets in last 7 days">Tgt 7d</span>',
        '<span title="Auditability risk band: how legible is this operator?">Audit</span>',
    ]
    header_html = "<tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr>"

    table_html = (
        '<div style="overflow-x:auto">'
        '<table id="registry-table" style="width:100%;border-collapse:collapse">'
        f'<thead>{header_html}</thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table></div>'
    )
    sections.append(table_html)

    # Methods footer
    sections.append(
        f'<div class="card" style="margin-top:2rem;font-size:0.85rem;opacity:0.7">'
        f'<p><strong>Methods:</strong> Data from Labelwatch\'s labelers table. '
        f'Includes labelers discovered via batch enumeration, Jetstream, and '
        f'labeler-lists backstop. Activity metrics from label_events. '
        f'Not guaranteed exhaustive. '
        f'Generated: {gen_at}.</p>'
        f'</div>'
    )

    # Search/filter JS
    sections.append("""
    <script>
    (function() {
      var search = document.getElementById('registry-search');
      var hideTest = document.getElementById('hide-test-dev');
      var hideInactive = document.getElementById('hide-inactive');
      var rows = document.querySelectorAll('#registry-table tbody tr');

      function filter() {
        var q = search.value.toLowerCase();
        var ht = hideTest.checked;
        var hi = hideInactive.checked;
        rows.forEach(function(tr) {
          var s = tr.getAttribute('data-search') || '';
          var td = tr.getAttribute('data-test-dev') === '1';
          var ia = tr.getAttribute('data-inactive') === '1';
          var show = true;
          if (q && s.indexOf(q) === -1) show = false;
          if (ht && td) show = false;
          if (hi && ia) show = false;
          tr.style.display = show ? '' : 'none';
        });
      }

      search.addEventListener('input', filter);
      hideTest.addEventListener('change', filter);
      hideInactive.addEventListener('change', filter);
      filter();
    })();
    </script>
    """)

    body = "\n".join(sections)
    return _layout(
        "Labeler Registry",
        body,
        description="Directory of all known labelers on the AT Protocol network.",
    )
