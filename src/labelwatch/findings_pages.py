"""Findings pages — frozen historical findings + live operational scans.

Distinct from `findings.py`, which formats boundary-finding posts for
Bluesky. This module publishes findings PAGES into the report output
that Caddy serves from labelwatch.neutral.zone.

Discipline (T-002):
  Frozen findings make claims.
  Live pages report current measurements.
  Do not let one silently convert into the other.

Two distinct surfaces, both served by `report.py`:

  /findings/operator-maturity/   ← frozen 2026-06-08 snapshot
                                   receipts + regression + doctrine
                                   SOURCE: docs/findings/operator-maturity/

  /operator-maturity/             ← live scan from current DB
                                   regenerated each report run
                                   SOURCE: this module

The frozen page is an admissible historical claim with pinned
receipts. The live page is a current operational surface that may
differ from the frozen snapshot as the ecosystem changes. Both pages
link to each other; neither silently overwrites the other's claims.
"""
from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from html import escape
from typing import Any, Dict, List


PLATFORM_ROOT_DID = "did:plc:ar7c4by46qjdydhdevvrndac"  # moderation.bsky.app

# docs/findings is at repo root, two dirs up from this module
# (src/labelwatch/findings_pages.py → src/labelwatch/ → src/ → repo root).
_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
_FROZEN_FINDINGS_SRC = os.path.join(_REPO_ROOT, "docs", "findings")


# ---------------------------------------------------------------------
# FROZEN FINDINGS — copy/render docs/findings/<topic>/ into served output
# ---------------------------------------------------------------------

def list_frozen_topics() -> List[str]:
    """Return topic dir names under docs/findings/ that have an
    index.md to render."""
    if not os.path.isdir(_FROZEN_FINDINGS_SRC):
        return []
    return sorted(
        d for d in os.listdir(_FROZEN_FINDINGS_SRC)
        if os.path.isdir(os.path.join(_FROZEN_FINDINGS_SRC, d))
        and os.path.exists(os.path.join(_FROZEN_FINDINGS_SRC, d, "index.md"))
    )


def _read_topic_title(topic: str) -> str:
    """Pull the first H1 from <topic>/index.md as the page title."""
    path = os.path.join(_FROZEN_FINDINGS_SRC, topic, "index.md")
    try:
        with open(path) as f:
            for line in f:
                if line.startswith("# "):
                    return line[2:].strip()
    except OSError:
        pass
    return f"Labelwatch finding: {topic}"


def _render_markdown_to_html(md_text: str) -> str:
    """Render markdown to HTML. Falls back to `<pre>` wrapping if the
    markdown library is unavailable (so the publish step never crashes
    on dep absence)."""
    try:
        import markdown as md_lib  # type: ignore
    except ImportError:
        return f"<pre>{escape(md_text)}</pre>"
    return md_lib.markdown(
        md_text,
        extensions=["fenced_code", "tables", "toc"],
    )


def _render_frozen_topic_body(topic: str) -> str:
    """Render <topic>/index.md → HTML body. Returns inner HTML (without
    _layout wrapper); caller adds the shell."""
    src = os.path.join(_FROZEN_FINDINGS_SRC, topic, "index.md")
    with open(src) as f:
        md_text = f.read()
    body = _render_markdown_to_html(md_text)
    banner = """
<aside class="findings-frozen-banner" style="border:1px solid var(--border,#ccc);border-left:4px solid var(--accent,#2980b9);padding:0.8rem 1rem;margin:0 0 1rem 0;background:var(--bg-muted,#f6f7f9);">
  <p style="margin:0;"><strong>Frozen finding.</strong> Admissible historical claim based on a fixed snapshot; receipts, regression checks, and doctrine remain pinned to the snapshot date. A <a href="/operator-maturity/">live operator-maturity scan</a> reports current measurements and may differ as the ecosystem changes — do not conflate the two.</p>
</aside>
"""
    return banner + body


def install_frozen_findings(out_dir: str, layout_fn) -> int:
    """Copy docs/findings/<topic>/ into out_dir/findings/<topic>/ for
    every topic that has an index.md. Renders index.md → index.html
    wrapped in the standard Labelwatch layout. Copies artifacts/ and
    regression/ subdirectories verbatim so URLs in the rendered HTML
    resolve. Also writes /findings/index.html listing all topics.

    Returns the count of topics installed.
    """
    topics = list_frozen_topics()
    if not topics:
        return 0

    findings_root = os.path.join(out_dir, "findings")
    os.makedirs(findings_root, exist_ok=True)

    for topic in topics:
        topic_src = os.path.join(_FROZEN_FINDINGS_SRC, topic)
        topic_out = os.path.join(findings_root, topic)
        os.makedirs(topic_out, exist_ok=True)

        title = _read_topic_title(topic)
        body = _render_frozen_topic_body(topic)
        canonical = f"/findings/{topic}/"
        description = (
            f"Frozen Labelwatch finding: {title}. Admissible historical "
            "claim with pinned receipts and regression checks."
        )
        page_html = layout_fn(title, body, canonical=canonical,
                              description=description)
        with open(os.path.join(topic_out, "index.html"), "w") as f:
            f.write(page_html)

        # Preserve the raw markdown alongside the rendered HTML so the
        # snapshot is fully reproducible from the served surface.
        shutil.copy2(
            os.path.join(topic_src, "index.md"),
            os.path.join(topic_out, "index.md"),
        )
        # Receipts + regression: copy whole subdirectories so URLs in
        # the rendered page actually resolve.
        for subdir in ("artifacts", "regression"):
            src = os.path.join(topic_src, subdir)
            if os.path.isdir(src):
                dst = os.path.join(topic_out, subdir)
                if os.path.exists(dst):
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)

    # Top-level findings index
    items = []
    for topic in topics:
        title = _read_topic_title(topic)
        items.append(
            f'<li><a href="{escape(topic)}/">{escape(title)}</a> '
            f'<span class="small">(<code>{escape(topic)}</code>)</span></li>'
        )
    index_body = (
        "<h1>Labelwatch findings</h1>\n"
        "<p>Frozen admissible historical claims. Each finding pins a "
        "snapshot of receipts + regression checks at the date it was "
        "published; the underlying ecosystem may have changed since.</p>\n"
        "<ul>\n" + "\n".join(items) + "\n</ul>\n"
        '<p class="small"><a href="/operator-maturity/">Live '
        "operator-maturity scan</a> (current data; may differ from the "
        "frozen finding).</p>"
    )
    index_html = layout_fn(
        "Labelwatch findings", index_body,
        canonical="/findings/",
        description=(
            "Labelwatch findings — frozen admissible historical claims."
        ),
    )
    with open(os.path.join(findings_root, "index.html"), "w") as f:
        f.write(index_html)

    return len(topics)


# ---------------------------------------------------------------------
# LIVE OPERATOR-MATURITY SCAN — regenerated from current DB each run
# ---------------------------------------------------------------------

_LIVE_SCAN_SQL = """
WITH latest_record AS (
  SELECT
    de.labeler_did,
    de.record_json,
    de.discovered_at,
    ROW_NUMBER() OVER (
      PARTITION BY de.labeler_did
      ORDER BY de.discovered_at DESC
    ) AS rn
  FROM discovery_events de
  WHERE de.operation IN ('create','update')
    AND json_extract(de.record_json,'$.policies.labelValueDefinitions') IS NOT NULL
),
service_record_stats AS (
  SELECT
    labeler_did,
    COUNT(*) AS service_record_revisions
  FROM discovery_events
  WHERE operation IN ('create','update')
    AND json_extract(record_json,'$.policies.labelValueDefinitions') IS NOT NULL
  GROUP BY labeler_did
)
SELECT
  l.labeler_did,
  COALESCE(l.handle, '') AS handle,
  COALESCE(l.display_name, '') AS display_name,
  COALESCE(l.labeler_class, 'third_party') AS labeler_class,
  l.is_reference,
  COALESCE(l.events_30d, 0) AS events_30d,
  COALESCE(l.unique_targets_30d, 0) AS unique_targets_30d,
  COALESCE(l.likely_test_dev, 0) AS likely_test_dev,
  COALESCE(srs.service_record_revisions, 0) AS service_record_revisions,
  lr.record_json AS latest_record_json
FROM labelers l
LEFT JOIN service_record_stats srs ON l.labeler_did = srs.labeler_did
LEFT JOIN latest_record lr ON l.labeler_did = lr.labeler_did AND lr.rn = 1
WHERE COALESCE(l.events_30d, 0) > 0
   OR srs.service_record_revisions > 0
ORDER BY l.events_30d DESC NULLS LAST
"""


def _classify_live_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Compute maturity_class + key flags. Mirrors
    docs/analysis/tools/operator_maturity_scan.py's logic exactly so
    the live page and the frozen scanner agree on cohort definitions."""
    record_json = row.get("latest_record_json")
    def_count = 0
    explains = False
    if record_json:
        try:
            rec = json.loads(record_json)
            defs = (rec.get("policies") or {}).get("labelValueDefinitions") or []
            def_count = len(defs)
            for d in defs:
                locales = d.get("locales") or []
                if any(
                    (l.get("name") or "").strip() or (l.get("description") or "").strip()
                    for l in locales
                ):
                    explains = True
                    break
        except (json.JSONDecodeError, TypeError):
            pass

    events_30d = row["events_30d"]
    did = row["labeler_did"]
    if did == PLATFORM_ROOT_DID:
        maturity = "platform-root"
    elif row["likely_test_dev"]:
        maturity = "experimental"
    elif events_30d == 0 and row["service_record_revisions"] > 0:
        maturity = "abandoned"
    elif events_30d == 0:
        maturity = "unknown"
    elif events_30d >= 10000 and def_count >= 1:
        maturity = "moderation-infrastructure"
    elif events_30d >= 100 and def_count >= 1 and explains:
        maturity = "community-service"
    elif events_30d >= 10 and def_count >= 1:
        maturity = "personal/reputational"
    elif events_30d < 10:
        maturity = "experimental"
    else:
        maturity = "unknown"

    return {
        "did": did,
        "handle": row["handle"] or "<unknown>",
        "labeler_class": row["labeler_class"],
        "events_30d": events_30d,
        "unique_targets_30d": row["unique_targets_30d"],
        "latest_label_def_count": def_count,
        "service_record_revisions": row["service_record_revisions"],
        "maturity_class": maturity,
    }


def _compute_live_cohorts(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply the F-007 / F-008 cohort definitions to live data."""
    f007 = [
        r for r in rows
        if r["events_30d"] > 1000 and r["latest_label_def_count"] == 0
    ]
    abandoned = [r for r in rows if r["maturity_class"] == "abandoned"]
    f008_substantial = [
        r for r in abandoned if r["latest_label_def_count"] >= 6
    ]
    histogram: Dict[str, int] = {}
    for r in rows:
        histogram[r["maturity_class"]] = histogram.get(r["maturity_class"], 0) + 1
    return {
        "f007_count": len(f007),
        "f008_abandoned_total": len(abandoned),
        "f008_substantial_scope": len(f008_substantial),
        "histogram": histogram,
        "total_rows": len(rows),
    }


def render_live_operator_maturity_html(conn, layout_fn) -> str:
    """Render the LIVE operator-maturity page from a current DB
    connection. Returns the full layout-wrapped HTML.

    This page is regenerated each report run. It MUST not be confused
    with the frozen finding at /findings/operator-maturity/.
    """
    rows_raw = [dict(r) for r in conn.execute(_LIVE_SCAN_SQL).fetchall()]
    rows = [_classify_live_row(r) for r in rows_raw]
    cohorts = _compute_live_cohorts(rows)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    banner = """
<aside class="live-scan-banner" style="border:1px solid var(--border,#ccc);border-left:4px solid var(--accent,#c0392b);padding:0.8rem 1rem;margin:0 0 1rem 0;background:var(--bg-muted,#f6f7f9);">
  <p style="margin:0;"><strong>Live operational scan.</strong> Current measurements from labelwatch's database; numbers drift as the ecosystem changes. For an <em>admissible historical claim</em> with pinned receipts and regression checks, see the <a href="/findings/operator-maturity/">frozen 2026-06-08 finding</a>.</p>
</aside>
"""

    provenance = f"""
<dl class="live-scan-provenance">
  <dt>Generated at</dt><dd>{escape(now_iso)} (UTC)</dd>
  <dt>Source DB / window</dt><dd>labelwatch.db, <code>events_30d</code> = last 30 days as of generation</dd>
  <dt>Definitions source</dt><dd>latest labeler service record from <code>discovery_events</code> (or snapshot fallback per Bundle F)</dd>
  <dt>Rows scanned</dt><dd>{cohorts['total_rows']}</dd>
  <dt>Caveat</dt><dd><code>maturity_class</code> is a heuristic, NOT a normative judgment about any specific labeler (D-002 in <a href="/findings/operator-maturity/">the frozen finding</a>)</dd>
</dl>
"""

    f007_count = cohorts["f007_count"]
    f008_abandoned = cohorts["f008_abandoned_total"]
    f008_substantial = cohorts["f008_substantial_scope"]
    central_table = f"""
<table class="doctrine-triad">
  <thead><tr>
    <th>Property observed</th>
    <th>Does not imply</th>
    <th>Current evidence (this scan)</th>
  </tr></thead>
  <tbody>
    <tr>
      <td>Label emission</td>
      <td>Declared consumer semantics</td>
      <td>{f007_count} high-volume emitters publish zero definitions
          (F-007 cohort: events_30d &gt; 1000 with no labelValueDefinitions)</td>
    </tr>
    <tr>
      <td>Declared consumer semantics</td>
      <td>Operational liveness</td>
      <td>{f008_abandoned} abandoned labelers; {f008_substantial} retain
          substantial declared scope (≥6 definitions) despite zero recent
          emissions (F-008 cohort)</td>
    </tr>
    <tr>
      <td>Operational liveness</td>
      <td>Moderation authority</td>
      <td>0/7 sampled production Bluesky clients hardcode any
          third-party labeler as a default
          (<a href="/findings/operator-maturity/">consumer-conversion census</a>;
          point-in-time, not regenerated here)</td>
    </tr>
  </tbody>
</table>
"""

    hist_rows = "".join(
        f"<tr><td>{escape(cls)}</td><td>{count}</td></tr>"
        for cls, count in sorted(cohorts["histogram"].items(), key=lambda kv: -kv[1])
    )
    histogram_table = f"""
<h2>Maturity class histogram</h2>
<p class="small">Heuristic categories (D-002 caveat above).</p>
<table>
  <thead><tr><th>maturity_class</th><th>count</th></tr></thead>
  <tbody>{hist_rows}</tbody>
</table>
"""

    top_rows = sorted(rows, key=lambda r: -r["events_30d"])[:20]
    top_html_rows = "".join(
        '<tr>'
        f'<td>{escape(r["maturity_class"])}</td>'
        f'<td><code>{escape(r["handle"])}</code></td>'
        f'<td>{r["events_30d"]:,}</td>'
        f'<td>{r["latest_label_def_count"]}</td>'
        f'<td>{r["service_record_revisions"]}</td>'
        '</tr>'
        for r in top_rows
    )
    top_table = f"""
<h2>Top 20 by events_30d (live)</h2>
<table>
  <thead><tr>
    <th>maturity_class</th><th>handle</th><th>events_30d</th>
    <th>defs</th><th>service_record_revisions</th>
  </tr></thead>
  <tbody>{top_html_rows}</tbody>
</table>
"""

    body = (
        banner
        + "<h1>Live operator-maturity scan</h1>\n"
        + provenance
        + "<h2>Doctrine triad — current evidence</h2>\n"
        + central_table
        + histogram_table
        + top_table
        + '<p class="small">Frozen historical finding (2026-06-08 snapshot, '
          'with full receipts + regression): '
          '<a href="/findings/operator-maturity/">findings/operator-maturity/</a></p>\n'
    )
    return layout_fn(
        "Live operator-maturity scan",
        body,
        canonical="/operator-maturity/",
        description=(
            "Live operator-maturity scan from labelwatch's current DB. "
            "For the admissible historical finding, see "
            "/findings/operator-maturity/."
        ),
    )


def install_live_operator_maturity(out_dir: str, conn, layout_fn) -> None:
    """Generate /operator-maturity/index.html into out_dir."""
    target_dir = os.path.join(out_dir, "operator-maturity")
    os.makedirs(target_dir, exist_ok=True)
    html = render_live_operator_maturity_html(conn, layout_fn)
    with open(os.path.join(target_dir, "index.html"), "w") as f:
        f.write(html)
