from __future__ import annotations

import json
import os
import shutil
import uuid
from importlib import metadata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Dict, List, Optional

from . import db
from .receipts import config_hash as config_hash_fn
from .utils import format_ts, get_git_commit, parse_ts


def _did_slug(did: str) -> str:
    """Convert DID to URL-safe slug: did:plc:abc123 → did-plc-abc123."""
    return did.replace(":", "-")


def _handle_cache(conn) -> Dict[str, Optional[str]]:
    rows = conn.execute("SELECT labeler_did, handle FROM labelers").fetchall()
    return {r["labeler_did"]: r["handle"] for r in rows}


def _display_name_cache(conn) -> Dict[str, Optional[str]]:
    rows = conn.execute("SELECT labeler_did, display_name FROM labelers").fetchall()
    return {r["labeler_did"]: r["display_name"] for r in rows}


def _display_name(did: str, handles: Dict[str, Optional[str]], display_names: Optional[Dict[str, Optional[str]]] = None) -> str:
    if display_names:
        dn = display_names.get(did)
        if dn:
            return dn
    h = handles.get(did)
    if h:
        return f"{h}"
    return did


def _labeler_link(did: str, handles: Dict[str, Optional[str]], display_names: Optional[Dict[str, Optional[str]]] = None) -> str:
    slug = _did_slug(did)
    dn = display_names.get(did) if display_names else None
    h = handles.get(did)
    label = dn or h
    if label:
        return f'<a href="labeler/{slug}.html">{escape(label)}</a> <span class="small">({escape(did)})</span>'
    return f'<a href="labeler/{slug}.html">{escape(did)}</a>'


def _endpoint_dot(status: Optional[str]) -> str:
    if status == "accessible":
        return '<span class="endpoint-dot endpoint-ok" title="Accessible"></span>'
    elif status in ("auth_required", "unknown"):
        return '<span class="endpoint-dot endpoint-warn" title="' + escape(status or "unknown") + '"></span>'
    elif status == "down":
        return '<span class="endpoint-dot endpoint-down" title="Down"></span>'
    return '<span class="endpoint-dot endpoint-warn" title="Unknown"></span>'


def _confidence_badge(inputs_json: Optional[str]) -> str:
    if not inputs_json:
        return ""
    try:
        inputs = json.loads(inputs_json)
        conf = inputs.get("confidence", "")
        if conf == "low":
            return ' <span class="badge badge-low-conf">Low confidence</span>'
    except (json.JSONDecodeError, AttributeError):
        pass
    return ""


def _visibility_badge(vis_class: Optional[str]) -> str:
    badges = {
        "declared": ("Declared", "badge-stable"),
        "protocol_public": ("Protocol", "badge-burst"),
        "observed_only": ("Observed", "badge-fixated"),
        "unresolved": ("Unresolved", "badge-low-conf"),
    }
    label, cls = badges.get(vis_class or "unresolved", ("Unknown", "badge-low-conf"))
    return f'<span class="badge {cls}">{escape(label)}</span>'


STYLE = """
:root {
  --bg: #fff; --fg: #111; --fg-muted: #666; --border: #ddd;
  --link: #0b5394; --link-hover-bg: #f0f7fb;
  --card-bg: #fff; --card-border: #ddd;
  --anomaly-bg: #fff8f0;
  --methods-bg: #f8f9fa; --methods-border: #e9ecef;
  --ref-bg: #f0f7fb; --ref-border: #b8d4e3;
  --warmup-bg: #fff3cd; --warmup-border: #ffc107;
  --rollup-bg: #f8f9fa; --rollup-border: #e9ecef;
  --sparkline-stroke: #0b5394;
  --badge-stable-bg: #d4edda; --badge-stable-fg: #155724;
  --badge-burst-bg: #fff3cd; --badge-burst-fg: #856404;
  --badge-churn-bg: #f8d7da; --badge-churn-fg: #721c24;
  --badge-fixated-bg: #ffe0cc; --badge-fixated-fg: #7a3300;
  --badge-flipflop-bg: #e2d5f1; --badge-flipflop-fg: #3d1f6e;
  --badge-lowconf-bg: #e2e3e5; --badge-lowconf-fg: #6c757d;
  --pre-bg: #f5f5f5;
}
[data-theme="dark"] {
  --bg: #1a1a2e; --fg: #e0e0e0; --fg-muted: #999; --border: #333;
  --link: #6db3f2; --link-hover-bg: #252545;
  --card-bg: #16213e; --card-border: #333;
  --anomaly-bg: #2a2218;
  --methods-bg: #16213e; --methods-border: #333;
  --ref-bg: #0f2a3e; --ref-border: #1a5276;
  --warmup-bg: #332b00; --warmup-border: #665500;
  --rollup-bg: #16213e; --rollup-border: #333;
  --sparkline-stroke: #6db3f2;
  --badge-stable-bg: #1e3a2a; --badge-stable-fg: #8fd6a8;
  --badge-burst-bg: #3a3520; --badge-burst-fg: #e6c866;
  --badge-churn-bg: #3a2020; --badge-churn-fg: #e68888;
  --badge-fixated-bg: #3a2a1a; --badge-fixated-fg: #e6a866;
  --badge-flipflop-bg: #2a2040; --badge-flipflop-fg: #c0a8e6;
  --badge-lowconf-bg: #2a2a2a; --badge-lowconf-fg: #aaa;
  --pre-bg: #16213e;
}
body { font-family: Georgia, "Times New Roman", serif; margin: 2rem; color: var(--fg); background: var(--bg); }
header { margin-bottom: 2rem; display: flex; justify-content: space-between; align-items: flex-start; }
header > div { flex: 1; }
h1, h2, h3 { font-family: "Gill Sans", "Trebuchet MS", sans-serif; }
table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
th, td { border-bottom: 1px solid var(--border); padding: 0.5rem; text-align: left; }
.small { color: var(--fg-muted); font-size: 0.9rem; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; }
.card { border: 1px solid var(--card-border); padding: 1rem; border-radius: 6px; background: var(--card-bg); }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }
code { font-family: "Courier New", monospace; }
pre { background: var(--pre-bg); padding: 0.5rem; border-radius: 4px; overflow-x: auto; }
.badge { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 3px; font-size: 0.8rem; font-weight: bold; margin-right: 0.3rem; }
.badge-stable { background: var(--badge-stable-bg); color: var(--badge-stable-fg); }
.badge-burst { background: var(--badge-burst-bg); color: var(--badge-burst-fg); }
.badge-churn { background: var(--badge-churn-bg); color: var(--badge-churn-fg); }
.badge-fixated { background: var(--badge-fixated-bg); color: var(--badge-fixated-fg); }
.badge-flipflop { background: var(--badge-flipflop-bg); color: var(--badge-flipflop-fg); }
.health-bar { display: flex; gap: 1.5rem; align-items: center; margin: 0.5rem 0; }
.health-metric { text-align: center; }
.health-metric .value { font-size: 1.4rem; font-weight: bold; }
.health-metric .label { font-size: 0.75rem; color: var(--fg-muted); }
.sparkline { vertical-align: middle; }
.anomaly-row { background: var(--anomaly-bg); }
.methods { background: var(--methods-bg); border: 1px solid var(--methods-border); padding: 1rem; border-radius: 6px; margin-top: 2rem; font-size: 0.85rem; }
.reference-lane { border: 2px solid var(--ref-border); background: var(--ref-bg); padding: 1rem; border-radius: 6px; margin-bottom: 1.5rem; }
.reference-lane h2 { margin-top: 0; color: var(--link); }
.badge-low-conf { background: var(--badge-lowconf-bg); color: var(--badge-lowconf-fg); font-weight: normal; }
.endpoint-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.3rem; vertical-align: middle; }
.endpoint-ok { background: #28a745; }
.endpoint-warn { background: #ffc107; }
.endpoint-down { background: #dc3545; }
.class-group { margin-bottom: 2rem; }
.class-group h3 { color: var(--fg-muted); border-bottom: 1px solid var(--border); padding-bottom: 0.3rem; }
.tab-bar { display: flex; gap: 0; border-bottom: 2px solid var(--border); margin-bottom: 1rem; }
.tab-bar button { background: none; border: none; padding: 0.5rem 1rem; cursor: pointer; font-size: 0.95rem; color: var(--fg); border-bottom: 2px solid transparent; margin-bottom: -2px; }
.tab-bar button.active { border-bottom-color: var(--link); color: var(--link); font-weight: bold; }
.tab-bar button:hover { background: var(--link-hover-bg); }
.warmup-banner { background: var(--warmup-bg); border: 1px solid var(--warmup-border); padding: 0.75rem 1rem; border-radius: 6px; margin-bottom: 1rem; font-size: 0.9rem; }
.census-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin: 1rem 0; }
.census-card { border: 1px solid var(--card-border); padding: 0.75rem; border-radius: 6px; text-align: center; background: var(--card-bg); }
.census-card .value { font-size: 1.6rem; font-weight: bold; }
.census-card .label { font-size: 0.75rem; color: var(--fg-muted); }
.evidence-section { margin-top: 1rem; }
.evidence-section summary { cursor: pointer; font-weight: bold; color: var(--link); }
.evidence-item { margin: 0.3rem 0; font-size: 0.85rem; }
.rollup { background: var(--rollup-bg); border: 1px solid var(--rollup-border); padding: 0.5rem; border-radius: 4px; margin: 0.5rem 0; }
.rollup summary { cursor: pointer; font-size: 0.9rem; }
.hidden { display: none; }
.theme-toggle { background: none; border: 1px solid var(--border); border-radius: 4px; padding: 0.3rem 0.6rem; cursor: pointer; font-size: 0.85rem; color: var(--fg-muted); }
.theme-toggle:hover { background: var(--link-hover-bg); }
.explainer { background: var(--methods-bg); border: 1px solid var(--methods-border); padding: 1rem 1.25rem; border-radius: 6px; margin-bottom: 1.5rem; max-width: 52rem; font-size: 0.95rem; line-height: 1.5; }
.explainer p { margin: 0.4rem 0; }
.labeler-context { color: var(--fg-muted); font-size: 0.9rem; margin-bottom: 0.75rem; }
"""

TRIAGE_JS = """
<script>
(function() {
  var tabs = document.querySelectorAll('.tab-bar button');
  var rows = document.querySelectorAll('.labeler-row');
  var testToggle = document.getElementById('toggle-test-dev');
  var inactiveToggle = document.getElementById('toggle-inactive');

  function applyFilters() {
    var active = document.querySelector('.tab-bar button.active');
    var view = active ? active.dataset.view : 'all';
    var showTest = testToggle && testToggle.checked;
    var showInactive = inactiveToggle && inactiveToggle.checked;
    var shown = 0;
    rows.forEach(function(row) {
      var isTest = row.dataset.testDev === '1';
      var isInactive = row.dataset.inactive === '1';
      var matchView = true;

      if (view === 'active') matchView = row.dataset.events7d !== '0';
      else if (view === 'alerts') matchView = row.dataset.alertCount !== '0';
      else if (view === 'new') matchView = row.dataset.isNew === '1';
      else if (view === 'opaque') matchView = row.dataset.opaque === '1';

      var visible = matchView;
      if (!showTest && isTest) visible = false;
      if (!showInactive && isInactive) visible = false;

      row.style.display = visible ? '' : 'none';
      if (visible) shown++;
    });
    // Update count badges
    tabs.forEach(function(btn) {
      var v = btn.dataset.view;
      var count = 0;
      rows.forEach(function(r) {
        var isTest = r.dataset.testDev === '1';
        var isInactive = r.dataset.inactive === '1';
        if (!showTest && isTest) return;
        if (!showInactive && isInactive) return;
        if (v === 'all') count++;
        else if (v === 'active' && r.dataset.events7d !== '0') count++;
        else if (v === 'alerts' && r.dataset.alertCount !== '0') count++;
        else if (v === 'new' && r.dataset.isNew === '1') count++;
        else if (v === 'opaque' && r.dataset.opaque === '1') count++;
      });
      btn.querySelector('.tab-count').textContent = '(' + count + ')';
    });
  }

  tabs.forEach(function(btn) {
    btn.addEventListener('click', function() {
      tabs.forEach(function(b) { b.classList.remove('active'); });
      btn.classList.add('active');
      applyFilters();
    });
  });

  if (testToggle) testToggle.addEventListener('change', applyFilters);
  if (inactiveToggle) inactiveToggle.addEventListener('change', applyFilters);

  applyFilters();
})();
</script>
"""


def _write(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


THEME_JS = """
<script>
(function() {
  var stored = localStorage.getItem('lw-theme');
  if (stored) {
    document.documentElement.setAttribute('data-theme', stored);
  } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    document.documentElement.setAttribute('data-theme', 'dark');
  }
})();
</script>
"""

THEME_TOGGLE_JS = """
<script>
(function() {
  var btn = document.getElementById('theme-toggle');
  if (!btn) return;
  function update() {
    var current = document.documentElement.getAttribute('data-theme');
    btn.textContent = current === 'dark' ? 'Light mode' : 'Dark mode';
  }
  btn.addEventListener('click', function() {
    var current = document.documentElement.getAttribute('data-theme');
    var next = current === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('lw-theme', next);
    update();
  });
  update();
})();
</script>
"""


def _layout(title: str, body: str, canonical: str = "") -> str:
    canonical_tag = f'\n<link rel="canonical" href="{escape(canonical)}" />' if canonical else ""
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<meta http-equiv="Cache-Control" content="no-cache, must-revalidate" />
<title>{escape(title)}</title>{canonical_tag}
<style>{STYLE}</style>
{THEME_JS}
</head>
<body>
<header>
<div>
<h1>{escape(title)}</h1>
<p class="small">Generated by labelwatch</p>
</div>
<button id="theme-toggle" class="theme-toggle">Dark mode</button>
</header>
{body}
{THEME_TOGGLE_JS}
</body>
</html>"""


def _table(headers: List[str], rows: List[List[str]]) -> str:
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _sparkline_svg(values: List[int], width: int = 120, height: int = 24) -> str:
    if not values or max(values) == 0:
        return f'<svg class="sparkline" width="{width}" height="{height}"></svg>'
    n = len(values)
    peak = max(values)
    pad = 1
    points = []
    for i, v in enumerate(values):
        x = pad + (i / max(n - 1, 1)) * (width - 2 * pad)
        y = height - pad - (v / peak) * (height - 2 * pad)
        points.append(f"{x:.1f},{y:.1f}")
    polyline = " ".join(points)
    return (
        f'<svg class="sparkline" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        f'<polyline points="{polyline}" fill="none" stroke="var(--sparkline-stroke, #0b5394)" stroke-width="1.5" />'
        f'</svg>'
    )


def _hourly_counts(conn, labeler_did: str, start: str, end: str, buckets: int = 168) -> List[int]:
    rows = conn.execute(
        "SELECT ts FROM label_events WHERE labeler_did=? AND ts>=? AND ts<? ORDER BY ts",
        (labeler_did, start, end),
    ).fetchall()
    if not rows:
        return [0] * buckets
    start_dt = parse_ts(start)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    counts = [0] * buckets
    for r in rows:
        dt = _parse_ts_safe(r["ts"])
        if dt is None:
            continue
        offset_hours = int((dt - start_dt).total_seconds() / 3600)
        idx = max(0, min(buckets - 1, offset_hours))
        counts[idx] += 1
    return counts


def _labeler_badges(conn, labeler_did: str, start: str, end: str) -> List[tuple[str, str]]:
    alert_rows = conn.execute(
        "SELECT rule_id FROM alerts WHERE labeler_did=? AND ts>=? AND ts<=?",
        (labeler_did, start, end),
    ).fetchall()
    rules_fired = {r["rule_id"] for r in alert_rows}
    badges = []
    if "label_rate_spike" in rules_fired:
        badges.append(("Burst-prone", "badge-burst"))
    if "churn_index" in rules_fired:
        badges.append(("High churn", "badge-churn"))
    if "target_concentration" in rules_fired:
        badges.append(("Target-fixated", "badge-fixated"))
    if "flip_flop" in rules_fired:
        badges.append(("Reversal-heavy", "badge-flipflop"))
    if not badges:
        badges.append(("Stable", "badge-stable"))
    return badges


def _badges_html(badges: List[tuple[str, str]]) -> str:
    return " ".join(f'<span class="badge {cls}">{escape(label)}</span>' for label, cls in badges)


def _labeler_health_card(conn, labeler_did: str, start_7d: str, now_ts: str, sparkline_counts: List[int]) -> str:
    events_7d = conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=? AND ts<=?",
        (labeler_did, start_7d, now_ts),
    ).fetchone()["c"]
    unique_targets = conn.execute(
        "SELECT COUNT(DISTINCT uri) AS c FROM label_events WHERE labeler_did=? AND ts>=? AND ts<=?",
        (labeler_did, start_7d, now_ts),
    ).fetchone()["c"]
    alert_count = conn.execute(
        "SELECT COUNT(*) AS c FROM alerts WHERE labeler_did=? AND ts>=? AND ts<=?",
        (labeler_did, start_7d, now_ts),
    ).fetchone()["c"]
    target_spread = f"{unique_targets}/{events_7d}" if events_7d else "0/0"
    sparkline = _sparkline_svg(sparkline_counts)
    badges = _labeler_badges(conn, labeler_did, start_7d, now_ts)

    return f"""
<div class="card">
  <div class="health-bar">
    <div class="health-metric"><div class="value">{events_7d}</div><div class="label">Events (7d)</div></div>
    <div class="health-metric"><div class="value">{target_spread}</div><div class="label">Targets/Events</div></div>
    <div class="health-metric"><div class="value">{alert_count}</div><div class="label">Anomalies</div></div>
    <div class="health-metric">{sparkline}<div class="label">Activity (7d)</div></div>
  </div>
  <div>{_badges_html(badges)}</div>
</div>
"""


def _evidence_expander(conn, labeler_did: str, row) -> str:
    """Render a <details> expander showing classification evidence."""
    reason = row["classification_reason"] or "No classification yet"
    version = row["classification_version"] or "unknown"
    classified_at = row["classified_at"] or "never"

    evidence_rows = db.get_evidence(conn, labeler_did)
    evidence_html = ""
    for ev in evidence_rows[:20]:
        evidence_html += f'<div class="evidence-item">{escape(ev["evidence_type"])}: {escape(str(ev["evidence_value"]))} <span class="small">({escape(ev["ts"])})</span></div>'
    if not evidence_rows:
        evidence_html = '<div class="evidence-item">No evidence records yet.</div>'

    return f"""
<div class="evidence-section">
<details><summary>Why classified this way</summary>
<div class="card" style="margin-top:0.5rem">
  <div><strong>Reason:</strong> {escape(reason)}</div>
  <div><strong>Classifier version:</strong> {escape(version)}</div>
  <div><strong>Classified at:</strong> {escape(classified_at)}</div>
  <h4>Evidence surfaces</h4>
  {evidence_html}
</div>
</details>
</div>
"""


METHODS_HTML = """
<div class="methods">
<h3>Methods and caveats</h3>
<ul>
<li><strong>What is a labeler?</strong> A third-party service that publishes tags (labels)
about Bluesky posts or accounts. Clients choose which labelers to subscribe to
and how to interpret labels (ignore / warn / hide).</li>
<li>This site observes labeler behavior only. No content analysis, no user profiling,
no moderation actions.</li>
<li>Rules detect geometric patterns (rate anomalies, target distribution, churn).
Thresholds are configurable.</li>
<li>Every alert includes a receipted hash over rule_id, config, inputs, and evidence for reproducibility.</li>
<li>Sparklines show hourly event counts over 7 days. Badges summarize which rules fired in the period.</li>
<li>Observation surface: label events from com.atproto.label.queryLabels and labeler declarations from com.atproto.sync.listReposByCollection. Does not observe content, profiles, or social graph.</li>
<li>Classification is based on structured evidence from multiple surfaces (registry declaration, DID document, endpoint probing, observed label activity). Each labeler page includes a "Why classified this way" expander with full evidence.</li>
</ul>
</div>
"""


def _alerts_by_rule(conn, start: str, end: str) -> Dict[str, int]:
    rows = conn.execute(
        "SELECT rule_id, COUNT(*) AS c FROM alerts WHERE ts>=? AND ts<=? GROUP BY rule_id",
        (start, end),
    ).fetchall()
    return {r["rule_id"]: r["c"] for r in rows}


def _top_labelers(conn, start: str, end: str, limit: int = 10) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT labeler_did, COUNT(*) AS c
        FROM alerts
        WHERE ts>=? AND ts<=?
        GROUP BY labeler_did
        ORDER BY c DESC
        LIMIT ?
        """,
        (start, end, limit),
    ).fetchall()
    return [{"labeler_did": r["labeler_did"], "count": r["c"]} for r in rows]


def _labeler_activity(conn, labeler_did: str, start: str, end: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=? AND ts<=?",
        (labeler_did, start, end),
    ).fetchone()["c"]


def _top_targets(conn, labeler_did: str, start: str, end: str, limit: int = 10) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT uri, COUNT(*) AS c
        FROM label_events
        WHERE labeler_did=? AND ts>=? AND ts<=?
        GROUP BY uri
        ORDER BY c DESC
        LIMIT ?
        """,
        (labeler_did, start, end, limit),
    ).fetchall()
    return [{"uri": r["uri"], "count": r["c"]} for r in rows]


def _alert_events(conn, evidence_hashes: List[str]) -> List[Dict[str, Any]]:
    if not evidence_hashes:
        return []
    placeholders = ",".join(["?"] * len(evidence_hashes))
    rows = conn.execute(
        f"SELECT id, ts, uri, val, neg, cid, event_hash FROM label_events WHERE event_hash IN ({placeholders})",
        evidence_hashes,
    ).fetchall()
    return [dict(r) for r in rows]


def _count_naive_timestamps(conn, table: str) -> int:
    row = conn.execute(
        f"""
        SELECT SUM(
            CASE
                WHEN substr(ts, -1) = 'Z' THEN 0
                WHEN (substr(ts, -6, 1) IN ('+', '-') AND substr(ts, -3, 1) = ':') THEN 0
                ELSE 1
            END
        ) AS c
        FROM {table}
        """
    ).fetchone()
    return int(row["c"] or 0)


def _max_ts(conn, table: str) -> Optional[str]:
    row = conn.execute(f"SELECT MAX(ts) AS ts FROM {table}").fetchone()
    return row["ts"] if row and row["ts"] else None


def _parse_ts_safe(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = parse_ts(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _get_package_version() -> Optional[str]:
    try:
        return metadata.version("labelwatch")
    except metadata.PackageNotFoundError:
        return None


def _prepare_out_dir(out_dir: str) -> str:
    parent = os.path.dirname(os.path.abspath(out_dir)) or "."
    os.makedirs(parent, exist_ok=True)
    tmp_dir = os.path.join(parent, f".report-tmp-{uuid.uuid4().hex}")
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def _commit_out_dir(tmp_dir: str, out_dir: str) -> None:
    if os.path.exists(out_dir):
        backup = out_dir + ".prev"
        if os.path.exists(backup):
            shutil.rmtree(backup)
        os.replace(out_dir, backup)
    os.replace(tmp_dir, out_dir)


def _census_counts(conn) -> Dict[str, Dict[str, int]]:
    """Compute counts by visibility_class, reachability_state, confidence, auditability."""
    result: Dict[str, Dict[str, int]] = {
        "visibility_class": {},
        "reachability_state": {},
        "classification_confidence": {},
        "auditability": {},
    }
    for field in result:
        rows = conn.execute(
            f"SELECT COALESCE({field}, 'unknown') AS val, COUNT(*) AS c FROM labelers GROUP BY val"
        ).fetchall()
        result[field] = {r["val"]: r["c"] for r in rows}
    return result


def _alert_rollups(alerts_list, handles, display_names) -> str:
    """Group low-confidence alerts from same scan into collapsible rollups."""
    # Group by (rule_id, ts) where confidence is low
    rollup_groups: Dict[tuple, list] = defaultdict(list)
    standalone = []
    for r in alerts_list:
        try:
            inputs = json.loads(r["inputs_json"])
            conf = inputs.get("confidence", "high")
        except (json.JSONDecodeError, AttributeError):
            conf = "high"
        if conf == "low":
            rollup_groups[(r["rule_id"], r["ts"])].append(r)
        else:
            standalone.append(r)

    html_parts = []
    # Render standalone alerts normally
    for r in standalone[:50]:
        labeler_cell = _display_name(r["labeler_did"], handles, display_names)
        conf_badge = _confidence_badge(r["inputs_json"])
        html_parts.append(
            f"<tr>"
            f"<td><a href=\"alert/{r['id']}.html\">{r['id']}</a></td>"
            f"<td>{escape(r['rule_id'])}{conf_badge}</td>"
            f"<td>{escape(labeler_cell)}</td>"
            f"<td>{escape(r['ts'])}</td>"
            f"</tr>"
        )

    # Render rollups
    for (rule_id, ts), group in sorted(rollup_groups.items(), key=lambda x: x[0][1], reverse=True):
        if len(group) <= 2:
            # Small groups: render individually
            for r in group:
                labeler_cell = _display_name(r["labeler_did"], handles, display_names)
                conf_badge = _confidence_badge(r["inputs_json"])
                html_parts.append(
                    f"<tr class=\"anomaly-row\">"
                    f"<td><a href=\"alert/{r['id']}.html\">{r['id']}</a></td>"
                    f"<td>{escape(r['rule_id'])}{conf_badge}</td>"
                    f"<td>{escape(labeler_cell)}</td>"
                    f"<td>{escape(r['ts'])}</td>"
                    f"</tr>"
                )
        else:
            # Rollup
            detail_rows = ""
            for r in group:
                labeler_cell = _display_name(r["labeler_did"], handles, display_names)
                detail_rows += (
                    f"<tr class=\"anomaly-row\">"
                    f"<td><a href=\"alert/{r['id']}.html\">{r['id']}</a></td>"
                    f"<td>{escape(r['rule_id'])}</td>"
                    f"<td>{escape(labeler_cell)}</td>"
                    f"<td>{escape(r['ts'])}</td>"
                    f"</tr>"
                )
            html_parts.append(
                f"<tr><td colspan=\"4\">"
                f"<details class=\"rollup\"><summary>{escape(rule_id)} "
                f"<span class=\"badge badge-low-conf\">Low confidence</span>: {len(group)} labelers</summary>"
                f"<table><tbody>{detail_rows}</tbody></table>"
                f"</details></td></tr>"
            )

    return "".join(html_parts)


def generate_report(conn, out_dir: str, now: Optional[datetime] = None) -> None:
    real_now = datetime.now(timezone.utc)
    if now is None:
        now = real_now
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    clamped = False
    if now > real_now:
        now = real_now
        clamped = True
    now_ts = format_ts(now)

    last_ingest = _max_ts(conn, "label_events")
    last_scan = _max_ts(conn, "alerts")
    last_discovery = db.get_meta(conn, "last_discovery_ts")

    max_label = _parse_ts_safe(last_ingest)
    max_alert = _parse_ts_safe(last_scan)
    max_raw_dt = None
    if max_label and max_alert:
        max_raw_dt = max(max_label, max_alert)
    else:
        max_raw_dt = max_label or max_alert
    max_raw_ts = format_ts(max_raw_dt) if max_raw_dt else None
    skew_seconds = 0
    if max_raw_dt:
        skew_seconds = max(0, int((max_raw_dt - real_now).total_seconds()))

    naive_count = _count_naive_timestamps(conn, "label_events") + _count_naive_timestamps(conn, "alerts")
    timestamps_assumed_utc = naive_count > 0

    cfg_hash_latest = None
    cfg_row = conn.execute("SELECT config_hash FROM alerts ORDER BY ts DESC LIMIT 1").fetchone()
    if cfg_row:
        cfg_hash_latest = cfg_row["config_hash"]
    if cfg_hash_latest is None:
        cfg_hash_latest = config_hash_fn({"rules": ["label_rate_spike", "flip_flop"]})

    build_signature = {
        "package_version": _get_package_version(),
        "schema_version": db.SCHEMA_VERSION,
        "git_commit": get_git_commit(),
        "config_hash": cfg_hash_latest,
    }

    schema_version_source = "db" if db.get_schema_version(conn) is not None else "code"

    start_24h = format_ts(now - timedelta(hours=24))
    start_7d = format_ts(now - timedelta(days=7))
    start_30d = format_ts(now - timedelta(days=30))

    alerts_24h = _alerts_by_rule(conn, start_24h, now_ts)
    alerts_7d = _alerts_by_rule(conn, start_7d, now_ts)
    top_labelers = _top_labelers(conn, start_7d, now_ts)

    labelers = conn.execute("SELECT * FROM labelers ORDER BY labeler_did").fetchall()
    alerts = conn.execute("SELECT * FROM alerts ORDER BY ts DESC").fetchall()
    handles = _handle_cache(conn)
    display_names = _display_name_cache(conn)

    # Census counts
    census = _census_counts(conn)
    test_dev_count = conn.execute("SELECT COUNT(*) AS c FROM labelers WHERE likely_test_dev=1").fetchone()["c"]
    warmup_count = conn.execute(
        "SELECT COUNT(*) AS c FROM labelers WHERE scan_count < 3"
    ).fetchone()["c"]

    # Partition labelers into reference and non-reference
    ref_labelers = [r for r in labelers if r["is_reference"]]
    nonref_labelers = [r for r in labelers if not r["is_reference"]]

    # Heartbeat timestamps from runner loop
    heartbeats = {}
    for hb_key in ("last_ingest_ok_ts", "last_scan_ok_ts", "last_report_ok_ts", "last_discovery_ok_ts"):
        heartbeats[hb_key] = db.get_meta(conn, hb_key)

    overview = {
        "api_version": "v0",
        "generated_at": now_ts,
        "last_ingest": last_ingest,
        "last_scan": last_scan,
        "last_discovery": last_discovery,
        "heartbeats": heartbeats,
        "schema_version": db.SCHEMA_VERSION,
        "schema_version_source": schema_version_source,
        "alerts_by_rule_24h": alerts_24h,
        "alerts_by_rule_7d": alerts_7d,
        "top_labelers_7d": top_labelers,
        "labeler_count": len(labelers),
        "alert_count": len(alerts),
        "now_clamped_to_real_time": clamped,
        "max_raw_timestamp_seen": max_raw_ts,
        "max_skew_seconds": skew_seconds,
        "timestamps_assumed_utc": timestamps_assumed_utc,
        "naive_timestamp_count": naive_count,
        "build_signature": build_signature,
        "census": census,
        "test_dev_count": test_dev_count,
    }

    tmp_dir = _prepare_out_dir(out_dir)
    _write_json(os.path.join(tmp_dir, "overview.json"), overview)

    labeler_rows_json = []
    for row in labelers:
        did = row["labeler_did"]
        slug = _did_slug(did)
        labeler_rows_json.append({
            "labeler_did": did,
            "handle": handles.get(did),
            "display_name": display_names.get(did),
            "labeler_class": row["labeler_class"],
            "is_reference": bool(row["is_reference"]),
            "endpoint_status": row["endpoint_status"],
            "visibility_class": row["visibility_class"],
            "reachability_state": row["reachability_state"],
            "auditability": row["auditability"],
            "classification_confidence": row["classification_confidence"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "href": f"labeler/{slug}.html",
        })
    _write_json(os.path.join(tmp_dir, "labelers.json"), labeler_rows_json)

    alert_rows_json = []
    for row in alerts:
        alert_rows_json.append({
            "id": row["id"],
            "rule_id": row["rule_id"],
            "labeler_did": row["labeler_did"],
            "ts": row["ts"],
            "href": f"alert/{row['id']}.html",
        })
    _write_json(os.path.join(tmp_dir, "alerts.json"), alert_rows_json)

    # --- Staleness indicators ---
    staleness_cards = f"""
<div class="grid">
  <div class="card"><h3>Generated</h3><div>{escape(now_ts)}</div></div>
  <div class="card"><h3>Last ingest</h3><div>{escape(str(last_ingest or 'never'))}</div></div>
  <div class="card"><h3>Last scan</h3><div>{escape(str(last_scan or 'never'))}</div></div>
  <div class="card"><h3>Last discovery</h3><div>{escape(str(last_discovery or 'never'))}</div></div>
  <div class="card"><h3>Labelers</h3><div>{len(labelers)}</div></div>
  <div class="card"><h3>Alerts</h3><div>{len(alerts)}</div></div>
"""
    if skew_seconds > 0:
        staleness_cards += f'  <div class="card"><h3>Clock skew</h3><div>{skew_seconds}s</div></div>'
    staleness_cards += "</div>"

    # --- Warm-up banner ---
    warmup_banner = ""
    if warmup_count > 0:
        warmup_banner = f'<div class="warmup-banner">Baselines forming: {warmup_count} labeler{"s" if warmup_count != 1 else ""} still in warm-up period.</div>'

    def dict_rows(d: Dict[str, int]) -> List[List[str]]:
        return [[escape(k), str(v)] for k, v in sorted(d.items(), key=lambda x: x[0])]

    overview_tables = ""
    if alerts_24h:
        overview_tables += "<h2>Alerts by rule (24h)</h2>" + _table(["rule_id", "count"], dict_rows(alerts_24h))
    if alerts_7d:
        overview_tables += "<h2>Alerts by rule (7d)</h2>" + _table(["rule_id", "count"], dict_rows(alerts_7d))

    top_rows = [[_labeler_link(r["labeler_did"], handles, display_names), str(r["count"])] for r in top_labelers]
    if top_rows:
        overview_tables += "<h2>Top labelers by alerts (7d)</h2>" + _table(["labeler", "count"], top_rows)

    build_rows = [
        ["package_version", escape(str(build_signature["package_version"]))],
        ["schema_version", escape(str(build_signature["schema_version"]))],
        ["git_commit", escape(str(build_signature["git_commit"]))],
        ["config_hash", f"<code>{escape(str(build_signature['config_hash']))}</code>"],
    ]
    build_table = "<h2>Build signature</h2>" + _table(["field", "value"], build_rows)

    # Reference lane
    reference_lane = ""
    if ref_labelers:
        ref_cards = ""
        for r in ref_labelers:
            did = r["labeler_did"]
            counts = _hourly_counts(conn, did, start_7d, now_ts)
            ref_card = _labeler_health_card(conn, did, start_7d, now_ts, counts)
            ref_cards += f'<h3>{_labeler_link(did, handles, display_names)}</h3>{ref_card}'
        reference_lane = f'<div class="reference-lane"><h2>Reference labelers</h2>{ref_cards}</div>'

    # --- Triage view with tabs ---
    # Pre-compute per-labeler data attributes
    labeler_alert_counts = {}
    for r in alerts:
        labeler_alert_counts[r["labeler_did"]] = labeler_alert_counts.get(r["labeler_did"], 0) + 1

    tab_bar = f"""
<div class="tab-bar">
  <button data-view="active">Active <span class="tab-count"></span></button>
  <button class="active" data-view="alerts">Alerts <span class="tab-count"></span></button>
  <button data-view="new">New <span class="tab-count"></span></button>
  <button data-view="opaque">Opaque <span class="tab-count"></span></button>
  <button data-view="all">All <span class="tab-count"></span></button>
</div>
<div style="margin:0.5rem 0;font-size:0.85rem;">
  <label><input type="checkbox" id="toggle-test-dev"> Show test/dev ({test_dev_count})</label>
  <label style="margin-left:1rem;"><input type="checkbox" id="toggle-inactive"> Show inactive &gt;30d</label>
</div>
"""

    labeler_table_header = '<table><thead><tr><th>labeler</th><th>visibility</th><th>endpoint</th><th>first_seen</th><th>last_seen</th><th>activity</th><th>behavior</th></tr></thead><tbody>'
    labeler_table_rows_html = ""

    for r in nonref_labelers:
        did = r["labeler_did"]
        badges = _labeler_badges(conn, did, start_7d, now_ts)
        counts = _hourly_counts(conn, did, start_7d, now_ts)
        spark = _sparkline_svg(counts)
        ep_status = r["endpoint_status"] if r["endpoint_status"] else "unknown"
        vis_class = r["visibility_class"] or "unresolved"
        reach = r["reachability_state"] or "unknown"
        events_7d = sum(counts)
        alert_count = labeler_alert_counts.get(did, 0)
        is_test = r["likely_test_dev"] or 0
        is_new = "1" if r["first_seen"] and r["first_seen"] >= start_7d else "0"
        is_opaque = "1" if vis_class in ("observed_only", "unresolved") or reach in ("auth_required", "down") else "0"
        last_seen_dt = _parse_ts_safe(r["last_seen"])
        is_inactive = "1" if last_seen_dt and last_seen_dt < _parse_ts_safe(start_30d) else "0"

        labeler_table_rows_html += (
            f'<tr class="labeler-row" '
            f'data-events7d="{events_7d}" data-alert-count="{alert_count}" '
            f'data-test-dev="{is_test}" data-is-new="{is_new}" '
            f'data-opaque="{is_opaque}" data-inactive="{is_inactive}">'
            f'<td>{_labeler_link(did, handles, display_names)}</td>'
            f'<td>{_visibility_badge(vis_class)}</td>'
            f'<td>{_endpoint_dot(ep_status)}</td>'
            f'<td>{escape(str(r["first_seen"]))}</td>'
            f'<td>{escape(str(r["last_seen"]))}</td>'
            f'<td>{spark}</td>'
            f'<td>{_badges_html(badges)}</td>'
            f'</tr>'
        )

    labeler_section = f"<h2>Labelers</h2>{tab_bar}{labeler_table_header}{labeler_table_rows_html}</tbody></table>"

    # --- Alert rollups ---
    alert_head = "<tr><th>id</th><th>rule_id</th><th>labeler</th><th>ts</th></tr>"
    rollup_html = _alert_rollups(list(alerts[:200]), handles, display_names)
    alert_links = f"<h2>Recent alerts</h2><table><thead>{alert_head}</thead><tbody>{rollup_html}</tbody></table>"

    naive_banner = ""
    if naive_count > 0:
        naive_banner = f"<p class=\"small\">Note: {naive_count} timestamps lacked timezone info and were assumed UTC.</p>"

    explainer_html = """
<div class="explainer">
  <p><strong>Labelwatch</strong> tracks labeler services on the Bluesky network.</p>
  <p>A <dfn>labeler</dfn> is a third-party service that attaches tags to posts or accounts.
  Your Bluesky app decides what to do with those tags \u2014 ignore, warn, or hide.
  Most labelers are topical or curational (yes, including K-pop). Some are moderation/safety.</p>
  <p>This page shows what labelers exist, what they\u2019re emitting, and when behavior changes.
  It observes \u2014 it doesn\u2019t moderate anything by itself.</p>
</div>
"""

    overview_html = _layout(
        "Labelwatch overview",
        explainer_html + staleness_cards + naive_banner + warmup_banner + reference_lane +
        build_table + overview_tables + labeler_section + alert_links + METHODS_HTML + TRIAGE_JS,
    )
    _write(os.path.join(tmp_dir, "index.html"), overview_html)

    # --- Census page ---
    census_body = '<h2>Discovery Census</h2>'
    census_body += '<div class="census-grid">'
    census_body += f'<div class="census-card"><div class="value">{len(labelers)}</div><div class="label">Total labelers</div></div>'
    census_body += f'<div class="census-card"><div class="value">{test_dev_count}</div><div class="label">Test/dev</div></div>'
    census_body += f'<div class="census-card"><div class="value">{warmup_count}</div><div class="label">Warming up</div></div>'
    census_body += '</div>'

    for field_name, field_label in [
        ("visibility_class", "Visibility Class"),
        ("reachability_state", "Reachability State"),
        ("classification_confidence", "Classification Confidence"),
        ("auditability", "Auditability"),
    ]:
        counts = census.get(field_name, {})
        census_body += f'<h3>{escape(field_label)}</h3>'
        census_body += '<div class="census-grid">'
        for val, cnt in sorted(counts.items()):
            census_body += f'<div class="census-card"><div class="value">{cnt}</div><div class="label">{escape(val)}</div></div>'
        census_body += '</div>'

    census_body += f'<p class="small">Last census: {escape(now_ts)}</p>'
    census_body += '<p><a href="index.html">Back to overview</a></p>'
    census_html = _layout("Labelwatch Census", census_body)
    _write(os.path.join(tmp_dir, "census.html"), census_html)

    # --- Per-labeler pages ---
    anomaly_rules = {"label_rate_spike", "flip_flop", "target_concentration", "churn_index"}

    for row in labelers:
        did = row["labeler_did"]
        slug = _did_slug(did)
        alerts_rows = conn.execute(
            "SELECT * FROM alerts WHERE labeler_did=? ORDER BY ts DESC",
            (did,),
        ).fetchall()
        events_24h = _labeler_activity(conn, did, start_24h, now_ts)
        events_7d = _labeler_activity(conn, did, start_7d, now_ts)
        top_targets = _top_targets(conn, did, start_7d, now_ts)

        payload = {
            "labeler_did": did,
            "handle": handles.get(did),
            "display_name": display_names.get(did),
            "labeler_class": row["labeler_class"],
            "is_reference": bool(row["is_reference"]),
            "endpoint_status": row["endpoint_status"],
            "visibility_class": row["visibility_class"],
            "reachability_state": row["reachability_state"],
            "auditability": row["auditability"],
            "classification_confidence": row["classification_confidence"],
            "classification_reason": row["classification_reason"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "events_24h": events_24h,
            "events_7d": events_7d,
            "alerts": [dict(r) for r in alerts_rows],
            "top_targets_7d": top_targets,
        }
        _write_json(os.path.join(tmp_dir, "labeler", f"{slug}.json"), payload)

        sparkline_counts = _hourly_counts(conn, did, start_7d, now_ts)
        health_card = _labeler_health_card(conn, did, start_7d, now_ts, sparkline_counts)

        handle = handles.get(did)
        dn = display_names.get(did)
        labeler_label = dn or handle
        labeler_title = f"{labeler_label} ({did})" if labeler_label else did
        ep_status = row["endpoint_status"] if row["endpoint_status"] else "unknown"
        ep_dot = _endpoint_dot(ep_status)
        vis_class = row["visibility_class"] or "unresolved"
        reach_state = row["reachability_state"] or "unknown"
        audit = row["auditability"] or "low"
        class_label = row["labeler_class"] or "third_party"
        ref_tag = ' <span class="badge badge-stable">Reference</span>' if row["is_reference"] else ""

        # External links
        profile_link = ""
        ext_links = []
        if handle:
            ext_links.append(f'<a href="https://bsky.app/profile/{escape(handle)}" target="_blank">Open on Bluesky</a>')
        if did.startswith("did:plc:"):
            plc_id = did.split(":", 2)[2]
            ext_links.append(f'<a href="https://plc.directory/{escape(did)}" target="_blank">PLC directory</a>')
        elif did.startswith("did:web:"):
            ext_links.append(f'<a href="https://{escape(did[8:])}/.well-known/did.json" target="_blank">DID document</a>')
        if ext_links:
            links_html = " &middot; ".join(ext_links)
            profile_link = f'<div class="card"><h3>Links</h3><div>{links_html}</div></div>'

        # Warmup/sparse indicators
        scan_count = row["scan_count"] or 0
        warmup_indicator = ""
        if scan_count < 3:
            warmup_indicator = '<div class="warmup-banner">This labeler is still in warm-up period (insufficient scan history).</div>'
        elif events_7d == 0 and events_24h == 0:
            warmup_indicator = '<p class="small">Insufficient volume: no events observed in the last 7 days.</p>'

        info_card = f"""
<div class="grid">
  <div class="card"><h3>Labeler</h3><div>{('<strong>' + escape(labeler_label) + '</strong><br/>' if labeler_label else '')}<code>{escape(did)}</code>{ref_tag}</div></div>
  <div class="card"><h3>Classification</h3><div>{_visibility_badge(vis_class)} {escape(vis_class)}</div></div>
  <div class="card"><h3>Reachability</h3><div>{ep_dot} {escape(reach_state)}</div></div>
  <div class="card"><h3>Auditability</h3><div>{escape(audit)}</div></div>
  <div class="card"><h3>Class</h3><div>{escape(class_label)}</div></div>
  <div class="card"><h3>First seen</h3><div>{escape(str(row['first_seen']))}</div></div>
  <div class="card"><h3>Last seen</h3><div>{escape(str(row['last_seen']))}</div></div>
  <div class="card"><h3>Events (24h)</h3><div>{events_24h}</div></div>
  <div class="card"><h3>Events (7d)</h3><div>{events_7d}</div></div>
  <div class="card"><h3>Alerts</h3><div>{len(alerts_rows)}</div></div>
  {profile_link}
</div>
"""
        evidence_section = _evidence_expander(conn, did, row)

        targets_table = ""
        if top_targets:
            targets_table = "<h2>Top targets (7d)</h2>" + _table(
                ["uri", "count"],
                [[escape(t["uri"]), str(t["count"])] for t in top_targets],
            )

        # Probe history section
        probe_history = db.get_probe_history(conn, did, limit=10)
        probe_section = "<h2>Probe history</h2>"
        if probe_history:
            probe_rows = []
            for p in probe_history:
                probe_rows.append([
                    escape(p["ts"]),
                    escape(p["normalized_status"]),
                    str(p["http_status"] or ""),
                    str(p["latency_ms"] or "") + ("ms" if p["latency_ms"] else ""),
                    escape(p["failure_type"] or ""),
                ])
            probe_section += _table(["ts", "status", "http", "latency", "failure"], probe_rows)
        else:
            probe_section += "<p class=\"small\">No probe history recorded yet.</p>"

        alert_detail_rows = []
        for r in alerts_rows:
            is_anomaly = r["rule_id"] in anomaly_rules
            row_class = ' class="anomaly-row"' if is_anomaly else ""
            alert_detail_rows.append(
                f"<tr{row_class}>"
                f"<td><a href=\"../alert/{r['id']}.html\">{r['id']}</a></td>"
                f"<td>{escape(r['rule_id'])}</td>"
                f"<td>{escape(r['ts'])}</td>"
                f"</tr>"
            )
        alert_head = "<tr><th>id</th><th>rule_id</th><th>ts</th></tr>"
        alerts_table = f"<h2>Alerts timeline</h2><table><thead>{alert_head}</thead><tbody>{''.join(alert_detail_rows)}</tbody></table>"

        labeler_context = '<p class="labeler-context">This is a labeler service. It publishes labels about posts and accounts on the Bluesky network.</p>'

        html = _layout(
            f"Labeler: {labeler_title}",
            f"<p><a href=\"../index.html\">Overview</a> | <a href=\"../census.html\">Census</a></p>"
            + labeler_context + warmup_indicator + health_card + info_card + evidence_section
            + targets_table + probe_section + alerts_table + METHODS_HTML,
            canonical=f"labeler/{slug}.html",
        )
        _write(os.path.join(tmp_dir, "labeler", f"{slug}.html"), html)

    # --- Per-alert pages ---
    for row in alerts:
        evidence_hashes = json.loads(row["evidence_hashes_json"])
        events = _alert_events(conn, evidence_hashes)
        payload = {
            "alert": dict(row),
            "evidence_events": events,
        }
        _write_json(os.path.join(tmp_dir, "alert", f"{row['id']}.json"), payload)

        receipt_table = _table(
            ["field", "value"],
            [["rule_id", escape(row["rule_id"])],
             ["labeler_did", escape(row["labeler_did"])],
             ["ts", escape(row["ts"])],
             ["config_hash", f"<code>{escape(row['config_hash'])}</code>"],
             ["receipt_hash", f"<code>{escape(row['receipt_hash'])}</code>"],
             ["inputs", f"<pre>{escape(row['inputs_json'])}</pre>"],
             ["evidence_hashes", f"<pre>{escape(row['evidence_hashes_json'])}</pre>"],
            ],
        )
        events_table = "<p>No evidence events recorded.</p>"
        if events:
            events_table = _table(
                ["id", "ts", "uri", "val", "neg", "cid", "event_hash"],
                [[str(e["id"]), escape(e["ts"]), escape(e["uri"]), escape(e["val"]), str(e["neg"]), escape(str(e["cid"])), f"<code>{escape(e['event_hash'])}</code>"] for e in events],
            )
        html = _layout(
            f"Alert {row['id']}",
            f"<p><a href=\"../index.html\">Overview</a></p>" + receipt_table + "<h2>Evidence events</h2>" + events_table,
        )
        _write(os.path.join(tmp_dir, "alert", f"{row['id']}.html"), html)

    _commit_out_dir(tmp_dir, out_dir)
