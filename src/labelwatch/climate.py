"""My Label Climate — per-author label activity report.

Queries rollup tables (derived_author_day, derived_author_labeler_day) and
raw label_events to assemble a JSON payload and standalone HTML page showing
what labeling activity has targeted a given DID's posts.
"""
from __future__ import annotations

import html
import json
import os
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .report import (
    STYLE,
    THEME_JS,
    THEME_TOGGLE_JS,
    _layout,
    _sparkline_svg,
    _table,
    _write_json,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _regime_badge(state: Optional[str]) -> str:
    """Render a small regime-state badge."""
    if not state:
        return '<span class="badge badge-low-conf">unknown</span>'
    badges = {
        "stable": ("Stable", "badge-stable"),
        "burst": ("Burst", "badge-burst"),
        "fixated": ("Fixated", "badge-fixated"),
        "warming": ("Warming", "badge-low-conf"),
    }
    label, cls = badges.get(state, (state.title(), "badge-low-conf"))
    return f'<span class="badge {cls}">{html.escape(label)}</span>'


def _at_uri_to_bsky_link(uri: str) -> str:
    """Convert at:// URI to a bsky.app link.

    at://did:plc:abc/app.bsky.feed.post/xyz
    → https://bsky.app/profile/did:plc:abc/post/xyz
    """
    if not uri or not uri.startswith("at://"):
        return html.escape(uri or "")
    parts = uri[5:].split("/", 3)  # did, collection, rkey
    if len(parts) < 3:
        return html.escape(uri)
    did, collection, rkey = parts[0], parts[1], parts[2]
    if collection != "app.bsky.feed.post":
        return html.escape(uri)
    safe_did = urllib.parse.quote(did, safe=":")
    safe_rkey = urllib.parse.quote(rkey, safe="")
    url = f"https://bsky.app/profile/{safe_did}/post/{safe_rkey}"
    short = f"{did[:20]}…/{rkey}" if len(did) > 20 else f"{did}/{rkey}"
    return f'<a href="{html.escape(url)}">{html.escape(short)}</a>'


def _delta_text(current: int, prev: int) -> str:
    """Return colored +N / -N / '—' delta string."""
    diff = current - prev
    if diff > 0:
        return f'<span style="color:var(--accent-red,#c0392b)">+{diff}</span>'
    if diff < 0:
        return f'<span style="color:var(--accent-green,#27ae60)">{diff}</span>'
    return "—"


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def _query_summary(conn, target_did: str, start_day_epoch: int,
                   end_day_epoch: int, start_iso: str) -> Dict[str, Any]:
    """Aggregate summary over the window."""
    # From rollup: events, applies, removes
    row = conn.execute(
        "SELECT COALESCE(SUM(events),0) AS events, "
        "       COALESCE(SUM(applies),0) AS applies, "
        "       COALESCE(SUM(removes),0) AS removes "
        "FROM derived_author_day "
        "WHERE author_did = ? AND day_epoch >= ? AND day_epoch < ?",
        (target_did, start_day_epoch, end_day_epoch),
    ).fetchone()

    # Unique posts labeled — COUNT DISTINCT from raw events
    posts_row = conn.execute(
        "SELECT COUNT(DISTINCT uri) AS unique_posts "
        "FROM label_events "
        "WHERE target_did = ? AND ts >= ? "
        "  AND uri LIKE 'at://%/app.bsky.feed.post/%'",
        (target_did, start_iso),
    ).fetchone()

    # Distinct labelers — from per-labeler rollup
    labelers_row = conn.execute(
        "SELECT COUNT(DISTINCT labeler_did) AS labelers "
        "FROM derived_author_labeler_day "
        "WHERE author_did = ? AND day_epoch >= ? AND day_epoch < ?",
        (target_did, start_day_epoch, end_day_epoch),
    ).fetchone()

    # Distinct label values — from raw events
    vals_row = conn.execute(
        "SELECT COUNT(DISTINCT val) AS vals "
        "FROM label_events "
        "WHERE target_did = ? AND ts >= ? "
        "  AND uri LIKE 'at://%/app.bsky.feed.post/%'",
        (target_did, start_iso),
    ).fetchone()

    return {
        "label_actions": row["events"],
        "applies": row["applies"],
        "removes": row["removes"],
        "unique_posts": posts_row["unique_posts"],
        "labelers": labelers_row["labelers"],
        "label_values": vals_row["vals"],
    }


def _query_week_deltas(conn, target_did: str,
                       now_day_epoch: int) -> Dict[str, Any]:
    """Current 7d vs previous 7d from derived_author_day."""
    this_start = (now_day_epoch // 86400 - 6) * 86400
    this_end = now_day_epoch + 86400  # inclusive of today
    prev_start = (now_day_epoch // 86400 - 13) * 86400
    prev_end = this_start

    def _agg(s, e):
        row = conn.execute(
            "SELECT COALESCE(SUM(events),0) AS events "
            "FROM derived_author_day "
            "WHERE author_did = ? AND day_epoch >= ? AND day_epoch < ?",
            (target_did, s, e),
        ).fetchone()
        lab_row = conn.execute(
            "SELECT COUNT(DISTINCT labeler_did) AS labelers "
            "FROM derived_author_labeler_day "
            "WHERE author_did = ? AND day_epoch >= ? AND day_epoch < ?",
            (target_did, s, e),
        ).fetchone()
        return {"events": row["events"], "labelers": lab_row["labelers"]}

    this_w = _agg(this_start, this_end)
    prev_w = _agg(prev_start, prev_end)

    return {
        "events_this_week": this_w["events"],
        "events_prev_week": prev_w["events"],
        "events_delta": this_w["events"] - prev_w["events"],
        "labelers_this_week": this_w["labelers"],
        "labelers_prev_week": prev_w["labelers"],
        "labelers_delta": this_w["labelers"] - prev_w["labelers"],
    }


def _query_top_labelers(conn, target_did: str, start_day_epoch: int,
                        end_day_epoch: int) -> List[Dict[str, Any]]:
    """Top labelers by event count, enriched with handle/regime."""
    rows = conn.execute(
        "SELECT labeler_did, "
        "       SUM(events) AS events, "
        "       SUM(applies) AS applies, "
        "       SUM(removes) AS removes, "
        "       SUM(targets) AS targets "
        "FROM derived_author_labeler_day "
        "WHERE author_did = ? AND day_epoch >= ? AND day_epoch < ? "
        "GROUP BY labeler_did "
        "ORDER BY SUM(events) DESC, labeler_did ASC",
        (target_did, start_day_epoch, end_day_epoch),
    ).fetchall()

    result = []
    for r in rows:
        lab = conn.execute(
            "SELECT handle, regime_state FROM labelers WHERE labeler_did = ?",
            (r["labeler_did"],),
        ).fetchone()
        handle = lab["handle"] if lab else None
        regime = lab["regime_state"] if lab else None

        # One-liner narrative
        ev = r["events"]
        rem = r["removes"]
        tgt = r["targets"]
        if rem > 0:
            one_liner = f"{ev} labels, {rem} reversed"
        elif tgt > 1:
            one_liner = f"{ev} labels across {tgt} posts"
        else:
            one_liner = f"{ev} labels"

        result.append({
            "labeler_did": r["labeler_did"],
            "handle": handle,
            "regime_state": regime,
            "events": ev,
            "applies": r["applies"],
            "removes": rem,
            "targets": tgt,
            "one_liner": one_liner,
        })
    return result


def _query_top_values(conn, target_did: str,
                      start_iso: str) -> List[Dict[str, Any]]:
    """Top label values from raw label_events."""
    rows = conn.execute(
        "SELECT val, "
        "       SUM(CASE WHEN neg = 0 THEN 1 ELSE 0 END) AS applies, "
        "       SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END) AS removes "
        "FROM label_events "
        "WHERE target_did = ? AND ts >= ? "
        "  AND uri LIKE 'at://%/app.bsky.feed.post/%' "
        "GROUP BY val "
        "ORDER BY (SUM(CASE WHEN neg=0 THEN 1 ELSE 0 END) + "
        "          SUM(CASE WHEN neg=1 THEN 1 ELSE 0 END)) DESC, "
        "         SUM(CASE WHEN neg=0 THEN 1 ELSE 0 END) DESC, "
        "         val ASC",
        (target_did, start_iso),
    ).fetchall()
    return [{"val": r["val"], "applies": r["applies"], "removes": r["removes"]}
            for r in rows]


def _query_daily_series(conn, target_did: str, start_day_epoch: int,
                        end_day_epoch: int) -> List[Dict[str, Any]]:
    """Daily event counts from derived_author_day."""
    rows = conn.execute(
        "SELECT day_epoch, events, applies, removes "
        "FROM derived_author_day "
        "WHERE author_did = ? AND day_epoch >= ? AND day_epoch < ? "
        "ORDER BY day_epoch",
        (target_did, start_day_epoch, end_day_epoch),
    ).fetchall()
    result = []
    for r in rows:
        day_str = datetime.fromtimestamp(r["day_epoch"], tz=timezone.utc).strftime("%Y-%m-%d")
        result.append({
            "date": day_str,
            "events": r["events"],
            "applies": r["applies"],
            "removes": r["removes"],
        })
    return result


def _query_recent_receipts(conn, target_did: str, start_iso: str,
                           labeler_dids: List[str]) -> List[Dict[str, Any]]:
    """Recent label events for this target, scoped to known labelers."""
    if not labeler_dids:
        return []
    placeholders = ",".join("?" for _ in labeler_dids)
    rows = conn.execute(
        f"SELECT labeler_did, uri, val, neg, ts "
        f"FROM label_events "
        f"WHERE target_did = ? AND ts >= ? "
        f"  AND labeler_did IN ({placeholders}) "
        f"ORDER BY ts DESC LIMIT 15",
        [target_did, start_iso] + labeler_dids,
    ).fetchall()
    result = []
    for r in rows:
        result.append({
            "labeler_did": r["labeler_did"],
            "uri": r["uri"],
            "val": r["val"],
            "type": "remove" if r["neg"] else "apply",
            "ts": r["ts"],
        })
    return result


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def generate_climate(conn, target_did: str, window_days: int = 30,
                     out_dir: str = ".", fmt: str = "both") -> Dict[str, Any]:
    """Generate label climate report for a target DID.

    Returns the payload dict. Writes JSON/HTML to out_dir based on fmt.
    """
    # Clamp window
    window_days = max(1, min(window_days, 60))

    now_epoch = int(time.time())
    now_day_epoch = (now_epoch // 86400) * 86400
    start_day_epoch = ((now_epoch // 86400) - window_days + 1) * 86400
    end_day_epoch = now_day_epoch + 86400  # inclusive of today
    start_iso = datetime.fromtimestamp(start_day_epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT00:00:00Z"
    )

    summary = _query_summary(conn, target_did, start_day_epoch, end_day_epoch, start_iso)

    if summary["label_actions"] == 0:
        payload: Dict[str, Any] = {
            "empty": True,
            "target_did": target_did,
            "window_days": window_days,
            "message": f"No label activity found for {target_did} in the last {window_days} days.",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        if fmt in ("json", "both"):
            _atomic_write_json(os.path.join(out_dir, "climate.json"), payload)
        if fmt in ("html", "both"):
            _atomic_write_html(out_dir, payload, target_did, window_days)
        return payload

    week_deltas = _query_week_deltas(conn, target_did, now_day_epoch)
    top_labelers = _query_top_labelers(conn, target_did, start_day_epoch, end_day_epoch)
    top_values = _query_top_values(conn, target_did, start_iso)
    daily_series = _query_daily_series(conn, target_did, start_day_epoch, end_day_epoch)
    labeler_dids = [l["labeler_did"] for l in top_labelers]
    recent_receipts = _query_recent_receipts(conn, target_did, start_iso, labeler_dids)

    payload = {
        "empty": False,
        "target_did": target_did,
        "window_days": window_days,
        "summary": summary,
        "week_deltas": week_deltas,
        "top_labelers": top_labelers,
        "top_values": top_values,
        "daily_series": daily_series,
        "recent_receipts": recent_receipts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    if fmt in ("json", "both"):
        _atomic_write_json(os.path.join(out_dir, "climate.json"), payload)
    if fmt in ("html", "both"):
        _atomic_write_html(out_dir, payload, target_did, window_days)

    return payload


# ---------------------------------------------------------------------------
# Atomic writers
# ---------------------------------------------------------------------------

def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path) or ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def _atomic_write_html(out_dir: str, payload: Dict[str, Any],
                       target_did: str, window_days: int) -> None:
    os.makedirs(out_dir, exist_ok=True)
    content = _render_html(payload, target_did, window_days)
    fd, tmp = tempfile.mkstemp(dir=out_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, os.path.join(out_dir, "climate.html"))
    except BaseException:
        os.unlink(tmp)
        raise


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def _render_html(payload: Dict[str, Any], target_did: str,
                 window_days: int) -> str:
    """Render the climate payload as a standalone HTML page."""
    escaped_did = html.escape(target_did)
    title = f"Label Climate — {escaped_did}"

    if payload.get("empty"):
        body = (
            '<div class="card">'
            f'<p>{html.escape(payload.get("message", "No label activity found."))}</p>'
            '</div>'
        )
        return _layout(title, body)

    summary = payload["summary"]
    week = payload["week_deltas"]
    sections: List[str] = []

    # --- Summary cards ---
    daily = payload.get("daily_series", [])
    spark_vals = [d["events"] for d in daily]
    spark = _sparkline_svg(spark_vals, width=120, height=24)
    events_delta = _delta_text(week["events_this_week"], week["events_prev_week"])

    cards = f"""
    <div class="grid">
      <div class="health-metric">
        <div class="metric-label">Label Actions</div>
        <div class="metric-value">{summary["label_actions"]}</div>
        <div>{spark} {events_delta} vs prev 7d</div>
      </div>
      <div class="health-metric">
        <div class="metric-label">Unique Posts Labeled</div>
        <div class="metric-value">{summary["unique_posts"]}</div>
      </div>
      <div class="health-metric">
        <div class="metric-label">Applies</div>
        <div class="metric-value">{summary["applies"]}</div>
      </div>
      <div class="health-metric">
        <div class="metric-label">Removes</div>
        <div class="metric-value">{summary["removes"]}</div>
      </div>
      <div class="health-metric">
        <div class="metric-label">Labelers</div>
        <div class="metric-value">{summary["labelers"]}</div>
      </div>
      <div class="health-metric">
        <div class="metric-label">Label Values</div>
        <div class="metric-value">{summary["label_values"]}</div>
      </div>
    </div>
    """
    sections.append(cards)

    # --- Top Labelers ---
    if payload.get("top_labelers"):
        rows = []
        for l in payload["top_labelers"]:
            handle_str = html.escape(l["handle"]) if l["handle"] else html.escape(l["labeler_did"])
            badge = _regime_badge(l.get("regime_state"))
            rows.append([
                f"{handle_str} {badge}",
                html.escape(l["one_liner"]),
                str(l["events"]),
                str(l["applies"]),
                str(l["removes"]),
                str(l["targets"]),
            ])
        sections.append(
            '<h2>Top Labelers</h2>'
            + _table(["Labeler", "Summary", "Events", "Applies", "Removes", "Posts"], rows)
        )

    # --- Top Label Values ---
    if payload.get("top_values"):
        rows = []
        for v in payload["top_values"]:
            rows.append([
                html.escape(v["val"]),
                str(v["applies"]),
                str(v["removes"]),
            ])
        sections.append(
            '<h2>Top Label Values</h2>'
            + _table(["Value", "Applies", "Removes"], rows)
        )

    # --- Daily Activity ---
    if daily:
        wide_spark = _sparkline_svg(spark_vals, width=400, height=60)
        sections.append(
            '<h2>Daily Activity</h2>'
            f'<div class="card">{wide_spark}</div>'
        )

    # --- Recent Receipts ---
    if payload.get("recent_receipts"):
        rows = []
        for r in payload["recent_receipts"]:
            ts_str = html.escape(r["ts"][:19].replace("T", " "))
            labeler_str = html.escape(r["labeler_did"])
            uri_link = _at_uri_to_bsky_link(r["uri"])
            val_str = html.escape(r["val"])
            type_str = html.escape(r["type"])
            rows.append([ts_str, labeler_str, uri_link, val_str, type_str])
        sections.append(
            '<h2>Recent Receipts</h2>'
            + _table(["Time", "Labeler", "URI", "Label", "Type"], rows)
        )

    # --- Methods footer ---
    gen_at = html.escape(payload.get("generated_at", ""))
    sections.append(
        '<div class="card" style="margin-top:2rem;font-size:0.85rem;opacity:0.7">'
        f'<p><strong>Methods:</strong> Data from labelwatch rollup tables '
        f'(derived_author_day, derived_author_labeler_day) and raw label_events. '
        f'Window: {window_days} days. Post-only filter (app.bsky.feed.post). '
        f'Generated: {gen_at}.</p>'
        '</div>'
    )

    body = "\n".join(sections)
    return _layout(title, body)
