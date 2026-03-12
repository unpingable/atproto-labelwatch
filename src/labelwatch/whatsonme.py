"""What's on me? — Account-level label lookup.

Uses labelwatch's own ingested label_events as the primary data source.
This sees labels from ALL labelers in the registry, not just ones the user
subscribes to — which is the whole point. queryLabels requires you to already
know who to ask; labelwatch already knows.
"""
from __future__ import annotations

import html
import json
import logging
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .resolve import resolve_handle, resolve_handle_to_did

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Local DB query (primary source)
# ---------------------------------------------------------------------------

def fetch_account_labels_from_db(
    conn, did: str,
) -> List[Dict[str, Any]]:
    """Fetch all labels on a DID from labelwatch's own label_events table.

    This is the good path: we see labels from every labeler we've ever
    ingested, not just ones the user subscribes to.
    """
    rows = conn.execute(
        "SELECT labeler_did, uri, val, neg, ts, cid, exp "
        "FROM label_events WHERE uri = ? "
        "ORDER BY ts DESC LIMIT 500",
        (did,),
    ).fetchall()

    labels = []
    for r in rows:
        label: Dict[str, Any] = {
            "src": r["labeler_did"],
            "uri": r["uri"],
            "val": r["val"],
            "cts": r["ts"],
        }
        if r["neg"]:
            label["neg"] = True
        if r["cid"]:
            label["cid"] = r["cid"]
        if r["exp"]:
            label["exp"] = r["exp"]
        labels.append(label)

    return labels


def fetch_profile(did: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
    """Fetch basic profile info via app.bsky.actor.getProfile."""
    url = f"{APPVIEW_BASE}/app.bsky.actor.getProfile?actor={urllib.parse.quote(did)}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        log.debug("getProfile failed for %s", did, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Label state computation
# ---------------------------------------------------------------------------

def compute_label_state(
    raw_labels: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Compute effective label state from raw label events.

    Groups by (src, val) and determines whether each is active, cleared,
    or expired. Returns {"active": [...], "cleared": [...], "expired": [...]}.
    """
    # Group by (src, val, uri)
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for label in raw_labels:
        key = (label.get("src", ""), label.get("val", ""), label.get("uri", ""))
        groups.setdefault(key, []).append(label)

    active = []
    cleared = []
    expired = []

    now = datetime.now(timezone.utc)

    for (src, val, uri), events in groups.items():
        # Sort by cts (created timestamp)
        events.sort(key=lambda e: e.get("cts", ""))
        latest = events[-1]

        entry = {
            "src": src,
            "val": val,
            "uri": uri,
            "cts": latest.get("cts", ""),
            "cid": latest.get("cid"),
            "event_count": len(events),
        }

        # Check if negated
        if latest.get("neg"):
            entry["cleared_at"] = latest.get("cts", "")
            cleared.append(entry)
            continue

        # Check if expired
        exp = latest.get("exp")
        if exp:
            try:
                exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                if exp_dt <= now:
                    entry["expired_at"] = exp
                    expired.append(entry)
                    continue
            except (ValueError, TypeError):
                pass

        active.append(entry)

    # Sort each group by timestamp descending
    for group in (active, cleared, expired):
        group.sort(key=lambda e: e.get("cts", ""), reverse=True)

    return {"active": active, "cleared": cleared, "expired": expired}


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def resolve_identifier(identifier: str) -> Optional[str]:
    """Resolve an identifier (DID or @handle) to a DID.

    Returns None if resolution fails.
    """
    identifier = identifier.strip()
    if identifier.startswith("did:"):
        return identifier
    return resolve_handle_to_did(identifier)


def generate_whatsonme(
    identifier: str,
    sources: Optional[List[str]] = None,
    conn=None,
) -> Dict[str, Any]:
    """Generate a 'What's on me?' report for an identifier (DID or handle).

    If conn is provided, queries labelwatch's own DB (sees all labelers).
    Otherwise falls back to AppView queryLabels (requires sources).

    Returns a payload dict with profile info, label state, and raw events.
    """
    # Resolve identifier
    did = resolve_identifier(identifier)
    if not did:
        return {
            "error": True,
            "message": f"Could not resolve identifier: {identifier}",
        }

    # Fetch profile
    profile = fetch_profile(did)

    # Fetch labels — prefer local DB
    if conn is not None:
        raw_labels = fetch_account_labels_from_db(conn, did)
    else:
        raw_labels = []  # no DB, no AppView without sources
        log.warning("whatsonme called without DB connection — results will be empty")
    state = compute_label_state(raw_labels)

    # Collect unique sources
    all_sources = sorted({label.get("src", "") for label in raw_labels} - {""})

    # Resolve source handles — from local DB if available, else network
    source_handles: Dict[str, Optional[str]] = {}
    for src in all_sources:
        if conn is not None:
            row = conn.execute(
                "SELECT handle FROM labelers WHERE labeler_did = ?", (src,),
            ).fetchone()
            source_handles[src] = row["handle"] if row and row["handle"] else None
        else:
            source_handles[src] = resolve_handle(src)

    payload: Dict[str, Any] = {
        "did": did,
        "handle": profile.get("handle") if profile else identifier.lstrip("@"),
        "display_name": profile.get("displayName") if profile else None,
        "avatar": profile.get("avatar") if profile else None,
        "active_labels": state["active"],
        "cleared_labels": state["cleared"],
        "expired_labels": state["expired"],
        "total_active": len(state["active"]),
        "total_sources": len(all_sources),
        "sources": [
            {"did": s, "handle": source_handles.get(s)} for s in all_sources
        ],
        "raw_events": raw_labels,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    return payload


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def _render_whatsonme_html(payload: Dict[str, Any]) -> str:
    """Render a 'What's on me?' payload as standalone HTML."""
    from .report import STYLE, THEME_JS, THEME_TOGGLE_JS, _layout, _table

    if payload.get("error"):
        body = (
            '<div class="card" style="text-align:center;padding:2rem;">'
            f'<p style="color:var(--accent-red,#c0392b)">{html.escape(payload["message"])}</p>'
            '</div>'
        )
        return _layout("What's on me?", body)

    did = html.escape(payload["did"])
    handle = html.escape(payload.get("handle") or "")
    display_name = html.escape(payload.get("display_name") or "")
    total_active = payload["total_active"]
    total_sources = payload["total_sources"]
    gen_at = html.escape(payload.get("generated_at", ""))

    sections: List[str] = []

    # Nav
    sections.append(
        '<p class="small" style="margin-bottom:0.5rem;">'
        '<a href="/">&larr; Back to dashboard</a></p>'
    )

    # Header card
    avatar_html = ""
    if payload.get("avatar"):
        avatar_html = (
            f'<img src="{html.escape(payload["avatar"])}" '
            f'style="width:48px;height:48px;border-radius:50%;margin-right:1rem;vertical-align:middle" '
            f'alt="">'
        )

    header = (
        '<div class="card" style="display:flex;align-items:center;gap:1rem;padding:1.2rem">'
        f'{avatar_html}'
        '<div>'
        f'<div style="font-size:1.2rem;font-weight:600">'
        f'{"@" + handle if handle else did}</div>'
    )
    if display_name:
        header += f'<div class="small" style="opacity:0.7">{display_name}</div>'
    if handle:
        header += f'<div class="small" style="opacity:0.5;font-family:monospace">{did}</div>'
    header += '</div></div>'
    sections.append(header)

    # Summary cards
    sections.append(f"""
    <div class="grid">
      <div class="card health-metric">
        <div class="label">Active Labels</div>
        <div class="value">{total_active}</div>
      </div>
      <div class="card health-metric">
        <div class="label">Sources</div>
        <div class="value">{total_sources}</div>
      </div>
      <div class="card health-metric">
        <div class="label">Cleared</div>
        <div class="value">{len(payload.get("cleared_labels", []))}</div>
      </div>
      <div class="card health-metric">
        <div class="label">Expired</div>
        <div class="value">{len(payload.get("expired_labels", []))}</div>
      </div>
    </div>
    """)

    # Build source handle lookup
    source_map = {}
    for s in payload.get("sources", []):
        source_map[s["did"]] = s.get("handle")

    def _src_display(src_did: str) -> str:
        h = source_map.get(src_did)
        if h:
            return f'@{html.escape(h)}'
        return html.escape(src_did)

    # Active labels
    if payload.get("active_labels"):
        rows = []
        for label in payload["active_labels"]:
            rows.append([
                html.escape(label["val"]),
                _src_display(label["src"]),
                html.escape(label["cts"][:19].replace("T", " ")) if label.get("cts") else "—",
            ])
        sections.append(
            '<h2>Active Now</h2>'
            + _table(["Label", "Source", "Applied"], rows)
        )
    else:
        sections.append(
            '<h2>Active Now</h2>'
            '<div class="card" style="text-align:center;padding:1.5rem;">'
            '<p>No active account labels</p>'
            '</div>'
        )

    # Cleared labels
    if payload.get("cleared_labels"):
        rows = []
        for label in payload["cleared_labels"]:
            rows.append([
                html.escape(label["val"]),
                _src_display(label["src"]),
                html.escape(label.get("cleared_at", "")[:19].replace("T", " ")),
            ])
        sections.append(
            '<h2>Recently Cleared</h2>'
            + _table(["Label", "Source", "Cleared"], rows)
        )

    # Expired labels
    if payload.get("expired_labels"):
        rows = []
        for label in payload["expired_labels"]:
            rows.append([
                html.escape(label["val"]),
                _src_display(label["src"]),
                html.escape(label.get("expired_at", "")[:19].replace("T", " ")),
            ])
        sections.append(
            '<h2>Expired</h2>'
            + _table(["Label", "Source", "Expired"], rows)
        )

    # Raw event trail
    if payload.get("raw_events"):
        rows = []
        for ev in sorted(payload["raw_events"],
                         key=lambda e: e.get("cts", ""), reverse=True)[:50]:
            neg_str = "negate" if ev.get("neg") else "apply"
            rows.append([
                html.escape(ev.get("cts", "")[:19].replace("T", " ")),
                html.escape(ev.get("val", "")),
                _src_display(ev.get("src", "")),
                neg_str,
                html.escape(ev.get("cid", "")[:12]) if ev.get("cid") else "—",
            ])
        sections.append(
            '<h2>Raw Event Trail</h2>'
            + _table(["Time", "Value", "Source", "Action", "CID"], rows)
        )

    # Methods footer
    sections.append(
        '<div class="card" style="margin-top:2rem;font-size:0.85rem;opacity:0.7">'
        f'<p><strong>Methods:</strong> Direct query via '
        f'<code>com.atproto.label.queryLabels</code> against the AppView. '
        f'Shows account-level labels (subject = DID), not post-level labels. '
        f'Label state computed from negation and expiry fields. '
        f'Generated: {gen_at}.</p>'
        '</div>'
    )

    title = f"What's on me? — @{handle}" if handle else f"What's on me? — {did}"
    body = "\n".join(sections)
    return _layout(title, body)
