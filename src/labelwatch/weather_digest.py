"""labelwatch.weather_digest.v0 — weekly network-weather digest.

Chatty 2026-06-09: "auto-generated weekly weather digest is journalist bait
that writes itself from queries you already run." Posts emitter activity,
churn, conflicts, provenance changes, and notable concentrations — never
verdicts about subjects.

Doctrine:
  - Weather, never verdict ([constraint_weather_not_verdict]) — describe
    labeler behavior and network conditions, not subject status.
  - The unit of observation is the labeler, not the labeled.
  - One emitter-description quote when available; never editorialize.
  - Co-presence is not corroboration; we don't validate findings by
    cross-referencing labelers.

Output formats:
  - text  — markdown-ish, good for sharing or piping to `labelwatch post`
  - json  — structured for cron / RSS / external consumers
  - bluesky — single 300-grapheme-safe post (compact compression of the
              text format)

This module does NOT auto-post. Wire it with the existing `labelwatch post`
when ready: `labelwatch post "$(labelwatch weather-digest --format bluesky)"`.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from . import frontdoor
from .utils import format_ts, get_git_commit, hash_sha256, now_utc, stable_json

log = logging.getLogger(__name__)


RECEIPT_KIND = "labelwatch.weather_digest.v0"
RECEIPT_SCHEMA_VERSION = 1

# Per-section caps. Digest is a digest, not a dump.
MAX_NEW_LABELERS = 10
MAX_WENT_DARK = 10
MAX_CONCENTRATIONS = 5
MAX_DESCRIPTION_QUOTE_CHARS = 220


# ---------------------------------------------------------------------------
# Section queries
# ---------------------------------------------------------------------------

def new_labelers(conn: sqlite3.Connection, since: str, limit: int = MAX_NEW_LABELERS) -> list[dict]:
    """Labelers whose first_seen lands in the digest window."""
    rows = conn.execute(
        "SELECT labeler_did, handle, display_name, first_seen, events_7d, "
        "events_30d, regime_state, description "
        "FROM labelers "
        "WHERE first_seen >= ? "
        "ORDER BY first_seen DESC, events_7d DESC "
        "LIMIT ?",
        (since, limit),
    ).fetchall()
    return [
        {
            "labeler_did": r["labeler_did"],
            "handle": r["handle"],
            "display_name": r["display_name"],
            "first_seen": r["first_seen"],
            "events_7d": r["events_7d"] or 0,
            "events_30d": r["events_30d"] or 0,
            "regime_state": r["regime_state"],
            "description": _trim_description(r["description"]),
        }
        for r in rows
    ]


def went_dark(conn: sqlite3.Connection, limit: int = MAX_WENT_DARK) -> list[dict]:
    """Labelers that emitted in the last 30d but not the last 7d.

    Surfaces "the labeler that's been silent for a week" — descriptive,
    not adjudicative."""
    rows = conn.execute(
        "SELECT labeler_did, handle, last_seen, events_7d, events_30d, "
        "regime_state, endpoint_status "
        "FROM labelers "
        "WHERE events_30d > 0 AND COALESCE(events_7d, 0) = 0 "
        "ORDER BY events_30d DESC "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "labeler_did": r["labeler_did"],
            "handle": r["handle"],
            "last_seen": r["last_seen"],
            "events_7d": r["events_7d"] or 0,
            "events_30d": r["events_30d"] or 0,
            "regime_state": r["regime_state"],
            "endpoint_status": r["endpoint_status"],
        }
        for r in rows
    ]


def notable_concentrations(
    conn: sqlite3.Connection, limit: int = MAX_CONCENTRATIONS
) -> list[dict]:
    """Single-labeler/single-value pairs with the highest 7d volume.

    Surfaces "this labeler has been emitting this exact value at high
    volume" — the kind of pattern that's worth a paragraph without
    editorializing about what the value means."""
    # 7d cutoff: clip on the labeler.events_7d signal, but compute precise
    # ranking from the per-labeler events_30d × val join. Keep small.
    rows = conn.execute(
        "SELECT labelers.labeler_did, labelers.handle, labelers.events_7d, "
        "labelers.events_30d, labelers.description "
        "FROM labelers "
        "WHERE COALESCE(labelers.events_7d, 0) >= 1000 "
        "ORDER BY labelers.events_7d DESC "
        "LIMIT ?",
        (limit * 4,),  # over-fetch so we have headroom after we drop hand-picked dropouts
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        if len(out) >= limit:
            break
        out.append({
            "labeler_did": r["labeler_did"],
            "handle": r["handle"],
            "events_7d": r["events_7d"] or 0,
            "events_30d": r["events_30d"] or 0,
            "description": _trim_description(r["description"]),
        })
    return out


def _trim_description(text: Optional[str]) -> Optional[str]:
    """Trim a labeler's self-declared description for inline quoting."""
    if not text:
        return None
    text = " ".join(text.split())  # collapse whitespace
    if len(text) > MAX_DESCRIPTION_QUOTE_CHARS:
        return text[: MAX_DESCRIPTION_QUOTE_CHARS - 1].rstrip() + "…"
    return text


# ---------------------------------------------------------------------------
# Digest assembly
# ---------------------------------------------------------------------------

def build_digest(
    conn: sqlite3.Connection,
    *,
    now: Optional[datetime] = None,
    window_days: int = 7,
) -> dict:
    """Build the weekly weather digest payload."""
    if now is None:
        now = now_utc()
    since = format_ts(now - timedelta(days=window_days))

    weather = frontdoor.network_weather(conn, now=now)

    digest = {
        "receipt_kind": RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": format_ts(now),
        "git_commit": get_git_commit(),
        "window_days": window_days,
        "window_start": since,
        "weather": {
            "signals": weather["signals"],
            "attribution": weather["attribution"],
            "total_labelers": weather["total_labelers"],
            "emitting_this_week": weather["emitting_this_week"],
            "events_7d_total": weather["events_7d_total"],
            "unreachable": weather["unreachable"],
        },
        "new_labelers": new_labelers(conn, since),
        "went_dark": went_dark(conn),
        "notable_concentrations": notable_concentrations(conn),
    }
    digest_for_hash = {k: v for k, v in digest.items() if k != "receipt_hash"}
    digest["receipt_hash"] = hash_sha256(stable_json(digest_for_hash))
    return digest


# ---------------------------------------------------------------------------
# Rendering — text (markdown-ish), JSON, bluesky-compact
# ---------------------------------------------------------------------------

def _format_count(n: int) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def render_text(digest: dict) -> str:
    """Markdown-ish digest, good for sharing / Substack / piping to Bluesky.
    Long form. Surfaces the same facts the JSON has, with weather-not-verdict
    framing."""
    w = digest["weather"]
    out: list[str] = []
    out.append(f"# Labelwatch network weather — week of {digest['generated_at'][:10]}")
    out.append("")
    out.append(f"**Network weather:** {', '.join(w['signals'])}")
    out.append(
        f"{_format_count(w['total_labelers'])} labelers · "
        f"{_format_count(w['emitting_this_week'])} emitting this week · "
        f"{_format_count(w['events_7d_total'])} events in 7d · "
        f"{_format_count(w['unreachable'])} unreachable"
    )
    if w["attribution"]:
        out.append(f"_({w['attribution']})_")
    out.append("")

    if digest["new_labelers"]:
        out.append(f"## New labelers ({len(digest['new_labelers'])} this week)")
        for nl in digest["new_labelers"]:
            handle = nl["handle"] or nl["labeler_did"]
            events = f"{_format_count(nl['events_7d'])} events 7d"
            line = f"- **{handle}** — first seen {nl['first_seen'][:10]}, {events}"
            if nl["regime_state"]:
                line += f", regime: {nl['regime_state']}"
            out.append(line)
            if nl["description"]:
                out.append(f"  > {nl['description']}")
        out.append("")

    if digest["went_dark"]:
        out.append(f"## Went dark ({len(digest['went_dark'])} this week)")
        out.append(
            "_Labelers active in the last 30 days but silent for the last 7. "
            "Descriptive observation — silence may be intentional._"
        )
        for d in digest["went_dark"]:
            handle = d["handle"] or d["labeler_did"]
            out.append(
                f"- **{handle}** — last seen {(d['last_seen'] or '?')[:10]}, "
                f"{_format_count(d['events_30d'])} events 30d, "
                f"endpoint: {d['endpoint_status'] or '?'}"
            )
        out.append("")

    if digest["notable_concentrations"]:
        out.append("## Notable concentrations (7d volume)")
        out.append(
            "_Labelers emitting at high volume this week. Volume is not a "
            "verdict; it's emitter activity._"
        )
        for c in digest["notable_concentrations"]:
            handle = c["handle"] or c["labeler_did"]
            out.append(
                f"- **{handle}** — {_format_count(c['events_7d'])} events 7d, "
                f"{_format_count(c['events_30d'])} 30d"
            )
            if c["description"]:
                out.append(f"  > {c['description']}")
        out.append("")

    out.append("---")
    out.append(
        "Generated by labelwatch.weather_digest.v0 · "
        f"see system dashboard at https://labelwatch.neutral.zone/methodology.html"
    )
    return "\n".join(out)


def render_bluesky(digest: dict) -> str:
    """300-char-safe Bluesky post. Compact weather + one or two facts.

    Doctrine: weather, never verdict. Format is meteorology."""
    w = digest["weather"]
    signals = ", ".join(w["signals"])
    line1 = f"Labelwatch weather: {signals}."
    line2 = (
        f"{_format_count(w['total_labelers'])} labelers · "
        f"{_format_count(w['emitting_this_week'])} emitting · "
        f"{_format_count(w['events_7d_total'])} events 7d · "
        f"{_format_count(w['unreachable'])} unreachable"
    )
    facts: list[str] = []
    if digest["new_labelers"]:
        n = len(digest["new_labelers"])
        facts.append(f"{n} new labeler{'s' if n != 1 else ''} this week")
    if digest["went_dark"]:
        n = len(digest["went_dark"])
        facts.append(f"{n} went dark")
    facts_line = "; ".join(facts) if facts else ""

    post = f"{line1}\n{line2}"
    if facts_line:
        post += f"\n{facts_line}."
    post += "\nhttps://labelwatch.neutral.zone"

    # Bluesky's grapheme cap is 300; we approximate by len() since our copy
    # is ASCII + a few separators. Trim if needed.
    if len(post) > 300:
        post = post[:297].rstrip() + "…"
    return post


def render_json(digest: dict, *, indent: int = 2) -> str:
    return json.dumps(digest, indent=indent)
