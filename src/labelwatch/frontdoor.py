"""whatsonme.frontdoor.v0 — lookup-first subject view.

Implements the surface contract in docs/analysis/subject-lookup-frontdoor-001.md:
given a handle or DID, return observed labeler testimony touching that subject,
with explicit refusal states and bounded outputs.

Surface posture (composes with weather-not-verdict + observation-export-custody):
- The subject is the QUERY, never the FINDING.
- Output language describes labelers, not subjects.
- Plain-language sentence uses "appears", not "is", for derived classifications.
- Explicit non-outputs (enforced in code + copy):
    truth_about_subject
    trust_score_for_labeler
    unified_risk_score
    moderation_recommendation

Publication-gate: the frontdoor consumes the canonical
`labelwatch.index_audit.v1` receipt for `whatsonme.frontdoor.v0`. If no fresh
admissible receipt exists, every lookup returns a refusal — no silent degrade.
"""

from __future__ import annotations

import glob
import html as html_lib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from .label_family import classify_authority_effect, normalize_family
from .utils import format_ts, now_utc

log = logging.getLogger(__name__)


SURFACE = "whatsonme.frontdoor.v0"
AUDIT_RECEIPT_KIND = "labelwatch.index_audit.v1"

# Refusal states — must match docs/analysis/subject-lookup-frontdoor-001.md.
REFUSAL_STATES = (
    "subject_not_found",
    "handle_resolution_ambiguous",
    "no_observed_labels",
    "index_audit_missing",
    "query_shape_unbounded",
    "insufficient_labeler_profile",
    "insufficient_temporal_history",
)

ADMISSIBLE_VERDICTS = {"admissible", "admissible_with_debt"}

# Sentence vocabulary (chatty 2026-06-10): conservative, observer-side language.
# - "stable" via regime_state in {"stable", "bursty"} — bursty is volume-y, not
#   shape-volatile. "warming_up" → insufficient-history (we don't know yet).
# - "churny" via {"flapping", "degraded"} — observed instability of classification.
# - "stale" via {"inactive", "dark_operational", "ghost_declared"} — emitter silent.
_STABILITY_MAP = {
    "stable": "stable",
    "bursty": "stable",
    "flapping": "churny",
    "degraded": "churny",
    "inactive": "stale",
    "dark_operational": "stale",
    "ghost_declared": "stale",
    "warming_up": "insufficient-history",
}

_AUDITABILITY_MAP = {
    "high": "easy",
    "medium": "limited",
    "low": "hard",
}


# ---------------------------------------------------------------------------
# Result data shape
# ---------------------------------------------------------------------------

@dataclass
class LabelerCard:
    labeler_did: str
    handle: Optional[str]
    event_count: int
    label_values: list[dict]          # [{val, count, first_seen, last_seen, authority_effect}]
    authority_effects: dict[str, int]  # bucket → count
    regime_state: Optional[str]
    auditability: Optional[str]
    last_seen: Optional[str]
    events_7d: Optional[int]
    events_30d: Optional[int]
    plain_language_sentence: str
    classification_changed: bool      # temporal coherence: did labeler flip?
    # Negative-space — fields the card MUST NOT contain. Kept here so an
    # accidental field-name collision turns into a test failure.
    _forbidden_fields: tuple = field(
        default=(
            "truth_about_subject",
            "trust_score_for_labeler",
            "unified_risk_score",
            "moderation_recommendation",
        ),
        repr=False,
    )


@dataclass
class FrontdoorResult:
    surface: str
    consumer_surface_version: str
    generated_at: str

    # Identity resolution.
    input_identifier: str
    subject_did: Optional[str]
    subject_handle: Optional[str]

    # Cards (one per labeler touching subject).
    labelers: list[LabelerCard]

    # Refusal state — only set when admissible lookup is not possible.
    refusal: Optional[str]
    refusal_detail: Optional[str]

    # Audit-gate provenance.
    audit_verdict: Optional[str]
    audit_receipt_path: Optional[str]
    audit_generated_at: Optional[str]


# ---------------------------------------------------------------------------
# Audit-receipt gate
# ---------------------------------------------------------------------------

def _default_receipts_dir() -> str:
    # Receipts live in <repo>/docs/analysis/receipts/ relative to this file.
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(here, "..", "..", "docs", "analysis", "receipts"))


def find_latest_audit_receipt(
    receipts_dir: Optional[str] = None,
    consumer_surface: str = SURFACE,
) -> Optional[dict]:
    """Return the parsed receipt with the latest generated_at, or None.

    Looks for files matching
        labelwatch.index_audit.{consumer_surface}.*.json
    in `receipts_dir` (default: <repo>/docs/analysis/receipts/).
    """
    receipts_dir = receipts_dir or os.environ.get(
        "LABELWATCH_AUDIT_RECEIPTS_DIR"
    ) or _default_receipts_dir()
    pattern = os.path.join(
        receipts_dir, f"labelwatch.index_audit.{consumer_surface}.*.json"
    )
    candidates = sorted(glob.glob(pattern))
    if not candidates:
        return None

    # Pick the one with the latest generated_at; fall back to filename order.
    parsed: list[tuple[str, dict, str]] = []
    for path in candidates:
        try:
            with open(path, "r", encoding="utf-8") as f:
                receipt = json.load(f)
        except (OSError, json.JSONDecodeError):
            log.warning("frontdoor: could not parse receipt %s", path, exc_info=True)
            continue
        ts = receipt.get("generated_at") or ""
        parsed.append((ts, receipt, path))
    if not parsed:
        return None
    parsed.sort(key=lambda x: x[0])
    ts, receipt, path = parsed[-1]
    receipt["_receipt_path"] = path
    return receipt


def audit_gate_status(receipt: Optional[dict]) -> tuple[bool, Optional[str], Optional[str]]:
    """Returns (admissible, refusal_state, detail).

    refusal_state is None when admissible; otherwise one of
    'index_audit_missing' / 'query_shape_unbounded'.
    """
    if receipt is None:
        return (False, "index_audit_missing", "no labelwatch.index_audit.v1 receipt found")
    verdict = receipt.get("overall_verdict") or ""
    if verdict in ADMISSIBLE_VERDICTS:
        return (True, None, None)
    # Map specific refused_* verdicts to the surface's refusal vocabulary.
    if verdict == "refused_query_shape_unbounded":
        return (False, "query_shape_unbounded", verdict)
    # Everything else is treated as missing/unusable.
    return (False, "index_audit_missing", verdict or "audit verdict missing")


# ---------------------------------------------------------------------------
# Identity resolution
# ---------------------------------------------------------------------------

_DID_PREFIXES = ("did:plc:", "did:web:")


def _looks_like_did(s: str) -> bool:
    return any(s.startswith(p) for p in _DID_PREFIXES)


def resolve_input(
    identifier: str,
    handle_resolver=None,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Resolve (handle | DID | @handle) → (did, handle, refusal_state).

    `handle_resolver` defaults to .resolve.resolve_handle_to_did but is
    injectable for testing.
    """
    if handle_resolver is None:
        from .resolve import resolve_handle_to_did
        handle_resolver = resolve_handle_to_did

    raw = (identifier or "").strip()
    if not raw:
        return (None, None, "subject_not_found")

    if _looks_like_did(raw):
        # Loose validation; downstream queries don't need stricter shape.
        if not re.match(r"^did:(plc|web):[A-Za-z0-9._:%-]+$", raw):
            return (None, None, "subject_not_found")
        return (raw, None, None)

    handle = raw.lstrip("@")
    try:
        did = handle_resolver(handle)
    except Exception:
        log.exception("frontdoor: handle resolution raised")
        did = None
    if not did:
        return (None, handle, "subject_not_found")
    return (did, handle, None)


# ---------------------------------------------------------------------------
# Query inventory — matches Q2/Q3/Q6/Q8 from the surface contract / audit
# ---------------------------------------------------------------------------

_Q3_LABEL_VALUES = (
    "SELECT labeler_did, val, "
    "COUNT(*) AS event_count, "
    "MIN(ts) AS first_seen, "
    "MAX(ts) AS last_seen "
    "FROM label_events "
    "WHERE target_did = ? "
    "GROUP BY labeler_did, val"
)

_Q6_LABELER_PROFILE = (
    "SELECT labeler_did, handle, regime_state, auditability, "
    "last_seen, events_7d, events_30d "
    "FROM labelers "
    "WHERE labeler_did = ?"
)

_Q8_COHERENCE = (
    "SELECT labeler_did, val, ts, neg "
    "FROM label_events "
    "WHERE target_did = ? "
    "ORDER BY labeler_did, ts"
)


def _classification_changed(events_for_labeler: list[Mapping]) -> bool:
    """Detect whether a labeler's claim about the subject has flipped over time.

    A flip = same val toggling neg, or distinct vals appearing across the
    timeline. This is descriptive (testimony shifted), not adjudicative
    (labeler was 'wrong').
    """
    if not events_for_labeler:
        return False
    seen_states: set[tuple[str, bool]] = set()
    for ev in events_for_labeler:
        state = (ev["val"], bool(ev["neg"]))
        seen_states.add(state)
        if len(seen_states) > 1:
            # Two distinct (val, neg) pairs from the same labeler against the
            # same subject = observable shift in testimony.
            return True
    return False


def plain_language_sentence(
    labeler_row: Mapping,
    authority_effects: Mapping[str, int],
) -> str:
    """Generate the per-labeler card sentence.

    Rules (chatty 2026-06-10):
    - "appears", not "is", for derived classifications.
    - Never say the label is true.
    - Never say the subject is risky.
    - Never rank labelers globally.
    - Never emit a unified score.
    """
    if authority_effects:
        ae_bucket = max(authority_effects.items(), key=lambda kv: kv[1])[0]
    else:
        ae_bucket = "unclassified"

    regime = (labeler_row.get("regime_state") if labeler_row else None) or ""
    stability = _STABILITY_MAP.get(regime, "insufficient-history")

    auditability_raw = (labeler_row.get("auditability") if labeler_row else None) or ""
    auditability = _AUDITABILITY_MAP.get(auditability_raw, "unknown")

    return (
        f"This labeler mostly emits {ae_bucket} labels, "
        f"appears {stability} over time, "
        f"and is {auditability} to audit from available evidence."
    )


def _build_labeler_card(
    labeler_did: str,
    rows_for_labeler: list[Mapping],   # Q3 rows: (labeler_did, val, event_count, first_seen, last_seen)
    profile_row: Optional[Mapping],     # Q6: labelers PK row
    coherence_events: list[Mapping],    # Q8 events for this labeler+subject
) -> LabelerCard:
    label_values: list[dict] = []
    authority_effects: dict[str, int] = {}
    event_count_total = 0
    last_seen: Optional[str] = None
    for r in rows_for_labeler:
        family = normalize_family(r["val"])
        effect = classify_authority_effect(family)
        c = int(r["event_count"])
        label_values.append({
            "val": r["val"],
            "count": c,
            "first_seen": r["first_seen"],
            "last_seen": r["last_seen"],
            "authority_effect": effect,
        })
        authority_effects[effect] = authority_effects.get(effect, 0) + c
        event_count_total += c
        if r["last_seen"] and (last_seen is None or r["last_seen"] > last_seen):
            last_seen = r["last_seen"]

    handle = (profile_row.get("handle") if profile_row else None) or None
    regime = (profile_row.get("regime_state") if profile_row else None) or None
    auditability = (profile_row.get("auditability") if profile_row else None) or None
    events_7d = profile_row.get("events_7d") if profile_row else None
    events_30d = profile_row.get("events_30d") if profile_row else None

    sentence = plain_language_sentence(profile_row or {}, authority_effects)
    changed = _classification_changed(coherence_events)

    return LabelerCard(
        labeler_did=labeler_did,
        handle=handle,
        event_count=event_count_total,
        label_values=sorted(label_values, key=lambda lv: (-lv["count"], lv["val"])),
        authority_effects=authority_effects,
        regime_state=regime,
        auditability=auditability,
        last_seen=last_seen,
        events_7d=events_7d,
        events_30d=events_30d,
        plain_language_sentence=sentence,
        classification_changed=changed,
    )


# ---------------------------------------------------------------------------
# Top-level lookup
# ---------------------------------------------------------------------------

def lookup_subject(
    conn: sqlite3.Connection,
    identifier: str,
    *,
    audit_receipt: Optional[dict] = None,
    handle_resolver=None,
) -> FrontdoorResult:
    """Run the frontdoor lookup for a handle or DID.

    Always returns a FrontdoorResult. Refusal states are first-class outputs;
    the frontdoor does not raise for any expected condition.
    """
    generated_at = format_ts(now_utc())

    # Step 1: audit-gate.
    admissible, gate_refusal, gate_detail = audit_gate_status(audit_receipt)
    audit_verdict = audit_receipt.get("overall_verdict") if audit_receipt else None
    audit_path = audit_receipt.get("_receipt_path") if audit_receipt else None
    audit_ts = audit_receipt.get("generated_at") if audit_receipt else None

    if not admissible:
        return FrontdoorResult(
            surface=SURFACE,
            consumer_surface_version="v0",
            generated_at=generated_at,
            input_identifier=identifier,
            subject_did=None,
            subject_handle=None,
            labelers=[],
            refusal=gate_refusal,
            refusal_detail=gate_detail,
            audit_verdict=audit_verdict,
            audit_receipt_path=audit_path,
            audit_generated_at=audit_ts,
        )

    # Step 2: resolve identity.
    did, handle, id_refusal = resolve_input(identifier, handle_resolver=handle_resolver)
    if id_refusal is not None:
        return FrontdoorResult(
            surface=SURFACE,
            consumer_surface_version="v0",
            generated_at=generated_at,
            input_identifier=identifier,
            subject_did=did,
            subject_handle=handle,
            labelers=[],
            refusal=id_refusal,
            refusal_detail=f"could not resolve input {identifier!r}",
            audit_verdict=audit_verdict,
            audit_receipt_path=audit_path,
            audit_generated_at=audit_ts,
        )

    # Step 3: Q3 — per (labeler, val) rollup against subject.
    q3_rows = [dict(r) for r in conn.execute(_Q3_LABEL_VALUES, (did,)).fetchall()]
    if not q3_rows:
        return FrontdoorResult(
            surface=SURFACE,
            consumer_surface_version="v0",
            generated_at=generated_at,
            input_identifier=identifier,
            subject_did=did,
            subject_handle=handle,
            labelers=[],
            refusal="no_observed_labels",
            refusal_detail=f"no label_events against target_did={did}",
            audit_verdict=audit_verdict,
            audit_receipt_path=audit_path,
            audit_generated_at=audit_ts,
        )

    # Group Q3 rows by labeler.
    by_labeler: dict[str, list[dict]] = {}
    for r in q3_rows:
        by_labeler.setdefault(r["labeler_did"], []).append(r)

    # Step 4: Q8 — temporal coherence (one query, grouped client-side).
    q8_rows = [dict(r) for r in conn.execute(_Q8_COHERENCE, (did,)).fetchall()]
    coherence_by_labeler: dict[str, list[dict]] = {}
    for r in q8_rows:
        coherence_by_labeler.setdefault(r["labeler_did"], []).append(r)

    # Step 5: Q6 — per-labeler profile (one round-trip per labeler, indexed PK).
    cards: list[LabelerCard] = []
    for labeler_did, rows_for_labeler in by_labeler.items():
        profile_row = conn.execute(_Q6_LABELER_PROFILE, (labeler_did,)).fetchone()
        profile = dict(profile_row) if profile_row is not None else None
        coherence_events = coherence_by_labeler.get(labeler_did, [])
        card = _build_labeler_card(
            labeler_did=labeler_did,
            rows_for_labeler=rows_for_labeler,
            profile_row=profile,
            coherence_events=coherence_events,
        )
        cards.append(card)

    # Stable, observer-side ordering: most-active labeler first, then DID.
    cards.sort(key=lambda c: (-c.event_count, c.labeler_did))

    return FrontdoorResult(
        surface=SURFACE,
        consumer_surface_version="v0",
        generated_at=generated_at,
        input_identifier=identifier,
        subject_did=did,
        subject_handle=handle,
        labelers=cards,
        refusal=None,
        refusal_detail=None,
        audit_verdict=audit_verdict,
        audit_receipt_path=audit_path,
        audit_generated_at=audit_ts,
    )


# ---------------------------------------------------------------------------
# JSON + HTML rendering
# ---------------------------------------------------------------------------

# Explicit non-outputs. Render functions MUST NOT include these keys / phrases.
_FORBIDDEN_FIELD_NAMES = (
    "truth_about_subject",
    "trust_score_for_labeler",
    "unified_risk_score",
    "moderation_recommendation",
)

# Phrases banned from rendered copy. Validated by the test suite, not by code
# at runtime — running a substring scan on rendered HTML on every request would
# be theatre. Tests assert the templates below don't emit these.
FORBIDDEN_PHRASES = (
    "trust score",
    "risk score",
    "moderation recommendation",
    "should be moderated",
    # Subject-adjudicating constructions:
    "subject is",
    "this user is a",
    "this account is",
    "verdict on",
)


def result_to_json(result: FrontdoorResult) -> dict:
    """Serialize the result to a JSON-safe dict. Strips any forbidden field
    names from cards, defensively."""
    def _card_dict(c: LabelerCard) -> dict:
        d = asdict(c)
        d.pop("_forbidden_fields", None)
        for fn in _FORBIDDEN_FIELD_NAMES:
            d.pop(fn, None)
        return d

    payload = asdict(result)
    payload["labelers"] = [_card_dict(c) for c in result.labelers]
    return payload


# ----- HTML helpers -------------------------------------------------------

def _esc(s: Any) -> str:
    if s is None:
        return ""
    return html_lib.escape(str(s))


def _format_count(n: Optional[int]) -> str:
    if n is None:
        return "—"
    return f"{int(n):,}"


# ----- Card rendering -----------------------------------------------------

def _render_authority_breakdown_html(authority_effects: dict[str, int]) -> str:
    if not authority_effects:
        return "<p class=\"empty\">No authority-effect classification available.</p>"
    rows = sorted(authority_effects.items(), key=lambda kv: (-kv[1], kv[0]))
    body = "".join(
        f"<tr><th scope=\"row\">{_esc(effect)}</th>"
        f"<td>{_format_count(count)}</td></tr>"
        for effect, count in rows
    )
    return (
        "<table class=\"effects\"><caption>Authority effect (observed)</caption>"
        "<thead><tr><th>Effect</th><th>Events</th></tr></thead>"
        f"<tbody>{body}</tbody></table>"
    )


def _render_label_values_html(label_values: list[dict]) -> str:
    if not label_values:
        return ""
    rows = "".join(
        f"<tr>"
        f"<td>{_esc(lv['val'])}</td>"
        f"<td>{_esc(lv.get('authority_effect') or '—')}</td>"
        f"<td>{_format_count(lv.get('count'))}</td>"
        f"<td>{_esc(lv.get('first_seen') or '—')}</td>"
        f"<td>{_esc(lv.get('last_seen') or '—')}</td>"
        f"</tr>"
        for lv in label_values
    )
    return (
        "<details><summary>Observed label values (raw)</summary>"
        "<table class=\"values\">"
        "<thead><tr>"
        "<th>val</th><th>authority effect</th><th>events</th>"
        "<th>first seen</th><th>last seen</th>"
        "</tr></thead>"
        f"<tbody>{rows}</tbody></table></details>"
    )


def render_labeler_card_html(card: LabelerCard) -> str:
    """Render one per-labeler card.

    Order is: chatty-mandated 'Use this to see' + 'Not for' frames at the
    top → plain-language sentence → authority breakdown → expandable raw
    tables. The card describes the labeler, not the subject.
    """
    use_for, not_for = _card_disclaimers(card)
    handle_display = card.handle or "(handle not resolved)"

    classification_note = ""
    if card.classification_changed:
        classification_note = (
            "<p class=\"coherence-note\">"
            "Labeler's classification of this subject has changed at least once "
            "in the observed window. Testimony shifted; the labeler did not "
            "necessarily err."
            "</p>"
        )

    return (
        "<article class=\"labeler-card\">"
        "<header>"
        f"<h3>{_esc(handle_display)}</h3>"
        f"<p class=\"did\">{_esc(card.labeler_did)}</p>"
        "</header>"
        f"<p class=\"frame use-for\"><strong>Use this to see:</strong> {_esc(use_for)}</p>"
        f"<p class=\"frame not-for\"><strong>Not for:</strong> {_esc(not_for)}</p>"
        f"<p class=\"sentence\">{_esc(card.plain_language_sentence)}</p>"
        f"{classification_note}"
        f"<p class=\"summary\">"
        f"Observed events against subject: {_format_count(card.event_count)}"
        f" &middot; Last seen: {_esc(card.last_seen or '—')}"
        f" &middot; Labeler activity 7d/30d: {_format_count(card.events_7d)}/{_format_count(card.events_30d)}"
        "</p>"
        f"{_render_authority_breakdown_html(card.authority_effects)}"
        f"{_render_label_values_html(card.label_values)}"
        "</article>"
    )


def _card_disclaimers(card: LabelerCard) -> tuple[str, str]:
    """Per-card 'Use this to see' / 'Not for' framing.

    Conservative defaults; specific labelers can be enriched later from
    declared records / about-page text. The framing is the same for every
    card today: what an observer can read out, and what they cannot."""
    use_for = (
        "what this labeler has said about this subject, and what kind of "
        "authority each label attempts to exercise."
    )
    not_for = (
        "deciding whether the subject is 'good' or 'bad', whether the label "
        "is true, or whether action should be taken. The frontdoor publishes "
        "observations of testimony, not adjudication of subjects."
    )
    return use_for, not_for


# ----- Result rendering ---------------------------------------------------

_REFUSAL_COPY = {
    "subject_not_found": (
        "Subject not found",
        "We couldn't resolve that handle or DID. Check spelling, or try the "
        "full DID (e.g. did:plc:abc…). The frontdoor does not query "
        "external label sources on cache miss — only what labelwatch has "
        "ingested.",
    ),
    "handle_resolution_ambiguous": (
        "Handle resolution ambiguous",
        "This handle resolves to more than one DID across services. Please "
        "provide the DID directly.",
    ),
    "no_observed_labels": (
        "No labels observed",
        "Labelwatch has ingested no label_events against this subject across "
        "any of the labelers in its registry. This is an absence of "
        "observation, not a finding about the subject.",
    ),
    "index_audit_missing": (
        "Lookup paused",
        "The lookup surface is gated on a fresh labelwatch.index_audit.v1 "
        "receipt. No admissible receipt is currently available; the surface "
        "refuses rather than continue blindly.",
    ),
    "query_shape_unbounded": (
        "Lookup paused",
        "The lookup surface's query shape is currently unbounded against this "
        "DB. The surface refuses publication until index remediation lands.",
    ),
    "insufficient_labeler_profile": (
        "Insufficient labeler profile",
        "One or more labelers touching this subject lack enough evidence to "
        "populate the dials. Card details are incomplete.",
    ),
    "insufficient_temporal_history": (
        "Insufficient temporal history",
        "This subject has labels but not enough history to populate the "
        "temporal coherence summary.",
    ),
}


def render_refusal_html(result: FrontdoorResult) -> str:
    title, body = _REFUSAL_COPY.get(
        result.refusal or "",
        ("Refusal", result.refusal_detail or "Unspecified refusal."),
    )
    return (
        "<section class=\"refusal\">"
        f"<h2>{_esc(title)}</h2>"
        f"<p>{_esc(body)}</p>"
        + (
            f"<p class=\"refusal-detail\">{_esc(result.refusal_detail)}</p>"
            if result.refusal_detail
            else ""
        )
        + "</section>"
    )


def render_result_body_html(result: FrontdoorResult) -> str:
    """Render the inner body of the result page. Caller wraps in layout."""
    if result.refusal:
        return render_refusal_html(result)

    subject_handle = result.subject_handle or "(handle unresolved)"
    cards_html = "".join(render_labeler_card_html(c) for c in result.labelers)

    return (
        "<section class=\"subject\">"
        f"<h2>{_esc(subject_handle)}</h2>"
        f"<p class=\"did\">{_esc(result.subject_did)}</p>"
        f"<p class=\"observed-count\">"
        f"{len(result.labelers)} observed labelers touching this subject."
        "</p>"
        "<p class=\"disclaimer\">"
        "Labelwatch publishes observations of labeler testimony. The "
        "labels below are claims by labelers about this subject; their "
        "presence here does not validate them. We do not adjudicate "
        "subjects, rank labelers, or produce a unified score."
        "</p>"
        f"{cards_html}"
        "</section>"
    )


def render_result_page_html(result: FrontdoorResult) -> str:
    """Full HTML page (standalone; safe to serve directly)."""
    title = (
        f"What's observed on {result.subject_handle or result.subject_did or 'subject'} — labelwatch"
        if not result.refusal
        else f"Lookup — {result.refusal} — labelwatch"
    )
    body = render_result_body_html(result)
    audit_footer = ""
    if result.audit_verdict:
        audit_footer = (
            "<footer class=\"audit-footer\">"
            f"Audit verdict: <code>{_esc(result.audit_verdict)}</code> "
            f"as of <time datetime=\"{_esc(result.audit_generated_at)}\">"
            f"{_esc(result.audit_generated_at)}</time>"
            "</footer>"
        )
    return (
        "<!doctype html>"
        "<html lang=\"en\"><head>"
        "<meta charset=\"utf-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
        f"<title>{_esc(title)}</title>"
        f"<style>{_RESULT_CSS}</style>"
        "</head><body>"
        "<header class=\"top\">"
        "<a class=\"home-link\" href=\"/\">labelwatch</a>"
        " &middot; "
        "<a class=\"method-link\" href=\"/methodology.html\">methodology</a>"
        "</header>"
        f"<main>{body}</main>"
        f"{audit_footer}"
        "</body></html>"
    )


def render_homepage_html(audit_receipt: Optional[dict] = None) -> str:
    """Lookup-first homepage. The methodology page lives at /methodology.html."""
    admissible, refusal_state, _ = audit_gate_status(audit_receipt)
    audit_verdict = audit_receipt.get("overall_verdict") if audit_receipt else None
    audit_ts = audit_receipt.get("generated_at") if audit_receipt else None

    pause_banner = ""
    if not admissible:
        pause_banner = (
            "<aside class=\"banner pause\">"
            "<strong>Lookups paused:</strong> "
            "the lookup surface is gated on a fresh "
            "<code>labelwatch.index_audit.v1</code> receipt and none is "
            "currently admissible. The form below will return a refusal."
            "</aside>"
        )

    audit_footer = ""
    if audit_verdict:
        audit_footer = (
            "<footer class=\"audit-footer\">"
            f"Audit verdict: <code>{_esc(audit_verdict)}</code> "
            f"as of <time datetime=\"{_esc(audit_ts)}\">{_esc(audit_ts)}</time>"
            " &middot; <a href=\"/methodology.html\">how this works</a>"
            "</footer>"
        )

    return (
        "<!doctype html>"
        "<html lang=\"en\"><head>"
        "<meta charset=\"utf-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
        "<title>labelwatch — what's observed on a Bluesky account?</title>"
        f"<style>{_HOMEPAGE_CSS}</style>"
        "</head><body>"
        "<header class=\"top\"><strong>labelwatch</strong>"
        " &middot; <a href=\"/methodology.html\">methodology</a>"
        " &middot; <a href=\"/about\">about</a>"
        "</header>"
        "<main>"
        "<h1>What's observed on a Bluesky account?</h1>"
        "<p class=\"lede\">"
        "Paste a handle or DID. See which labelers have emitted labels "
        "against that account, what kind of authority each label attempts "
        "to exercise, and how stable each labeler's emission shape has been."
        " <strong>Labelwatch publishes observations of testimony.</strong>"
        " We do not adjudicate subjects, validate labels, rank labelers, "
        "or produce a unified score."
        "</p>"
        f"{pause_banner}"
        "<form id=\"lookup-form\" class=\"lookup\" method=\"get\" "
        "action=\"/v1/frontdoor\">"
        "<label for=\"q\">Handle or DID:</label>"
        "<input id=\"q\" name=\"q\" type=\"text\" "
        "placeholder=\"alice.bsky.social or did:plc:…\" autofocus required/>"
        "<button type=\"submit\">Look up</button>"
        "</form>"
        "<details class=\"non-outputs\">"
        "<summary>What this page will never tell you</summary>"
        "<ul>"
        "<li>Whether the subject is 'good' or 'bad'</li>"
        "<li>Whether any label is 'true'</li>"
        "<li>A unified trust or risk score for any labeler</li>"
        "<li>Any moderation recommendation</li>"
        "</ul>"
        "<p>If you want methodology, that's now at <a href=\"/methodology.html\">/methodology.html</a>.</p>"
        "</details>"
        "</main>"
        f"{audit_footer}"
        "<script>"
        # Submit form via GET to /v1/frontdoor?q=…; server redirects to the
        # canonical /v1/frontdoor/{did} once resolved.
        "(function(){"
        "var f=document.getElementById('lookup-form');"
        "if(!f) return;"
        "f.addEventListener('submit', function(e){"
        "  var q=document.getElementById('q').value.trim();"
        "  if(!q){e.preventDefault();return;}"
        "});"
        "})();"
        "</script>"
        "</body></html>"
    )


# ---------------------------------------------------------------------------
# CSS — kept inline to avoid Caddy static-asset coordination for v0
# ---------------------------------------------------------------------------

_BASE_CSS = """
:root { color-scheme: light dark; --fg:#1a1a1a; --bg:#fafafa; --muted:#666; --border:#ddd; --accent:#205080; }
@media (prefers-color-scheme: dark) {
  :root { --fg:#e8e8e8; --bg:#111; --muted:#999; --border:#333; --accent:#9ab4cc; }
}
* { box-sizing: border-box; }
body { font: 16px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; color:var(--fg); background:var(--bg); margin:0; }
header.top { padding:14px 28px; border-bottom:1px solid var(--border); font-size:14px; }
header.top a { color:var(--accent); text-decoration:none; }
header.top a:hover { text-decoration:underline; }
main { max-width:880px; margin:0 auto; padding:32px 28px 96px; }
h1 { font-size:1.8rem; margin:0 0 .3em; }
h2 { font-size:1.3rem; margin-top:2em; }
p.lede { font-size:1.05rem; color:var(--muted); }
footer.audit-footer { max-width:880px; margin:48px auto 32px; padding:12px 28px; border-top:1px solid var(--border); font-size:.85rem; color:var(--muted); }
"""

_HOMEPAGE_CSS = _BASE_CSS + """
form.lookup { margin:28px 0; display:flex; flex-wrap:wrap; gap:8px; align-items:center; }
form.lookup label { font-weight:600; }
form.lookup input { flex:1 1 320px; padding:10px 12px; font-size:1rem; border:1px solid var(--border); border-radius:6px; background:transparent; color:var(--fg); }
form.lookup button { padding:10px 18px; font-size:1rem; border:1px solid var(--accent); background:var(--accent); color:white; border-radius:6px; cursor:pointer; }
details.non-outputs { margin-top:32px; border-top:1px dashed var(--border); padding-top:16px; }
details.non-outputs summary { cursor:pointer; font-weight:600; }
aside.banner.pause { margin:20px 0; padding:14px 16px; border-left:4px solid #b07700; background:rgba(176, 119, 0, 0.08); }
"""

_RESULT_CSS = _BASE_CSS + """
p.did { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:.85rem; color:var(--muted); word-break:break-all; }
p.observed-count { font-weight:600; }
p.disclaimer { font-size:.92rem; color:var(--muted); border-left:3px solid var(--border); padding:6px 12px; }
article.labeler-card { margin:24px 0; padding:20px; border:1px solid var(--border); border-radius:8px; background:rgba(0,0,0,0.015); }
article.labeler-card h3 { margin:0 0 4px; font-size:1.1rem; }
p.frame { font-size:.9rem; margin:6px 0; }
p.frame.use-for strong { color:var(--accent); }
p.frame.not-for strong { color:#a03030; }
p.sentence { font-size:1.05rem; margin:14px 0; font-weight:500; }
p.coherence-note { font-size:.85rem; color:var(--muted); border-left:3px solid #a07a30; padding:4px 12px; margin:8px 0; }
p.summary { font-size:.88rem; color:var(--muted); }
table { width:100%; border-collapse:collapse; margin:12px 0; font-size:.9rem; }
table th, table td { padding:6px 10px; border-bottom:1px solid var(--border); text-align:left; }
table caption { text-align:left; font-weight:600; padding:6px 0; }
details { margin-top:12px; }
details summary { cursor:pointer; color:var(--accent); }
section.refusal { padding:24px; border:1px solid var(--border); border-radius:8px; }
section.refusal h2 { margin-top:0; }
p.refusal-detail { font-family: ui-monospace, monospace; font-size:.85rem; color:var(--muted); }
"""
