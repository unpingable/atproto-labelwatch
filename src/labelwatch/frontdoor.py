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

from .label_family import (
    LABELER_DEFAULT_EFFECT,
    classify_authority_effect,
    normalize_family,
)
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
    "subject_too_dense",
)

# Defensive ceiling for high-volume subjects.
#
# Load probe history (gap-spec subject-lookup-sql-aggregation-001):
#   2026-06-10T071856Z  cap=∞       p99=24309ms  refused_unbounded
#   2026-06-10T074342Z  cap=10000   p99= 8720ms  refused_unbounded  68 gated
#                       cap= 2000   tightened after observing 8k events × 12
#                                   labelers took ~3.7s.
#   2026-06-11T154213Z  cap=1_000_000 (SQL aggregation Q8a/Q8b/Q8c)
#                                   p99=28315ms  refused_unbounded  0 gated
#                                   The SQL-side rewrite did NOT deliver the
#                                   wall-time win the spec assumed. Bottleneck
#                                   is per-row scan cost (idx walks 100k entries
#                                   for dense subjects), not Python-side
#                                   aggregation. Cap reverted to 2000 to keep
#                                   the surface safe.
#                       cap= 2000   restored after probe failure; awaiting new
#                                   gap-spec for the actual bottleneck (likely
#                                   pre-aggregated per-subject materialization
#                                   or cold-path migration).
#
# Tunable via env so the cap can be relaxed without redeploying.
MAX_EVENTS_FOR_AGGREGATION = int(
    os.environ.get("LABELWATCH_FRONTDOOR_MAX_EVENTS", "2000")
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
    # Attachment-locus epistemics: WHERE the label attached.
    # locus_counts: {locus_kind → event_count}. Locus kinds:
    #   account / profile / post / list / list_item / feed_generator / record / unknown
    locus_counts: dict[str, int] = field(default_factory=dict)
    # labeled_records: per-URI breakdown for non-account loci. Capped per
    # labeler to bound the result-page size; URI list is observational, not
    # adjudicative — same weather-not-verdict discipline.
    labeled_records: list[dict] = field(default_factory=list)
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

# Q8 was a per-row fetch + Python aggregation; for dense subjects this is
# O(events) walked twice. Replaced 2026-06-11 with three SQL-aggregated
# queries per gap-spec subject-lookup-sql-aggregation-001:
#
#   Q8a — distinct (val,neg) states per labeler  →  classification_changed
#   Q8b — locus bucket counts per labeler        →  locus_counts dict
#   Q8c — top-50 labeled-record URIs per labeler →  labeled_records list
#
# Each query SEARCHes via idx_label_events_target_did_ts; result sets are
# bounded by labeler count (Q8a/Q8b) or by per-labeler top-N (Q8c).

_Q8A_DISTINCT_STATES = (
    "SELECT labeler_did, COUNT(DISTINCT val || '|' || neg) AS distinct_states "
    "FROM label_events "
    "WHERE target_did = ? "
    "GROUP BY labeler_did"
)

# Locus buckets match attachment_locus() output exactly. Each row gets
# exactly one bucket (CASE branches are mutually exclusive and exhaustive).
_Q8B_LOCUS = (
    "SELECT labeler_did, "
    "CASE "
    "WHEN uri LIKE 'did:%' THEN 'account' "
    "WHEN uri LIKE 'at://%/app.bsky.feed.post/%' THEN 'post' "
    "WHEN uri LIKE 'at://%/app.bsky.actor.profile/%' THEN 'profile' "
    "WHEN uri LIKE 'at://%/app.bsky.graph.list/%' THEN 'list' "
    "WHEN uri LIKE 'at://%/app.bsky.graph.listitem/%' THEN 'list_item' "
    "WHEN uri LIKE 'at://%/app.bsky.feed.generator/%' THEN 'feed_generator' "
    "WHEN uri LIKE 'at://%/app.bsky.graph.starterpack/%' THEN 'starterpack' "
    "WHEN uri LIKE 'at://%' THEN 'record' "
    "ELSE 'unknown' "
    "END AS locus, "
    "COUNT(*) AS event_count "
    "FROM label_events "
    "WHERE target_did = ? "
    "GROUP BY labeler_did, locus"
)

# Q8c — per-labeler top-50 URIs (non-account) with val breakdown.
# Window function bounds per-labeler cardinality so dense subjects don't
# stream thousands of rows back. Result: at most ~50 URIs × few vals
# × num_labelers per subject.
_Q8C_LABELED_RECORDS = (
    "WITH per_uri_val AS ( "
    "  SELECT labeler_did, uri, val, "
    "         COUNT(*) AS val_count, "
    "         MIN(ts) AS first_seen, "
    "         MAX(ts) AS last_seen "
    "  FROM label_events "
    "  WHERE target_did = ? AND uri NOT LIKE 'did:%' "
    "  GROUP BY labeler_did, uri, val "
    "), "
    "uri_totals AS ( "
    "  SELECT labeler_did, uri, SUM(val_count) AS uri_total "
    "  FROM per_uri_val "
    "  GROUP BY labeler_did, uri "
    "), "
    "ranked AS ( "
    "  SELECT labeler_did, uri, uri_total, "
    "         ROW_NUMBER() OVER ("
    "           PARTITION BY labeler_did ORDER BY uri_total DESC, uri"
    "         ) AS rn "
    "  FROM uri_totals "
    ") "
    "SELECT puv.labeler_did, puv.uri, puv.val, puv.val_count, "
    "       puv.first_seen, puv.last_seen, r.uri_total "
    "FROM per_uri_val puv "
    "JOIN ranked r ON r.labeler_did = puv.labeler_did AND r.uri = puv.uri "
    "WHERE r.rn <= ? "
    "ORDER BY puv.labeler_did, r.rn, puv.val"
)


# Cap on per-labeler labeled-records exposed in the result. The full set is
# always reflected in locus_counts; the per-URI list is a UI affordance for
# auditors, not a complete dump.
MAX_LABELED_RECORDS_PER_LABELER = 50


# ATProto record collections we name explicitly. Anything else under an
# at:// URI lands in "record"; non-at:// URIs that are also not bare DIDs
# fall to "unknown".
_LEXICON_LOCUS_MAP = {
    "app.bsky.actor.profile": "profile",
    "app.bsky.feed.post": "post",
    "app.bsky.feed.generator": "feed_generator",
    "app.bsky.graph.list": "list",
    "app.bsky.graph.listitem": "list_item",
    "app.bsky.graph.starterpack": "starterpack",
}


def attachment_locus(uri: Optional[str], target_did: Optional[str]) -> str:
    """Classify where a label attached.

    Returns one of:
        account / profile / post / list / list_item / feed_generator /
        starterpack / record / unknown

    The ATProto label model has `subject.uri` either as a DID (account-level
    label) or an `at://did/collection/rkey` URI (record-level). For account
    labels the URI is the DID. For record labels the URI is the AT-URI and
    the *account* it touches is target_did (extracted by parse_target_did).

    No external state; no network. Pure string classification.
    """
    if not uri:
        return "unknown"
    # Account-level labels: uri == DID (or target_did when explicitly set).
    if uri.startswith("did:"):
        return "account"
    if target_did and uri == target_did:
        return "account"
    if not uri.startswith("at://"):
        return "unknown"
    # at://did/{collection}/{rkey}
    # Splitting on "/" with maxsplit=4 gives: ['at:', '', 'did:plc:xxx', collection, rkey]
    parts = uri.split("/", 4)
    if len(parts) < 4:
        return "record"
    collection = parts[3]
    return _LEXICON_LOCUS_MAP.get(collection, "record")


# Classification-flip detection: SQL Q8a returns COUNT(DISTINCT val||'|'||neg)
# per labeler. distinct_states > 1 means the labeler has emitted at least two
# distinct (val, neg) tuples against the subject — observable shift in testimony.
# Pure-function helper retained for unit testing / readability.
def _classification_changed_from_distinct(distinct_states: int) -> bool:
    return distinct_states > 1


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
    distinct_states: int,               # Q8a: COUNT(DISTINCT val||'|'||neg) for this labeler
    locus_counts: dict[str, int],       # Q8b: locus bucket → event_count for this labeler
    labeled_records: list[dict],        # Q8c: top-N URI rollup for this labeler (already capped + sorted)
) -> LabelerCard:
    label_values: list[dict] = []
    authority_effects: dict[str, int] = {}
    event_count_total = 0
    last_seen: Optional[str] = None

    # Labeler-context fallback for unknown effects: if the family-level map
    # returns "unknown" AND this labeler has a LABELER_DEFAULT_EFFECT entry,
    # use the labeler default. The label-level mapping always wins; this
    # only resolves the unknown case. See LABELER_DEFAULT_EFFECT comment in
    # label_family.py for the tension and antisubstack/oracle examples.
    labeler_default = LABELER_DEFAULT_EFFECT.get(labeler_did)

    for r in rows_for_labeler:
        family = normalize_family(r["val"])
        effect = classify_authority_effect(family)
        if effect == "unknown" and labeler_default:
            effect = labeler_default
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
    changed = _classification_changed_from_distinct(distinct_states)

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
        locus_counts=locus_counts,
        labeled_records=labeled_records,
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

    # Step 2.5: density circuit breaker. Pre-count Q8 rows for the subject
    # using the same target_did index. If above the cap, refuse cleanly
    # rather than do O(N) Python aggregation. See MAX_EVENTS_FOR_AGGREGATION.
    try:
        event_count_row = conn.execute(
            "SELECT COUNT(*) AS c FROM label_events WHERE target_did = ?",
            (did,),
        ).fetchone()
        event_count = int(event_count_row["c"]) if event_count_row else 0
    except sqlite3.Error:
        event_count = 0
    if event_count > MAX_EVENTS_FOR_AGGREGATION:
        return FrontdoorResult(
            surface=SURFACE,
            consumer_surface_version="v0",
            generated_at=generated_at,
            input_identifier=identifier,
            subject_did=did,
            subject_handle=handle,
            labelers=[],
            refusal="subject_too_dense",
            refusal_detail=(
                f"{event_count:,} label events against this subject exceed the "
                f"per-page aggregation cap ({MAX_EVENTS_FOR_AGGREGATION:,}). "
                f"The lookup surface refuses subjects above the cap until "
                f"SQL-side aggregation lands."
            ),
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

    # Step 4: Q8a — distinct (val, neg) states per labeler (classification flips).
    distinct_states_by_labeler: dict[str, int] = {
        r["labeler_did"]: int(r["distinct_states"])
        for r in conn.execute(_Q8A_DISTINCT_STATES, (did,)).fetchall()
    }

    # Step 5: Q8b — locus bucket counts per labeler (account/post/profile/...).
    locus_by_labeler: dict[str, dict[str, int]] = {}
    for r in conn.execute(_Q8B_LOCUS, (did,)).fetchall():
        locus_by_labeler.setdefault(r["labeler_did"], {})[r["locus"]] = int(r["event_count"])

    # Step 6: Q8c — top-N URI rollup per labeler (non-account loci, val breakdown).
    # The window function caps per-labeler URIs at MAX_LABELED_RECORDS_PER_LABELER;
    # the same labeler/uri may appear multiple times (one row per val) — we
    # assemble entries client-side.
    records_by_labeler: dict[str, dict[str, dict]] = {}  # labeler -> uri -> entry
    for r in conn.execute(
        _Q8C_LABELED_RECORDS, (did, MAX_LABELED_RECORDS_PER_LABELER)
    ).fetchall():
        labeler = r["labeler_did"]
        uri = r["uri"]
        per_uri = records_by_labeler.setdefault(labeler, {})
        entry = per_uri.get(uri)
        if entry is None:
            entry = {
                "uri": uri,
                "locus": attachment_locus(uri, did),
                "count": 0,
                "vals": {},
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
            }
            per_uri[uri] = entry
        vc = int(r["val_count"])
        entry["count"] += vc
        entry["vals"][r["val"]] = entry["vals"].get(r["val"], 0) + vc
        if r["first_seen"] and r["first_seen"] < entry["first_seen"]:
            entry["first_seen"] = r["first_seen"]
        if r["last_seen"] and r["last_seen"] > entry["last_seen"]:
            entry["last_seen"] = r["last_seen"]

    # Step 7: Q6 — per-labeler profile (one round-trip per labeler, indexed PK).
    cards: list[LabelerCard] = []
    for labeler_did, rows_for_labeler in by_labeler.items():
        profile_row = conn.execute(_Q6_LABELER_PROFILE, (labeler_did,)).fetchone()
        profile = dict(profile_row) if profile_row is not None else None
        distinct_states = distinct_states_by_labeler.get(labeler_did, 0)
        locus_counts = locus_by_labeler.get(labeler_did, {})
        records_dict = records_by_labeler.get(labeler_did, {})
        labeled_records = sorted(
            records_dict.values(),
            key=lambda e: (-e["count"], e["uri"]),
        )
        # Stable vals list inside each record entry.
        for entry in labeled_records:
            entry["vals"] = sorted(entry["vals"].items(), key=lambda kv: (-kv[1], kv[0]))
        card = _build_labeler_card(
            labeler_did=labeler_did,
            rows_for_labeler=rows_for_labeler,
            profile_row=profile,
            distinct_states=distinct_states,
            locus_counts=locus_counts,
            labeled_records=labeled_records,
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
# Network weather (small dimension-table summary)
# ---------------------------------------------------------------------------
#
# These queries do NOT touch label_events directly. They scan the small
# `labelers` table (~500 rows) and the small `alerts` table (filtered by
# rule_id + ts via idx_alerts_rule_ts). They are NOT part of the
# labelwatch.index_audit.v1 inventory, which gates the per-subject lookup
# path over label_events. Same detection rule as in chatty's spec: small
# dimension/config tables can scan without panic.

def network_weather(
    conn: sqlite3.Connection,
    *,
    now: Optional[datetime] = None,
) -> dict:
    """Compute the lookup-page network weather strip.

    Returns a dict with:
        total_labelers
        emitting_this_week
        events_7d_total
        unreachable
        signals          # list of weather words: "noisy" / "churny" / "degraded" / "calm"
        attribution      # one-line "what triggered each signal"
        computed_at      # ISO ts
    """
    if now is None:
        now = now_utc()

    total = conn.execute("SELECT COUNT(*) AS c FROM labelers").fetchone()["c"] or 0
    emitting = conn.execute(
        "SELECT COUNT(*) AS c FROM labelers WHERE events_7d > 0"
    ).fetchone()["c"] or 0
    events_7d_total = conn.execute(
        "SELECT COALESCE(SUM(events_7d), 0) AS s FROM labelers"
    ).fetchone()["s"] or 0
    unreachable = conn.execute(
        "SELECT COUNT(*) AS c FROM labelers WHERE endpoint_status = 'down' AND events_30d > 0"
    ).fetchone()["c"] or 0

    # Alert-side counts for the tagline (matches report.py:2880+ semantics).
    since_24h = format_ts(now - _timedelta_24h())
    try:
        spike_24h = conn.execute(
            "SELECT COUNT(*) AS c FROM alerts WHERE rule_id = 'label_rate_spike' AND ts > ?",
            (since_24h,),
        ).fetchone()["c"] or 0
    except sqlite3.Error:
        spike_24h = 0
    try:
        churn_24h = conn.execute(
            "SELECT COUNT(*) AS c FROM alerts WHERE rule_id = 'churn_index' AND ts > ?",
            (since_24h,),
        ).fetchone()["c"] or 0
    except sqlite3.Error:
        churn_24h = 0

    signals: list[str] = []
    attribution: list[str] = []
    if spike_24h > 10:
        signals.append("noisy")
        attribution.append(f"{spike_24h} rate-spike alerts (24h)")
    if churn_24h > 50:
        signals.append("churny")
        attribution.append(f"{churn_24h} churn alerts (24h)")
    if unreachable > 5:
        signals.append("degraded")
        attribution.append(f"{unreachable} labelers unreachable")
    if not signals:
        signals.append("calm")
        attribution.append("no triggers crossed")

    return {
        "total_labelers": int(total),
        "emitting_this_week": int(emitting),
        "events_7d_total": int(events_7d_total),
        "unreachable": int(unreachable),
        "signals": signals,
        "attribution": " · ".join(attribution),
        "computed_at": format_ts(now),
    }


def _timedelta_24h():
    from datetime import timedelta
    return timedelta(hours=24)


def _format_events_compact(n: int) -> str:
    """Format big counts compactly: 2400000 → '2.4M', 12500 → '12.5K'."""
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _render_weather_strip_html(weather: Optional[dict]) -> str:
    """Compact one-line strip linking to /methodology.html."""
    if not weather:
        return ""
    signals_str = ", ".join(weather["signals"]) or "calm"
    return (
        "<aside class=\"weather-strip\">"
        f"<p class=\"weather-line\">"
        f"<span class=\"weather-label\">Network weather:</span> "
        f"<strong>{_esc(signals_str)}</strong>"
        "</p>"
        f"<p class=\"weather-counts\">"
        f"{_format_count(weather['total_labelers'])} labelers"
        f" &middot; {_format_count(weather['emitting_this_week'])} emitting this week"
        f" &middot; {_format_events_compact(weather['events_7d_total'])} events in 7d"
        f" &middot; {_format_count(weather['unreachable'])} unreachable"
        "</p>"
        f"<p class=\"weather-link\">"
        f"<a href=\"/methodology.html\">Open system dashboard &amp; graphs &rarr;</a>"
        "</p>"
        "</aside>"
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


# Display labels for the locus_kind values produced by attachment_locus().
_LOCUS_LABELS = {
    "account": "account-level",
    "profile": "profile record",
    "post": "post",
    "list": "list",
    "list_item": "list-item",
    "feed_generator": "feed generator",
    "starterpack": "starter pack",
    "record": "record",
    "unknown": "unknown locus",
}


def _render_locus_summary_html(locus_counts: dict[str, int]) -> str:
    """Compact line: 'Where attached: account 8 · post 51 · profile 1'.

    Empty when no events observed (locus_counts empty)."""
    if not locus_counts:
        return ""
    total = sum(locus_counts.values())
    if total == 0:
        return ""
    parts = sorted(locus_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    chips = " &middot; ".join(
        f"<span class=\"locus-chip\">"
        f"{_esc(_LOCUS_LABELS.get(k, k))} <b>{_format_count(v)}</b>"
        f"</span>"
        for k, v in parts
    )
    return f"<p class=\"locus\"><strong>Where attached:</strong> {chips}</p>"


# ---------------------------------------------------------------------------
# Bluesky URL helpers — clickable witness/target affordances.
#
# Acceptance (chatty 2026-06-10 "custody triangle"):
#   - labeler handle  → https://bsky.app/profile/<labeler_did>      (witness)
#   - post target     → https://bsky.app/profile/<did>/post/<rkey>  (what)
#   - subject account → https://bsky.app/profile/<did>              (subject)
# Unknown record types stay raw-text-only; no link is invented. No post text
# is fetched — clickable link only.
# ---------------------------------------------------------------------------

_BSKY_PROFILE_BASE = "https://bsky.app/profile/"


def bsky_profile_url(did: str) -> Optional[str]:
    """Build a profile URL for any actor DID (subject or labeler)."""
    if not did or not did.startswith("did:"):
        return None
    return f"{_BSKY_PROFILE_BASE}{did}"


def bsky_post_url(at_uri: str) -> Optional[str]:
    """Build a bsky.app post URL from an at:// URI, or None for non-post URIs.

    Handles `at://did:.../app.bsky.feed.post/<rkey>` only. Other record types
    return None — the renderer falls back to raw URI display."""
    if not at_uri or not at_uri.startswith("at://"):
        return None
    parts = at_uri.split("/", 4)
    if len(parts) < 5:
        return None
    _, _, did, collection, rkey = parts
    if collection != "app.bsky.feed.post":
        return None
    if not did.startswith("did:"):
        return None
    return f"{_BSKY_PROFILE_BASE}{did}/post/{rkey}"


def _ext_link(url: str, text: str, *, css_class: str = "") -> str:
    """Render an external link with rel=noopener noreferrer + target=_blank."""
    cls = f' class="{css_class}"' if css_class else ""
    return (
        f'<a href="{_esc(url)}"{cls} target="_blank" '
        f'rel="noopener noreferrer">{_esc(text)}</a>'
    )


def _truncate_uri(uri: str, max_len: int = 80) -> str:
    """Truncate an at:// URI for table display, keeping the collection + rkey suffix."""
    if not uri or len(uri) <= max_len:
        return uri
    # Show "at://did:plc:XXXX…/collection/rkey" — keep the suffix
    if uri.startswith("at://"):
        parts = uri.split("/", 4)
        if len(parts) == 5:
            head = f"{parts[0]}//{parts[2][:20]}…"
            tail = f"{parts[3]}/{parts[4]}"
            shrunk = f"{head}/{tail}"
            if len(shrunk) <= max_len:
                return shrunk
            return shrunk[: max_len - 1] + "…"
    return uri[: max_len - 1] + "…"


def _render_labeled_records_html(labeled_records: list[dict], total_non_account: int) -> str:
    """Expandable per-URI table for non-account labels.

    Capped at MAX_LABELED_RECORDS_PER_LABELER; surfaces "and N more" when capped.
    Post URIs render a "View post ↗" link to bsky.app; other record types stay
    as raw at:// text. No post text is fetched."""
    if not labeled_records:
        return ""
    rows: list[str] = []
    for entry in labeled_records:
        vals_str = ", ".join(
            f"{_esc(v)} ({c})" for v, c in entry["vals"][:5]
        )
        if len(entry["vals"]) > 5:
            vals_str += f", +{len(entry['vals']) - 5} more"

        # Target column: link for post URIs, raw text + truncation otherwise.
        post_url = bsky_post_url(entry["uri"])
        if post_url:
            link_html = _ext_link(post_url, "View post ↗", css_class="post-link")
            target_html = (
                f"{link_html}"
                f"<br><code class=\"raw-uri\">{_esc(_truncate_uri(entry['uri']))}</code>"
            )
        else:
            target_html = f"<code class=\"raw-uri\">{_esc(_truncate_uri(entry['uri']))}</code>"

        rows.append(
            "<tr>"
            f"<td>{target_html}</td>"
            f"<td>{_esc(_LOCUS_LABELS.get(entry['locus'], entry['locus']))}</td>"
            f"<td>{_format_count(entry['count'])}</td>"
            f"<td>{vals_str}</td>"
            f"<td>{_esc(entry['first_seen'])}</td>"
            f"<td>{_esc(entry['last_seen'])}</td>"
            "</tr>"
        )
    capped_note = ""
    shown = sum(e["count"] for e in labeled_records)
    if total_non_account > shown:
        capped_note = (
            f"<p class=\"capped-note\">Showing top {len(labeled_records)} "
            f"labeled records ({_format_count(shown)} events). "
            f"{_format_count(total_non_account - shown)} additional non-account "
            f"events not listed.</p>"
        )
    return (
        "<details><summary>Show labeled records (non-account)</summary>"
        f"{capped_note}"
        "<table class=\"records\">"
        "<thead><tr>"
        "<th>target</th><th>locus</th><th>events</th>"
        "<th>vals</th><th>first seen</th><th>last seen</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></details>"
    )


def render_labeler_card_html(card: LabelerCard) -> str:
    """Render one per-labeler card.

    Order: per-labeler header (clickable witness) → plain-language sentence
    → attachment locus → authority breakdown → expandable raw tables. The
    page-level Use/Not framing lives once at the top of the page, not per
    card (chatty 2026-06-10).
    """
    handle_display = card.handle or "(handle not resolved)"
    labeler_profile_url = bsky_profile_url(card.labeler_did)
    if labeler_profile_url:
        handle_html = _ext_link(
            labeler_profile_url, handle_display, css_class="labeler-handle"
        )
        view_profile_link = (
            " " + _ext_link(
                labeler_profile_url, "View profile ↗",
                css_class="labeler-profile-link",
            )
        )
    else:
        handle_html = _esc(handle_display)
        view_profile_link = ""

    classification_note = ""
    if card.classification_changed:
        classification_note = (
            "<p class=\"coherence-note\">"
            "Labeler's classification of this subject has changed at least once "
            "in the observed window. Testimony shifted; the labeler did not "
            "necessarily err."
            "</p>"
        )

    # locus_counts may include "account" plus various record-level loci.
    non_account_total = sum(
        v for k, v in card.locus_counts.items() if k != "account"
    )

    return (
        "<article class=\"labeler-card\">"
        "<header>"
        f"<h3>{handle_html}{view_profile_link}</h3>"
        f"<p class=\"did\">{_esc(card.labeler_did)}</p>"
        "</header>"
        f"<p class=\"sentence\">{_esc(card.plain_language_sentence)}</p>"
        f"{classification_note}"
        f"<p class=\"summary\">"
        f"Observed events against subject: {_format_count(card.event_count)}"
        f" &middot; Last seen: {_esc(card.last_seen or '—')}"
        f" &middot; Labeler activity 7d/30d: {_format_count(card.events_7d)}/{_format_count(card.events_30d)}"
        "</p>"
        f"{_render_locus_summary_html(card.locus_counts)}"
        f"{_render_authority_breakdown_html(card.authority_effects)}"
        f"{_render_labeled_records_html(card.labeled_records, non_account_total)}"
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
    "subject_too_dense": (
        "Subject too dense for the v0 surface",
        "This subject has more labels than the lookup surface can currently "
        "aggregate per request. The shape audit verdicts the index path as "
        "admissible, but the Python-side aggregation is O(events). Until "
        "SQL-side aggregation lands, the surface refuses cleanly rather than "
        "render slowly. The labels are still observed and in the DB; just "
        "not flattened into a card view here yet.",
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


_GLOBAL_USE_NOT_BLOCK = (
    "<aside class=\"use-not\">"
    "<p><strong>Use:</strong> observed labels attached to this account or to "
    "records authored by it, plus the authority effect Labelwatch assigns to "
    "those labels.</p>"
    "<p><strong>Not:</strong> truth, ranking, or recommended action. "
    "We publish observations of testimony, not adjudication.</p>"
    "</aside>"
)


def _page_level_locus_rollup(labelers: list[LabelerCard]) -> dict[str, int]:
    """Sum locus_counts across all labelers touching the subject. Used to
    surface the actual mix (account vs post vs record vs ...) at page level,
    not just per-card. Empty when no events observed."""
    total: dict[str, int] = {}
    for card in labelers:
        for k, v in card.locus_counts.items():
            total[k] = total.get(k, 0) + v
    return total


def _render_page_locus_rollup_html(rollup: dict[str, int]) -> str:
    """Page-level 'Where attached, across all labelers' line. Mirrors the
    per-card chip strip; same vocabulary, same order. Empty when no events."""
    if not rollup or sum(rollup.values()) == 0:
        return ""
    parts = sorted(rollup.items(), key=lambda kv: (-kv[1], kv[0]))
    chips = " &middot; ".join(
        f"<span class=\"locus-chip\">"
        f"{_esc(_LOCUS_LABELS.get(k, k))} <b>{_format_count(v)}</b>"
        f"</span>"
        for k, v in parts
    )
    return (
        "<p class=\"locus page-locus\">"
        "<strong>Where attached (all labelers):</strong> "
        f"{chips}</p>"
    )


def render_result_body_html(result: FrontdoorResult) -> str:
    """Render the inner body of the result page. Caller wraps in layout."""
    if result.refusal:
        return render_refusal_html(result)

    subject_handle = result.subject_handle or "(handle unresolved)"
    cards_html = "".join(render_labeler_card_html(c) for c in result.labelers)

    # Subject header: handle clickable to bsky profile if DID available.
    subject_profile_url = bsky_profile_url(result.subject_did or "")
    if subject_profile_url:
        subject_header = _ext_link(
            subject_profile_url, subject_handle, css_class="subject-handle"
        )
        view_subject_link = (
            " " + _ext_link(
                subject_profile_url, "View profile ↗",
                css_class="subject-profile-link",
            )
        )
    else:
        subject_header = _esc(subject_handle)
        view_subject_link = ""

    # Locus-honest framing: "against this account" implied account-level
    # accusation when most labels are post-level. Heading + subtitle make
    # the scope explicit (chatty 2026-06-10): labels touching the account
    # OR records authored by it.
    rollup = _page_level_locus_rollup(result.labelers)
    rollup_html = _render_page_locus_rollup_html(rollup)
    total_events = sum(rollup.values()) if rollup else 0

    return (
        "<section class=\"subject\">"
        f"<h2>Observed labels touching {subject_header}{view_subject_link}</h2>"
        f"<p class=\"did\">{_esc(result.subject_did)}</p>"
        "<p class=\"scope-note\">"
        "Includes labels attached directly to the account and labels attached "
        "to posts or records authored by it."
        "</p>"
        f"<p class=\"observed-count\">"
        f"{len(result.labelers)} labelers &middot; "
        f"{_format_count(total_events)} events observed."
        "</p>"
        f"{rollup_html}"
        f"{_GLOBAL_USE_NOT_BLOCK}"
        f"{cards_html}"
        "</section>"
    )


# Public site URL — used for og:url + canonical. Override via env in test/dev.
_SITE_URL = os.environ.get("LABELWATCH_SITE_URL", "https://labelwatch.neutral.zone")

# Homepage social-card copy. Keep this sharp: the card is doing first-contact
# duty whether it deserves the job or not.
_HOMEPAGE_TITLE = "Labelwatch — observable labeler activity on Bluesky"
_HOMEPAGE_DESCRIPTION = (
    "Labelers are testimony. Labelwatch shows what is observed, what is "
    "bounded, and where testimony becomes consequence."
)


def _render_social_meta(
    title: str,
    description: str,
    *,
    canonical: Optional[str] = None,
    og_type: str = "website",
) -> str:
    """OG + Twitter card meta + canonical link. Used by frontdoor's hand-rolled
    head templates (homepage, result page) which do not go through report._layout.
    """
    canonical_tag = (
        f"<link rel=\"canonical\" href=\"{_esc(canonical)}\"/>"
        if canonical else ""
    )
    og_url_tag = (
        f"<meta property=\"og:url\" content=\"{_esc(canonical)}\"/>"
        if canonical else ""
    )
    return (
        f"<meta name=\"description\" content=\"{_esc(description)}\"/>"
        f"<meta property=\"og:title\" content=\"{_esc(title)}\"/>"
        f"<meta property=\"og:description\" content=\"{_esc(description)}\"/>"
        f"<meta property=\"og:type\" content=\"{_esc(og_type)}\"/>"
        f"{og_url_tag}"
        f"<meta property=\"og:site_name\" content=\"Labelwatch\"/>"
        f"<meta name=\"twitter:card\" content=\"summary\"/>"
        f"<meta name=\"twitter:title\" content=\"{_esc(title)}\"/>"
        f"<meta name=\"twitter:description\" content=\"{_esc(description)}\"/>"
        f"{canonical_tag}"
    )


def render_result_page_html(
    result: FrontdoorResult,
    *,
    weather: Optional[dict] = None,
) -> str:
    """Full HTML page (standalone; safe to serve directly)."""
    title = (
        f"Observed labels touching {result.subject_handle or result.subject_did or 'subject'} — labelwatch"
        if not result.refusal
        else f"Lookup — {result.refusal} — labelwatch"
    )
    body = render_result_body_html(result)

    # CTA back to the system view, near the bottom of the result.
    cta_block = (
        "<aside class=\"system-cta\">"
        "<p><strong>This is the subject view.</strong> "
        "For the whole labeling network — network weather, authority-effect "
        "graphs, concentration, hosting, contradictions, alerts, registry — "
        "see the <a href=\"/methodology.html\">system dashboard &amp; graphs</a>."
        "</p>"
        "</aside>"
    )

    weather_strip = _render_weather_strip_html(weather) if weather else ""

    audit_footer = ""
    if result.audit_verdict:
        audit_footer = (
            "<footer class=\"audit-footer\">"
            f"Audit verdict: <code>{_esc(result.audit_verdict)}</code> "
            f"as of <time datetime=\"{_esc(result.audit_generated_at)}\">"
            f"{_esc(result.audit_generated_at)}</time>"
            " &middot; <a href=\"/methodology.html\">system dashboard &amp; graphs</a>"
            "</footer>"
        )
    # Social card description for a subject view — page is per-subject, so the
    # description focuses on the action and the doctrinal frame, not a specific
    # subject. Keeps the card honest when shared in a thread.
    result_description = (
        "Observed labeler testimony attached to this Bluesky account and its "
        "posts. Labelwatch publishes observations, not verdicts."
    )
    social_meta = _render_social_meta(
        title=title,
        description=result_description,
        og_type="article",
    )
    return (
        "<!doctype html>"
        "<html lang=\"en\"><head>"
        "<meta charset=\"utf-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
        f"<title>{_esc(title)}</title>"
        f"{social_meta}"
        f"<style>{_RESULT_CSS}</style>"
        f"{_THEME_SYNC_JS}"
        "</head><body>"
        "<header class=\"top\">"
        "<a class=\"home-link\" href=\"/\"><strong>labelwatch</strong></a>"
        " &middot; "
        "<a class=\"dashboard-link\" href=\"/methodology.html\">system dashboard &amp; graphs</a>"
        " &middot; "
        "<a href=\"/about\">about</a>"
        "</header>"
        f"<main>{weather_strip}{body}{cta_block}</main>"
        f"{audit_footer}"
        "</body></html>"
    )


def render_homepage_html(
    audit_receipt: Optional[dict] = None,
    *,
    weather: Optional[dict] = None,
) -> str:
    """Lookup-first homepage. The system dashboard (with graphs) lives at /methodology.html."""
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

    weather_strip = _render_weather_strip_html(weather) if weather else ""

    # "Want the system view?" block — chatty 2026-06-10 CTA for the relocated
    # graph/dashboard surface. Methodology page is the system dashboard;
    # nav label is renamed for accuracy.
    system_view_cta = (
        "<aside class=\"system-cta\">"
        "<h2>Want the system view?</h2>"
        "<p>"
        "Network weather, authority-effect graphs, concentration, hosting, "
        "contradictions, alerts, and the labeler registry."
        "</p>"
        "<p><a class=\"button-link\" href=\"/methodology.html\">"
        "Open system dashboard &amp; graphs &rarr;"
        "</a></p>"
        "</aside>"
    )

    audit_footer = ""
    if audit_verdict:
        audit_footer = (
            "<footer class=\"audit-footer\">"
            f"Audit verdict: <code>{_esc(audit_verdict)}</code> "
            f"as of <time datetime=\"{_esc(audit_ts)}\">{_esc(audit_ts)}</time>"
            " &middot; <a href=\"/methodology.html\">system dashboard &amp; graphs</a>"
            "</footer>"
        )

    social_meta = _render_social_meta(
        title=_HOMEPAGE_TITLE,
        description=_HOMEPAGE_DESCRIPTION,
        canonical=_SITE_URL + "/",
    )
    return (
        "<!doctype html>"
        "<html lang=\"en\"><head>"
        "<meta charset=\"utf-8\"/>"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>"
        f"<title>{_esc(_HOMEPAGE_TITLE)}</title>"
        f"{social_meta}"
        f"<style>{_HOMEPAGE_CSS}</style>"
        f"{_THEME_SYNC_JS}"
        "</head><body>"
        "<header class=\"top\"><strong>labelwatch</strong>"
        " &middot; <a class=\"dashboard-link\" href=\"/methodology.html\">"
        "system dashboard &amp; graphs</a>"
        " &middot; <a href=\"/about\">about</a>"
        "</header>"
        "<main>"
        "<h1>What's observed on a Bluesky account or its posts?</h1>"
        "<p class=\"lede\">"
        "Paste a handle or DID. See labeler testimony attached to that "
        "account and to records authored by it &mdash; account-level labels, "
        "profile labels, and post-level labels &mdash; what kind of authority "
        "each label attempts, and how stable each labeler's emission shape "
        "has been."
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
        f"{weather_strip}"
        f"{system_view_cta}"
        "<details class=\"non-outputs\">"
        "<summary>What this page will never tell you</summary>"
        "<ul>"
        "<li>Whether the subject is 'good' or 'bad'</li>"
        "<li>Whether any label is 'true'</li>"
        "<li>A unified trust or risk score for any labeler</li>"
        "<li>Any moderation recommendation</li>"
        "</ul>"
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
/* Token palette deliberately matches report.py STYLE / [data-theme="dark"]
   so frontdoor and the system-dashboard page render with the same colors
   in both modes. Frontdoor uses prefers-color-scheme; methodology uses
   data-theme="dark" set by JS that ALSO honors prefers-color-scheme on
   first load, so both surfaces flip together. */
:root {
  color-scheme: light dark;
  --fg:#111; --bg:#fff; --muted:#666; --border:#ddd; --accent:#0b5394;
  --bg-muted:#f6f7f9;
}
@media (prefers-color-scheme: dark) {
  :root {
    --fg:#e0e0e0; --bg:#1a1a2e; --muted:#999; --border:#333; --accent:#6db3f2;
    --bg-muted:#16213e;
  }
}
/* Honor explicit data-theme override (set by the methodology theme toggle JS
   when the user clicks Light/Dark on the system-dashboard page). Keeps the
   manual toggle's state consistent when navigating to the frontdoor. */
[data-theme="dark"] {
  --fg:#e0e0e0; --bg:#1a1a2e; --muted:#999; --border:#333; --accent:#6db3f2;
  --bg-muted:#16213e;
}
[data-theme="light"] {
  --fg:#111; --bg:#fff; --muted:#666; --border:#ddd; --accent:#0b5394;
  --bg-muted:#f6f7f9;
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

# Tiny inline JS shared by homepage + result page. Reads the same `lw-theme`
# localStorage key the methodology page uses, so a user who toggles Dark on
# the system dashboard sees the frontdoor honor that choice on navigation.
_THEME_SYNC_JS = (
    "<script>"
    "(function(){"
    "var s=localStorage.getItem('lw-theme');"
    "if(s){document.documentElement.setAttribute('data-theme',s);}"
    "})();"
    "</script>"
)

_SHARED_COMPONENTS_CSS = """
aside.weather-strip { margin:20px 0; padding:14px 16px; border:1px solid var(--border); border-radius:8px; background:rgba(0,0,0,0.02); }
@media (prefers-color-scheme: dark) { aside.weather-strip { background:rgba(255,255,255,0.03); } }
aside.weather-strip p { margin:4px 0; }
aside.weather-strip .weather-label { color:var(--muted); }
aside.weather-strip .weather-counts { font-size:.92rem; color:var(--muted); }
aside.weather-strip .weather-link a { color:var(--accent); text-decoration:none; font-size:.92rem; }
aside.weather-strip .weather-link a:hover { text-decoration:underline; }
aside.system-cta { margin:32px 0; padding:20px 22px; border:1px solid var(--border); border-radius:8px; background:linear-gradient(180deg, rgba(32,80,128,0.04), transparent); }
@media (prefers-color-scheme: dark) { aside.system-cta { background:linear-gradient(180deg, rgba(154,180,204,0.06), transparent); } }
aside.system-cta h2 { margin:0 0 6px; font-size:1.05rem; }
aside.system-cta p { margin:6px 0; }
a.button-link { display:inline-block; padding:9px 16px; background:var(--accent); color:white; text-decoration:none; border-radius:6px; font-weight:600; }
a.button-link:hover { filter:brightness(1.1); text-decoration:none; }
"""

_HOMEPAGE_CSS = _BASE_CSS + _SHARED_COMPONENTS_CSS + """
form.lookup { margin:28px 0; display:flex; flex-wrap:wrap; gap:8px; align-items:center; }
form.lookup label { font-weight:600; }
form.lookup input { flex:1 1 320px; padding:10px 12px; font-size:1rem; border:1px solid var(--border); border-radius:6px; background:transparent; color:var(--fg); }
form.lookup button { padding:10px 18px; font-size:1rem; border:1px solid var(--accent); background:var(--accent); color:white; border-radius:6px; cursor:pointer; }
details.non-outputs { margin-top:32px; border-top:1px dashed var(--border); padding-top:16px; }
details.non-outputs summary { cursor:pointer; font-weight:600; }
aside.banner.pause { margin:20px 0; padding:14px 16px; border-left:4px solid #b07700; background:rgba(176, 119, 0, 0.08); }
a.dashboard-link { font-weight:600; }
"""

_RESULT_CSS = _BASE_CSS + _SHARED_COMPONENTS_CSS + """
p.did { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:.85rem; color:var(--muted); word-break:break-all; }
p.observed-count { font-weight:600; }
p.scope-note { font-size:.92rem; color:var(--muted); margin:8px 0; }
p.locus.page-locus { padding:8px 12px; background:rgba(0,0,0,0.02); border-radius:6px; margin:8px 0 16px; }
@media (prefers-color-scheme: dark) { p.locus.page-locus { background:rgba(255,255,255,0.03); } }
[data-theme="dark"] p.locus.page-locus { background:rgba(255,255,255,0.03); }
aside.use-not { margin:16px 0 28px; padding:14px 16px; border-left:3px solid var(--border); background:rgba(0,0,0,0.02); font-size:.92rem; }
@media (prefers-color-scheme: dark) { aside.use-not { background:rgba(255,255,255,0.03); } }
aside.use-not p { margin:6px 0; }
aside.use-not strong { color:var(--accent); }
article.labeler-card { margin:24px 0; padding:20px; border:1px solid var(--border); border-radius:8px; background:rgba(0,0,0,0.015); }
@media (prefers-color-scheme: dark) { article.labeler-card { background:rgba(255,255,255,0.02); } }
article.labeler-card h3 { margin:0 0 4px; font-size:1.1rem; }
p.sentence { font-size:1.05rem; margin:14px 0; font-weight:500; }
p.coherence-note { font-size:.85rem; color:var(--muted); border-left:3px solid #a07a30; padding:4px 12px; margin:8px 0; }
p.summary { font-size:.88rem; color:var(--muted); }
p.locus { font-size:.92rem; margin:10px 0; }
p.locus .locus-chip { display:inline-block; padding:1px 8px; margin:2px 4px 2px 0; border-radius:11px; background:rgba(0,0,0,0.05); }
@media (prefers-color-scheme: dark) { p.locus .locus-chip { background:rgba(255,255,255,0.07); } }
p.capped-note { font-size:.85rem; color:var(--muted); margin:6px 0; }
table { width:100%; border-collapse:collapse; margin:12px 0; font-size:.9rem; }
table th, table td { padding:6px 10px; border-bottom:1px solid var(--border); text-align:left; }
table caption { text-align:left; font-weight:600; padding:6px 0; }
table.records td code { font-size:.82rem; word-break:break-all; }
table.records td a.post-link { font-weight:600; }
table.records td code.raw-uri { display:inline-block; margin-top:4px; color:var(--muted); font-size:.78rem; }
article.labeler-card h3 a.labeler-handle { color:inherit; text-decoration:none; }
article.labeler-card h3 a.labeler-handle:hover { text-decoration:underline; }
a.labeler-profile-link, a.subject-profile-link, a.post-link { font-size:.85rem; color:var(--accent); text-decoration:none; font-weight:500; }
a.labeler-profile-link:hover, a.subject-profile-link:hover, a.post-link:hover { text-decoration:underline; }
section.subject h2 a.subject-handle { color:inherit; text-decoration:none; }
section.subject h2 a.subject-handle:hover { text-decoration:underline; }
details { margin-top:12px; }
details summary { cursor:pointer; color:var(--accent); }
section.refusal { padding:24px; border:1px solid var(--border); border-radius:8px; }
section.refusal h2 { margin-top:0; }
p.refusal-detail { font-family: ui-monospace, monospace; font-size:.85rem; color:var(--muted); }
a.dashboard-link { font-weight:600; }
"""
