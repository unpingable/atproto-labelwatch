"""Tier-2 label-value classification from emitter self-description.

Doctrine:

    Emitter descriptions are TESTIMONY, not truth.

The labeler has already written what its label means. This module reads
that testimony and produces a structured classification with the
emitter's own words preserved as provenance. We do not adopt the
emitter's framing as our own; we cite it.

Tier stack (the caller composes these):

    exact_profile        — registry's manual AUTHORITY_EFFECT_MAP entry
                           (handled by label_family.classify_authority_effect;
                           ALWAYS wins when present)
    emitter_described    — THIS MODULE: derive from emitter's labelValueDefinition
    pattern_profile      — regex over the label string (separate tier)
    raw_fallback         — the label has no exact / emitter / pattern coverage

Acceptance discipline (from the operator brief):

    fundraising-link should classify as
      emitter_described / post / advisory(-ish) / neutral / no scope creep

    fringe-media should classify as
      emitter_described / mixed / reputational / editorial / scope-creep flagged
      (post-scoped per definition body, but description discusses accounts)

No LLM. No semantic grand theory. Transparent regex/rule heuristics over
the labelValueDefinition's severity/blurs/defaultSetting + locale text.
Every classification result carries the source excerpt so a human auditor
can second-guess any individual call without re-running anything.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


# --- enum vocabularies ---------------------------------------------------

# semantic_source: which tier produced this classification.
SEMANTIC_SOURCE_EXACT_PROFILE = "exact_profile"
SEMANTIC_SOURCE_EMITTER_DESCRIBED = "emitter_described"
SEMANTIC_SOURCE_PATTERN_PROFILE = "pattern_profile"
SEMANTIC_SOURCE_RAW_FALLBACK = "raw_fallback"

# target_scope: what kind of subject the label is talking about.
TARGET_SCOPE_POST = "post"
TARGET_SCOPE_ACCOUNT = "account"
TARGET_SCOPE_PROFILE = "profile"
TARGET_SCOPE_MIXED = "mixed"
TARGET_SCOPE_AMBIGUOUS = "ambiguous"
TARGET_SCOPE_EXTERNAL_ENTITY = "external_entity"

# authority_effect: what kind of authority the label claims.
AUTHORITY_EFFECT_DESCRIPTIVE = "descriptive"
AUTHORITY_EFFECT_ADVISORY = "advisory"
AUTHORITY_EFFECT_REPUTATIONAL = "reputational"
AUTHORITY_EFFECT_VISIBILITY_AFFECTING = "visibility_affecting"
AUTHORITY_EFFECT_ENFORCEMENT_INSTRUCTION = "enforcement_instruction"
AUTHORITY_EFFECT_AMBIGUOUS = "ambiguous"

# classification_basis: what evidence the result rested on.
BASIS_EMITTER_LOCALE_DESCRIPTION = "emitter_locale_description"
BASIS_EMITTER_LABEL_METADATA = "emitter_label_metadata"
BASIS_LOCAL_PATTERN_RULE = "local_pattern_rule"
BASIS_REGISTRY_MANUAL_PROFILE = "registry_manual_profile"


# --- linguistic signals --------------------------------------------------

# Words that suggest the labeler is making an editorial / value-laden
# judgment rather than just describing. Match is case-insensitive and on
# word boundaries. These are signals, not proofs; collected from observed
# labeler self-descriptions in discovery_events.
_EDITORIAL_WORDS = {
    "disinformation", "misinformation", "disinfo", "misinfo",
    "fringe", "extreme", "extremist", "extremism",
    "launder", "laundering", "laundered",
    "conspiracy", "conspiracies",
    "fake", "hoax", "propaganda",
    "manipulation", "manipulated", "manipulative",
    "inauthentic", "fraudulent", "malicious",
    "harmful", "toxic",
    "abuse", "abusive", "abuser",
    "hate", "hateful", "hatred",
    "supremacist", "nazi", "fascist",
    "spam", "spammer", "spammy",
    "bot", "botlike", "automation",
    "rude", "obnoxious",
    "low-quality", "low quality",
    "stigmatiz", "stigmatize", "stigmatized",
    "dunk",
}

# Words about post-shaped subjects.
_POST_TERMS = {
    "post", "posts", "posting",
    "content", "media", "image", "images", "video",
    "message", "messages",
    "reply", "replies", "thread", "threads",
    "skeet", "skeets",
    "link", "links", "url",
}

# Words about account-shaped subjects.
_ACCOUNT_TERMS = {
    "account", "accounts",
    "user", "users",
    "actor", "actors",
    "identity", "identities",
    "person", "people", "individual", "individuals",
}

# Words about profile-shaped subjects (sub-set of account; often distinct
# in protocol/UX contexts).
_PROFILE_TERMS = {
    "profile", "profiles",
    "bio", "biography", "biographies",
    "handle", "handles",
    "avatar", "banner",
}

# Words about external entities (the label is extracting / pointing at
# something that is not the labeled record itself).
_EXTERNAL_ENTITY_TERMS = {
    "wallet", "wallets",
    "contract", "contracts",
    "address", "addresses",
    "domain", "domains",
    "site", "sites", "website",
    "publisher", "publishers", "source", "sources",
    "outlet", "outlets",
}

# "Never applied to X" exclusion: lets a description bound its own scope.
_EXCLUSION_RX = re.compile(
    r"never\s+(?:applied|used)\s+(?:to|for|on)\s+(\w+)",
    re.IGNORECASE,
)
# "Applied to X" / "Used for X" inclusion phrasing.
_INCLUSION_RX = re.compile(
    r"applied\s+to\s+(\w+)|used\s+for\s+(\w+)|marks?\s+(\w+)|labels?\s+(\w+)",
    re.IGNORECASE,
)

# Words that suggest pure description / advisory framing (low authority).
_DESCRIPTIVE_VERBS = {
    "describe", "describes", "describing",
    "mark", "marks", "marking",
    "tag", "tags", "tagging",
    "identif", "identifies", "identifying",
    "denot", "denotes",
    "flag", "flags", "flagging",
    "indicate", "indicates",
    "represent", "represents",
}


# --- main classifier -----------------------------------------------------

def classify_via_emitter(
    label_value: str,
    emitter_definition: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Classify a label_value using the emitter's published
    labelValueDefinition (severity / blurs / defaultSetting / locales).

    Returns None when the emitter definition is missing or has no usable
    locale text (caller falls through to tier 3 / raw_fallback).

    Returns a dict with the following keys when classification succeeds:

      semantic_source:           "emitter_described"
      classification_basis:      "emitter_locale_description" or
                                 "emitter_label_metadata"
      authority_effect:          one of the AUTHORITY_EFFECT_* constants
      target_scope:              one of the TARGET_SCOPE_* constants
      tone:                      "editorial" | "neutral"
      scope_caveat:              str | None (free-text flag when scope is
                                              mixed or explicitly bounded)
      evidence:                  dict carrying the verbatim emitter
                                 metadata + the description excerpt that
                                 drove the classification (provenance, NOT
                                 derived prose)
    """
    if not emitter_definition or not isinstance(emitter_definition, dict):
        return None

    # Prefer English locale description; fall back to any locale's name
    # or description; metadata-only classification is the last resort.
    locales = emitter_definition.get("locales") or []
    locale, desc_text = _pick_description(locales)
    severity = emitter_definition.get("severity")
    blurs = emitter_definition.get("blurs")
    default_setting = emitter_definition.get("defaultSetting")
    adult_only = bool(emitter_definition.get("adultOnly"))

    if not desc_text and severity is None and blurs is None and default_setting is None:
        return None

    if desc_text:
        target_scope, scope_caveat = _detect_target_scope(desc_text)
        tone = _detect_tone(desc_text)
        authority_effect = _detect_authority_effect(
            desc_text, severity, blurs, default_setting, tone,
        )
        basis = BASIS_EMITTER_LOCALE_DESCRIPTION
    else:
        # Metadata-only path: use severity/blurs/defaultSetting alone.
        target_scope = TARGET_SCOPE_AMBIGUOUS
        scope_caveat = "no locale description; target scope inferred only from metadata"
        tone = "neutral"
        authority_effect = _authority_from_metadata_only(
            severity, blurs, default_setting,
        )
        basis = BASIS_EMITTER_LABEL_METADATA

    return {
        "semantic_source": SEMANTIC_SOURCE_EMITTER_DESCRIBED,
        "classification_basis": basis,
        "authority_effect": authority_effect,
        "target_scope": target_scope,
        "tone": tone,
        "scope_caveat": scope_caveat,
        "evidence": {
            "label_value": label_value,
            "severity": severity,
            "blurs": blurs,
            "defaultSetting": default_setting,
            "adultOnly": adult_only,
            "locale": locale,
            "description_excerpt": (desc_text or "")[:500],
        },
    }


# --- pattern tier (tier 3) -----------------------------------------------

# Pattern profiles applied ONLY when the emitter description is absent or
# unusable. Each entry is (regex, target_scope, authority_effect, note).
# Conservative by design — the seam belongs to the emitter, not us.
_PATTERN_PROFILES: List[Tuple[re.Pattern, str, str, str]] = [
    # Content-format / source-artifact markers.
    (re.compile(r".*-screenshot$"), TARGET_SCOPE_POST,
     AUTHORITY_EFFECT_DESCRIPTIVE,
     "content-format pattern: *-screenshot"),

    # Well-known publisher tokens (source markers, content-feature scope).
    (re.compile(r"^(substack|nytimes|cbsnews|medium|wsj|wapo|guardian|bbc|reuters|ap|cnn|foxnews|nbc|abc)$"),
     TARGET_SCOPE_AMBIGUOUS,
     AUTHORITY_EFFECT_DESCRIPTIVE,
     "publisher/source token; scope depends on emitter's framing"),

    # Crypto / external-entity extraction.
    (re.compile(r"^store-tz2at-(wallets|contracts)$"),
     TARGET_SCOPE_EXTERNAL_ENTITY,
     AUTHORITY_EFFECT_DESCRIPTIVE,
     "crypto entity extraction"),

    # Content-feature link markers.
    (re.compile(r".*-link$"), TARGET_SCOPE_POST,
     AUTHORITY_EFFECT_ADVISORY,
     "content-feature pattern: *-link"),

    # Behavioral-rate markers.
    (re.compile(r"^made-over-\w+-posts-\w+$"),
     TARGET_SCOPE_ACCOUNT,
     AUTHORITY_EFFECT_ADVISORY,
     "behavioral-rate pattern: made-over-N-posts-*"),

    # Protocol integrity diagnostics.
    (re.compile(r".*replyref.*"), TARGET_SCOPE_AMBIGUOUS,
     AUTHORITY_EFFECT_DESCRIPTIVE,
     "protocol-integrity pattern: *replyref* (scope depends on emitter)"),
]


def classify_via_pattern(label_value: str) -> Optional[Dict[str, Any]]:
    """Last-resort regex match over the label string itself."""
    for rx, scope, effect, note in _PATTERN_PROFILES:
        if rx.match(label_value):
            return {
                "semantic_source": SEMANTIC_SOURCE_PATTERN_PROFILE,
                "classification_basis": BASIS_LOCAL_PATTERN_RULE,
                "authority_effect": effect,
                "target_scope": scope,
                "tone": "unknown",
                "scope_caveat": None,
                "evidence": {
                    "label_value": label_value,
                    "matched_pattern": rx.pattern,
                    "pattern_note": note,
                },
            }
    return None


def classify_raw_fallback(label_value: str) -> Dict[str, Any]:
    """The label has no exact / emitter / pattern coverage."""
    return {
        "semantic_source": SEMANTIC_SOURCE_RAW_FALLBACK,
        "classification_basis": None,
        "authority_effect": AUTHORITY_EFFECT_AMBIGUOUS,
        "target_scope": TARGET_SCOPE_AMBIGUOUS,
        "tone": "unknown",
        "scope_caveat": "no profile, no emitter description, no pattern match",
        "evidence": {"label_value": label_value},
    }


# --- internals -----------------------------------------------------------

def _pick_description(locales: List[Dict[str, Any]]) -> Tuple[Optional[str], str]:
    """Return (locale_tag, description_text) preferring English."""
    # English first
    for loc in locales:
        if (loc.get("lang") or "").lower() in ("en", "en-us", "en-gb"):
            desc = (loc.get("description") or "").strip()
            name = (loc.get("name") or "").strip()
            text = (name + ". " + desc).strip(". ")
            if text:
                return (loc.get("lang"), text)
    # Any locale with text
    for loc in locales:
        desc = (loc.get("description") or "").strip()
        name = (loc.get("name") or "").strip()
        text = (name + ". " + desc).strip(". ")
        if text:
            return (loc.get("lang"), text)
    return (None, "")


def _detect_target_scope(text: str) -> Tuple[str, Optional[str]]:
    """Determine target_scope + an optional scope_caveat string."""
    lower = text.lower()
    has_post = _any_word_present(lower, _POST_TERMS)
    has_account = _any_word_present(lower, _ACCOUNT_TERMS)
    has_profile = _any_word_present(lower, _PROFILE_TERMS)
    has_external = _any_word_present(lower, _EXTERNAL_ENTITY_TERMS)

    # Explicit exclusion phrasing ("never applied to profiles") is a
    # strong scope-bound signal even when other terms appear.
    excl = _EXCLUSION_RX.search(text)
    if excl:
        excluded = excl.group(1).lower()
        if excluded in _ACCOUNT_TERMS | _PROFILE_TERMS:
            return TARGET_SCOPE_POST, (
                f"emitter explicitly says never applied to {excluded}"
            )
        if excluded in _POST_TERMS:
            scope = TARGET_SCOPE_PROFILE if has_profile else TARGET_SCOPE_ACCOUNT
            return scope, (
                f"emitter explicitly says never applied to {excluded}"
            )

    account_like = has_account or has_profile
    if has_post and account_like:
        return TARGET_SCOPE_MIXED, (
            "description discusses both post-shape and account/profile-shape "
            "subjects; scope creeps between layers"
        )
    if has_external and (has_post or account_like):
        return TARGET_SCOPE_MIXED, (
            "description references an external entity alongside "
            "post/account scope"
        )
    if has_post:
        return TARGET_SCOPE_POST, None
    if has_profile:
        return TARGET_SCOPE_PROFILE, None
    if has_account:
        return TARGET_SCOPE_ACCOUNT, None
    if has_external:
        return TARGET_SCOPE_EXTERNAL_ENTITY, None
    return TARGET_SCOPE_AMBIGUOUS, "no scope-bearing terms detected"


def _detect_tone(text: str) -> str:
    """Return 'editorial' if any value-laden word is present; else 'neutral'."""
    lower = text.lower()
    for word in _EDITORIAL_WORDS:
        # word-boundary match for multi-word phrases too
        if word in lower:
            # tighten: must be a token boundary, not a substring of an
            # unrelated word
            pattern = r"\b" + re.escape(word) + r"\b"
            if re.search(pattern, lower):
                return "editorial"
    return "neutral"


def _detect_authority_effect(
    text: str,
    severity: Optional[str],
    blurs: Optional[str],
    default_setting: Optional[str],
    tone: str,
) -> str:
    """Combine emitter metadata + locale tone to pick authority_effect."""
    lower = text.lower()

    # Enforcement-instruction shape: bang-prefixed protocol labels are
    # caught by tier 1 already; emitter-tier rarely sees them.
    if default_setting == "hide" and tone == "editorial":
        return AUTHORITY_EFFECT_VISIBILITY_AFFECTING
    if default_setting in ("hide", "warn"):
        # visibility-affecting if the emitter's default setting is to
        # actively change rendering, even with a neutral tone
        return AUTHORITY_EFFECT_VISIBILITY_AFFECTING

    # Editorial tone → reputational claim, regardless of severity
    if tone == "editorial":
        return AUTHORITY_EFFECT_REPUTATIONAL

    # severity=alert with neutral tone: still advisory (consumer should
    # take notice) but not reputational
    if severity == "alert":
        return AUTHORITY_EFFECT_ADVISORY
    if severity == "inform":
        # inform + descriptive verb → descriptive; else advisory
        for verb in _DESCRIPTIVE_VERBS:
            if verb in lower:
                return AUTHORITY_EFFECT_DESCRIPTIVE
        return AUTHORITY_EFFECT_ADVISORY

    # No severity, neutral tone, descriptive verb → descriptive
    for verb in _DESCRIPTIVE_VERBS:
        if verb in lower:
            return AUTHORITY_EFFECT_DESCRIPTIVE
    return AUTHORITY_EFFECT_AMBIGUOUS


def _authority_from_metadata_only(
    severity: Optional[str],
    blurs: Optional[str],
    default_setting: Optional[str],
) -> str:
    if default_setting in ("hide", "warn"):
        return AUTHORITY_EFFECT_VISIBILITY_AFFECTING
    if severity == "alert":
        return AUTHORITY_EFFECT_ADVISORY
    if severity == "inform":
        return AUTHORITY_EFFECT_ADVISORY
    return AUTHORITY_EFFECT_AMBIGUOUS


def _any_word_present(text_lower: str, vocab) -> bool:
    """Return True if any word in vocab appears as a whole token in text_lower."""
    for word in vocab:
        pattern = r"\b" + re.escape(word) + r"\b"
        if re.search(pattern, text_lower):
            return True
    return False


# --- enrichment helpers (DB-aware; used by report.py) ---------------------

def classify_one(
    label_value: str,
    emitter_definition: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run the tier cascade on a single label_value, optionally with an
    emitter labelValueDefinition. Returns a classification dict — never
    None; falls through to raw_fallback when no tier matches.

    Tier 1 (exact_profile) is handled by the caller via
    label_family.classify_authority_effect; this function covers
    tiers 2, 3, and 4.
    """
    if emitter_definition:
        result = classify_via_emitter(label_value, emitter_definition)
        if result is not None:
            return result
    result = classify_via_pattern(label_value)
    if result is not None:
        return result
    return classify_raw_fallback(label_value)


def lookup_emitter_definition(
    conn, labeler_did: str, label_value: str,
) -> Optional[Dict[str, Any]]:
    """Pull the labeler's latest service record from discovery_events
    and extract the matching labelValueDefinition. Returns None when
    no record / no matching definition. Pure-ish (single read query)."""
    import json as _json
    row = conn.execute(
        """
        SELECT record_json FROM discovery_events
        WHERE labeler_did = ?
          AND operation IN ('create','update')
          AND json_extract(record_json,'$.policies.labelValueDefinitions') IS NOT NULL
        ORDER BY discovered_at DESC LIMIT 1
        """,
        (labeler_did,),
    ).fetchone()
    if row is None:
        return None
    try:
        rec = _json.loads(row[0] if not hasattr(row, "keys") else row["record_json"])
    except (TypeError, ValueError):
        return None
    defs = (rec.get("policies") or {}).get("labelValueDefinitions") or []
    for d in defs:
        if d.get("identifier") == label_value:
            return d
    return None


def enrich_top_vals_with_tier_classification(
    conn, top_vals: List[Dict[str, Any]], *, per_val_limit: int = 50,
) -> List[Dict[str, Any]]:
    """Augment each top_vals entry with a tier-2/3/4 classification.

    Strategy: for each (value, family) entry, pick a top emitter and
    try its service record. If no labeler has a usable definition, fall
    through to pattern, then raw_fallback. Returns a new list (does not
    mutate input). per_val_limit caps how many entries get enriched
    (default 50; bigger ledgers shouldn't burn report-thread time).
    """
    out: List[Dict[str, Any]] = []
    for v in top_vals[:per_val_limit]:
        label_value = v.get("value", "")
        emitter_definition = _find_any_emitter_definition(conn, label_value)
        result = classify_one(label_value, emitter_definition)
        enriched = dict(v)
        enriched["tier_classification"] = result
        out.append(enriched)
    return out


def _find_any_emitter_definition(
    conn, label_value: str,
) -> Optional[Dict[str, Any]]:
    """Search discovery_events for ANY labeler whose service record
    contains a labelValueDefinition for this label_value. Returns the
    first matching definition found (newest-first within each labeler).
    """
    import json as _json
    rows = conn.execute(
        """
        SELECT labeler_did, record_json FROM discovery_events de
        WHERE operation IN ('create','update')
          AND EXISTS (
            SELECT 1 FROM json_each(
              json_extract(de.record_json,'$.policies.labelValueDefinitions')
            ) j WHERE json_extract(j.value,'$.identifier') = ?
          )
        ORDER BY discovered_at DESC LIMIT 5
        """,
        (label_value,),
    ).fetchall()
    for row in rows:
        try:
            rec = _json.loads(row[1] if not hasattr(row, "keys") else row["record_json"])
        except (TypeError, ValueError):
            continue
        defs = (rec.get("policies") or {}).get("labelValueDefinitions") or []
        for d in defs:
            if d.get("identifier") == label_value:
                return d
    return None


def tier_histogram(classified: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count results by semantic_source. Useful for the tier-shift
    headline in the report."""
    h: Dict[str, int] = {}
    for entry in classified:
        src = entry.get("tier_classification", {}).get("semantic_source", "unknown")
        h[src] = h.get(src, 0) + 1
    return h
