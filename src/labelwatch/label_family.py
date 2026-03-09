"""Label family normalization + domain classification (versioned).

Maps raw label values to coarser "families" for cross-labeler comparison.
Two-step process:
  1. canonicalize(val) — strip/lower/underscore
  2. map_to_family(canon) — collapse synonyms via FAMILY_MAP

Domain classification adds a third axis: moderation / metadata / novelty.
Used by boundary Phase 2 to separate real moderation conflicts from
badge-ecosystem orthogonality.

FAMILY_MAP is versioned. Bump FAMILY_VERSION when changing the map so
derived artifacts (edges, summaries) are keyed by the version that produced them.
"""
from __future__ import annotations

FAMILY_VERSION = "v2"

# Synonym mapping: canonical form → family bucket.
# Even a small map eliminates the biggest source of fake disagreement.
# Keys are canonicalized values (lowercase, underscored).
FAMILY_MAP: dict[str, str] = {
    # Sexual/adult content
    "porn": "adult-sexual",
    "pornography": "adult-sexual",
    "nsfw": "adult-sexual",
    "adult": "adult-sexual",
    "adult_content": "adult-sexual",
    "sexual": "adult-sexual",
    "nudity": "nudity",
    "graphic-media": "graphic-media",
    # Spam/scam
    "spam": "spam",
    "scam": "spam",
    "junk": "spam",
    # Misleading/misinfo
    "misleading": "misleading",
    "misinformation": "misleading",
    "misinfo": "misleading",
    "disinformation": "misleading",
    "disinfo": "misleading",
    "false_information": "misleading",
    # Harassment/abuse
    "harassment": "harassment",
    "abuse": "harassment",
    "bullying": "harassment",
    # Hate/extremism
    "hate": "hate",
    "extremism": "hate",
    "hate_speech": "hate",
    # Violence
    "violence": "violence",
    "gore": "violence",
    "violent": "violence",
    # Impersonation
    "impersonation": "impersonation",
    "impersonate": "impersonation",
    # ATProto standard moderation actions
    "!warn": "mod-warn",
    "!hide": "mod-hide",
    "!no-unauthenticated": "mod-gate",
    "!no-promote": "mod-gate",
    "!takedown": "mod-takedown",
}


# Domain classification: family → domain.
# Used by boundary Phase 2 to filter moderation conflicts from badge noise.
# Families not in this map go through heuristic fallback in classify_domain().
DOMAIN_MAP: dict[str, str] = {
    # Moderation families (from FAMILY_MAP outputs)
    "adult-sexual": "moderation",
    "nudity": "moderation",
    "graphic-media": "moderation",
    "spam": "moderation",
    "misleading": "moderation",
    "harassment": "moderation",
    "hate": "moderation",
    "violence": "moderation",
    "impersonation": "moderation",
    "mod-warn": "moderation",
    "mod-hide": "moderation",
    "mod-gate": "moderation",
    "mod-takedown": "moderation",
    # Common moderation families from wild labelers (not in FAMILY_MAP)
    "reply-link-spam": "moderation",
    "general-spam": "moderation",
    "likely-nsfw": "moderation",
    "coordinated-abuse": "moderation",
    "contains-slur": "moderation",
    # Metadata/stats families
    "handle-changed": "metadata",
    "high-metadata-changes-five": "metadata",
    "bot-reply": "metadata",
    "site-standard": "metadata",
    "some-blocks": "metadata",
    "modlist-author": "metadata",
    # Political (separate from moderation — political tagging, not enforcement)
    "uspol": "political",
    "government": "political",
}

# Substrings that indicate moderation intent even for unmapped families.
_MODERATION_KEYWORDS = (
    "spam", "nsfw", "abuse", "harass", "hate", "violen", "porn",
    "scam", "mislead", "misinfo", "takedown", "slur", "threat",
)


def classify_domain(family: str) -> str:
    """Classify a family name into a domain: moderation, metadata, political, or novelty.

    Uses a three-step cascade:
    1. Explicit DOMAIN_MAP lookup
    2. Keyword heuristic for unmapped moderation families
    3. Default to novelty (catches badge/gamification labelers)
    """
    # 1. Explicit mapping
    domain = DOMAIN_MAP.get(family)
    if domain:
        return domain

    # 2. ATProto mod action prefix
    if family.startswith("!"):
        return "moderation"

    # 3. Keyword heuristic — conservative substring check
    for kw in _MODERATION_KEYWORDS:
        if kw in family:
            return "moderation"

    # 4. Default: novelty/badge
    return "novelty"


def canonicalize(val: str | None) -> str:
    """Canonicalize a raw label value: strip, lowercase, spaces→underscores."""
    if not val:
        return "<null>"
    return val.strip().lower().replace(" ", "_")


def normalize_family(val: str | None) -> str:
    """Normalize a label value to its family name.

    Two steps: canonicalize → map via FAMILY_MAP.
    Unmapped values pass through as their canonical form.
    """
    canon = canonicalize(val)
    return FAMILY_MAP.get(canon, canon)
