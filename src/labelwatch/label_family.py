"""Label family normalization (versioned).

Maps raw label values to coarser "families" for cross-labeler comparison.
Two-step process:
  1. canonicalize(val) — strip/lower/underscore
  2. map_to_family(canon) — collapse synonyms via FAMILY_MAP

FAMILY_MAP is versioned. Bump FAMILY_VERSION when changing the map so
derived artifacts (edges, summaries) are keyed by the version that produced them.
"""
from __future__ import annotations

FAMILY_VERSION = "v1"

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
