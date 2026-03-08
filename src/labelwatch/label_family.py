"""Label family normalization (versioned).

Maps raw label values to coarser "families" for cross-labeler comparison.
v1: simple normalization (strip, lowercase, spaces→underscores).
Future: configurable mapper to collapse noisy values into semantic buckets.
"""
from __future__ import annotations

FAMILY_VERSION = "v1"


def normalize_family(val: str | None) -> str:
    """Normalize a label value to its canonical family name.

    v1 rule: strip whitespace, lowercase, replace spaces with underscores.
    """
    if not val:
        return "<null>"
    return val.strip().lower().replace(" ", "_")
