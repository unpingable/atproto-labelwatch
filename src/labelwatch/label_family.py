"""Label family normalization, domain/polarity/kind classification (versioned).

Maps raw label values to coarser "families" for cross-labeler comparison.
Two-step process:
  1. canonicalize(val) — strip/lower/underscore
  2. map_to_family(canon) — collapse synonyms via FAMILY_MAP

Three classification axes on families:
  - Domain:   moderation / metadata / novelty / political / identity
  - Polarity: negative / cautionary / positive / badge / unknown
  - Kind:     policy_claim / protocol_action / status_signal / decorative / unknown

FAMILY_MAP is versioned. Bump FAMILY_VERSION when changing the map so
derived artifacts (edges, summaries) are keyed by the version that produced them.
"""
from __future__ import annotations

import re

FAMILY_VERSION = "v3"

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
    "likely-nsfw": "adult-sexual",
    "nsfw-label": "adult-sexual",
    "nudity": "nudity",
    "graphic-media": "graphic-media",
    # Spam/scam — collapse variants so "spam" vs "shopping-spam" isn't a conflict
    "spam": "spam",
    "scam": "spam",
    "junk": "spam",
    "shopping-spam": "spam",
    "general-spam": "spam",
    "reply-link-spam": "spam",
    # Misleading/misinfo
    "misleading": "misleading",
    "misinformation": "misleading",
    "misinfo": "misleading",
    "disinformation": "misleading",
    "disinfo": "misleading",
    "false_information": "misleading",
    # Harassment/abuse — collapse variants
    "harassment": "harassment",
    "abuse": "harassment",
    "bullying": "harassment",
    "coordinated-abuse": "harassment",
    "engagement-abuse": "harassment",
    # Hate/extremism
    "hate": "hate",
    "extremism": "hate",
    "hate_speech": "hate",
    "contains-slur": "hate",
    "new-acct-slurs": "hate",
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
    # Inauthenticity — distinct from spam (intent vs behavior)
    "inauthentic-fundraising": "inauthenticity",
    "inauth-fundraising": "inauthenticity",
    "suspect-inauthentic": "inauthenticity",
    "platform-manipulation": "inauthenticity",
}


# Domain classification: family → domain.
# Used by boundary Phase 2 to filter moderation conflicts from badge noise.
# Families not in this map go through heuristic fallback in classify_domain().
DOMAIN_MAP: dict[str, str] = {
    # ── Moderation families (from FAMILY_MAP outputs) ──
    "adult-sexual": "moderation",
    "nudity": "moderation",
    "graphic-media": "moderation",
    "spam": "moderation",
    "misleading": "moderation",
    "harassment": "moderation",
    "hate": "moderation",
    "violence": "moderation",
    "impersonation": "moderation",
    "inauthenticity": "moderation",
    "mod-warn": "moderation",
    "mod-hide": "moderation",
    "mod-gate": "moderation",
    "mod-takedown": "moderation",

    # ── Behavioral/stats families ──
    # Account metadata
    "handle-changed": "metadata",
    "many-handle-chgs": "metadata",
    "some-blocks": "metadata",
    "mass-blocks": "metadata",
    "bot-reply": "metadata",
    "bot": "metadata",
    "modlist-author": "metadata",
    "new-acct-replies": "metadata",
    "no-dms": "metadata",
    # Posting stats
    "posting-daily-made-over-25-posts-yesterday": "metadata",
    "posting-daily-made-over-25-replies-yesterday": "metadata",
    "posting-daily-made-over-100-posts-yesterday": "metadata",
    "posting-daily-made-over-100-replies-yesterday": "metadata",
    "posting-monthly-posts-more-than-10-per-day": "metadata",
    "posting-monthly-posts-more-than-20-per-day": "metadata",
    "posting-monthly-replies-more-than-10-per-day": "metadata",
    "posting-monthly-replies-more-than-20-per-day": "metadata",
    # Activity patterns
    "no-gap-more-than-one-hours": "metadata",
    "no-gap-more-than-two-hours": "metadata",
    "no-gap-more-than-four-hours": "metadata",
    # Metadata change velocity
    "high-metadata-changes-five": "metadata",
    "high-metadata-changes-ten": "metadata",
    "high-metadata-changes-fifty": "metadata",
    "metadata-monthly-changes-low": "metadata",
    "metadata-monthly-changes-medium": "metadata",
    "metadata-monthly-changes-high": "metadata",
    # Follow behavior
    "bulk-following": "metadata",
    "follow-farming": "metadata",
    "mass-follow-high": "metadata",
    "mass-follow-mid": "metadata",
    "high-follow-churn-one-hundred": "metadata",
    "high-follow-churn-five-hundred": "metadata",
    "weekly-high-churn-12000": "metadata",
    # URL reuse
    "posted-same-url-low": "metadata",
    "posted-same-url-mid": "metadata",
    "posted-same-url-high": "metadata",
    # Quality / engagement metrics
    "low-quality-replies": "metadata",
    "fringe-media": "metadata",
    "amplifier": "metadata",
    "engagementfarmer": "metadata",
    # Site/PDS identifiers (infrastructure labelers)
    "site-standard": "metadata",
    "internal-independent": "metadata",
    "internal-other": "metadata",

    # ── Political (tagging, not enforcement) ──
    "uspol": "political",
    "government": "political",
    "trump": "political",
    "trumpface": "political",
    "maga-trump": "political",
    "elon-musk": "political",
    "inverted-red-triangle": "political",
    "hammer-sickle": "political",
    "terf-gc": "political",
    "gaza-genocide-supporter": "political",

    # ── Identity (community boundary labels) ──
    "gay-post": "identity",
    "gay-user": "identity",
    "trans-post": "identity",
    "sapphic": "identity",
    "bisexual": "identity",
    "pan": "identity",
    "religion": "identity",
    # Pronoun labels
    "he": "identity",
    "she": "identity",
    "they": "identity",
    "it": "identity",
    "hethey": "identity",
    "shethey": "identity",
    "sheher": "identity",
    "hehim": "identity",
    "theythem": "identity",

    # ── Content type labels ──
    "scat-post": "novelty",
    "urine": "novelty",
    "feces": "novelty",
    "diaper": "novelty",
    "animalistic-mask": "novelty",
    "troll": "novelty",
    "intolerance": "novelty",
    "intolerant": "novelty",
    "sports-betting": "novelty",
    "spoiler-parent": "novelty",
}

# Keyword heuristic: words (not substrings!) that indicate moderation intent
# for families not in DOMAIN_MAP. Word-boundary matching prevents "ai-hater"
# from matching "hate" or "exhausted-dancing" from matching anything.
_MODERATION_KEYWORDS_RE = re.compile(
    r"(?:^|[-_])"  # start of string or separator
    r"(?:spam|nsfw|abuse|harass|hate|violen|porn|scam|mislead|misinfo|takedown|slur|threat)"
    r"(?:[-_]|$)",  # separator or end of string
    re.IGNORECASE,
)


# Polarity classification: what a label *does* to its target.
# Only covers families where polarity is unambiguous. Families not in this
# map get "unknown" — better than guessing wrong.
POLARITY_MAP: dict[str, str] = {
    # ── Negative: restrictive, punitive, or removal-intent ──
    "spam": "negative",
    "harassment": "negative",
    "hate": "negative",
    "violence": "negative",
    "adult-sexual": "negative",
    "misleading": "negative",
    "impersonation": "negative",
    "inauthenticity": "negative",
    "mod-hide": "negative",
    "mod-takedown": "negative",

    # ── Cautionary: informational warning, not removal ──
    "nudity": "cautionary",
    "graphic-media": "cautionary",
    "mod-warn": "cautionary",
    "mod-gate": "cautionary",

    # ── Badge: decorative / community / gamification ──
    # (Novelty-domain families default to badge via classify_polarity fallback)
}


# Kind classification: what sort of thing a label IS.
# policy_claim = assertion about content/account (diagnosis)
# protocol_action = enforcement/consumer instruction (action)
# status_signal = observable behavioral metric (measurement)
# decorative = badge/flair/community marker
# unknown = can't tell
KIND_MAP: dict[str, str] = {
    # ── Policy claims: assertions about content character ──
    "spam": "policy_claim",
    "harassment": "policy_claim",
    "hate": "policy_claim",
    "violence": "policy_claim",
    "adult-sexual": "policy_claim",
    "nudity": "policy_claim",
    "graphic-media": "policy_claim",
    "misleading": "policy_claim",
    "impersonation": "policy_claim",
    "inauthenticity": "policy_claim",

    # ── Protocol actions: enforcement instructions to consumers ──
    "mod-warn": "protocol_action",
    "mod-hide": "protocol_action",
    "mod-gate": "protocol_action",
    "mod-takedown": "protocol_action",

    # ── Status signals: behavioral measurements ──
    "handle-changed": "status_signal",
    "many-handle-chgs": "status_signal",
    "some-blocks": "status_signal",
    "mass-blocks": "status_signal",
    "bot-reply": "status_signal",
    "bot": "status_signal",
    "modlist-author": "status_signal",
    "new-acct-replies": "status_signal",
    "no-dms": "status_signal",
    "bulk-following": "status_signal",
    "follow-farming": "status_signal",
    "mass-follow-high": "status_signal",
    "mass-follow-mid": "status_signal",
    "high-follow-churn-one-hundred": "status_signal",
    "high-follow-churn-five-hundred": "status_signal",
    "weekly-high-churn-12000": "status_signal",
    "low-quality-replies": "status_signal",
    "fringe-media": "status_signal",
    "amplifier": "status_signal",
    "engagementfarmer": "status_signal",
    "site-standard": "status_signal",
    "internal-independent": "status_signal",
    "internal-other": "status_signal",
    # Posting stats
    "posting-daily-made-over-25-posts-yesterday": "status_signal",
    "posting-daily-made-over-25-replies-yesterday": "status_signal",
    "posting-daily-made-over-100-posts-yesterday": "status_signal",
    "posting-daily-made-over-100-replies-yesterday": "status_signal",
    "posting-monthly-posts-more-than-10-per-day": "status_signal",
    "posting-monthly-posts-more-than-20-per-day": "status_signal",
    "posting-monthly-replies-more-than-10-per-day": "status_signal",
    "posting-monthly-replies-more-than-20-per-day": "status_signal",
    "no-gap-more-than-one-hours": "status_signal",
    "no-gap-more-than-two-hours": "status_signal",
    "no-gap-more-than-four-hours": "status_signal",
    "high-metadata-changes-five": "status_signal",
    "high-metadata-changes-ten": "status_signal",
    "high-metadata-changes-fifty": "status_signal",
    "metadata-monthly-changes-low": "status_signal",
    "metadata-monthly-changes-medium": "status_signal",
    "metadata-monthly-changes-high": "status_signal",
    "posted-same-url-low": "status_signal",
    "posted-same-url-mid": "status_signal",
    "posted-same-url-high": "status_signal",
}


# Authority effect classification: what role the label plays in the
# control/reputation surface — what kind of authority it attempts to exercise.
#
# This is a structural classification of the LABEL, not an inference about
# labeler intent. It describes the shape of the claim, not its truth value
# and not the labeler's motive.
#
# Values:
#   enforcement_instruction — instructs consumers/clients to remove or block
#   visibility_affecting    — changes reach / hides content from default views
#   advisory                — informational warning, leaves visibility to client
#   reputational            — attaches a normative/character claim to the subject
#   descriptive             — community/identity/context marker, no system effect
#   telemetry               — a measured behavioral metric the labeler observed
#   decorative              — badge/flair/novelty marker
#   unknown                 — cannot classify from family alone
#
# Distinctions worth keeping clear when reading the map:
#   - policy_claim families like `spam`/`hate` are reputational. They assert a
#     normative judgment about the subject; the !-prefixed mod actions are the
#     separate actuators.
#   - `mod-warn` is advisory; `mod-gate`/`mod-hide` affect visibility;
#     `mod-takedown` is an enforcement instruction.
#   - Identity families (`gay-post`, pronouns) are classified descriptive: the
#     label's authority effect is "none"; any hostile social effect comes from
#     issuer/context the schema cannot see. This is not a claim that identity
#     labels are benign — it's a claim about what the label, as a string,
#     instructs the system to do.
AUTHORITY_EFFECT_MAP: dict[str, str] = {
    # ── Enforcement instructions (actuators that remove) ──
    "mod-takedown": "enforcement_instruction",

    # ── Visibility-affecting (reach control, hides from default views) ──
    "mod-hide": "visibility_affecting",
    "mod-gate": "visibility_affecting",

    # ── Advisory (informational warning; client decides) ──
    "mod-warn": "advisory",
    "nudity": "advisory",
    "graphic-media": "advisory",

    # ── Reputational (normative claim about the subject) ──
    # Policy-claim moderation families
    "spam": "reputational",
    "misleading": "reputational",
    "harassment": "reputational",
    "hate": "reputational",
    "violence": "reputational",
    "adult-sexual": "reputational",
    "impersonation": "reputational",
    "inauthenticity": "reputational",
    # Political/affiliation tagging that attaches normative charge
    "uspol": "reputational",
    "government": "reputational",
    "trump": "reputational",
    "trumpface": "reputational",
    "maga-trump": "reputational",
    "elon-musk": "reputational",
    "inverted-red-triangle": "reputational",
    "hammer-sickle": "reputational",
    "terf-gc": "reputational",
    "gaza-genocide-supporter": "reputational",
    # Stance/category accusations: structurally reputational — the string marks
    # a target by socially charged stance rather than recording behavior,
    # infrastructure, or enforcement. Not a judgment on whether the accusation
    # is fair; a judgment on the shape of what the label asks the system to do.
    "ai-hater": "reputational",
    "substack-platforms-nazis": "reputational",
    # Verdict-shaped metrics (interpretive, not raw counts)
    "fringe-media": "reputational",
    "amplifier": "reputational",
    "engagementfarmer": "reputational",
    "low-quality-replies": "reputational",
    "modlist-author": "reputational",
    "troll": "reputational",
    "intolerance": "reputational",
    "intolerant": "reputational",

    # ── Descriptive (community/identity/context markers) ──
    "gay-post": "descriptive",
    "gay-user": "descriptive",
    "trans-post": "descriptive",
    "sapphic": "descriptive",
    "bisexual": "descriptive",
    "pan": "descriptive",
    "religion": "descriptive",
    "he": "descriptive",
    "she": "descriptive",
    "they": "descriptive",
    "it": "descriptive",
    "hethey": "descriptive",
    "shethey": "descriptive",
    "sheher": "descriptive",
    "hehim": "descriptive",
    "theythem": "descriptive",

    # ── Telemetry (raw behavioral metrics: counts, rates, gaps, thresholds) ──
    "handle-changed": "telemetry",
    "many-handle-chgs": "telemetry",
    "some-blocks": "telemetry",
    "mass-blocks": "telemetry",
    "bot": "telemetry",
    "bot-reply": "telemetry",
    "new-acct-replies": "telemetry",
    "no-dms": "telemetry",
    "bulk-following": "telemetry",
    "follow-farming": "telemetry",
    "mass-follow-high": "telemetry",
    "mass-follow-mid": "telemetry",
    "high-follow-churn-one-hundred": "telemetry",
    "high-follow-churn-five-hundred": "telemetry",
    "weekly-high-churn-12000": "telemetry",
    "posting-daily-made-over-25-posts-yesterday": "telemetry",
    "posting-daily-made-over-25-replies-yesterday": "telemetry",
    "posting-daily-made-over-100-posts-yesterday": "telemetry",
    "posting-daily-made-over-100-replies-yesterday": "telemetry",
    "posting-monthly-posts-more-than-10-per-day": "telemetry",
    "posting-monthly-posts-more-than-20-per-day": "telemetry",
    "posting-monthly-replies-more-than-10-per-day": "telemetry",
    "posting-monthly-replies-more-than-20-per-day": "telemetry",
    "no-gap-more-than-one-hours": "telemetry",
    "no-gap-more-than-two-hours": "telemetry",
    "no-gap-more-than-four-hours": "telemetry",
    "high-metadata-changes-five": "telemetry",
    "high-metadata-changes-ten": "telemetry",
    "high-metadata-changes-fifty": "telemetry",
    "metadata-monthly-changes-low": "telemetry",
    "metadata-monthly-changes-medium": "telemetry",
    "metadata-monthly-changes-high": "telemetry",
    "posted-same-url-low": "telemetry",
    "posted-same-url-mid": "telemetry",
    "posted-same-url-high": "telemetry",
    # Infrastructure / site markers — observation of where, not verdict
    "site-standard": "telemetry",
    "internal-independent": "telemetry",
    "internal-other": "telemetry",

    # ── Decorative (badge / flair / novelty) ──
    "scat-post": "decorative",
    "urine": "decorative",
    "feces": "decorative",
    "diaper": "decorative",
    "animalistic-mask": "decorative",
    "sports-betting": "decorative",
    "spoiler-parent": "decorative",
}


# Human-readable copy for each authority_effect group. Descriptive only.
AUTHORITY_EFFECT_COPY: dict[str, str] = {
    "enforcement_instruction": (
        "Labels that instruct consumers to remove or take down content."
    ),
    "visibility_affecting": (
        "Labels that affect reach — hide from default views or gate access."
    ),
    "advisory": (
        "Labels that warn the consumer without affecting visibility directly."
    ),
    "reputational": (
        "Labels that appear to function as a reputational claim about the "
        "subject. Not an inference about labeler intent."
    ),
    "descriptive": (
        "Labels that mark community, identity, or context, with no inherent "
        "system effect. Authority effect described here is structural; "
        "social effect depends on issuer and context."
    ),
    "telemetry": (
        "Labels that report a measured behavioral metric (counts, rates, "
        "gaps, thresholds) observed by the labeler."
    ),
    "decorative": (
        "Labels that function as badges, flair, or novelty markers."
    ),
    "unknown": (
        "Labels not classified by the current authority_effect map. "
        "Listed individually so they are not silently dropped."
    ),
}


# Stable ordering for report rendering. Higher-authority effects first;
# decorative/unknown last. Used by JSON and HTML emitters.
AUTHORITY_EFFECT_ORDER: tuple[str, ...] = (
    "enforcement_instruction",
    "visibility_affecting",
    "advisory",
    "reputational",
    "descriptive",
    "telemetry",
    "decorative",
    "unknown",
)


def classify_authority_effect(family: str) -> str:
    """Classify what kind of authority a label family attempts to exercise.

    Returns one of:
      enforcement_instruction, visibility_affecting, advisory, reputational,
      descriptive, telemetry, decorative, unknown.

    Strictly explicit. No structural fallback: if a label cannot be assigned
    from AUTHORITY_EFFECT_MAP, returns "unknown". Unknown is a valid report
    finding — it surfaces labels the namespace has grown around without the
    classifier guessing. Do not collapse this into a domain/kind synonym by
    adding inferred fallbacks.

    Does not infer labeler intent. Identity/descriptive families are classified
    by what the label, as a string, asks the system to do — not by how a
    particular issuer may be using it.

    See `classify_authority_effect_with_labeler_context()` for the narrow
    labeler-level fallback used when a labeler emits an entire bespoke
    decorative namespace too large to enumerate by value.
    """
    return AUTHORITY_EFFECT_MAP.get(family, "unknown")


# Labeler-level authority-effect hint: applied ONLY as a fallback for labels
# that are not in AUTHORITY_EFFECT_MAP and are emitted by labelers whose entire
# namespace is admittedly bespoke/decorative by self-declared design.
#
# Tension this resolves: oracle.posters.rip alone emits 200+ distinct
# "manner of death" strings. Enumerating each in AUTHORITY_EFFECT_MAP is not
# maintainable. Leaving them in `unknown` clutters the schema-gap finding with
# labels nobody actually disputes are decorative.
#
# Tension this introduces: violates the "classify the label, not the labeler"
# rule for the long-tail fallback case. Kept narrow:
#
#   - Only used when AUTHORITY_EFFECT_MAP has no mapping for the val.
#   - Only applies if ALL labelers emitting the val are in this hint map AND
#     agree on the effect (see inventory aggregation).
#   - Label-level mapping always wins. If a hinted labeler ever emits `spam`,
#     `spam` stays reputational; the labeler hint cannot override it.
#
# Mirrored in the rejection note for the social_function axis: persisting an
# editorial framing as schema is the line. Mapping "these three labelers
# self-describe as toy" is closer to a registry annotation than an editorial
# verdict.
LABELER_DEFAULT_EFFECT: dict[str, str] = {
    # oracle.posters.rip — "manner of death" oracle/fortune labeler.
    # 200+ distinct val strings, all decorative by self-declared theme.
    "did:plc:fqfzpua2rp5io5nmxcixvdvm": "decorative",
    # waffles.mosphere.at — "PANCAKES" novelty labeler.
    "did:plc:bpcllqvnvx3dlyrcblqkusat": "decorative",
    # NOTE: bskyttrpg.bsky.social (D&D class labels) is reported by the user as
    # an obvious decorative labeler but is not yet in the labelers registry
    # at the time of this map's last update. Add its DID here when discovery
    # resolves it.
}


def classify_kind(family: str) -> str:
    """Classify what sort of thing a label family is.

    Returns: policy_claim, protocol_action, status_signal, decorative, or unknown.

    Static map first. Falls back based on domain and prefix patterns.
    """
    kind = KIND_MAP.get(family)
    if kind:
        return kind

    # ! prefix = protocol/enforcement action
    if family.startswith("!"):
        return "protocol_action"

    # Novelty-domain families are decorative
    domain = classify_domain(family)
    if domain == "novelty":
        return "decorative"

    # Political and identity labels are claims (tagging, not enforcement)
    if domain in ("political", "identity"):
        return "policy_claim"

    return "unknown"


def classify_polarity(family: str) -> str:
    """Classify a family's polarity: what a label does to its target.

    Returns: negative, cautionary, positive, badge, or unknown.

    Static map first. Falls back to badge for novelty-domain families,
    unknown for everything else.
    """
    pol = POLARITY_MAP.get(family)
    if pol:
        return pol

    # Novelty-domain families are decorative by nature
    domain = classify_domain(family)
    if domain == "novelty":
        return "badge"

    return "unknown"


def classify_domain(family: str) -> str:
    """Classify a family name into a domain.

    Returns: moderation, metadata, political, identity, or novelty.

    Uses a three-step cascade:
    1. Explicit DOMAIN_MAP lookup (highest priority)
    2. ATProto mod action prefix (!)
    3. Word-boundary keyword heuristic for unmapped families
    4. Default to novelty (catches badge/gamification labelers)
    """
    # 1. Explicit mapping — always wins
    domain = DOMAIN_MAP.get(family)
    if domain:
        return domain

    # 2. ATProto mod action prefix
    if family.startswith("!"):
        return "moderation"

    # 3. Word-boundary keyword heuristic (not substring)
    if _MODERATION_KEYWORDS_RE.search(family):
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
