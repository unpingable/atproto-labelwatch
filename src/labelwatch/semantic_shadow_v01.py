"""LW-JEV-SHADOW V0.1 qualification rule (DESIGN Amendment 2026-09-17-C).

Pure functions: no network, no database, no clock. Nothing in the running
sidecar calls this module yet; it exists so the frozen rule and the Part H
validation can be executed and tested offline.

Principle: context explains why we looked; evidence explains why we believe.

- Context signals (structural lineage, dt, relation type, thread depth,
  languages) are carried in ``Context`` and can only *downgrade* (§3 step 2).
  They are never an evidence channel.
- The only admitted anchors are ``shared_url_domain`` (with the frozen
  generic-host stop-list), ``shared_hashtag`` and ``lexical_jaccard >= 0.20``.
- ``shared_distinctive_token`` is validation instrumentation only.
  ``decide`` does not accept it, so it cannot influence a disposition or a
  review route.
- ``QUALIFIED_EXPLICIT`` means "B textually references identifiable content
  of A". Nothing else, and never propagation, stance or influence.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from . import semantic_shadow as shadow
from . import semantic_shadow_jev as jev

RULE_VERSION = "lw-jev-shadow-v0.1"
AMENDMENT = "DESIGN.md Amendment 2026-09-17-C"

# §3: fixed a priori; no calibration meaning; never tuned on L2, H or R.
TAU_E = 0.90

QUALIFIED_EXPLICIT = "QUALIFIED_EXPLICIT"
UNDERDETERMINED = "UNDERDETERMINED"
NOT_QUALIFIED = "NOT_QUALIFIED"
SEMANTIC_UNAVAILABLE = "SEMANTIC_UNAVAILABLE"
DISPOSITIONS = (QUALIFIED_EXPLICIT, UNDERDETERMINED, NOT_QUALIFIED,
                SEMANTIC_UNAVAILABLE)

# Review routes (§2.C), in primary-route precedence order. A route sends the
# edge to the human-review sample; it never qualifies or disqualifies.
# ``semantic_unavailable`` is an explicit route for failed/refused semantics
# (requested at implementation time; it does not affect any disposition).
ROUTE_SEMANTIC_UNAVAILABLE = "semantic_unavailable"
ROUTE_AMBIGUITY = "ambiguity"
ROUTE_ANCHOR_SEMANTIC_CONFLICT = "anchor_semantic_conflict"
ROUTE_INFERRED_CLAIM = "inferred_claim"
ROUTE_NONE = "none"
ROUTES = (ROUTE_SEMANTIC_UNAVAILABLE, ROUTE_AMBIGUITY,
          ROUTE_ANCHOR_SEMANTIC_CONFLICT, ROUTE_INFERRED_CLAIM)

DEPENDENT = frozenset({"dependent_direct", "dependent_indirect"})
NOT_DEPENDENT = frozenset({"independent_lexical", "unrelated"})
NO_WEIGHT = frozenset({"underdetermined"})
UNDERDETERMINING_ARGMAX = frozenset({"dependent_indirect", "underdetermined"})

# §2.B: generic hosts whose registered domain is not a reference anchor.
# Fixed before any Part H item was authored. Includes multi-part public
# suffixes, because the existing registered-domain helper keeps only the
# last two labels (news.example.co.uk -> co.uk).
GENERIC_HOSTS = frozenset({
    "bsky.app", "bsky.social", "t.co", "bit.ly", "tinyurl.com", "ow.ly",
    "buff.ly", "lnkd.in", "youtu.be", "youtube.com", "twitter.com", "x.com",
    "instagram.com", "facebook.com", "fb.me", "fb.watch", "tiktok.com",
    "reddit.com", "redd.it", "google.com", "goo.gl", "linktr.ee",
    "medium.com", "substack.com", "github.com", "wikipedia.org",
    "archive.org", "archive.ph", "imgur.com", "giphy.com", "tenor.com",
    "spotify.com", "apple.com", "amazon.com", "amzn.to", "threads.net",
    "mastodon.social", "discord.gg", "patreon.com", "ko-fi.com",
    "tumblr.com", "twitch.tv", "linkedin.com", "wordpress.com",
    "blogspot.com", "co.uk", "org.uk", "ac.uk", "gov.uk", "com.au",
    "net.au", "org.au", "co.nz", "co.jp", "ne.jp", "com.br", "co.in",
    "co.za", "com.mx", "com.ar", "co.kr",
})

# Validation-only long-word stop-list (§2.B (b)). Case-folded. Includes the
# packet redaction placeholder ``mention``.
LONG_WORD_STOPLIST = frozenset({
    "mention", "because", "however", "without", "through", "between",
    "another", "nothing", "something", "everything", "anything", "someone",
    "everyone", "anyone", "already", "actually", "probably", "whatever",
    "whether", "although", "together", "yourself", "himself", "herself",
    "themselves", "ourselves", "against", "several", "certain", "whenever",
    "wherever", "instead", "perhaps", "therefore", "otherwise", "usually",
    "exactly", "literally", "honestly", "basically", "seriously",
    "definitely", "absolutely", "completely", "anymore", "someday",
    "sometimes", "everybody", "somebody", "tomorrow",
    "yesterday", "tonight", "morning", "evening", "weekend", "thinking",
    "looking", "getting", "talking", "working", "watching", "reading",
    "writing", "posting", "thought", "believe", "remember", "understand",
    "anybody", "shouldn", "también",
    "después", "entonces", "siempre", "todavía", "pourquoi", "toujours",
    "beaucoup", "maintenant", "quelque", "vielleicht", "trotzdem",
    "natürlich", "wirklich", "sondern", "eigentlich", "obrigado",
    "enquanto", "portanto", "qualquer", "davvero", "comunque", "insomma",
})

_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)


def _digest(values) -> str:
    return hashlib.sha256("\n".join(sorted(values)).encode("utf-8")).hexdigest()


GENERIC_HOSTS_SHA256 = _digest(GENERIC_HOSTS)
LONG_WORD_STOPLIST_SHA256 = _digest(LONG_WORD_STOPLIST)

FROZEN_PARAMETERS = {
    "rule_version": RULE_VERSION,
    "amendment": AMENDMENT,
    "tau_e": TAU_E,
    "lexical_jaccard_min": shadow.LEXICAL_JACCARD_MIN,
    "repeat_top1_max": jev.REPEAT_TOP1_MAX,
    "repeat_gap_max": jev.REPEAT_GAP_MAX,
    "ambiguity_min_median_p": jev.AMBIGUITY_MIN_MEDIAN_P,
    "generic_hosts_sha256": GENERIC_HOSTS_SHA256,
    "long_word_stoplist_sha256": LONG_WORD_STOPLIST_SHA256,
    "admitted_anchors": ["shared_url_domain", "shared_hashtag",
                         "lexical_overlap"],
    "validation_only_features": ["shared_distinctive_token.numeric",
                                 "shared_distinctive_token.long_word"],
}


def rule_digest() -> str:
    """sha256 over the frozen V0.1 parameters; binds a run to this rule."""
    return hashlib.sha256(
        json.dumps(FROZEN_PARAMETERS, sort_keys=True).encode("utf-8")).hexdigest()


RULE_DIGEST = None  # set below, after the helper is defined


def collapse(relation: Optional[str]) -> Optional[str]:
    """§5.4 decision collapse."""
    if relation in DEPENDENT:
        return "dependent"
    if relation in NOT_DEPENDENT:
        return "not_dependent"
    if relation in NO_WEIGHT:
        return "no_weight"
    return None


# --- context (never evidence) ---------------------------------------------------

@dataclass(frozen=True)
class Context:
    """Candidate-generation / context signals (§2.A). ``structural_lineage``
    is always true for a candidate and is recorded for completeness only."""
    relation_type: str
    b_text_empty: bool
    dt_seconds: Optional[int]
    thread_depth_b: Optional[int]
    langs_a: Tuple[str, ...] = ()
    langs_b: Tuple[str, ...] = ()
    structural_lineage: bool = True


def context_from_packet(packet: dict) -> Context:
    return Context(
        relation_type=packet["relation_type"],
        b_text_empty=not (packet.get("text_b") or "").strip(),
        dt_seconds=packet.get("dt_seconds"),
        thread_depth_b=packet.get("thread_depth_b"),
        langs_a=tuple(packet.get("langs_a") or ()),
        langs_b=tuple(packet.get("langs_b") or ()),
    )


# --- admitted anchors (evidence) ----------------------------------------------

@dataclass(frozen=True)
class AnchorEvidence:
    """V0.1 admitted text anchors. Computed from the two packet texts only;
    no context signal is an input."""
    shared_url_domain: bool
    shared_hashtag: bool
    lexical_overlap: bool
    lexical_jaccard: float

    @property
    def present(self) -> bool:
        return self.shared_url_domain or self.shared_hashtag or self.lexical_overlap

    @property
    def anchor_type(self) -> str:
        kinds = [k for k in ("shared_url_domain", "shared_hashtag",
                             "lexical_overlap") if getattr(self, k)]
        if not kinds:
            return "none"
        return kinds[0] if len(kinds) == 1 else "multiple"


def _non_generic_domains(text: str) -> set:
    return {d for d in shadow._domains(text) if d not in GENERIC_HOSTS}


def anchors(text_a: str, text_b: str) -> AnchorEvidence:
    """Admitted anchors over (already redacted, truncated) packet texts."""
    da, db = _non_generic_domains(text_a), _non_generic_domains(text_b)
    jac = shadow.lexical_jaccard(text_a, text_b)
    return AnchorEvidence(
        shared_url_domain=bool(da & db),
        shared_hashtag=shadow.shared_hashtag(text_a, text_b),
        lexical_overlap=jac >= shadow.LEXICAL_JACCARD_MIN,
        lexical_jaccard=jac,
    )


def anchors_from_packet(packet: dict) -> AnchorEvidence:
    return anchors(packet.get("text_a") or "", packet.get("text_b") or "")


# --- validation-only instrumentation ---------------------------------------------

@dataclass(frozen=True)
class DistinctiveTokenFeature:
    """``shared_distinctive_token``: VALIDATION ONLY (Amendment C §2.B, §9.2).
    Records booleans and counts only, never the tokens (which are text)."""
    numeric: bool
    long_word: bool
    numeric_count: int
    long_word_count: int
    validation_only: bool = True


def _tokens(text: str) -> set:
    return {t.casefold() for t in _TOKEN_RE.findall(text)}


def distinctive_token_feature(text_a: str, text_b: str) -> DistinctiveTokenFeature:
    shared = _tokens(text_a) & _tokens(text_b)
    numeric = {t for t in shared if t.isdigit() and len(t) >= 2}
    long_word = {t for t in shared
                 if t.isalpha() and len(t) >= 7 and t not in LONG_WORD_STOPLIST}
    return DistinctiveTokenFeature(
        numeric=bool(numeric), long_word=bool(long_word),
        numeric_count=len(numeric), long_word_count=len(long_word))


# --- semantic view of one adjudication -------------------------------------------

@dataclass(frozen=True)
class SemanticView:
    status: str
    first_probabilities: Optional[Dict[str, float]]
    first_argmax: Optional[str]
    repeat_triggered: bool
    replicates_spent: int
    median_argmax: Optional[str]
    median_ambiguity_set: Tuple[str, ...]


def semantic_view(adj: jev.Adjudication,
                  options: Sequence[str] = shadow.RELATION_OPTIONS) -> SemanticView:
    """Reduce an adjudication to the facts the rule may read. The first call
    decides ``J_explicit``; replicates can only downgrade."""
    records = list(adj.records or [])
    first = records[0] if records else None
    ok = adj.status == jev.STATUS_OK and first is not None \
        and first.get("status") == jev.STATUS_OK and first.get("probabilities")
    if not ok:
        return SemanticView(status=adj.status if adj.status != jev.STATUS_OK
                            else jev.STATUS_CONTRACT_ERROR,
                            first_probabilities=None, first_argmax=None,
                            repeat_triggered=False, replicates_spent=0,
                            median_argmax=None, median_ambiguity_set=())
    probs = {o: float(first["probabilities"][o]) for o in options}
    return SemanticView(
        status=jev.STATUS_OK,
        first_probabilities=probs,
        first_argmax=jev.recompute_argmax(probs, options),
        repeat_triggered=jev.repeat_triggered(probs, options),
        replicates_spent=sum(1 for r in records[1:] if jev.network_attempted(r)),
        median_argmax=adj.argmax,
        median_ambiguity_set=tuple(adj.ambiguity_set or ()),
    )


def j_explicit(sem: SemanticView) -> bool:
    """§3: ok, no replicate trigger or spend, first-call recomputed argmax is
    ``dependent_direct`` with p >= TAU_E."""
    return (sem.status == jev.STATUS_OK
            and not sem.repeat_triggered
            and sem.replicates_spent == 0
            and sem.first_argmax == "dependent_direct"
            and sem.first_probabilities["dependent_direct"] >= TAU_E)


# --- the rule ------------------------------------------------------------------

@dataclass(frozen=True)
class Outcome:
    disposition: str
    rule_step: int
    review_routes: Tuple[str, ...]
    j_explicit: bool
    anchor_present: bool
    rule_version: str = RULE_VERSION

    @property
    def review_route(self) -> str:
        return self.review_routes[0] if self.review_routes else ROUTE_NONE


def review_routes(anchor: AnchorEvidence, sem: SemanticView) -> Tuple[str, ...]:
    jx = j_explicit(sem)
    routes: List[str] = []
    if sem.status != jev.STATUS_OK:
        routes.append(ROUTE_SEMANTIC_UNAVAILABLE)
    else:
        groups = {collapse(o) for o in sem.median_ambiguity_set}
        if len(groups) > 1 or sem.repeat_triggered:
            routes.append(ROUTE_AMBIGUITY)
    if anchor.present != jx:
        routes.append(ROUTE_ANCHOR_SEMANTIC_CONFLICT)
    if sem.status == jev.STATUS_OK and (
            sem.median_argmax == "dependent_indirect"
            or (sem.median_argmax in DEPENDENT and not anchor.present)):
        routes.append(ROUTE_INFERRED_CLAIM)
    return tuple(routes)


def decide(context: Context, anchor: AnchorEvidence,
           sem: SemanticView) -> Outcome:
    """Amendment C §3, evaluated in order. Deliberately takes no
    distinctive-token argument."""
    jx = j_explicit(sem)
    routes = review_routes(anchor, sem)

    def out(disposition, step):
        return Outcome(disposition=disposition, rule_step=step,
                       review_routes=routes, j_explicit=jx,
                       anchor_present=anchor.present)

    if sem.status != jev.STATUS_OK:
        return out(SEMANTIC_UNAVAILABLE, 1)
    if context.relation_type == "repost" and context.b_text_empty:
        return out(NOT_QUALIFIED, 2)
    if anchor.present and jx:
        return out(QUALIFIED_EXPLICIT, 3)
    if sem.median_argmax in UNDERDETERMINING_ARGMAX or sem.replicates_spent > 0:
        return out(UNDERDETERMINED, 4)
    return out(NOT_QUALIFIED, 5)


def evaluate_packet(packet: dict, adj: jev.Adjudication) -> dict:
    """Full V0.1 record for one edge: context, admitted anchors, the rule
    outcome, and (separately, validation-only) the distinctive-token
    feature. Contains no text."""
    ctx = context_from_packet(packet)
    anchor = anchors_from_packet(packet)
    sem = semantic_view(adj)
    outcome = decide(ctx, anchor, sem)
    token = distinctive_token_feature(packet.get("text_a") or "",
                                      packet.get("text_b") or "")
    return {
        "rule_version": RULE_VERSION,
        "disposition": outcome.disposition,
        "rule_step": outcome.rule_step,
        "review_route": outcome.review_route,
        "review_routes": list(outcome.review_routes),
        "j_explicit": outcome.j_explicit,
        "anchor_present": outcome.anchor_present,
        "anchors": {**asdict(anchor), "anchor_type": anchor.anchor_type},
        "context": {k: v for k, v in asdict(ctx).items()},
        "semantic": {
            "status": sem.status, "first_argmax": sem.first_argmax,
            "first_p_dependent_direct": (sem.first_probabilities or {}).get(
                "dependent_direct"),
            "repeat_triggered": sem.repeat_triggered,
            "replicates_spent": sem.replicates_spent,
            "median_argmax": sem.median_argmax,
            "median_ambiguity_set": list(sem.median_ambiguity_set),
        },
        "validation_only": {
            "shared_distinctive_token": asdict(token),
        },
    }


RULE_DIGEST = rule_digest()
