"""Live shadow semantic sidecar (LW-JEV-SHADOW-v0, DESIGN §5).

Named analytic question (frozen, doctrine requirement):

    For candidate post-relation edges that already have structural
    evidence (reply/quote/repost lineage), does author-blind semantic
    adjudication agree with the structural channel, at what ambiguity
    rate, and at what apparent false-attribution rate — as qualification
    evidence only?

Boundaries (DESIGN §8): no post text is persisted (digests only); raw
text is held in memory for the duration of the call; no account-level
semantic score; no semantic result feeds suppression/mute/moderation/
admission/collection policy; no public output. The sidecar shares no
tables with the main DB and is killable via env without migration or
data repair (§5.8).

Daemon architecture follows discovery_stream.py: Jetstream consumer with
its OWN sidecar cursor key (never jetstream_discovery_cursor), reconnect
loop, single-writer worker, WAL refresh discipline, ops heartbeat,
SIGTERM handling. Jev never discovers subjects — candidate edges come
from structural record fields only (§5.2).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import sys
import time
import uuid
from collections import Counter, OrderedDict, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import websockets
import websockets.exceptions

from . import receipts as receipts_mod
from . import semantic_shadow_db as sdb
from . import semantic_shadow_jev as jev
from .config import Config
from .utils import format_ts, get_git_commit, hash_sha256, now_utc, parse_ts, stable_json

log = logging.getLogger(__name__)

# --- frozen vocabulary and thresholds (DESIGN §5.4, §5.5) ---------------------

RELATION_OPTIONS = (
    "dependent_direct",
    "dependent_indirect",
    "independent_lexical",
    "unrelated",
    "underdetermined",
)
DEPENDENT_RELATIONS = frozenset({"dependent_direct", "dependent_indirect"})
DECISION_FAMILY = "edge_relation"
PACKET_KIND = "labelwatch.edge_evidence/v0"

TEXT_MAX_CHARS = 500
LEXICAL_JACCARD_MIN = 0.20  # §5.5, frozen
SEMANTIC_MIN_MEDIAN_P = 0.50  # §5.5, frozen
TEMPORAL_MAX_SECONDS = 3600  # §5.5, frozen
SHINGLE_N = 5  # deterministic character shingle size (§5.3)
CHANNEL_ORDER = ("structural_lineage", "lexical", "semantic", "temporal")

# Deterministic sampling (§5.2): sha256(edge_id || day) mod M < cap-scaled
# slot, targeting <= semantic_shadow_daily_cap adjudications/day. The slot
# limit is scaled from the cap against an assumed raw candidate volume; the
# persisted per-day adjudication counter is the hard backstop.
SAMPLING_MODULUS = 10000
ASSUMED_DAILY_CANDIDATES = 2_000_000

# --- environment and receipt conventions --------------------------------------

KILL_SWITCH_ENV = "LABELWATCH_SEMANTIC_SHADOW_DISABLED"  # precedent: CLIMATE_API_DISABLED
AXOLOTL_PATH_ENV = "LABELWATCH_AXOLOTL_PATH"

ACTIVATION_RECEIPT_KIND = "labelwatch.semantic_shadow_activation.v0"
RUN_MANIFEST_RECEIPT_KIND = "labelwatch.semantic_shadow_run_manifest.v0"
CLOSEOUT_RECEIPT_KIND = "labelwatch.semantic_shadow_closeout.v0"
EDGE_RECEIPT_KIND = "labelwatch.semantic_shadow_edge.v0"
EVAL_RECEIPT_KIND = "labelwatch.semantic_shadow_fixture_eval.v0"
EVAL_CALLS_RECEIPT_KIND = "labelwatch.semantic_shadow_fixture_eval_calls.v0"
HOLDOUT_CALLS_RECEIPT_KIND = "labelwatch.semantic_shadow_holdout_eval_calls.v1"
RECEIPT_SCHEMA_VERSION = 0

IMPLEMENTED_BY = "kimi-coder-subagent (JEV-DUAL-SHADOW-V0)"

JETSTREAM_URL = os.environ.get(
    "JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe")
WANTED_COLLECTIONS = ["app.bsky.feed.post", "app.bsky.feed.repost"]
CURSOR_SAVE_INTERVAL = 60
CURSOR_REWIND_US = 3_000_000  # 3s rewind on reconnect (discovery_stream parity)
STATS_INTERVAL = 60
WAL_REFRESH_INTERVAL = 600  # discovery_stream parity; see docs/OPS_HAZARDS.md
OPS_HEARTBEAT_INTERVAL = 30
EPISODE_REBUILD_INTERVAL = 900
PRUNE_INTERVAL = 3600
WORK_QUEUE_MAX = 200
DISK_FLOOR_GB = 14.0  # state.py pilot precedent

GATE_UNAVAILABLE = "axolotl_unavailable"
GATE_NON_LOAD_BEARING = "ambiguity_non_load_bearing"
GATE_LOAD_BEARING = "clarification_load_bearing"

DISPOSITION_SAMPLED_PENDING = "sampled_pending"
DISPOSITION_ADJUDICATED = "adjudicated"
DISPOSITION_SEMANTIC_UNAVAILABLE = "semantic_unavailable"
DISPOSITION_BUDGET_REFUSED = "budget_refused"


def shadow_enabled(cfg: Config, env=None) -> Tuple[bool, Optional[str]]:
    """Effective enable state: config flag AND not the env kill switch.
    Returns (enabled, disabled_reason)."""
    env = os.environ if env is None else env
    if env.get(KILL_SWITCH_ENV) == "1":
        return False, "kill_switch"
    if not cfg.semantic_shadow_enabled:
        return False, "flag_off"
    return True, None


def default_receipts_dir(sidecar_path: str) -> str:
    return os.path.join(
        os.path.dirname(os.path.abspath(sidecar_path)), "semantic-shadow-receipts"
    )


# --- text utilities (packet construction, §5.3) --------------------------------

_REDACTIONS = (
    (re.compile(r"at://\S+"), "[at-uri]"),
    (re.compile(r"\bdid:[a-z]+:[A-Za-z0-9._:%-]+"), "[did]"),
    (re.compile(r"@[A-Za-z0-9][A-Za-z0-9.-]*\.[A-Za-z]{2,}"), "[mention]"),
)

# Patterns that must never appear in a serialized outbound packet. The
# packet builder redacts them from text; the tripwire verifies. Handles
# are matched in @-mention form; bare domains are content (the
# shared_url_domain feature needs them) and are not account identifiers
# on their own.
_TRIPWIRE_PATTERNS = (
    re.compile(r"did:[a-z]+:[A-Za-z0-9._:%-]+"),
    re.compile(r"at://"),
    re.compile(r"@[A-Za-z0-9][A-Za-z0-9.-]*\.[A-Za-z]{2,}"),
)

_URL_RE = re.compile(r"https?://([^\s/)\]>\"']+)", re.IGNORECASE)
_HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")


def redact_identifiers(text: str) -> str:
    """Remove DIDs, AT-URIs and @-mention handles from text before it can
    leave the process (§5.3 author-blindness)."""
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def packet_privacy_scan(packet: dict) -> List[str]:
    """Privacy tripwire (§5.3): scan the serialized outbound packet for
    DID/handle/AT-URI patterns. Returns the list of hits; must be empty
    before the packet may be sent."""
    serialized = stable_json(packet)
    hits: List[str] = []
    for pattern in _TRIPWIRE_PATTERNS:
        hits.extend(m.group(0) for m in pattern.finditer(serialized))
    return hits


def _truncate(text: str) -> str:
    return text[:TEXT_MAX_CHARS]


def _normalize(text: str) -> str:
    return " ".join(text.lower().split())


def _shingles(text: str, n: int = SHINGLE_N) -> frozenset:
    if not text:
        return frozenset()
    if len(text) < n:
        return frozenset({text})
    return frozenset(text[i:i + n] for i in range(len(text) - n + 1))


def lexical_jaccard(text_a: str, text_b: str) -> float:
    """Deterministic 5-gram character-shingle Jaccard over normalized text.
    Two empty texts yield 0.0 — no lexical support, by convention."""
    sa = _shingles(_normalize(text_a))
    sb = _shingles(_normalize(text_b))
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _registered_domain(url_netloc: str) -> str:
    host = url_netloc.split("@")[-1].split(":")[0].lower()
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def _domains(text: str) -> set:
    return {
        _registered_domain(m.group(1))
        for m in _URL_RE.finditer(text)
        if m.group(1)
    }


def _hashtags(text: str) -> set:
    return {m.group(1).lower() for m in _HASHTAG_RE.finditer(text)}


def shared_url_domain(text_a: str, text_b: str) -> bool:
    da, db = _domains(text_a), _domains(text_b)
    return bool(da and db and da & db)


def shared_hashtag(text_a: str, text_b: str) -> bool:
    ha, hb = _hashtags(text_a), _hashtags(text_b)
    return bool(ha and hb and ha & hb)


def _ts_seconds(entry: dict) -> Optional[float]:
    """Best-effort event time: record createdAt, else Jetstream time_us."""
    created = entry.get("createdAt")
    if created:
        try:
            return parse_ts(created).timestamp()
        except (ValueError, TypeError):
            pass
    time_us = entry.get("time_us")
    if isinstance(time_us, (int, float)):
        return time_us / 1_000_000
    return None


# --- recent-post cache (§5.2) ----------------------------------------------------

class RecentPostCache:
    """Bounded in-memory recent-post cache: TTL = semantic_shadow_window_minutes,
    size = semantic_shadow_cache_size. Ephemeral by design — post text lives
    only here and for the duration of an adjudication call. A's record is
    fetched from this cache only; there is no network fetch of posts."""

    def __init__(self, max_size: int, ttl_minutes: int):
        self.max_size = max_size
        self.ttl_s = ttl_minutes * 60
        self._posts: "OrderedDict[str, dict]" = OrderedDict()

    def _evict_expired(self, now_mono: float) -> None:
        while self._posts:
            _, entry = next(iter(self._posts.items()))
            if now_mono - entry["stored_mono"] > self.ttl_s:
                self._posts.popitem(last=False)
            else:
                break

    def put(self, uri: str, entry: dict, now_mono: Optional[float] = None) -> None:
        now_mono = time.monotonic() if now_mono is None else now_mono
        self._evict_expired(now_mono)
        entry["stored_mono"] = now_mono
        self._posts[uri] = entry
        self._posts.move_to_end(uri)
        while len(self._posts) > self.max_size:
            self._posts.popitem(last=False)

    def get(self, uri: str, now_mono: Optional[float] = None) -> Optional[dict]:
        now_mono = time.monotonic() if now_mono is None else now_mono
        entry = self._posts.get(uri)
        if entry is None:
            return None
        if now_mono - entry["stored_mono"] > self.ttl_s:
            self._posts.pop(uri, None)
            return None
        return entry

    def thread_depth(self, parent_uri: Optional[str], limit: int = 16) -> Optional[int]:
        """Ancestors of B visible in the cache, A counting as depth 1.
        Partial by construction — the cache is a 15-minute window, not the
        thread. None when B is not a reply."""
        if not parent_uri:
            return None
        depth = 0
        uri = parent_uri
        while uri and depth < limit:
            entry = self.get(uri)
            if entry is None:
                break
            depth += 1
            uri = entry.get("parent_uri")
        return depth

    def __len__(self) -> int:
        return len(self._posts)


# --- structural candidate extraction (§5.2) --------------------------------------

def make_edge_id(relation_type: str, uri_a: str, uri_b: str) -> str:
    return hash_sha256(f"{relation_type}|{uri_a}|{uri_b}")


def extract_candidates(msg: dict) -> List[dict]:
    """Candidate edges from structural record fields only (§5.2).

    B replies to A (record.reply.parent/root), B quotes A
    (embed.record.uri), B reposts A (subject.uri). Aggressive early drop
    for anything else. No DIDs survive this function into persistence —
    they are used only to construct B's own URI in memory."""
    if msg.get("kind") != "commit":
        return []
    commit = msg.get("commit") or {}
    if commit.get("operation") != "create":
        return []
    collection = commit.get("collection")
    if collection not in WANTED_COLLECTIONS:
        return []
    record = commit.get("record") or {}
    did = msg.get("did")
    rkey = commit.get("rkey")
    if not did or not rkey:
        return []
    uri_b = f"at://{did}/{collection}/{rkey}"
    time_us = msg.get("time_us")

    if collection == "app.bsky.feed.repost":
        uri_a = (record.get("subject") or {}).get("uri")
        if not uri_a:
            return []
        return [{
            "relation_type": "repost",
            "uri_a": uri_a,
            "uri_b": uri_b,
            "b": {"text": "", "langs": [],
                  "createdAt": record.get("createdAt"), "time_us": time_us},
            "parent_uri": None,
        }]

    # app.bsky.feed.post — cacheable as a future parent regardless of
    # candidacy; candidates below.
    out = []
    reply = record.get("reply") or {}
    parent_uri = (reply.get("parent") or {}).get("uri") or (
        (reply.get("root") or {}).get("uri"))
    embed = record.get("embed") or {}
    quote_uri = None
    embed_record = embed.get("record")
    if isinstance(embed_record, dict):
        quote_uri = embed_record.get("uri")
        if quote_uri is None and isinstance(embed_record.get("record"), dict):
            # app.bsky.embed.recordWithMedia view shape
            quote_uri = embed_record["record"].get("uri")
    if parent_uri:
        out.append({
            "relation_type": "reply",
            "uri_a": parent_uri,
            "uri_b": uri_b,
            "b": {"text": record.get("text") or "",
                  "langs": record.get("langs") or [],
                  "createdAt": record.get("createdAt"), "time_us": time_us},
            "parent_uri": parent_uri,
        })
    if quote_uri:
        out.append({
            "relation_type": "quote",
            "uri_a": quote_uri,
            "uri_b": uri_b,
            "b": {"text": record.get("text") or "",
                  "langs": record.get("langs") or [],
                  "createdAt": record.get("createdAt"), "time_us": time_us},
            "parent_uri": parent_uri,
        })
    return out


def cache_entry_for(msg: dict) -> Optional[Tuple[str, dict]]:
    """(uri, entry) for a post create, or None. Every post is cached so
    later replies/quotes can find their A; reposts are never targets."""
    if msg.get("kind") != "commit":
        return None
    commit = msg.get("commit") or {}
    if commit.get("operation") != "create":
        return None
    if commit.get("collection") != "app.bsky.feed.post":
        return None
    did = msg.get("did")
    rkey = commit.get("rkey")
    if not did or not rkey:
        return None
    record = commit.get("record") or {}
    reply = record.get("reply") or {}
    parent_uri = (reply.get("parent") or {}).get("uri") or (
        (reply.get("root") or {}).get("uri"))
    uri = f"at://{did}/app.bsky.feed.post/{rkey}"
    entry = {
        "text": record.get("text") or "",
        "langs": record.get("langs") or [],
        "createdAt": record.get("createdAt"),
        "time_us": msg.get("time_us"),
        "parent_uri": parent_uri,
    }
    return uri, entry


def process_message(msg: dict, cache: RecentPostCache) -> Tuple[List[dict], int]:
    """One Jetstream message: fill the cache, return (resolved candidates,
    dropped-for-missing-A count). A is resolved from the local cache only
    (§5.2 — no network fetch of posts)."""
    candidates: List[dict] = []
    dropped = 0
    entry = cache_entry_for(msg)
    if entry is not None:
        cache.put(entry[0], entry[1])
    for cand in extract_candidates(msg):
        a = cache.get(cand["uri_a"])
        if a is None:
            dropped += 1
            continue
        cand["a"] = a
        if cand["relation_type"] == "reply":
            cand["thread_depth_b"] = cache.thread_depth(cand["parent_uri"])
        else:
            cand["thread_depth_b"] = None
        ts_b = _ts_seconds(cand["b"])
        ts_a = _ts_seconds(a)
        if ts_b is None or ts_a is None:
            dropped += 1
            continue
        cand["dt_seconds"] = int(abs(ts_b - ts_a))
        cand["day"] = datetime.fromtimestamp(ts_b, tz=timezone.utc).strftime("%Y-%m-%d")
        candidates.append(cand)
    return candidates, dropped


# --- evidence packet (§5.3) --------------------------------------------------------

def build_packet(cand: dict) -> dict:
    """Author-blind evidence packet. No DIDs, handles, display names,
    follower counts, account history, or Labelwatch dials; identifiers in
    text are redacted before packetization (the tripwire verifies)."""
    text_a = redact_identifiers(_truncate(cand["a"].get("text") or ""))
    text_b = redact_identifiers(_truncate(cand["b"].get("text") or ""))
    return {
        "packet_kind": PACKET_KIND,
        "relation_type": cand["relation_type"],
        "text_a": text_a,
        "text_b": text_b,
        "dt_seconds": cand["dt_seconds"],
        "langs_a": [str(l) for l in (cand["a"].get("langs") or [])][:8],
        "langs_b": [str(l) for l in (cand["b"].get("langs") or [])][:8],
        "shared_url_domain": shared_url_domain(text_a, text_b),
        "shared_hashtag": shared_hashtag(text_a, text_b),
        "lexical_jaccard": lexical_jaccard(text_a, text_b),
        "thread_depth_b": cand.get("thread_depth_b"),
    }


# The evidence packet travels as the request ``state`` and the option
# descriptions as ``criteria`` (documented System One shape); the
# instructions carry no packet content.
_QUESTION_TEMPLATE = (
    "Two social-media posts, A and B, are structurally linked: {relation_clause}. "
    "You see no author identity, no account history, and no network metadata for "
    "either post. Adjudicate the semantic relation of B to A from the evidence "
    "packet (the state) only. Choose exactly one option."
)

RELATION_CRITERIA = {
    "dependent_direct": "B explicitly responds to, references, or continues the "
    "specific content of A.",
    "dependent_indirect": "B depends on A's content obliquely — irony, subreference, "
    "implied continuation — without explicitly restating it.",
    "independent_lexical": "B shares surface wording with A but does not depend on "
    "A's content.",
    "unrelated": "B shows no semantic dependence on A.",
    "underdetermined": "the evidence shown cannot support any of the above.",
}

_RELATION_CLAUSES = {
    "reply": "B is a reply to A",
    "quote": "B quotes A",
    "repost": "B is a repost of A",
}


def build_question(packet: dict) -> str:
    return _QUESTION_TEMPLATE.format(
        relation_clause=_RELATION_CLAUSES[packet["relation_type"]],
    )


# --- deterministic sampling (§5.2) --------------------------------------------------

def sample_slot(edge_id: str, day: str, modulus: int = SAMPLING_MODULUS) -> int:
    return int(hash_sha256(f"{edge_id}|{day}"), 16) % modulus


def slot_limit(
    daily_cap: int,
    modulus: int = SAMPLING_MODULUS,
    assumed_candidates: int = ASSUMED_DAILY_CANDIDATES,
) -> int:
    """Cap-scaled slot threshold: with the assumed raw candidate volume, a
    slot below this limit admits ~daily_cap edges/day. The persisted per-day
    adjudication counter remains the hard cap."""
    return max(1, (daily_cap * modulus) // assumed_candidates)


def in_sample(edge_id: str, day: str, daily_cap: int) -> bool:
    return sample_slot(edge_id, day) < slot_limit(daily_cap)


# --- axolotl gate (§5.6) -----------------------------------------------------------

def resolve_axolotl(env=None):
    """Clean import-or-degrade (§5.6). The path comes from the
    LABELWATCH_AXOLOTL_PATH env var — no vendoring, no pip dependency.
    Returns the module or None; None means gate state axolotl_unavailable
    and the §2.6 repeat policy applies directly."""
    env = os.environ if env is None else env
    path = (env.get(AXOLOTL_PATH_ENV) or "").strip()
    if path and path not in sys.path:
        sys.path.insert(0, path)
    try:
        import axolotl
        return axolotl
    except Exception:
        return None


def axolotl_repeat_decision(ax, plausible: List[str]) -> Tuple[str, dict]:
    """Frame the unknown edge_relation over the plausible set; actions
    {count_toward_episode, exclude_edge}; permission: count only for
    dependent_* relations. If all plausible relations map to the same
    action the ambiguity is non-load-bearing and no repeat is spent.
    Axolotl never sees a scalar confidence."""
    domain = tuple(sorted(set(plausible)))
    if not domain:
        # Contract validation guarantees at least one option >= 0.10 in
        # practice; an empty plausible set is treated as non-load-bearing
        # rather than handed a malformed frame.
        return GATE_NON_LOAD_BEARING, {"plausible_set": [], "note": "empty_plausible_set"}
    frame = ax.ReasoningFrame(
        unknowns=(ax.Unknown(
            id="edge_relation",
            domain=domain,
            kind=ax.UnknownKind.FACT,
            provenance=ax.Provenance(
                supplied_by=jev.COMPONENT,
                detail="plausible set: options with replicate-median p >= 0.10"),
        ),),
        permission_constraints=(ax.PermissionConstraint(
            name="count_only_for_dependent_relations",
            permits=lambda world, action: (
                action == "exclude_edge"
                or world["edge_relation"] in DEPENDENT_RELATIONS
            ),
            provenance=ax.Provenance(supplied_by="jev-dual-shadow-v0 DESIGN §5.6"),
        ),),
        source="LW-JEV-SHADOW-v0 §5.6",
    )
    universe = ax.ActionUniverse(
        actions=("count_toward_episode", "exclude_edge"),
        outside_contract=("suppress", "mute", "moderate", "publish"),
        source="LW-JEV-SHADOW-v0 §5.6; §8 hard boundaries",
    )
    evaluation = ax.evaluate(frame, universe)
    load_bearing = evaluation.status != ax.Status.READY
    state = GATE_LOAD_BEARING if load_bearing else GATE_NON_LOAD_BEARING
    detail = {
        "plausible_set": list(domain),
        "axolotl_status": evaluation.status.value,
        "stop_reason": evaluation.stop_reason.name,
        "admissible_world_count": evaluation.admissible_world_count,
        "partitions": [
            {
                "permitted_actions": list(p.permitted_actions),
                "worlds": len(p.worlds),
            }
            for p in evaluation.partitions
        ],
    }
    return state, detail


# --- adjudication (client + gate; DB-free, runs off the event loop) ----------------

def adjudicate_packet(
    packet: dict,
    *,
    edge_id: str,
    day: str,
    api_key: Optional[str],
    flag_on: bool,
    max_calls: int,
    seed_state: Tuple[int, Optional[str], bool],
    axolotl_mod=None,
    record_sink: Optional[Callable[[dict], None]] = None,
) -> Tuple[jev.Adjudication, jev.MemoryClientState, dict]:
    """Run the Jev adjudication for one sampled edge.

    ``seed_state`` carries the durable daily counter / model anchor /
    model-stop flag into an in-memory client state so this function holds
    no DB handle and can run in a worker thread; the caller merges the
    resulting state back into the sidecar on the event-loop thread.
    ``record_sink`` receives each per-call record as it is produced.
    """
    calls_today, anchor, stopped = seed_state
    mem = jev.MemoryClientState(sink=record_sink)
    mem.set_calls_today(day, calls_today)
    if anchor:
        mem.set_model_anchor(day, anchor)
    if stopped:
        mem.set_model_stopped(day)
    client = jev.JevContractClient(
        decision_family=DECISION_FAMILY,
        options=RELATION_OPTIONS,
        api_key=api_key,
        flag_on=flag_on,
        max_calls=max_calls,
        state=mem,
    )
    gate_info: Dict[str, Any] = {
        "gate_state": GATE_UNAVAILABLE,
        "plausible_set": None,
        "detail": None,
    }
    repeat_decider = None
    if axolotl_mod is not None:
        def repeat_decider(records):  # type: ignore[no-redef]
            probs = records[0].get("probabilities") or {}
            plausible = [
                o for o in RELATION_OPTIONS
                if probs.get(o, 0.0) >= jev.AMBIGUITY_MIN_MEDIAN_P
            ]
            state, detail = axolotl_repeat_decision(axolotl_mod, plausible)
            gate_info.update(
                gate_state=state, plausible_set=plausible, detail=detail)
            return state == GATE_LOAD_BEARING
    adj = client.adjudicate(
        question_id=f"edge:{edge_id}",
        prompt=build_question(packet),
        day=day,
        repeat_decider=repeat_decider,
        state=packet,
        criteria=RELATION_CRITERIA,
    )
    return adj, mem, gate_info


# --- channels, qualification, fragility (§5.5) --------------------------------------

def semantic_channel(adj: jev.Adjudication) -> Optional[bool]:
    """True iff the replicate-median argmax is in dependent_* with median
    p >= 0.50. None when there is no semantic receipt (§5.8: any Jev
    failure means no semantic receipt for that edge)."""
    if adj.status != jev.STATUS_OK or not adj.median_probabilities:
        return None
    argmax = adj.argmax
    return bool(
        argmax in DEPENDENT_RELATIONS
        and adj.median_probabilities[argmax] >= SEMANTIC_MIN_MEDIAN_P
    )


def compute_channels(packet: dict, adj: jev.Adjudication) -> Dict[str, Optional[bool]]:
    return {
        # Always true for candidates — structural lineage is the prefilter.
        "structural_lineage": True,
        "lexical": packet["lexical_jaccard"] >= LEXICAL_JACCARD_MIN,
        "semantic": semantic_channel(adj),
        "temporal": packet["dt_seconds"] <= TEMPORAL_MAX_SECONDS,
    }


def shadow_qualified(channels: Dict[str, Optional[bool]]) -> bool:
    """Shadow-qualified only if >=2 channels agree (§5.5). A missing
    semantic channel (None) counts as not agreeing."""
    return sum(1 for v in channels.values() if v) >= 2


def fragility_flags(channels: Dict[str, Optional[bool]]) -> Dict[str, bool]:
    """Leave-one-channel-out: for each channel, would qualification survive
    removing it (§5.5)?"""
    out = {}
    for name in CHANNEL_ORDER:
        reduced = {k: v for k, v in channels.items() if k != name}
        out[f"drop_{name}"] = shadow_qualified(reduced)
    return out


def channel_pattern(channels: Dict[str, Optional[bool]]) -> str:
    """The channel-agreement pattern: names of agreeing channels in the
    frozen channel order, joined by '+'."""
    return "+".join(name for name in CHANNEL_ORDER if channels.get(name))


def rebuild_episode_summaries(conn, day: str, now: Optional[datetime] = None) -> int:
    """Aggregate shadow-qualified edges around a shared A (§5.5). Origins
    stay set-valued (uri_b digests); no account fields; probabilities are
    never summed across edges (correlated evidence)."""
    rows = conn.execute(
        "SELECT uri_a_sha256, uri_b_sha256, channel_structural, channel_lexical, "
        "channel_semantic, channel_temporal FROM edge "
        "WHERE day = ? AND disposition = ? AND shadow_qualified = 1",
        (day, DISPOSITION_ADJUDICATED),
    ).fetchall()
    groups: Dict[str, dict] = defaultdict(
        lambda: {"patterns": Counter(), "origins": set(), "count": 0}
    )
    for r in rows:
        g = groups[r["uri_a_sha256"]]
        channels = {
            "structural_lineage": bool(r["channel_structural"]),
            "lexical": bool(r["channel_lexical"]),
            "semantic": (
                None if r["channel_semantic"] is None else bool(r["channel_semantic"])
            ),
            "temporal": bool(r["channel_temporal"]),
        }
        g["patterns"][channel_pattern(channels)] += 1
        g["origins"].add(r["uri_b_sha256"])
        g["count"] += 1
    ts = format_ts(now or now_utc())
    for uri_a_sha, g in sorted(groups.items()):
        sdb.upsert_episode_summary(
            conn,
            episode_id=hash_sha256(f"episode|{day}|{uri_a_sha}"),
            day=day,
            root_uri_sha256=uri_a_sha,
            edge_count=g["count"],
            pattern_counts=dict(g["patterns"]),
            origin_digests=sorted(g["origins"]),
            updated_ts=ts,
        )
    conn.commit()
    return len(groups)


# --- receipts (§3) -------------------------------------------------------------------

def _write_receipt_file(receipts_dir: str, kind: str, receipt: dict) -> str:
    os.makedirs(receipts_dir, exist_ok=True)
    stamp = now_utc().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(receipts_dir, f"{kind}.{stamp}.{uuid.uuid4().hex[:8]}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(receipt, f, indent=2)
    return path


def write_activation_receipt(
    receipts_dir: str,
    cfg: Config,
    *,
    enabled: bool,
    disabled_reason: Optional[str],
    action: str,
    env=None,
) -> Tuple[dict, str]:
    """Activation receipt for BOTH flag states (§3, house file-receipt
    pattern). Written on every semantic-shadow invocation, on or off."""
    env = os.environ if env is None else env
    receipt = {
        "receipt_kind": ACTIVATION_RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": format_ts(now_utc()),
        "git_commit": get_git_commit(),
        "action": action,
        "enabled": enabled,
        "flag_on": cfg.semantic_shadow_enabled,
        "kill_switch": env.get(KILL_SWITCH_ENV) == "1",
        "disabled_reason": disabled_reason,
        "config_hash": receipts_mod.config_hash(cfg.to_receipt_dict()),
        "daily_cap": cfg.semantic_shadow_daily_cap,
        "window_minutes": cfg.semantic_shadow_window_minutes,
        "cache_size": cfg.semantic_shadow_cache_size,
        "authority": "observation only; no enforcement authority (DESIGN §1, §8)",
    }
    return receipt, _write_receipt_file(receipts_dir, ACTIVATION_RECEIPT_KIND, receipt)


def _module_digest(module) -> Optional[str]:
    path = getattr(module, "__file__", None)
    if not path:
        return None
    try:
        with open(path, "rb") as f:
            import hashlib
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return None


def build_run_manifest(cfg: Config, *, max_calls: int, axolotl_available: bool) -> dict:
    """Run manifest (§3): code digests, config hash, caps, thresholds,
    implementing-agent provenance."""
    from . import semantic_shadow as _self

    return {
        "receipt_kind": RUN_MANIFEST_RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": format_ts(now_utc()),
        "git_commit": get_git_commit(),
        "campaign": jev.CAMPAIGN,
        "component": jev.COMPONENT,
        "implemented_by": IMPLEMENTED_BY,
        "code_digests": {
            "semantic_shadow.py": _module_digest(_self),
            "semantic_shadow_jev.py": _module_digest(jev),
            "semantic_shadow_db.py": _module_digest(sdb),
        },
        "config_hash": receipts_mod.config_hash(cfg.to_receipt_dict()),
        "caps": {
            "daily_cap": cfg.semantic_shadow_daily_cap,
            "max_calls": max_calls,
            "window_minutes": cfg.semantic_shadow_window_minutes,
            "cache_size": cfg.semantic_shadow_cache_size,
        },
        "thresholds": {
            "lexical_jaccard_min": LEXICAL_JACCARD_MIN,
            "semantic_min_median_p": SEMANTIC_MIN_MEDIAN_P,
            "temporal_max_seconds": TEMPORAL_MAX_SECONDS,
            "repeat_top1_max": jev.REPEAT_TOP1_MAX,
            "repeat_gap_max": jev.REPEAT_GAP_MAX,
            "repeat_k": jev.REPEAT_K,
            "ambiguity_min_median_p": jev.AMBIGUITY_MIN_MEDIAN_P,
            "prob_sum_tolerance": jev.PROB_SUM_TOLERANCE,
            "sampling_modulus": SAMPLING_MODULUS,
            "assumed_daily_candidates": ASSUMED_DAILY_CANDIDATES,
            "slot_limit": slot_limit(cfg.semantic_shadow_daily_cap),
        },
        "relation_options": list(RELATION_OPTIONS),
        "axolotl": "available" if axolotl_available else "unavailable",
        "cost_rate_basis": (
            "$0.042 / 1M input tokens — unverified cookbook figure "
            "(JEV-CONTRACT-v0 §2.5)"
        ),
        "pid": os.getpid(),
        "authority": "observation only; no enforcement authority (DESIGN §1, §8)",
    }


def write_run_manifest(receipts_dir: str, manifest: dict) -> str:
    return _write_receipt_file(receipts_dir, RUN_MANIFEST_RECEIPT_KIND, manifest)


def write_closeout_receipt(
    receipts_dir: str,
    *,
    started_at: str,
    stop_reason: str,
    stats: "_Stats",
    conn=None,
    day: Optional[str] = None,
) -> Tuple[dict, str]:
    """Closeout receipt (§3): counts, tokens, cost, stop reason."""
    receipt: Dict[str, Any] = {
        "receipt_kind": CLOSEOUT_RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": format_ts(now_utc()),
        "git_commit": get_git_commit(),
        "campaign": jev.CAMPAIGN,
        "component": jev.COMPONENT,
        "started_at": started_at,
        "stop_reason": stop_reason,
        "counts": stats.as_dict(),
    }
    if conn is not None and day is not None:
        try:
            receipt["tokens_cost_today"] = {
                "calls": sdb.status_summary(conn, day)["calls_today"],
                "input_tokens": sdb.status_summary(conn, day)["input_tokens_today"],
                "output_tokens": sdb.status_summary(conn, day)["output_tokens_today"],
                "est_cost_usd": sdb.status_summary(conn, day)["est_cost_usd_today"],
                "model_anchor": sdb.status_summary(conn, day)["model_anchor_today"],
                "model_stopped": sdb.status_summary(conn, day)["model_stopped_today"],
            }
        except Exception:
            log.exception("closeout: failed to collect token/cost counters")
    return receipt, _write_receipt_file(receipts_dir, CLOSEOUT_RECEIPT_KIND, receipt)


# --- fixtures (§5.9) ------------------------------------------------------------------

FIXTURE_CLASSES = (
    "explicit_direct_reference",
    "subtweet",
    "irony",
    "vaguebooking",
    "unrelated_similarity",
    "structurally_linked_semantically_unrelated",
)
FIXTURE_REQUIRED_FIELDS = (
    "fixture_id", "fixture_class", "relation_type",
    "text_a", "text_b", "dt_seconds",
    "langs_a", "langs_b",
    "gold_relation", "acceptable_answers", "ambiguity_note",
)
FIXTURE_OPTIONAL_FIELDS = ("note",)
DEFAULT_FIXTURES_PATH = os.path.join("tests", "fixtures", "semantic_relations.jsonl")


def validate_fixture_record(obj: Any) -> List[str]:
    """Python validator for the semantic_relations fixture format; mirrors
    tests/fixtures/semantic_relations.schema.json. Returns a list of
    errors (empty = valid)."""
    errors: List[str] = []
    if not isinstance(obj, dict):
        return ["record is not an object"]
    for field_name in FIXTURE_REQUIRED_FIELDS:
        if field_name not in obj:
            errors.append(f"missing required field {field_name!r}")
    if errors:
        return errors
    known = set(FIXTURE_REQUIRED_FIELDS) | set(FIXTURE_OPTIONAL_FIELDS)
    for field_name in obj:
        if field_name not in known:
            errors.append(f"unknown field {field_name!r}")
    if not isinstance(obj["fixture_id"], str) or not obj["fixture_id"]:
        errors.append("fixture_id must be a non-empty string")
    if obj["fixture_class"] not in FIXTURE_CLASSES:
        errors.append(f"fixture_class {obj['fixture_class']!r} not in frozen class set")
    if obj["relation_type"] not in ("reply", "quote", "repost"):
        errors.append(f"relation_type {obj['relation_type']!r} not reply|quote|repost")
    for text_field in ("text_a", "text_b"):
        value = obj[text_field]
        if not isinstance(value, str):
            errors.append(f"{text_field} must be a string")
        elif len(value) > TEXT_MAX_CHARS:
            errors.append(f"{text_field} exceeds {TEXT_MAX_CHARS} chars")
        elif text_field == "text_a" and not value:
            errors.append("text_a must be non-empty")
        elif text_field == "text_b" and not value and obj["relation_type"] != "repost":
            errors.append("text_b may be empty only for repost fixtures")
    if not isinstance(obj["dt_seconds"], int) or isinstance(obj["dt_seconds"], bool) \
            or obj["dt_seconds"] < 0:
        errors.append("dt_seconds must be a non-negative integer")
    for langs_field in ("langs_a", "langs_b"):
        langs = obj[langs_field]
        if not isinstance(langs, list) or not all(isinstance(l, str) for l in langs):
            errors.append(f"{langs_field} must be a list of strings")
    if obj["gold_relation"] not in RELATION_OPTIONS:
        errors.append(f"gold_relation {obj['gold_relation']!r} not in §5.4 vocabulary")
    acceptable = obj["acceptable_answers"]
    if not isinstance(acceptable, list) or not acceptable:
        errors.append("acceptable_answers must be a non-empty list")
    else:
        bad = [a for a in acceptable if a not in RELATION_OPTIONS]
        if bad:
            errors.append(f"acceptable_answers outside §5.4 vocabulary: {bad}")
        if obj["gold_relation"] not in acceptable:
            errors.append("acceptable_answers must contain gold_relation")
    if not isinstance(obj["ambiguity_note"], str) or not obj["ambiguity_note"]:
        errors.append("ambiguity_note must be a non-empty string")
    if "note" in obj and not isinstance(obj["note"], str):
        errors.append("note must be a string")
    return errors


def validate_fixture_file(path: str) -> Tuple[List[dict], List[str]]:
    """Load + validate a fixtures JSONL file. Returns (records, errors)."""
    records: List[dict] = []
    errors: List[str] = []
    seen_ids = set()
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                errors.append(f"line {lineno}: invalid JSON: {e}")
                continue
            for err in validate_fixture_record(obj):
                errors.append(f"line {lineno}: {err}")
            if isinstance(obj, dict):
                fid = obj.get("fixture_id")
                if fid in seen_ids:
                    errors.append(f"line {lineno}: duplicate fixture_id {fid!r}")
                seen_ids.add(fid)
                records.append(obj)
    counts = Counter(r.get("fixture_class") for r in records)
    for cls in FIXTURE_CLASSES:
        if counts.get(cls, 0) < 10:
            errors.append(f"class {cls!r} has {counts.get(cls, 0)} pairs (need >= 10)")
    return records, errors


def fixture_to_candidate(record: dict) -> dict:
    """Fixture record -> candidate dict, exercising the real packetization
    path (redaction, jaccard, shared features)."""
    return {
        "relation_type": record["relation_type"],
        "a": {"text": record["text_a"], "langs": record["langs_a"]},
        "b": {"text": record["text_b"], "langs": record["langs_b"]},
        "dt_seconds": record["dt_seconds"],
        "thread_depth_b": None,
    }


class EvalCallLog:
    """Durable per-call evidence for ``eval-fixtures --live`` (§2.5, §3).

    One JSONL file per evaluation run in the receipts directory, named like
    every other receipt (``<kind>.<stamp>.<id>.jsonl``). Lines, in order:

    - ``header``: run identity, fixture file digest, caps, model alias.
    - ``call``: one per record the client produced, written, flushed and
      fsynced as it is produced. ``record`` is the unmodified §2.5 record.
      ``network_sent`` separates calls that reached TypeSafe from refusals
      before the network, and ``error_category`` names the failure class.
    - ``pair_result``: one per fixture pair, carrying enough to rebuild the
      eval result (medians, argmax, ambiguity set, gate, repeat decision).
    - ``closeout``: totals and completeness. A file without a closeout is
      an interrupted run; ``evaluation_complete`` is true only when every
      pair was adjudicated.

    No text (records carry digests only), no headers, no key material.
    """

    def __init__(self, receipts_dir: str, *, header: dict,
                 kind: str = EVAL_CALLS_RECEIPT_KIND):
        os.makedirs(receipts_dir, exist_ok=True)
        self.kind = kind
        self.eval_run_id = uuid.uuid4().hex
        stamp = now_utc().strftime("%Y%m%dT%H%M%SZ")
        self.path = os.path.join(
            receipts_dir, f"{kind}.{stamp}.{self.eval_run_id[:8]}.jsonl")
        self._fh = open(self.path, "x", encoding="utf-8")
        self.seq = 0
        self.call_ordinal = 0
        self.sent = 0
        self.model_anchor: Optional[str] = None
        self.input_tokens = 0
        self.output_tokens = 0
        self.est_cost_usd = 0.0
        self._write("header", {
            **header,
            "receipt_kind": kind,
            "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
            "record_fields": list(jev.CALL_RECORD_FIELDS),
        })

    def _write(self, line_kind: str, body: dict) -> None:
        self.seq += 1
        line = {"line_kind": line_kind, "eval_run_id": self.eval_run_id,
                "seq": self.seq, "written_at": format_ts(now_utc()), **body}
        self._fh.write(json.dumps(line, sort_keys=True) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def _error_category(self, record: dict, sent: bool) -> Optional[str]:
        status = record["status"]
        if status == jev.STATUS_OK:
            return None
        if not sent:
            if status == jev.STATUS_CONTRACT_ERROR:
                return "refused_before_network:model_version_stop"
            return f"refused_before_network:{status}"
        if status == jev.STATUS_CONTRACT_ERROR and record["model_resolved"] \
                and self.model_anchor \
                and record["model_resolved"] != self.model_anchor:
            return "sent:model_version_change"
        return f"sent:{status}"

    def call(self, record: dict, *, fixture_id: str, edge_id: str,
             axolotl_available: bool, first_probabilities: Optional[dict]) -> None:
        sent = jev.network_attempted(record)
        self.call_ordinal += 1
        category = self._error_category(record, sent)
        if sent:
            self.sent += 1
            if record["model_resolved"] and self.model_anchor is None:
                self.model_anchor = record["model_resolved"]
            self.input_tokens += record["input_tokens"] or 0
            self.output_tokens += record["output_tokens"] or 0
            self.est_cost_usd += record["est_cost_usd"] or 0.0
        repeat_trigger = None
        if record["replicate_index"] > 1:
            repeat_trigger = _repeat_basis(first_probabilities, axolotl_available)
        self._write("call", {
            "fixture_id": fixture_id,
            "edge_id": edge_id,
            "call_ordinal": self.call_ordinal,
            "sent_ordinal": self.sent if sent else None,
            "repeat_ordinal": record["replicate_index"],
            "network_sent": sent,
            "error_category": category,
            "choice_matches_argmax": (
                record["choice_returned"] == record["argmax_recomputed"]
                if record["status"] == jev.STATUS_OK else None),
            "confidence_note": "confidence_observed is observational only; "
                               "used in no decision",
            "repeat_trigger": repeat_trigger,
            "record": dict(record),
        })

    def pair_result(self, body: dict) -> None:
        self._write("pair_result", body)

    def closeout(self, body: dict) -> None:
        self._write("closeout", {
            **body,
            "call_ordinal_total": self.call_ordinal,
            "calls_sent": self.sent,
            "calls_refused_before_network": self.call_ordinal - self.sent,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "est_cost_usd": round(self.est_cost_usd, 8),
            "cost_rate_basis": (
                "$0.042 / 1M input tokens — unverified cookbook figure "
                "(JEV-CONTRACT-v0 §2.5)"),
        })

    def close(self) -> None:
        self._fh.close()


def _repeat_basis(first_probabilities: Optional[dict],
                  axolotl_available: bool) -> Optional[dict]:
    """Why a replicate set was spent: the §5.6 gate when axolotl is
    available, else the frozen §2.6 thresholds on the trigger call."""
    if axolotl_available:
        return {"basis": "axolotl_gate",
                "detail": "clarification_load_bearing"}
    if not first_probabilities:
        return None
    ps = sorted(first_probabilities.values(), reverse=True)
    p_top1 = round(ps[0], 2)
    gap = round(ps[0] - (ps[1] if len(ps) > 1 else 0.0), 2)
    reasons = []
    if p_top1 <= jev.REPEAT_TOP1_MAX + 1e-9:
        reasons.append(f"p_top1 {p_top1} <= {jev.REPEAT_TOP1_MAX}")
    if gap <= jev.REPEAT_GAP_MAX + 1e-9:
        reasons.append(f"top-2 gap {gap} <= {jev.REPEAT_GAP_MAX}")
    return {"basis": "threshold_policy_2_6", "p_top1": p_top1, "gap": gap,
            "reasons": reasons}


def read_eval_call_log(path: str) -> dict:
    """Load and check an eval call log. ``complete`` is true only when the
    header and closeout are present, sequence numbers are contiguous, the
    call-line count matches the closeout, and every pair was adjudicated."""
    lines = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            if raw.strip():
                lines.append(json.loads(raw))
    problems = []
    if not lines or lines[0]["line_kind"] != "header":
        problems.append("header missing")
    if [l["seq"] for l in lines] != list(range(1, len(lines) + 1)):
        problems.append("sequence not contiguous")
    if len({l["eval_run_id"] for l in lines}) > 1:
        problems.append("mixed eval_run_id")
    calls = [l for l in lines if l["line_kind"] == "call"]
    pairs = [l for l in lines if l["line_kind"] == "pair_result"]
    closeout = lines[-1] if lines and lines[-1]["line_kind"] == "closeout" else None
    if closeout is None:
        problems.append("closeout missing (interrupted run)")
    else:
        if closeout["call_ordinal_total"] != len(calls):
            problems.append("call count mismatch")
        if closeout["pairs_evaluated"] != len(pairs):
            problems.append("pair count mismatch")
    return {
        "path": path,
        "header": lines[0] if lines else None,
        "calls": calls,
        "pair_results": pairs,
        "closeout": closeout,
        "problems": problems,
        "complete": (not problems and closeout is not None
                     and bool(closeout["evaluation_complete"])),
    }


def eval_fixtures(
    fixtures_path: str,
    *,
    live: bool,
    api_key: Optional[str] = None,
    flag_on: bool = False,
    max_calls: int = 0,
    axolotl_mod=None,
    records_dir: Optional[str] = None,
) -> dict:
    """Fixture evaluation (§5.9).

    Offline mode validates the fixture file and runs the deterministic
    pipeline (packetization + privacy tripwire) only — no accuracy claim
    exists before the live evaluation runs. Live mode (--live, Phase 4)
    adjudicates every pair through the JEV-CONTRACT-v0 client and reports
    argmax vs gold/acceptable as a receipt. Live mode requires
    ``records_dir``: every per-call record is persisted there as it is
    produced (``EvalCallLog``), so live evidence never depends on an
    external capture."""
    if live and not records_dir:
        raise ValueError("live fixture evaluation requires records_dir")
    generated_at = format_ts(now_utc())
    try:
        records, errors = validate_fixture_file(fixtures_path)
    except OSError as e:
        records, errors = [], [f"cannot read fixtures: {e}"]
    class_counts = Counter(r.get("fixture_class") for r in records)
    receipt: Dict[str, Any] = {
        "receipt_kind": EVAL_RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": generated_at,
        "git_commit": get_git_commit(),
        "campaign": jev.CAMPAIGN,
        "component": jev.COMPONENT,
        "fixtures_path": fixtures_path,
        "live": live,
        "valid": not errors,
        "errors": errors,
        "fixture_count": len(records),
        "class_counts": dict(sorted(class_counts.items())),
        "results": [],
    }
    if errors:
        return receipt

    day = jev.utc_day()
    log_ = None
    if live:
        with open(fixtures_path, "rb") as f:
            fixtures_sha256 = hashlib.sha256(f.read()).hexdigest()
        log_ = EvalCallLog(records_dir, header={
            "generated_at": generated_at,
            "git_commit": receipt["git_commit"],
            "campaign": jev.CAMPAIGN,
            "component": jev.COMPONENT,
            "fixtures_path": fixtures_path,
            "fixtures_sha256": fixtures_sha256,
            "fixture_count": len(records),
            "max_calls": max_calls,
            "day": day,
            "model_requested": jev.MODEL_REQUESTED,
            "relation_options": list(RELATION_OPTIONS),
            "axolotl": "available" if axolotl_mod is not None else "unavailable",
            "preflight": jev.estimate_cost_usd(max_calls),
        })
        receipt["call_records_path"] = log_.path
    try:
        _eval_loop(records, receipt, live=live, api_key=api_key,
                   flag_on=flag_on, max_calls=max_calls,
                   axolotl_mod=axolotl_mod, day=day, log_=log_)
    except BaseException as e:
        if log_ is not None:
            log_.closeout({"run_finished": False,
                           "stop_reason": f"exception:{type(e).__name__}",
                           "pairs_total": len(records),
                           "pairs_evaluated": sum(
                               1 for r in receipt["results"] if "status" in r),
                           "evaluation_complete": False})
            log_.close()
        raise
    if log_ is not None:
        results = receipt["results"]
        adjudicated = sum(1 for r in results if r.get("status") == jev.STATUS_OK)
        statuses = Counter(r.get("status") for r in results)
        if adjudicated == len(results):
            stop_reason = "completed"
        elif any(r.get("model_stop") for r in results):
            stop_reason = "model_version_stop"
        elif statuses.get(jev.STATUS_BUDGET_REFUSED):
            stop_reason = "max_calls_reached"
        else:
            stop_reason = "pairs_unavailable"
        log_.closeout({
            "run_finished": True,
            "stop_reason": stop_reason,
            "pairs_total": len(results),
            "pairs_evaluated": len(results),
            "pairs_adjudicated": adjudicated,
            "pair_status_counts": dict(sorted(
                (str(k), v) for k, v in statuses.items())),
            "evaluation_complete": adjudicated == len(results),
            "model_anchor": log_.model_anchor,
        })
        log_.close()
        receipt["live_summary"]["evaluation_complete"] = adjudicated == len(results)
        receipt["live_summary"]["stop_reason"] = stop_reason
        receipt["live_summary"]["calls_recorded"] = log_.call_ordinal
        receipt["live_summary"]["calls_sent"] = log_.sent
    return receipt


def _eval_loop(records, receipt, *, live, api_key, flag_on, max_calls,
               axolotl_mod, day, log_) -> None:
    """The per-pair loop of ``eval_fixtures``; fills ``receipt`` in place."""
    calls_spent = 0
    model_anchor: Optional[str] = None
    model_stopped = False
    for record in records:
        packet = build_packet(fixture_to_candidate(record))
        entry: Dict[str, Any] = {
            "fixture_id": record["fixture_id"],
            "fixture_class": record["fixture_class"],
            "relation_type": record["relation_type"],
            "gold_relation": record["gold_relation"],
            "acceptable_answers": record["acceptable_answers"],
            "packet_sha256": hash_sha256(stable_json(packet)),
            "text_a_sha256": hash_sha256(packet["text_a"]),
            "text_b_sha256": hash_sha256(packet["text_b"]),
            "privacy_tripwire_hits": packet_privacy_scan(packet),
        }
        if live:
            edge_id = hash_sha256(f"fixture|{record['fixture_id']}")
            first: Dict[str, Any] = {}

            def sink(rec, _fid=record["fixture_id"], _eid=edge_id, _first=first):
                if rec["replicate_index"] == 1 and rec["probabilities"]:
                    _first["p"] = rec["probabilities"]
                log_.call(rec, fixture_id=_fid, edge_id=_eid,
                          axolotl_available=axolotl_mod is not None,
                          first_probabilities=_first.get("p"))

            stopped_before = model_stopped
            adj, mem, gate_info = adjudicate_packet(
                packet,
                edge_id=edge_id,
                day=day,
                api_key=api_key,
                flag_on=flag_on,
                max_calls=max_calls,
                seed_state=(calls_spent, model_anchor, model_stopped),
                axolotl_mod=axolotl_mod,
                record_sink=sink,
            )
            calls_spent = mem.calls_today(day)
            anchor_after = mem.model_anchor(day)
            if anchor_after:
                model_anchor = anchor_after
            if mem.is_model_stopped(day):
                model_stopped = True
            entry.update({
                "status": adj.status,
                "argmax": adj.argmax,
                "ambiguity_set": adj.ambiguity_set,
                "argmax_in_acceptable": (
                    adj.argmax in record["acceptable_answers"]
                    if adj.argmax is not None else None
                ),
                "gate_state": gate_info["gate_state"],
                "calls_spent": adj.calls_spent,
                "model_resolved": next(
                    (r["model_resolved"] for r in adj.records if r["model_resolved"]),
                    None,
                ),
                "model_stop": model_stopped and not stopped_before,
            })
            log_.pair_result({
                **{k: entry[k] for k in (
                    "fixture_id", "fixture_class", "relation_type",
                    "gold_relation", "acceptable_answers", "packet_sha256",
                    "text_a_sha256", "text_b_sha256", "privacy_tripwire_hits",
                    "status", "argmax", "ambiguity_set",
                    "argmax_in_acceptable", "gate_state", "calls_spent",
                    "model_resolved", "model_stop")},
                "edge_id": edge_id,
                "replicate_set_id": adj.replicate_set_id,
                "call_ids": [r["call_id"] for r in adj.records],
                "calls_sent": sum(1 for r in adj.records
                                  if jev.network_attempted(r)),
                "median_probabilities": adj.median_probabilities,
                "repeat_spent": adj.repeat_spent,
                "repeat_basis": (
                    _repeat_basis(adj.records[0]["probabilities"],
                                  axolotl_mod is not None)
                    if len(adj.records) > 1 else None),
                "gate_plausible_set": gate_info["plausible_set"],
                "gate_detail": gate_info["detail"],
                "semantic_channel": semantic_channel(adj),
            })
        receipt["results"].append(entry)
    if live:
        judged = [r for r in receipt["results"] if r.get("argmax") is not None]
        in_acceptable = [r for r in judged if r["argmax_in_acceptable"]]
        receipt["live_summary"] = {
            "adjudicated": len(judged),
            "argmax_in_acceptable": len(in_acceptable),
            "calls_spent_total": sum(r.get("calls_spent", 0) for r in receipt["results"]),
            "note": (
                "qualification evidence only; no accuracy claim is made "
                "before this live evaluation has run (DESIGN §5.9)"
            ),
        }


# --- daemon (§5.1/§5.2; modeled on discovery_stream.py) ------------------------------

class _Stats:
    """Producer counters for STATS lines, ops heartbeat, and closeout."""

    def __init__(self):
        self.msgs = 0
        self.posts_cached = 0
        self.candidates = 0
        self.dropped_no_parent = 0
        self.not_sampled = 0
        self.duplicates = 0
        self.sampled = 0
        self.adjudicated = 0
        self.semantic_unavailable = 0
        self.budget_refused = 0
        self.tripwire_drops = 0
        self.errors = 0
        self.parse_failures = 0
        self.queue_dropped = 0
        self.worker_failures = 0
        self.reconnects = 0
        self.started_at = time.monotonic()

    def as_dict(self) -> dict:
        return {
            "messages": self.msgs,
            "posts_cached": self.posts_cached,
            "candidates": self.candidates,
            "dropped_no_parent": self.dropped_no_parent,
            "not_sampled": self.not_sampled,
            "duplicates": self.duplicates,
            "sampled": self.sampled,
            "adjudicated": self.adjudicated,
            "semantic_unavailable": self.semantic_unavailable,
            "budget_refused": self.budget_refused,
            "tripwire_drops": self.tripwire_drops,
            "errors": self.errors,
            "parse_failures": self.parse_failures,
            "queue_dropped": self.queue_dropped,
            "worker_failures": self.worker_failures,
            "reconnects": self.reconnects,
        }

    def log(self, cursor: Optional[int]):
        uptime = int(time.monotonic() - self.started_at)
        log.info(
            "STATS msgs=%d cached=%d candidates=%d sampled=%d adjudicated=%d "
            "unavailable=%d budget_refused=%d not_sampled=%d dropped_no_parent=%d "
            "tripwire=%d errors=%d cursor=%s uptime=%ds",
            self.msgs, self.posts_cached, self.candidates, self.sampled,
            self.adjudicated, self.semantic_unavailable, self.budget_refused,
            self.not_sampled, self.dropped_no_parent, self.tripwire_drops,
            self.errors, cursor if cursor is not None else "none", uptime,
        )


def _build_ws_url(cursor: Optional[int] = None, base_url: Optional[str] = None) -> str:
    params = "&".join(f"wantedCollections={c}" for c in WANTED_COLLECTIONS)
    url = f"{base_url or JETSTREAM_URL}?{params}"
    if cursor is not None:
        url += f"&cursor={max(0, cursor - CURSOR_REWIND_US)}"
    return url


class _DaemonContext:
    """Run-scoped wiring shared by the worker: Jev credentials/caps, the
    durable client state, the axolotl module (or None), and config."""

    def __init__(self, cfg: Config, conn, api_key: Optional[str], max_calls: int,
                 axolotl_mod):
        self.daily_cap = cfg.semantic_shadow_daily_cap
        self.max_calls = max_calls
        self.api_key = api_key
        self.axolotl_mod = axolotl_mod
        self.client_state = sdb.SidecarClientState(conn)


_WORKER_BUSY_RETRY_MAX = 5
_WORKER_BUSY_BACKOFF_CAP_SEC = 60


async def _handle_candidate(conn, cand: dict, ctx: _DaemonContext, stats: _Stats):
    """Adjudicate one structural candidate: sample, persist the structural
    record, Jev + gate, channels, disposition. On any Jev failure there is
    no semantic receipt for the edge; the structural record remains (§5.8)."""
    edge_id = make_edge_id(cand["relation_type"], cand["uri_a"], cand["uri_b"])
    day = cand["day"]
    stats.candidates += 1
    if not in_sample(edge_id, day, ctx.daily_cap):
        stats.not_sampled += 1
        return
    if sdb.get_edge(conn, edge_id) is not None:
        stats.duplicates += 1
        return

    packet = build_packet(cand)
    hits = packet_privacy_scan(packet)
    if hits:
        # Fail closed on privacy: the packet must not leave the process.
        stats.tripwire_drops += 1
        log.error("privacy tripwire hit on edge %s (%d patterns); edge dropped",
                  edge_id[:16], len(hits))
        return

    now_s = format_ts(now_utc())
    edge_fields = {
        "edge_id": edge_id,
        "relation_type": cand["relation_type"],
        "uri_a_sha256": hash_sha256(cand["uri_a"]),
        "uri_b_sha256": hash_sha256(cand["uri_b"]),
        "text_a_sha256": hash_sha256(packet["text_a"]),
        "text_b_sha256": hash_sha256(packet["text_b"]),
        "day": day,
        "dt_seconds": packet["dt_seconds"],
        "lexical_jaccard": packet["lexical_jaccard"],
        "first_seen_ts": now_s,
        "updated_ts": now_s,
    }

    adjudications_today = int(sdb.meta_get(conn, f"adjudications:{day}") or 0)
    if adjudications_today >= ctx.daily_cap:
        sdb.insert_edge(conn, {**edge_fields,
                               "disposition": DISPOSITION_BUDGET_REFUSED})
        conn.commit()
        stats.budget_refused += 1
        return

    stats.sampled += 1
    sdb.insert_edge(conn, {**edge_fields, "disposition": DISPOSITION_SAMPLED_PENDING})
    conn.commit()

    state = ctx.client_state
    seed = (state.calls_today(day), state.model_anchor(day),
            state.is_model_stopped(day))
    adj, mem, gate_info = await asyncio.to_thread(
        adjudicate_packet,
        packet,
        edge_id=edge_id,
        day=day,
        api_key=ctx.api_key,
        flag_on=True,
        max_calls=ctx.max_calls,
        seed_state=seed,
        axolotl_mod=ctx.axolotl_mod,
    )
    # Merge the client state back on the event-loop thread (single writer).
    state.set_calls_today(day, mem.calls_today(day))
    for record in mem.records:
        state.record_call(record)
    anchor = mem.model_anchor(day)
    if anchor and not state.model_anchor(day):
        state.set_model_anchor(day, anchor)
    if mem.is_model_stopped(day):
        state.set_model_stopped(day)

    channels = compute_channels(packet, adj)
    disposition = (
        DISPOSITION_ADJUDICATED if adj.status == jev.STATUS_OK
        else DISPOSITION_SEMANTIC_UNAVAILABLE
    )
    sdb.finalize_edge(
        conn,
        edge_id,
        disposition=disposition,
        channels=channels,
        shadow_qualified=shadow_qualified(channels),
        fragility=fragility_flags(channels),
        gate_state=gate_info["gate_state"],
        semantic_argmax=adj.argmax,
        ambiguity=adj.ambiguity_set,
        updated_ts=format_ts(now_utc()),
    )
    sdb.meta_set(conn, f"adjudications:{day}", adjudications_today + 1)
    conn.commit()
    if disposition == DISPOSITION_ADJUDICATED:
        stats.adjudicated += 1
    else:
        stats.semantic_unavailable += 1
        log.warning("no semantic receipt for edge %s (status=%s); "
                    "structural record kept", edge_id[:16], adj.status)


async def _worker(conn, queue: asyncio.Queue, ctx: _DaemonContext,
                  stats: _Stats, fatal_error: asyncio.Event):
    """The ONLY task that writes to the sidecar DB (single-writer
    guarantee; discovery_stream parity). SQLITE_BUSY/LOCKED is recoverable
    contention with bounded retry; other sqlite3 errors are fatal."""
    while True:
        item = await queue.get()
        try:
            for retry in range(_WORKER_BUSY_RETRY_MAX + 1):
                try:
                    await _handle_candidate(conn, item, ctx, stats)
                    break  # success
                except sqlite3.OperationalError as e:
                    err_str = str(e).lower()
                    if "locked" in err_str or "busy" in err_str:
                        if retry == _WORKER_BUSY_RETRY_MAX:
                            stats.errors += 1
                            log.error("Dropping candidate after %d busy retries: %s",
                                      retry, e)
                            break
                        delay = min(2 ** retry, _WORKER_BUSY_BACKOFF_CAP_SEC) \
                            + random.uniform(0, 1)
                        log.warning("Sidecar busy/locked in worker, retrying in %.1fs "
                                    "(%d/%d): %s", delay, retry + 1,
                                    _WORKER_BUSY_RETRY_MAX, e)
                        await asyncio.sleep(delay)
                        continue
                    log.critical("Sidecar OperationalError in worker, forcing exit: %s", e)
                    stats.worker_failures += 1
                    fatal_error.set()
                    return
                except sqlite3.Error as e:
                    log.critical("Sidecar error in worker, forcing exit: %s", e)
                    stats.worker_failures += 1
                    fatal_error.set()
                    return
        except Exception:
            stats.errors += 1
            log.exception("Worker error processing candidate")
        finally:
            queue.task_done()


async def _stream_loop(conn, work_queue: asyncio.Queue, cache: RecentPostCache,
                       stats: _Stats, fatal_error: asyncio.Event,
                       runtime: dict, base_url: Optional[str] = None):
    """Connect to Jetstream and process messages until disconnect. The
    cursor lives in the sidecar meta under jetstream_semantic_shadow_cursor
    — never jetstream_discovery_cursor (§5.2)."""
    cursor_val = sdb.meta_get(conn, sdb.META_CURSOR)
    cursor = int(cursor_val) if cursor_val else None

    url = _build_ws_url(cursor, base_url)
    log.info("Connecting to %s", url)

    last_cursor_save = time.monotonic()
    last_stats_log = time.monotonic()
    last_msg_time_us = cursor

    async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
        runtime["connected"] = True
        async for raw in ws:
            if fatal_error.is_set():
                return
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                stats.errors += 1
                stats.parse_failures += 1
                log.debug("JSON parse error, skipping")
                continue

            stats.msgs += 1
            msg_time_us = msg.get("time_us")
            if msg_time_us is not None:
                last_msg_time_us = msg_time_us

            now_mono = time.monotonic()
            if now_mono - last_cursor_save >= CURSOR_SAVE_INTERVAL:
                if last_msg_time_us is not None:
                    sdb.meta_set(conn, sdb.META_CURSOR, str(last_msg_time_us))
                    ts_iso = datetime.fromtimestamp(
                        last_msg_time_us / 1_000_000, tz=timezone.utc
                    ).isoformat()
                    sdb.meta_set(conn, sdb.META_LAST_MSG_AT, ts_iso)
                    conn.commit()
                last_cursor_save = now_mono

            if now_mono - last_stats_log >= STATS_INTERVAL:
                stats.log(last_msg_time_us)
                last_stats_log = now_mono

            resolved, dropped = process_message(msg, cache)
            stats.dropped_no_parent += dropped
            stats.posts_cached = len(cache)
            for cand in resolved:
                try:
                    work_queue.put_nowait(cand)
                except asyncio.QueueFull:
                    stats.queue_dropped += 1
                    log.warning("Work queue full, dropping candidate edge")

    # Final cursor save on clean disconnect
    if last_msg_time_us is not None:
        sdb.meta_set(conn, sdb.META_CURSOR, str(last_msg_time_us))
        conn.commit()


async def _wal_refresh_loop(conn, interval: int = WAL_REFRESH_INTERVAL):
    """Periodic rollback + passive checkpoint so this long-lived connection
    never pins the sidecar WAL (discovery_stream parity; see the 2026-04-22
    incident in docs/OPS_HAZARDS.md)."""
    while True:
        await asyncio.sleep(interval)
        try:
            conn.rollback()
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception:
            log.exception("WAL refresh failed")


def _persist_ops(conn, work_queue: asyncio.Queue, stats: _Stats, runtime: dict) -> None:
    """Persist bounded producer facts; these are observations, not verdicts."""
    observed_at = format_ts(now_utc())
    values = {
        "ops:semantic_shadow:observed_at": observed_at,
        "ops:semantic_shadow:session_id": runtime["session_id"],
        "ops:semantic_shadow:connected": "true" if runtime.get("connected") else "false",
        "ops:semantic_shadow:queue_depth": str(work_queue.qsize()),
        "ops:semantic_shadow:queue_capacity": str(work_queue.maxsize),
    }
    for key, value in stats.as_dict().items():
        values[f"ops:semantic_shadow:{key}"] = str(value)
    for key, value in values.items():
        sdb.meta_set(conn, key, value)
    conn.commit()


async def _ops_heartbeat_loop(conn, work_queue: asyncio.Queue, stats: _Stats,
                              runtime: dict, interval: int = OPS_HEARTBEAT_INTERVAL):
    while True:
        try:
            _persist_ops(conn, work_queue, stats, runtime)
        except Exception:
            log.exception("Semantic shadow ops heartbeat failed")
        await asyncio.sleep(interval)


async def _maintenance_loop(conn, stats: _Stats):
    """Episode-summary rebuild for the current UTC day every
    EPISODE_REBUILD_INTERVAL; 30-day retention prune (§5.7) hourly."""
    last_prune = time.monotonic()
    while True:
        await asyncio.sleep(EPISODE_REBUILD_INTERVAL)
        try:
            day = jev.utc_day()
            n = rebuild_episode_summaries(conn, day)
            log.info("Episode summaries rebuilt for %s (%d episodes)", day, n)
        except Exception:
            log.exception("Episode summary rebuild failed")
        now_mono = time.monotonic()
        if now_mono - last_prune >= PRUNE_INTERVAL:
            last_prune = now_mono
            try:
                deleted = sdb.prune_old(conn, now_utc())
                if any(deleted.values()):
                    log.info("Retention prune deleted %s", deleted)
            except Exception:
                log.exception("Retention prune failed")


async def run(cfg: Config, *, sidecar_path: str, max_calls: int,
              receipts_dir: str, api_key: Optional[str],
              base_url: Optional[str] = None):
    """Main entry point for the semantic shadow daemon.

    The caller (CLI) has already written the activation receipt, printed
    the §2.9 pre-flight cost estimate, and required the key, the flag, and
    explicit spend confirmation. This coroutine writes the run manifest on
    start and the closeout receipt on exit."""
    free_gb = sdb.free_disk_gb(sidecar_path)
    if free_gb < DISK_FLOOR_GB:
        raise RuntimeError(
            f"free disk {free_gb:.2f}G below floor {DISK_FLOOR_GB}G; refusing to start"
        )
    conn = sdb.init_sidecar(sidecar_path)
    conn.execute("PRAGMA busy_timeout=120000")

    axolotl_mod = resolve_axolotl()
    manifest = build_run_manifest(cfg, max_calls=max_calls,
                                  axolotl_available=axolotl_mod is not None)
    manifest["sidecar_path"] = sidecar_path
    manifest["receipts_dir"] = receipts_dir
    manifest_path = write_run_manifest(receipts_dir, manifest)
    log.info("Run manifest written to %s", manifest_path)
    if axolotl_mod is None:
        log.info("axolotl not resolvable; gate state %s, §2.6 repeat policy "
                 "applies directly (set %s to enable)", GATE_UNAVAILABLE,
                 AXOLOTL_PATH_ENV)

    started_at = format_ts(now_utc())
    stats = _Stats()
    cache = RecentPostCache(cfg.semantic_shadow_cache_size,
                            cfg.semantic_shadow_window_minutes)
    work_queue: asyncio.Queue = asyncio.Queue(maxsize=WORK_QUEUE_MAX)
    runtime = {"session_id": uuid.uuid4().hex, "connected": False}
    ctx = _DaemonContext(cfg, conn, api_key, max_calls, axolotl_mod)

    for _attempt in range(10):
        try:
            sdb.meta_set(conn, "semantic_shadow_started_at", started_at)
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and _attempt < 9:
                delay = min(0.5 * (2 ** _attempt), 30) + random.uniform(0, 1)
                log.warning("Sidecar locked during startup, retrying in %.1fs (%d/10)",
                            delay, _attempt + 1)
                await asyncio.sleep(delay)
            else:
                raise
    _persist_ops(conn, work_queue, stats, runtime)

    fatal_error = asyncio.Event()
    shutdown_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def _signal_handler():
        log.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    worker_task = asyncio.create_task(_worker(conn, work_queue, ctx, stats, fatal_error))
    wal_refresh_task = asyncio.create_task(_wal_refresh_loop(conn))
    ops_heartbeat_task = asyncio.create_task(
        _ops_heartbeat_loop(conn, work_queue, stats, runtime))
    maintenance_task = asyncio.create_task(_maintenance_loop(conn, stats))

    def _check_task_health(task: asyncio.Task):
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.critical("Background task died: %s", exc)
            fatal_error.set()

    for task in (worker_task, wal_refresh_task, ops_heartbeat_task, maintenance_task):
        task.add_done_callback(_check_task_health)

    stop_reason = "shutdown"
    try:
        while not shutdown_event.is_set() and not fatal_error.is_set():
            try:
                await _stream_loop(conn, work_queue, cache, stats, fatal_error,
                                   runtime, base_url)
            except websockets.exceptions.ConnectionClosed:
                log.warning("Jetstream disconnected, reconnecting in 5s")
            except OSError as e:
                log.warning("Connection error: %s, reconnecting in 5s", e)
            except Exception:
                log.exception("Unexpected error in stream loop")

            runtime["connected"] = False
            stats.reconnects += 1
            try:
                _persist_ops(conn, work_queue, stats, runtime)
            except Exception:
                log.exception("Failed to persist disconnected state")

            if shutdown_event.is_set() or fatal_error.is_set():
                break
            await asyncio.sleep(5)
    finally:
        if fatal_error.is_set():
            stop_reason = "fatal_error"
        for task in (worker_task, wal_refresh_task, ops_heartbeat_task,
                     maintenance_task):
            task.cancel()

        cursor_val = sdb.meta_get(conn, sdb.META_CURSOR)
        if cursor_val:
            log.info("Final cursor: %s", cursor_val)
        stats.log(int(cursor_val) if cursor_val else None)
        runtime["connected"] = False
        try:
            rebuild_episode_summaries(conn, jev.utc_day())
        except Exception:
            log.exception("Final episode summary rebuild failed")
        try:
            _persist_ops(conn, work_queue, stats, runtime)
        except Exception:
            pass
        receipt, path = write_closeout_receipt(
            receipts_dir, started_at=started_at, stop_reason=stop_reason,
            stats=stats, conn=conn, day=jev.utc_day())
        log.info("Closeout receipt written to %s", path)
        conn.close()

        if fatal_error.is_set():
            log.critical("Exiting due to fatal error (sidecar write failure)")
            sys.exit(1)

        log.info("Semantic shadow shut down cleanly")
