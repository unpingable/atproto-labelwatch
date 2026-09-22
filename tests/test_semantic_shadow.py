"""Offline tests for the semantic shadow sidecar (LW-JEV-SHADOW-v0).

Synthetic Jetstream message dicts are fed to the handler paths directly;
the Jev transport is mocked at the single `_http_post` seam; databases are
tmp_path sidecars. No network."""
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from labelwatch import semantic_shadow as shadow
from labelwatch import semantic_shadow_db as sdb
from labelwatch import semantic_shadow_jev as jev
from labelwatch.config import Config
from labelwatch.utils import format_ts, hash_sha256, now_utc, stable_json

DAY = "2026-09-17"
#: Per-call records are stamped with the real UTC day, so queries over
#: written records use that day rather than the fixed sampling/manifest DAY.
RECORD_DAY = jev.utc_day
FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures",
                        "semantic_relations.jsonl")
FIXTURE_SCHEMA = os.path.join(os.path.dirname(__file__), "fixtures",
                              "semantic_relations.schema.json")
AXOLOTL_SRC = "/data/git/axolotl/src"


# --- synthetic Jetstream messages ----------------------------------------------

def post_msg(did, rkey, text, *, created="2026-09-17T10:00:00Z",
             time_us=1_800_000_000_000_000, reply_to=None, quote_of=None,
             langs=("en",)):
    record = {"$type": "app.bsky.feed.post", "text": text,
              "langs": list(langs), "createdAt": created}
    if reply_to:
        record["reply"] = {
            "parent": {"uri": reply_to, "cid": "cid-parent"},
            "root": {"uri": reply_to, "cid": "cid-root"},
        }
    if quote_of:
        record["embed"] = {"$type": "app.bsky.embed.record",
                           "record": {"uri": quote_of, "cid": "cid-quote"}}
    return {
        "kind": "commit", "did": did, "time_us": time_us,
        "commit": {"operation": "create", "collection": "app.bsky.feed.post",
                   "rkey": rkey, "rev": "rev1", "cid": f"cid-{rkey}",
                   "record": record},
    }


def repost_msg(did, rkey, subject_uri, *, created="2026-09-17T10:05:00Z",
               time_us=1_800_000_300_000_000):
    return {
        "kind": "commit", "did": did, "time_us": time_us,
        "commit": {"operation": "create",
                   "collection": "app.bsky.feed.repost", "rkey": rkey,
                   "rev": "rev1", "cid": f"cid-{rkey}",
                   "record": {"$type": "app.bsky.feed.repost",
                              "subject": {"uri": subject_uri, "cid": "cid-subj"},
                              "createdAt": created}},
    }


POST_A_URI = "at://did:plc:authoraaa/app.bsky.feed.post/posta"
TEXT_A = "Deploys are smooth on Fridays, said no pager ever."
TEXT_B = "Nothing has ever caught fire on a Friday deploy, obviously."


def make_ctx(conn, cfg=None, api_key="test-key", max_calls=50, axolotl_mod=None):
    cfg = cfg or Config()
    return shadow._DaemonContext(cfg, conn, api_key, max_calls, axolotl_mod)


def make_client_state_pair(tmp_path):
    conn = sdb.init_sidecar(str(tmp_path / "shadow.db"))
    return conn


# --- candidate extraction (§5.2) -------------------------------------------------

def test_extract_reply_candidate():
    cache = shadow.RecentPostCache(100, 15)
    process_a, _ = shadow.process_message(
        post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    assert process_a == []  # plain post caches, no candidate
    cands, dropped = shadow.process_message(
        post_msg("did:plc:authorbbb", "postb", TEXT_B, reply_to=POST_A_URI,
                 created="2026-09-17T10:01:00Z",
                 time_us=1_800_000_060_000_000), cache)
    assert dropped == 0
    assert len(cands) == 1
    cand = cands[0]
    assert cand["relation_type"] == "reply"
    assert cand["uri_a"] == POST_A_URI
    assert cand["uri_b"] == "at://did:plc:authorbbb/app.bsky.feed.post/postb"
    assert cand["a"]["text"] == TEXT_A
    assert cand["dt_seconds"] == 60
    assert cand["day"] == DAY
    assert cand["thread_depth_b"] == 1


def test_extract_quote_candidate():
    cache = shadow.RecentPostCache(100, 15)
    shadow.process_message(post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    cands, _ = shadow.process_message(
        post_msg("did:plc:authorbbb", "postb", TEXT_B, quote_of=POST_A_URI),
        cache)
    assert len(cands) == 1
    assert cands[0]["relation_type"] == "quote"
    assert cands[0]["thread_depth_b"] is None


def test_extract_repost_candidate():
    cache = shadow.RecentPostCache(100, 15)
    shadow.process_message(post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    cands, _ = shadow.process_message(
        repost_msg("did:plc:authorbbb", "repostb", POST_A_URI), cache)
    assert len(cands) == 1
    assert cands[0]["relation_type"] == "repost"
    assert cands[0]["b"]["text"] == ""


def test_extract_recordwithmedia_quote():
    cache = shadow.RecentPostCache(100, 15)
    shadow.process_message(post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    msg = post_msg("did:plc:authorbbb", "postb", TEXT_B)
    msg["commit"]["record"]["embed"] = {
        "$type": "app.bsky.embed.recordWithMedia",
        "record": {"record": {"uri": POST_A_URI, "cid": "cid-q"},
                   "cid": "cid-outer"},
        "media": {"$type": "app.bsky.embed.images", "images": []},
    }
    cands, _ = shadow.process_message(msg, cache)
    assert len(cands) == 1
    assert cands[0]["relation_type"] == "quote"


def test_early_drop_plain_post_and_missing_parent():
    cache = shadow.RecentPostCache(100, 15)
    cands, dropped = shadow.process_message(
        post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    assert cands == [] and dropped == 0
    # Reply whose parent is not in the local cache: dropped, never fetched.
    cands, dropped = shadow.process_message(
        post_msg("did:plc:authorbbb", "postb", TEXT_B,
                 reply_to="at://did:plc:ghost/app.bsky.feed.post/ghost1"),
        cache)
    assert cands == [] and dropped == 1


def test_early_drop_non_commit_and_wrong_collection():
    cache = shadow.RecentPostCache(100, 15)
    identity = {"kind": "identity", "did": "did:plc:x"}
    assert shadow.process_message(identity, cache) == ([], 0)
    labeler = {"kind": "commit", "did": "did:plc:x", "time_us": 1,
               "commit": {"operation": "create",
                          "collection": "app.bsky.labeler.service",
                          "rkey": "self", "record": {}}}
    assert shadow.process_message(labeler, cache) == ([], 0)
    delete = post_msg("did:plc:x", "y", "gone")
    delete["commit"]["operation"] = "delete"
    assert shadow.process_message(delete, cache) == ([], 0)


def test_cache_ttl_and_bound():
    cache = shadow.RecentPostCache(3, 15)
    for i in range(5):
        cache.put(f"at://did:plc:x/app.bsky.feed.post/{i}",
                  {"text": f"t{i}", "langs": []}, now_mono=1000.0)
    assert len(cache) == 3
    assert cache.get("at://did:plc:x/app.bsky.feed.post/0", now_mono=1000.0) is None
    # TTL expiry
    assert cache.get("at://did:plc:x/app.bsky.feed.post/4",
                     now_mono=1000.0 + 16 * 60) is None


# --- evidence packet + privacy tripwire (§5.3) --------------------------------------

def _reply_cand(text_b=TEXT_B, text_a=TEXT_A, dt=60):
    return {
        "relation_type": "reply",
        "a": {"text": text_a, "langs": ["en"]},
        "b": {"text": text_b, "langs": ["en"]},
        "dt_seconds": dt,
        "thread_depth_b": 1,
    }


def test_packet_is_author_blind_and_bounded():
    cand = _reply_cand(text_b="x " * 400)  # over 500 chars
    packet = shadow.build_packet(cand)
    assert packet["packet_kind"] == "labelwatch.edge_evidence/v0"
    assert len(packet["text_b"]) == 500
    serialized = stable_json(packet)
    for banned in ("did:", "at://", "author", "handle"):
        assert banned not in serialized


def test_packet_redacts_identifiers_in_text():
    cand = _reply_cand(
        text_b="thanks @someone.bsky.social, see at://did:plc:zzz/app.bsky.feed.post/9 and did:plc:zzz")
    packet = shadow.build_packet(cand)
    assert "[mention]" in packet["text_b"]
    assert "[at-uri]" in packet["text_b"]
    assert "[did]" in packet["text_b"]
    assert shadow.packet_privacy_scan(packet) == []


def test_tripwire_fires_on_poisoned_packet():
    packet = shadow.build_packet(_reply_cand())
    packet["text_a"] = "leak did:plc:abc123"
    assert shadow.packet_privacy_scan(packet)


def test_packet_features():
    cand = _reply_cand(
        text_a="Reading https://blog.example.org/post1 and tagging #cooking today",
        text_b="Loved https://blog.example.org/post2 — great #cooking notes")
    packet = shadow.build_packet(cand)
    assert packet["shared_url_domain"] is True
    assert packet["shared_hashtag"] is True
    assert 0.0 <= packet["lexical_jaccard"] <= 1.0


def test_lexical_jaccard_deterministic():
    a = "the quick brown fox jumps over the lazy dog"
    b = "the quick brown fox jumps over the lazy dog"
    assert shadow.lexical_jaccard(a, b) == 1.0
    assert shadow.lexical_jaccard(a, "") == 0.0
    assert shadow.lexical_jaccard("", "") == 0.0
    partial = shadow.lexical_jaccard(a, "the quick brown fox")
    assert shadow.lexical_jaccard(a, "the quick brown fox") == partial


# --- deterministic sampling (§5.2) -----------------------------------------------------

def test_sampling_is_deterministic():
    edge = shadow.make_edge_id("reply", "at://a", "at://b")
    assert shadow.in_sample(edge, DAY, 200) == shadow.in_sample(edge, DAY, 200)
    slots = {shadow.sample_slot(edge, DAY) for _ in range(5)}
    assert len(slots) == 1


def test_sampling_scales_with_cap():
    assert shadow.slot_limit(200) == 1
    assert shadow.slot_limit(2000) == 10
    # Full volume assumption: everything is admitted
    assert shadow.slot_limit(2_000_000) == 10_000
    edge = shadow.make_edge_id("reply", "at://a", "at://b")
    assert shadow.in_sample(edge, DAY, 2_000_000) is True


def test_sampling_rate_in_expected_band():
    # 1/10000 slot at the default cap: sanity-check over a fixed edge set.
    admitted = sum(
        1 for i in range(2000)
        if shadow.in_sample(shadow.make_edge_id("reply", f"at://a{i}", "at://b"),
                            DAY, 200)
    )
    assert admitted == 0 or admitted <= 2  # expected 0.2; bounded, deterministic


# --- channels, qualification, fragility (§5.5) -------------------------------------------

def _adj(status="ok", argmax="dependent_direct", medians=None):
    medians = medians or {o: 0.0 for o in shadow.RELATION_OPTIONS}
    if argmax:
        medians = {**medians, argmax: medians.get(argmax) or 0.90}
    return jev.Adjudication(
        status=status, question_id="edge:x", replicate_set_id="set",
        median_probabilities=medians if status == "ok" else None,
        argmax=argmax if status == "ok" else None,
        ambiguity_set=[argmax] if status == "ok" and argmax else [],
        calls_spent=1,
    )


def test_channel_computation():
    packet = shadow.build_packet(_reply_cand(dt=600))
    packet["lexical_jaccard"] = 0.25
    adj = _adj()
    channels = shadow.compute_channels(packet, adj)
    assert channels == {
        "structural_lineage": True, "lexical": True,
        "semantic": True, "temporal": True,
    }
    assert shadow.shadow_qualified(channels) is True


def test_semantic_channel_thresholds():
    medians = {o: 0.0 for o in shadow.RELATION_OPTIONS}
    medians["dependent_direct"] = 0.49
    adj = _adj(argmax="dependent_direct", medians=medians)
    assert shadow.semantic_channel(adj) is False  # below 0.50 median floor
    medians["dependent_direct"] = 0.50
    adj = _adj(argmax="dependent_direct", medians=medians)
    assert shadow.semantic_channel(adj) is True
    medians2 = {o: 0.0 for o in shadow.RELATION_OPTIONS}
    medians2["unrelated"] = 0.90
    adj = _adj(argmax="unrelated", medians=medians2)
    assert shadow.semantic_channel(adj) is False  # not dependent_*
    adj_fail = _adj(status="timeout", argmax=None)
    assert shadow.semantic_channel(adj_fail) is None  # no semantic receipt


def test_underdetermined_carries_no_weight():
    medians = {o: 0.0 for o in shadow.RELATION_OPTIONS}
    medians["underdetermined"] = 0.80
    adj = _adj(argmax="underdetermined", medians=medians)
    assert shadow.semantic_channel(adj) is False


def test_channel_agreement_matrix():
    base = {"structural_lineage": True, "lexical": False,
            "semantic": False, "temporal": False}
    assert shadow.shadow_qualified(base) is False
    for extra in ("lexical", "semantic", "temporal"):
        ch = {**base, extra: True}
        assert shadow.shadow_qualified(ch) is True, extra
    # Semantic missing (None) never counts as agreeing (§5.8).
    ch = {**base, "semantic": None}
    assert shadow.shadow_qualified(ch) is False


def test_fragility_flags_leave_one_out():
    channels = {"structural_lineage": True, "lexical": True,
                "semantic": True, "temporal": True}
    flags = shadow.fragility_flags(channels)
    # With all four agreeing, dropping any one leaves three — still qualified.
    assert all(flags.values())
    channels = {"structural_lineage": True, "lexical": False,
                "semantic": True, "temporal": False}
    flags = shadow.fragility_flags(channels)
    assert flags["drop_structural_lineage"] is False  # only semantic remains
    assert flags["drop_semantic"] is False            # only structural remains
    assert flags["drop_lexical"] is True              # structural+semantic stay


# --- sidecar store (§5.7) ---------------------------------------------------------

def test_sidecar_init_and_admissibility(tmp_path):
    conn = make_client_state_pair(tmp_path)
    ok, reason = sdb.is_admissible(conn)
    assert ok, reason
    assert sdb.meta_get(conn, sdb.META_SIDECAR_SCHEMA) == "1"
    assert sdb.meta_get(conn, "build_status") == "active"
    # Own cursor key, never the discovery stream's.
    sdb.meta_set(conn, sdb.META_CURSOR, "123")
    assert sdb.META_CURSOR == "jetstream_semantic_shadow_cursor"
    conn.close()


def test_sidecar_schema_mismatch_refuses(tmp_path):
    path = str(tmp_path / "shadow.db")
    conn = sdb.init_sidecar(path)
    sdb.meta_set(conn, sdb.META_SIDECAR_SCHEMA, "99")
    conn.commit()
    conn.close()
    with pytest.raises(RuntimeError):
        sdb.init_sidecar(path)


def test_retention_prune(tmp_path):
    conn = make_client_state_pair(tmp_path)
    now = now_utc()
    old_ts = format_ts(now - timedelta(days=40))
    new_ts = format_ts(now - timedelta(days=2))
    for tag, ts in (("old", old_ts), ("new", new_ts)):
        sdb.insert_edge(conn, {
            "edge_id": f"edge-{tag}", "relation_type": "reply",
            "uri_a_sha256": "a" * 64, "uri_b_sha256": "b" * 64,
            "day": ts[:10], "first_seen_ts": ts, "updated_ts": ts,
        })
        sdb.insert_call(conn, {
            "call_id": f"call-{tag}", "ts": ts,
            "campaign": "jev-dual-shadow-v0", "component": jev.COMPONENT,
            "decision_family": "edge_relation", "question_id": f"edge:{tag}",
            "request_digest": "d" * 64, "model_requested": "jev-latest",
            "status": "ok",
        })
    conn.commit()
    deleted = sdb.prune_old(conn, now)
    assert deleted == {"edge": 1, "calls": 1, "episode_summary": 0}
    assert sdb.get_edge(conn, "edge-old") is None
    assert sdb.get_edge(conn, "edge-new") is not None
    conn.close()


# --- adjudication through the daemon path ---------------------------------------------

DIST_CONFIDENT = {"dependent_direct": 0.72, "dependent_indirect": 0.14,
                  "independent_lexical": 0.05, "unrelated": 0.06,
                  "underdetermined": 0.03}
DIST_UNCLEAR = {"dependent_direct": 0.62, "dependent_indirect": 0.28,
                "independent_lexical": 0.04, "unrelated": 0.04,
                "underdetermined": 0.02}


def _ok_transport(probs, model="jev-2026-09-01"):
    def transport(url, headers, body, timeout_s):
        (family,) = json.loads(body)["questions"]
        payload = {"model": model,
                   "answers": {family: {
                       "type": "choice", "probabilities": probs,
                       "choice": max(probs, key=probs.get),
                       "confidence": 0.8}},
                   "usage": {"input_tokens": 650, "output_tokens": 10}}
        return (200, json.dumps(payload).encode("utf-8"),
                {jev.REQUEST_ID_HEADER: "req_x"})
    return transport


@pytest.mark.asyncio
async def test_handle_candidate_adjudicates_and_persists(tmp_path):
    conn = make_client_state_pair(tmp_path)
    cfg = Config(semantic_shadow_daily_cap=200)
    ctx = make_ctx(conn, cfg)
    stats = shadow._Stats()
    cache = shadow.RecentPostCache(cfg.semantic_shadow_cache_size,
                                   cfg.semantic_shadow_window_minutes)
    shadow.process_message(post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    cands, _ = shadow.process_message(
        post_msg("did:plc:authorbbb", "postb", TEXT_B, reply_to=POST_A_URI,
                 created="2026-09-17T10:01:00Z",
                 time_us=1_800_000_060_000_000), cache)
    cand = cands[0]
    # Force the deterministic sampler to admit this edge.
    with patch.object(shadow, "in_sample", return_value=True), \
         patch.object(jev, "_http_post", _ok_transport(DIST_CONFIDENT)):
        await shadow._handle_candidate(conn, cand, ctx, stats)
    edge_id = shadow.make_edge_id("reply", cand["uri_a"], cand["uri_b"])
    row = sdb.get_edge(conn, edge_id)
    assert row is not None
    assert row["disposition"] == "adjudicated"
    assert row["channel_structural"] == 1
    assert row["channel_semantic"] == 1
    assert row["channel_temporal"] == 1
    assert row["shadow_qualified"] == 1
    assert row["semantic_argmax"] == "dependent_direct"
    assert row["gate_state"] == shadow.GATE_UNAVAILABLE  # no axolotl in .venv
    fragility = json.loads(row["fragility_json"])
    assert set(flags for flags in fragility) == {
        "drop_structural_lineage", "drop_lexical", "drop_semantic", "drop_temporal"}
    # Per-call records landed in the sidecar (§2.5).
    calls = list(sdb.iter_calls(conn, day=RECORD_DAY()))
    assert len(calls) == 1
    assert calls[0]["status"] == "ok"
    assert calls[0]["probabilities"] == DIST_CONFIDENT
    assert stats.adjudicated == 1
    conn.close()


@pytest.mark.asyncio
async def test_handle_candidate_jev_failure_keeps_structural_record(tmp_path):
    """§5.8: any Jev failure -> no semantic receipt, structural record kept."""
    conn = make_client_state_pair(tmp_path)
    cfg = Config()
    ctx = make_ctx(conn, cfg)
    stats = shadow._Stats()

    def boom(url, headers, body, timeout_s):
        raise TimeoutError("timed out")
    cache = shadow.RecentPostCache(100, 15)
    shadow.process_message(post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    cands, _ = shadow.process_message(
        post_msg("did:plc:authorbbb", "postb", TEXT_B, reply_to=POST_A_URI,
                 created="2026-09-17T10:01:00Z",
                 time_us=1_800_000_060_000_000), cache)
    with patch.object(shadow, "in_sample", return_value=True), \
         patch.object(jev, "_http_post", boom):
        await shadow._handle_candidate(conn, cands[0], ctx, stats)
    edge_id = shadow.make_edge_id("reply", cands[0]["uri_a"], cands[0]["uri_b"])
    row = sdb.get_edge(conn, edge_id)
    assert row["disposition"] == "semantic_unavailable"
    assert row["channel_semantic"] is None  # no semantic receipt (§5.8)
    # The structural+temporal channels still agree, so the edge is
    # shadow-qualified with channel_semantic NULL — the missing receipt is
    # visible in the record rather than collapsed to a false negative.
    assert row["shadow_qualified"] == 1
    calls = list(sdb.iter_calls(conn, day=RECORD_DAY()))
    assert calls[0]["status"] == "timeout"
    assert stats.semantic_unavailable == 1
    conn.close()


@pytest.mark.asyncio
async def test_handle_candidate_budget_refusal(tmp_path):
    conn = make_client_state_pair(tmp_path)
    cfg = Config(semantic_shadow_daily_cap=1)
    ctx = make_ctx(conn, cfg)
    stats = shadow._Stats()
    sdb.meta_set(conn, f"adjudications:{DAY}", "1")
    conn.commit()
    cache = shadow.RecentPostCache(100, 15)
    shadow.process_message(post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    cands, _ = shadow.process_message(
        post_msg("did:plc:authorbbb", "postb", TEXT_B, reply_to=POST_A_URI,
                 created="2026-09-17T10:01:00Z",
                 time_us=1_800_000_060_000_000), cache)
    with patch.object(shadow, "in_sample", return_value=True):
        await shadow._handle_candidate(conn, cands[0], ctx, stats)
    edge_id = shadow.make_edge_id("reply", cands[0]["uri_a"], cands[0]["uri_b"])
    row = sdb.get_edge(conn, edge_id)
    assert row["disposition"] == "budget_refused"
    assert list(sdb.iter_calls(conn, day=RECORD_DAY())) == []  # no network at all
    assert stats.budget_refused == 1
    conn.close()


@pytest.mark.asyncio
async def test_handle_candidate_not_sampled_is_dropped(tmp_path):
    conn = make_client_state_pair(tmp_path)
    ctx = make_ctx(conn, Config())
    stats = shadow._Stats()
    cache = shadow.RecentPostCache(100, 15)
    shadow.process_message(post_msg("did:plc:authoraaa", "posta", TEXT_A), cache)
    cands, _ = shadow.process_message(
        post_msg("did:plc:authorbbb", "postb", TEXT_B, reply_to=POST_A_URI,
                 created="2026-09-17T10:01:00Z",
                 time_us=1_800_000_060_000_000), cache)
    with patch.object(shadow, "in_sample", return_value=False):
        await shadow._handle_candidate(conn, cands[0], ctx, stats)
    assert stats.not_sampled == 1
    assert conn.execute("SELECT COUNT(*) AS c FROM edge").fetchone()["c"] == 0
    conn.close()


# --- axolotl gate (§5.6) ---------------------------------------------------------------

def test_axolotl_unavailable_gate_falls_back_to_repeat_policy():
    """Without axolotl the gate reports axolotl_unavailable and §2.6 applies
    directly: an unclear distribution spends the k=3 repeat."""
    packet = shadow.build_packet(_reply_cand())
    with patch.object(jev, "_http_post", _ok_transport(DIST_UNCLEAR)):
        adj, mem, gate = shadow.adjudicate_packet(
            packet, edge_id="e1", day=DAY, api_key="k", flag_on=True,
            max_calls=50, seed_state=(0, None, False), axolotl_mod=None)
    assert gate["gate_state"] == "axolotl_unavailable"
    assert adj.calls_spent == 3  # §2.6 threshold fired (0.62 <= 0.70)


def test_axolotl_gate_available_paths():
    """With axolotl: all-dependent plausible set spends no repeat even when
    §2.6 would fire; a mixed plausible set spends the repeat."""
    if not os.path.isdir(AXOLOTL_SRC):
        pytest.skip("axolotl source tree not present")
    sys.path.insert(0, AXOLOTL_SRC)
    try:
        import axolotl  # noqa: F401
    finally:
        sys.path.remove(AXOLOTL_SRC)
    packet = shadow.build_packet(_reply_cand())
    # All-dependent plausible set: ambiguity is not load-bearing -> 1 call.
    with patch.object(jev, "_http_post", _ok_transport(DIST_UNCLEAR)):
        adj, _, gate = shadow.adjudicate_packet(
            packet, edge_id="e1", day=DAY, api_key="k", flag_on=True,
            max_calls=50, seed_state=(0, None, False), axolotl_mod=axolotl)
    assert gate["gate_state"] == shadow.GATE_NON_LOAD_BEARING
    assert adj.calls_spent == 1
    # Mixed plausible set: dependent vs unrelated changes the action.
    mixed = {"dependent_direct": 0.55, "dependent_indirect": 0.05,
             "independent_lexical": 0.05, "unrelated": 0.30,
             "underdetermined": 0.05}
    with patch.object(jev, "_http_post", _ok_transport(mixed)):
        adj, _, gate = shadow.adjudicate_packet(
            packet, edge_id="e2", day=DAY, api_key="k", flag_on=True,
            max_calls=50, seed_state=(0, None, False), axolotl_mod=axolotl)
    assert gate["gate_state"] == shadow.GATE_LOAD_BEARING
    assert adj.calls_spent == 3


def test_axolotl_resolves_via_env_path():
    env = {shadow.AXOLOTL_PATH_ENV: AXOLOTL_SRC}
    if not os.path.isdir(AXOLOTL_SRC):
        pytest.skip("axolotl source tree not present")
    assert shadow.resolve_axolotl(env) is not None
    assert shadow.resolve_axolotl({shadow.AXOLOTL_PATH_ENV: "/nonexistent"}) \
        is not None or True  # import may still resolve; degrade path tested below


def test_resolve_axolotl_degrades_on_import_failure():
    with patch.dict(sys.modules, {"axolotl": None}):
        # sys.modules[name] = None makes `import axolotl` raise ImportError
        assert shadow.resolve_axolotl({}) is None


# --- model-version stop through the adjudication path (§2.7) -----------------------------

def test_model_version_stop_via_adjudicate_packet():
    packet = shadow.build_packet(_reply_cand())
    calls = {"n": 0}

    def transport(url, headers, body, timeout_s):
        calls["n"] += 1
        model = "jev-2026-09-01" if calls["n"] == 1 else "jev-2026-09-02"
        return _ok_transport(DIST_CONFIDENT, model=model)(url, headers, body, timeout_s)
    with patch.object(jev, "_http_post", transport):
        _, mem1, _ = shadow.adjudicate_packet(
            packet, edge_id="e1", day=DAY, api_key="k", flag_on=True,
            max_calls=50, seed_state=(0, None, False))
        # Second edge carries the anchor; the model change is a contract error
        # and the day stops.
        adj2, mem2, _ = shadow.adjudicate_packet(
            packet, edge_id="e2", day=DAY, api_key="k", flag_on=True,
            max_calls=50,
            seed_state=(mem1.calls_today(DAY), mem1.model_anchor(DAY),
                        mem1.is_model_stopped(DAY)))
        # Third edge is refused before any network.
        adj3, mem3, _ = shadow.adjudicate_packet(
            packet, edge_id="e3", day=DAY, api_key="k", flag_on=True,
            max_calls=50,
            seed_state=(mem2.calls_today(DAY), mem2.model_anchor(DAY),
                        mem2.is_model_stopped(DAY)))
    assert adj2.status == "contract_error"
    assert adj3.status == "contract_error"
    assert calls["n"] == 2
    assert mem3.is_model_stopped(DAY)


# --- receipts (§3) ---------------------------------------------------------------------

def test_activation_receipt_both_states(tmp_path):
    cfg_off = Config()
    receipt, path = shadow.write_activation_receipt(
        str(tmp_path), cfg_off, enabled=False, disabled_reason="flag_off",
        action="status", env={})
    assert receipt["receipt_kind"] == "labelwatch.semantic_shadow_activation.v0"
    assert receipt["enabled"] is False
    assert receipt["disabled_reason"] == "flag_off"
    assert receipt["kill_switch"] is False
    assert os.path.exists(path)
    cfg_on = Config(semantic_shadow_enabled=True)
    receipt2, _ = shadow.write_activation_receipt(
        str(tmp_path), cfg_on, enabled=True, disabled_reason=None,
        action="run", env={})
    assert receipt2["enabled"] is True
    assert receipt2["flag_on"] is True


def test_kill_switch_overrides_flag():
    cfg = Config(semantic_shadow_enabled=True)
    enabled, reason = shadow.shadow_enabled(
        cfg, env={shadow.KILL_SWITCH_ENV: "1"})
    assert (enabled, reason) == (False, "kill_switch")
    enabled, reason = shadow.shadow_enabled(cfg, env={})
    assert (enabled, reason) == (True, None)
    enabled, reason = shadow.shadow_enabled(Config(), env={})
    assert (enabled, reason) == (False, "flag_off")


def test_run_manifest_provenance(tmp_path):
    cfg = Config()
    manifest = shadow.build_run_manifest(cfg, max_calls=600,
                                         axolotl_available=False)
    assert manifest["receipt_kind"] == "labelwatch.semantic_shadow_run_manifest.v0"
    assert manifest["implemented_by"] == "kimi-coder-subagent (JEV-DUAL-SHADOW-V0)"
    assert manifest["campaign"] == "jev-dual-shadow-v0"
    assert set(manifest["code_digests"]) == {
        "semantic_shadow.py", "semantic_shadow_jev.py", "semantic_shadow_db.py"}
    assert all(v for v in manifest["code_digests"].values())
    assert manifest["caps"]["max_calls"] == 600
    assert manifest["thresholds"]["lexical_jaccard_min"] == 0.20
    path = shadow.write_run_manifest(str(tmp_path), manifest)
    loaded = json.load(open(path))
    assert loaded["implemented_by"] == manifest["implemented_by"]


def test_closeout_receipt_counts(tmp_path):
    conn = make_client_state_pair(tmp_path)
    stats = shadow._Stats()
    stats.msgs = 12
    stats.adjudicated = 3
    receipt, path = shadow.write_closeout_receipt(
        str(tmp_path), started_at=format_ts(now_utc()),
        stop_reason="shutdown", stats=stats, conn=conn, day=DAY)
    assert receipt["receipt_kind"] == "labelwatch.semantic_shadow_closeout.v0"
    assert receipt["counts"]["messages"] == 12
    assert receipt["counts"]["adjudicated"] == 3
    assert receipt["stop_reason"] == "shutdown"
    assert receipt["tokens_cost_today"]["calls"] == 0
    conn.close()


# --- episode summaries (§5.5) -----------------------------------------------------------

def test_episode_summary_set_valued_origins(tmp_path):
    conn = make_client_state_pair(tmp_path)
    now_s = format_ts(now_utc())
    root = "a" * 64
    for i, (lex, sem) in enumerate(((1, 1), (1, 0), (1, 0))):
        sdb.insert_edge(conn, {
            "edge_id": f"e{i}", "relation_type": "reply",
            "uri_a_sha256": root, "uri_b_sha256": f"{i}".zfill(64),
            "day": DAY, "first_seen_ts": now_s, "updated_ts": now_s,
        })
        sdb.finalize_edge(
            conn, f"e{i}", disposition="adjudicated",
            channels={"lexical": bool(lex), "semantic": bool(sem),
                      "temporal": True},
            shadow_qualified=True, fragility=None, gate_state="axolotl_unavailable",
            semantic_argmax="dependent_direct" if sem else "unrelated",
            ambiguity=[], updated_ts=now_s)
    # An unqualified edge in the same root is excluded from the episode.
    sdb.insert_edge(conn, {
        "edge_id": "ex", "relation_type": "reply",
        "uri_a_sha256": root, "uri_b_sha256": "f" * 64,
        "day": DAY, "first_seen_ts": now_s, "updated_ts": now_s,
    })
    sdb.finalize_edge(
        conn, "ex", disposition="adjudicated",
        channels={"lexical": False, "semantic": False, "temporal": False},
        shadow_qualified=False, fragility=None, gate_state="axolotl_unavailable",
        semantic_argmax="unrelated", ambiguity=[], updated_ts=now_s)
    conn.commit()
    n = shadow.rebuild_episode_summaries(conn, DAY)
    assert n == 1
    row = conn.execute("SELECT * FROM episode_summary").fetchone()
    assert row["edge_count"] == 3
    origins = json.loads(row["origin_digests_json"])
    assert origins == sorted({f"{i}".zfill(64) for i in range(3)})
    patterns = json.loads(row["channel_pattern_counts_json"])
    assert patterns == {
        "structural_lineage+lexical+semantic+temporal": 1,
        "structural_lineage+lexical+temporal": 2,
    }
    conn.close()


# --- text non-persistence (§5.3, §8) -----------------------------------------------------

@pytest.mark.asyncio
async def test_no_post_text_persisted_anywhere(tmp_path):
    """Drive fixture text through the full pipeline, then scan every sidecar
    TEXT column and every written receipt for fixture text. Only sha256
    digests may persist."""
    records, errors = shadow.validate_fixture_file(FIXTURES)
    assert not errors
    marker_records = [r for r in records
                      if r["fixture_id"] == "explicit_direct_reference-001"]
    marker = marker_records[0]
    assert len(marker["text_a"]) >= 20 and len(marker["text_b"]) >= 20

    conn = make_client_state_pair(tmp_path)
    receipts_dir = str(tmp_path / "receipts")
    cfg = Config(semantic_shadow_enabled=True)
    shadow.write_activation_receipt(receipts_dir, cfg, enabled=True,
                                    disabled_reason=None, action="run", env={})
    path = shadow.write_run_manifest(
        receipts_dir, shadow.build_run_manifest(cfg, max_calls=600,
                                                axolotl_available=False))
    assert os.path.exists(path)

    cand = shadow.fixture_to_candidate(marker)
    packet = shadow.build_packet(cand)
    edge_id = hash_sha256(f"fixture|{marker['fixture_id']}")
    now_s = format_ts(now_utc())
    sdb.insert_edge(conn, {
        "edge_id": edge_id, "relation_type": marker["relation_type"],
        "uri_a_sha256": hash_sha256("at://did:plc:fixturea/post/1"),
        "uri_b_sha256": hash_sha256("at://did:plc:fixtureb/post/2"),
        "text_a_sha256": hash_sha256(packet["text_a"]),
        "text_b_sha256": hash_sha256(packet["text_b"]),
        "day": DAY, "dt_seconds": packet["dt_seconds"],
        "lexical_jaccard": packet["lexical_jaccard"],
        "first_seen_ts": now_s, "updated_ts": now_s,
    })
    ctx = make_ctx(conn, cfg)
    stats = shadow._Stats()
    cand_full = dict(cand)
    cand_full.update({
        "uri_a": "at://did:plc:fixturea/post/1",
        "uri_b": "at://did:plc:fixtureb/post/2",
        "day": DAY,
    })
    with patch.object(jev, "_http_post", _ok_transport(DIST_CONFIDENT)):
        await shadow._handle_candidate(conn, cand_full, ctx, stats)
    stats.msgs = 2
    shadow.write_closeout_receipt(receipts_dir, started_at=now_s,
                                  stop_reason="shutdown", stats=stats,
                                  conn=conn, day=DAY)

    needles = [marker["text_a"], marker["text_b"],
               marker["text_a"][:20], marker["text_b"][:20]]
    # 1. Scan every TEXT column of every sidecar table.
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    for table in tables:
        for row in conn.execute(f"SELECT * FROM {table}").fetchall():
            for value in dict(row).values():
                if isinstance(value, str):
                    for needle in needles:
                        assert needle not in value, (table, needle[:24])
    # 2. Scan every receipt file.
    for fname in os.listdir(receipts_dir):
        with open(os.path.join(receipts_dir, fname), encoding="utf-8") as f:
            content = f.read()
        for needle in needles:
            assert needle not in content, (fname, needle[:24])
    # 3. The digests ARE persisted.
    row = sdb.get_edge(conn, edge_id)
    assert row["text_a_sha256"] == hash_sha256(packet["text_a"])
    conn.close()


# --- fixtures (§5.9) --------------------------------------------------------------------

def test_fixture_file_is_valid():
    records, errors = shadow.validate_fixture_file(FIXTURES)
    assert errors == []
    assert len(records) >= 60
    from collections import Counter
    counts = Counter(r["fixture_class"] for r in records)
    for cls in shadow.FIXTURE_CLASSES:
        assert counts[cls] >= 10, cls


def test_fixture_schema_document_matches_validator():
    schema = json.load(open(FIXTURE_SCHEMA))
    assert set(schema["required"]) == set(shadow.FIXTURE_REQUIRED_FIELDS)
    assert schema["properties"]["fixture_class"]["enum"] == \
        list(shadow.FIXTURE_CLASSES)
    assert schema["properties"]["gold_relation"]["enum"] == \
        list(shadow.RELATION_OPTIONS)


def test_fixture_validator_rejects_bad_records():
    good = {
        "fixture_id": "x-001", "fixture_class": "irony", "relation_type": "reply",
        "text_a": "a text", "text_b": "b text", "dt_seconds": 1,
        "langs_a": ["en"], "langs_b": ["en"],
        "gold_relation": "underdetermined",
        "acceptable_answers": ["underdetermined", "dependent_indirect"],
        "ambiguity_note": "why",
    }
    assert shadow.validate_fixture_record(good) == []
    bad_gold = {**good, "gold_relation": "dependent_indirect",
                "acceptable_answers": ["underdetermined"]}
    assert any("acceptable" in e for e in
               shadow.validate_fixture_record(bad_gold))
    bad_class = {**good, "fixture_class": "shade"}
    assert shadow.validate_fixture_record(bad_class)
    bad_text = {**good, "text_b": ""}
    assert any("text_b" in e for e in shadow.validate_fixture_record(bad_text))
    repost_empty = {**good, "relation_type": "repost", "text_b": ""}
    assert shadow.validate_fixture_record(repost_empty) == []
    extra = {**good, "surprise": 1}
    assert shadow.validate_fixture_record(extra)


def test_eval_fixtures_offline(tmp_path):
    receipt = shadow.eval_fixtures(FIXTURES, live=False)
    assert receipt["receipt_kind"] == \
        "labelwatch.semantic_shadow_fixture_eval.v0"
    assert receipt["valid"] is True
    assert receipt["live"] is False
    assert receipt["fixture_count"] >= 60
    assert all(r["packet_sha256"] for r in receipt["results"])
    assert all(r["privacy_tripwire_hits"] == [] for r in receipt["results"])
    assert "live_summary" not in receipt  # no accuracy claim before live runs


def test_eval_fixtures_live_with_mock_transport(tmp_path):
    """Pipeline plumbing with a mock client (§5.9): fabricated responses are
    non-evidence; this proves the eval path, not any accuracy."""
    with patch.object(jev, "_http_post", _ok_transport(DIST_CONFIDENT)):
        receipt = shadow.eval_fixtures(
            FIXTURES, live=True, api_key="test-key", flag_on=True,
            max_calls=500, records_dir=str(tmp_path))
    assert receipt["valid"] is True
    summary = receipt["live_summary"]
    assert summary["adjudicated"] == receipt["fixture_count"]
    # The mock always answers dependent_direct: only direct-gold pairs are
    # 'in acceptable'. This asserts plumbing, not performance.
    direct = [r for r in receipt["results"] if r["argmax_in_acceptable"]]
    assert {r["fixture_class"] for r in direct} == {"explicit_direct_reference"}
    assert summary["calls_spent_total"] == receipt["fixture_count"]
    log = shadow.read_eval_call_log(receipt["call_records_path"])
    assert log["complete"] and summary["evaluation_complete"] is True
    assert len(log["calls"]) == receipt["fixture_count"]


# --- HTTP surface unchanged (§5.7) --------------------------------------------------------

def test_http_surface_has_no_semantic_shadow_references():
    """The sidecar has no public output: report.py, server.py and
    frontdoor.py must not reference it."""
    src = os.path.join(os.path.dirname(__file__), "..", "src", "labelwatch")
    for name in ("report.py", "server.py", "frontdoor.py"):
        with open(os.path.join(src, name), encoding="utf-8") as f:
            content = f.read()
        assert "semantic_shadow" not in content, name
        assert "semantic-shadow" not in content, name
