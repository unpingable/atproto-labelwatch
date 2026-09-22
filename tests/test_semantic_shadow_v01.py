"""V0.1 qualification rule (Amendment 2026-09-17-C §3) — mechanical guarantees.

Offline. Adjudications are built from fabricated §2.5-shaped records; nothing
here is evidence about any model.
"""
import inspect
import itertools

import pytest

from labelwatch import semantic_shadow as shadow
from labelwatch import semantic_shadow_jev as jev
from labelwatch import semantic_shadow_v01 as v01

OPTS = shadow.RELATION_OPTIONS
FAILED_STATUSES = ("http_error", "timeout", "contract_error", "budget_refused",
                   "key_absent", "flag_off")


def dist(top, p):
    """Distribution with ``top`` at p and the remainder on the next option."""
    out = {o: 0.0 for o in OPTS}
    out[top] = round(p, 2)
    other = OPTS[(OPTS.index(top) + 1) % len(OPTS)]
    out[other] = round(1.0 - p, 2)
    return out


def rec(probs, idx=1, status="ok", sent=True):
    return {"status": status, "probabilities": probs if status == "ok" else None,
            "argmax_recomputed": jev.recompute_argmax(probs, OPTS) if status == "ok" and probs else None,
            "replicate_index": idx, "latency_s": 0.3 if sent else None,
            "call_id": f"c{idx}", "replicate_set_id": "s"}


def adjudication(records):
    meds = jev.replicate_medians(records, OPTS)
    ok = any(r["status"] == "ok" for r in records)
    return jev.Adjudication(
        status="ok" if ok else records[-1]["status"], question_id="q",
        replicate_set_id="s", records=records, median_probabilities=meds,
        argmax=jev.recompute_argmax(meds, OPTS) if meds else None,
        ambiguity_set=jev.ambiguity_set(meds) if meds else [])


def single(top, p):
    return adjudication([rec(dist(top, p))])


def repeated(top, p, draws=3):
    return adjudication([rec(dist(top, p), i) for i in range(1, draws + 1)])


ANCHOR_ON = v01.AnchorEvidence(True, False, False, 0.0)
ANCHOR_OFF = v01.AnchorEvidence(False, False, False, 0.05)
CTX = v01.Context("reply", False, 60, 1, ("en",), ("en",))


def decide(ctx, anchor, adj):
    return v01.decide(ctx, anchor, v01.semantic_view(adj))


# --- the exhaustive sweep ---------------------------------------------------------

P_GRID = (0.30, 0.50, 0.69, 0.70, 0.71, 0.85, 0.89, 0.90, 0.91, 0.95, 1.00)
ANCHORS = [v01.AnchorEvidence(u, h, l, 0.3 if l else 0.1)
           for u, h, l in itertools.product((False, True), repeat=3)]
CONTEXTS = [v01.Context(rt, empty, dt, depth)
            for rt in ("reply", "quote", "repost")
            for empty in (False, True)
            for dt in (0, 5, 900, 3600, 86400, None)
            for depth in (None, 1, 7)]


def all_adjudications():
    for top in OPTS:
        for p in P_GRID:
            yield ("single", top, p), single(top, p)
            yield ("repeat", top, p), repeated(top, p)
    for status in FAILED_STATUSES:
        yield ("failed", status, None), adjudication([rec(None, status=status)])


def test_sweep_qualifies_only_on_anchor_and_first_call_explicit():
    seen = set()
    for (kind, top, p), adj in all_adjudications():
        sem = v01.semantic_view(adj)
        for anchor in ANCHORS:
            for ctx in CONTEXTS:
                out = v01.decide(ctx, anchor, sem)
                seen.add(out.disposition)
                expect = (kind == "single" and top == "dependent_direct"
                          and p >= 0.90 and p > 0.70 and anchor.present
                          and not (ctx.relation_type == "repost" and ctx.b_text_empty))
                assert (out.disposition == v01.QUALIFIED_EXPLICIT) == expect, \
                    (kind, top, p, anchor, ctx, out)
                if out.disposition == v01.QUALIFIED_EXPLICIT:
                    assert out.j_explicit and out.anchor_present
    assert seen == set(v01.DISPOSITIONS)


def test_context_never_upgrades():
    """Structure + timing alone never qualify; context can only downgrade."""
    for (_, _, _), adj in all_adjudications():
        sem = v01.semantic_view(adj)
        for anchor in ANCHORS:
            outs = {v01.decide(ctx, anchor, sem).disposition for ctx in CONTEXTS}
            base = v01.decide(CTX, anchor, sem).disposition
            if base != v01.QUALIFIED_EXPLICIT:
                assert v01.QUALIFIED_EXPLICIT not in outs


def test_structure_and_timing_alone_cannot_qualify():
    for ctx in CONTEXTS:
        for adj in (adjudication([rec(None, status="http_error")]),
                    single("unrelated", 0.95), single("underdetermined", 0.95)):
            assert decide(ctx, ANCHOR_OFF, adj).disposition != v01.QUALIFIED_EXPLICIT


def test_context_signals_are_not_anchor_inputs():
    params = inspect.signature(v01.anchors).parameters
    assert list(params) == ["text_a", "text_b"]
    fields = set(v01.AnchorEvidence.__dataclass_fields__)
    assert fields == {"shared_url_domain", "shared_hashtag", "lexical_overlap",
                      "lexical_jaccard"}
    packet = shadow.build_packet({
        "relation_type": "reply", "dt_seconds": 10, "thread_depth_b": 1,
        "a": {"text": "notes on the #harbour survey", "langs": ["en"]},
        "b": {"text": "reading the #harbour survey now", "langs": ["en"]}})
    base = v01.anchors_from_packet(packet)
    for rt, dt, depth in itertools.product(("reply", "quote", "repost"),
                                           (0, 3600, 10 ** 6), (None, 9)):
        varied = dict(packet, relation_type=rt, dt_seconds=dt, thread_depth_b=depth,
                      shared_url_domain=True, shared_hashtag=False, lexical_jaccard=0.99)
        assert v01.anchors_from_packet(varied) == base


def test_jev_alone_cannot_qualify():
    out = decide(CTX, ANCHOR_OFF, single("dependent_direct", 1.0))
    assert out.disposition == v01.NOT_QUALIFIED
    assert v01.ROUTE_ANCHOR_SEMANTIC_CONFLICT in out.review_routes
    assert v01.ROUTE_INFERRED_CLAIM in out.review_routes


def test_anchor_alone_cannot_qualify():
    for top in ("unrelated", "independent_lexical"):
        out = decide(CTX, ANCHOR_ON, single(top, 0.95))
        assert out.disposition == v01.NOT_QUALIFIED
        assert out.review_route == v01.ROUTE_ANCHOR_SEMANTIC_CONFLICT


@pytest.mark.parametrize("top", ["dependent_indirect", "underdetermined"])
def test_indirect_and_underdetermined_outputs_never_qualify(top):
    for p in P_GRID:
        adj = single(top, p)
        for anchor in ANCHORS:
            out = decide(CTX, anchor, adj)
            assert out.disposition != v01.QUALIFIED_EXPLICIT
            if adj.argmax == top:  # the output really is indirect/underdetermined
                assert out.disposition == v01.UNDERDETERMINED
                assert out.rule_step == 4


def test_indirect_output_routes_as_inferred_claim():
    out = decide(CTX, ANCHOR_ON, single("dependent_indirect", 0.99))
    assert v01.ROUTE_INFERRED_CLAIM in out.review_routes


def test_repeat_triggered_case_cannot_qualify():
    for p in P_GRID:
        adj = repeated("dependent_direct", p)
        out = decide(CTX, ANCHOR_ON, adj)
        assert out.disposition == v01.UNDERDETERMINED  # replicates spent
        if jev.repeat_triggered(adj.records[0]["probabilities"], OPTS):
            assert v01.ROUTE_AMBIGUITY in out.review_routes


def test_trigger_without_spend_still_cannot_qualify():
    # Trigger met but the replicate set was refused before the wire.
    probs = dist("dependent_direct", 0.70)
    adj = adjudication([rec(probs), rec(None, 2, "budget_refused", sent=False)])
    sem = v01.semantic_view(adj)
    assert sem.repeat_triggered and sem.replicates_spent == 0
    assert v01.decide(CTX, ANCHOR_ON, sem).disposition == v01.NOT_QUALIFIED


def test_no_high_first_call_can_trigger_a_repeat():
    """The downgrade-only property: p >= 0.90 can never meet the §2.6 trigger."""
    for p in (0.90, 0.91, 0.95, 0.99, 1.0):
        for rest in OPTS[1:]:
            probs = {o: 0.0 for o in OPTS}
            probs["dependent_direct"] = p
            probs[rest] = round(1 - p, 2)
            assert not jev.repeat_triggered(probs, OPTS)


@pytest.mark.parametrize("p", [0.30, 0.70, 0.71, 0.85, 0.89, 0.899])
def test_below_tau_cannot_qualify(p):
    out = decide(CTX, ANCHOR_ON, single("dependent_direct", p) if p != 0.899 else
                 adjudication([rec({**{o: 0.0 for o in OPTS},
                                    "dependent_direct": 0.899, "unrelated": 0.101})]))
    assert out.disposition != v01.QUALIFIED_EXPLICIT


def test_tau_boundary_is_inclusive():
    assert decide(CTX, ANCHOR_ON, single("dependent_direct", 0.90)).disposition \
        == v01.QUALIFIED_EXPLICIT
    assert v01.TAU_E == 0.90


@pytest.mark.parametrize("status", FAILED_STATUSES)
def test_unavailable_semantics_cannot_qualify(status):
    out = decide(CTX, ANCHOR_ON, adjudication([rec(None, status=status)]))
    assert out.disposition == v01.SEMANTIC_UNAVAILABLE
    assert out.rule_step == 1
    assert out.review_route == v01.ROUTE_SEMANTIC_UNAVAILABLE


def test_semantic_unavailable_is_distinct_from_underdetermined():
    a = decide(CTX, ANCHOR_OFF, adjudication([rec(None, status="timeout")]))
    b = decide(CTX, ANCHOR_OFF, single("underdetermined", 0.9))
    assert a.disposition == v01.SEMANTIC_UNAVAILABLE
    assert b.disposition == v01.UNDERDETERMINED


def test_underdetermined_is_terminal():
    """An underdetermined result stays underdetermined whatever anchors or
    context are present; nothing resolves it into a relation."""
    for anchor, ctx in itertools.product(ANCHORS, CONTEXTS):
        if ctx.relation_type == "repost" and ctx.b_text_empty:
            continue  # step 2 precedes step 4 (no content evidence)
        out = decide(ctx, anchor, single("underdetermined", 0.99))
        assert out.disposition == v01.UNDERDETERMINED


def test_repost_with_empty_text_is_not_qualified_even_if_jev_and_anchor_agree():
    ctx = v01.Context("repost", True, 5, None)
    out = decide(ctx, ANCHOR_ON, single("dependent_direct", 1.0))
    assert out.disposition == v01.NOT_QUALIFIED and out.rule_step == 2


def test_qualified_example_and_routes():
    out = decide(CTX, ANCHOR_ON, single("dependent_direct", 0.95))
    assert out.disposition == v01.QUALIFIED_EXPLICIT and out.rule_step == 3
    assert v01.ROUTE_ANCHOR_SEMANTIC_CONFLICT not in out.review_routes


def test_ambiguity_route_on_cross_group_median_set():
    probs = {o: 0.0 for o in OPTS}
    probs.update(unrelated=0.75, dependent_direct=0.25)
    out = decide(CTX, ANCHOR_OFF, adjudication([rec(probs)]))
    assert out.disposition == v01.NOT_QUALIFIED
    assert v01.ROUTE_AMBIGUITY in out.review_routes


def test_routes_never_qualify():
    """Every routed outcome in the sweep that qualifies does so only via the
    anchor ∧ J_explicit conjunction; routes are orthogonal."""
    for (_, _, _), adj in all_adjudications():
        sem = v01.semantic_view(adj)
        for anchor in ANCHORS:
            out = v01.decide(CTX, anchor, sem)
            assert set(out.review_routes) <= set(v01.ROUTES)
            if out.disposition == v01.QUALIFIED_EXPLICIT:
                assert anchor.present and v01.j_explicit(sem)


# --- validation-only distinctive token ----------------------------------------------

def test_decide_takes_no_distinctive_token_input():
    params = inspect.signature(v01.decide).parameters
    assert list(params) == ["context", "anchor", "sem"]
    for name in ("review_routes", "j_explicit", "anchors", "semantic_view"):
        assert "token" not in " ".join(inspect.signature(getattr(v01, name)).parameters)


def test_distinctive_token_has_zero_effect_on_disposition():
    base_a = "Budget meeting moved to room 42 after the renovation"
    variants = [
        ("Budget meeting moved to room 42 after the renovation", "Room 42 renovation delayed"),
        ("Budget meeting moved", "Budget talk"),
        ("Quarterly forecast 2031 published", "Forecast 2031 looks optimistic"),
    ]
    for a, b in variants:
        packet = shadow.build_packet({
            "relation_type": "reply", "dt_seconds": 30, "thread_depth_b": 1,
            "a": {"text": a, "langs": ["en"]}, "b": {"text": b, "langs": ["en"]}})
        for (_, _, _), adj in list(all_adjudications())[::7]:
            rec_ = v01.evaluate_packet(packet, adj)
            ctx = v01.context_from_packet(packet)
            direct = v01.decide(ctx, v01.anchors_from_packet(packet),
                                v01.semantic_view(adj))
            assert rec_["disposition"] == direct.disposition
            assert rec_["review_routes"] == list(direct.review_routes)
    assert base_a  # the texts above are synthetic plumbing strings


def test_distinctive_token_components_reported_separately():
    f = v01.distinctive_token_feature(
        "Shipped release 42 of the renderer yesterday",
        "Release 42 of the renderer is great")
    assert f.numeric and f.numeric_count == 1
    assert f.long_word and f.long_word_count == 2  # release, renderer (yesterday is stop-listed)
    assert f.validation_only is True
    g = v01.distinctive_token_feature("a 7 b", "c 7 d")  # single digit: not numeric
    assert not g.numeric and not g.long_word
    h = v01.distinctive_token_feature("thanks [mention] everything",
                                      "hi [mention] everything")
    assert not h.long_word  # stop-listed tokens only


def test_evaluate_packet_keeps_token_under_validation_only_and_no_text():
    text_a, text_b = "Notes on the 2031 harbour plan #harbour", "The #harbour plan for 2031"
    packet = shadow.build_packet({
        "relation_type": "quote", "dt_seconds": 30, "thread_depth_b": None,
        "a": {"text": text_a, "langs": ["en"]}, "b": {"text": text_b, "langs": ["en"]}})
    out = v01.evaluate_packet(packet, single("dependent_direct", 0.95))
    assert out["disposition"] == v01.QUALIFIED_EXPLICIT
    assert set(out["validation_only"]) == {"shared_distinctive_token"}
    assert "shared_distinctive_token" not in out["anchors"]
    import json
    blob = json.dumps(out)
    assert "harbour" not in blob and "2031" not in blob


# --- admitted anchors ------------------------------------------------------------

def test_generic_host_domains_are_not_anchors():
    a = "see https://bsky.app/profile/x and https://news.example.co.uk/a"
    b = "also https://bsky.app/other and https://sport.other.co.uk/b"
    assert not v01.anchors(a, b).shared_url_domain
    assert shadow.shared_url_domain(a, b)  # the packet feature is unchanged
    assert v01.anchors("https://example.org/a x", "https://www.example.org/b y").shared_url_domain


def test_hashtag_and_lexical_anchors():
    assert v01.anchors("tides #HarbourWatch", "see #harbourwatch").shared_hashtag
    same = "the committee approved the new cycling lanes downtown"
    ev = v01.anchors(same, same + " today")
    assert ev.lexical_overlap and ev.lexical_jaccard >= 0.20
    assert ev.anchor_type == "lexical_overlap"
    assert v01.anchors("apples", "oranges").anchor_type == "none"


def test_frozen_parameters_are_recorded():
    fp = v01.FROZEN_PARAMETERS
    assert fp["tau_e"] == 0.90
    assert fp["admitted_anchors"] == ["shared_url_domain", "shared_hashtag",
                                      "lexical_overlap"]
    assert fp["lexical_jaccard_min"] == shadow.LEXICAL_JACCARD_MIN == 0.20
    assert len(fp["generic_hosts_sha256"]) == 64
    assert v01.collapse("dependent_indirect") == "dependent"
    assert v01.DEPENDENT == shadow.DEPENDENT_RELATIONS


def test_v01_is_not_wired_into_the_runtime():
    import pathlib
    src = pathlib.Path(shadow.__file__).parent
    for name in ("semantic_shadow.py", "semantic_shadow_db.py", "runner.py",
                 "report.py", "server.py"):
        assert "semantic_shadow_v01" not in (src / name).read_text()
