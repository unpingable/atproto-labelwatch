"""Offline tests for the thin JEV-CONTRACT-v0 client (semantic_shadow_jev).

The transport is mocked at the single seam `_http_post`; fabricated
responses are non-evidence (typesafe harness pattern). No network."""
import json
import socket
import urllib.error
from unittest.mock import patch

import pytest

from labelwatch import semantic_shadow_jev as jev

OPTIONS = ("dependent_direct", "dependent_indirect", "independent_lexical",
           "unrelated", "underdetermined")
DAY = "2026-09-17"

# Valid distributions on the 0.01 transport grid, summing to 1.00.
DIST_CONFIDENT = {"dependent_direct": 0.72, "dependent_indirect": 0.14,
                  "independent_lexical": 0.05, "unrelated": 0.06,
                  "underdetermined": 0.03}
DIST_TOP1_BOUNDARY = {"dependent_direct": 0.70, "dependent_indirect": 0.12,
                      "independent_lexical": 0.07, "unrelated": 0.06,
                      "underdetermined": 0.05}
DIST_GAP_BOUNDARY = {"dependent_direct": 0.42, "dependent_indirect": 0.27,
                     "independent_lexical": 0.12, "unrelated": 0.11,
                     "underdetermined": 0.08}
DIST_AMBIGUOUS = {"dependent_direct": 0.55, "dependent_indirect": 0.05,
                  "independent_lexical": 0.25, "unrelated": 0.10,
                  "underdetermined": 0.05}


def make_transport(responder):
    """responder(body_dict, n_calls) -> (status, bytes[, headers]) or raises.
    Two-tuples get the default recorded-shape headers."""
    calls = []

    def transport(url, headers, body, timeout_s):
        calls.append(json.loads(body.decode("utf-8")))
        out = responder(calls[-1], len(calls))
        if len(out) == 2:
            out = (*out, {jev.REQUEST_ID_HEADER: "req_test",
                          "content-type": "application/json"})
        return out

    return transport, calls


def ok_response(probs, model="jev-2026-09-01", choice=None, confidence=0.9,
                family="edge_relation"):
    """Fabricated values in the documented (recorded) System One shape."""
    body = {
        "model": model,
        "answers": {family: {"type": "choice", "choice": choice,
                             "confidence": confidence,
                             "probabilities": probs}},
        "usage": {"input_tokens": 700, "output_tokens": 12},
    }
    return 200, json.dumps(body).encode("utf-8")


def make_client(state=None, **kw):
    kwargs = dict(
        decision_family="edge_relation",
        options=OPTIONS,
        api_key="test-key",
        flag_on=True,
        max_calls=50,
        state=state or jev.MemoryClientState(),
        sleeper=lambda _: None,
    )
    kwargs.update(kw)
    return jev.JevContractClient(**kwargs)


def adjudicate_with(probs, state=None, **client_kw):
    transport, calls = make_transport(lambda body, n: ok_response(probs))
    client = make_client(state=state, **client_kw)
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    return adj, calls


# --- happy path + §2.6 repeat policy -------------------------------------------

def test_confident_answer_spends_one_call():
    adj, calls = adjudicate_with(DIST_CONFIDENT)
    assert adj.status == "ok"
    assert adj.calls_spent == 1
    assert len(calls) == 1
    assert adj.argmax == "dependent_direct"
    assert adj.median_probabilities == DIST_CONFIDENT
    assert adj.ambiguity_set == ["dependent_direct", "dependent_indirect"]


def test_repeat_triggered_at_top1_boundary():
    """p_top1 == 0.70 is on the frozen grid and triggers k=3 (§2.6)."""
    adj, calls = adjudicate_with(DIST_TOP1_BOUNDARY)
    assert adj.status == "ok"
    assert adj.calls_spent == 3
    assert len(calls) == 3
    # Identical serialized repeats: same digest and set id, indices 1..3.
    digests = {r["request_digest"] for r in adj.records}
    assert len(digests) == 1
    assert [r["replicate_index"] for r in adj.records] == [1, 2, 3]
    assert len({r["replicate_set_id"] for r in adj.records}) == 1


def test_repeat_triggered_at_gap_boundary():
    """(p_top1 - p_top2) == 0.15 on the grid triggers k=3 (§2.6)."""
    adj, _ = adjudicate_with(DIST_GAP_BOUNDARY)
    assert adj.calls_spent == 3
    assert adj.repeat_spent is True


def test_no_repeat_just_above_both_boundaries():
    dist = {"dependent_direct": 0.71, "dependent_indirect": 0.15,
            "independent_lexical": 0.05, "unrelated": 0.05,
            "underdetermined": 0.04}
    adj, calls = adjudicate_with(dist)
    assert adj.calls_spent == 1
    assert len(calls) == 1


def test_ambiguity_set_uses_replicate_median_floor():
    adj, _ = adjudicate_with(DIST_AMBIGUOUS)
    # medians >= 0.10: dependent_direct 0.55, independent_lexical 0.25,
    # unrelated 0.10 (boundary inclusive)
    assert adj.ambiguity_set == ["dependent_direct", "independent_lexical",
                                 "unrelated"]


def test_repeat_decider_overrides_threshold_policy():
    """The §5.6 hook: an external decider suppresses the repeat spend."""
    transport, calls = make_transport(lambda b, n: ok_response(DIST_TOP1_BOUNDARY))
    client = make_client()
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY,
                                repeat_decider=lambda records: False)
    assert adj.calls_spent == 1
    assert len(calls) == 1


# --- §2.3 argmax ---------------------------------------------------------------

def test_argmax_recomputed_not_returned_choice():
    def responder(body, n):
        return ok_response(DIST_CONFIDENT, choice="unrelated")  # wrong on purpose
    transport, _ = make_transport(responder)
    client = make_client()
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    rec = adj.records[0]
    assert rec["argmax_recomputed"] == "dependent_direct"
    assert rec["choice_returned"] == "unrelated"
    assert rec["choice_argmax_discrepancy"] is True
    assert rec["status"] == "ok"  # flagged, not fatal


def test_argmax_tie_breaks_to_first_frozen_option():
    dist = {o: 0.20 for o in OPTIONS}
    adj, _ = adjudicate_with(dist)
    assert adj.records[0]["argmax_recomputed"] == "dependent_direct"


# --- §2.4 contract validation -----------------------------------------------------

def test_contract_error_on_probability_key_mismatch():
    bad = dict(DIST_CONFIDENT)
    bad["typo_option"] = bad.pop("underdetermined")
    adj, _ = adjudicate_with(bad)
    assert adj.status == "contract_error"
    assert adj.records[0]["status"] == "contract_error"


def test_contract_error_on_sum_drift():
    bad = {o: 0.30 for o in OPTIONS}  # sum 1.50
    adj, _ = adjudicate_with(bad)
    assert adj.status == "contract_error"


def test_contract_error_on_out_of_range_value():
    bad = dict(DIST_CONFIDENT)
    bad["dependent_direct"] = 1.20
    adj, _ = adjudicate_with(bad)
    assert adj.status == "contract_error"


def test_contract_error_on_non_object_response():
    transport, _ = make_transport(lambda b, n: (200, b"[1,2,3]"))
    client = make_client()
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    assert adj.status == "contract_error"


# --- §2.7 model-version stop --------------------------------------------------------

def test_model_version_change_stops_day():
    state = jev.MemoryClientState()

    def responder(body, n):
        model = "jev-2026-09-01" if n == 1 else "jev-2026-09-02"
        return ok_response(DIST_CONFIDENT, model=model)
    transport, calls = make_transport(responder)
    client = make_client(state=state)
    with patch.object(jev, "_http_post", transport):
        first = client.adjudicate(question_id="edge:1", prompt="p", day=DAY)
        second = client.adjudicate(question_id="edge:2", prompt="p", day=DAY)
        third = client.adjudicate(question_id="edge:3", prompt="p", day=DAY)
    assert first.status == "ok"
    # The call that observed the change carries contract_error (receipt written).
    assert second.status == "contract_error"
    assert second.records[0]["model_resolved"] == "jev-2026-09-02"
    # Further calls that day are refused before the network.
    assert third.status == "contract_error"
    assert len(calls) == 2
    assert state.is_model_stopped(DAY)


def test_model_anchor_stable_within_day():
    state = jev.MemoryClientState()
    transport, calls = make_transport(lambda b, n: ok_response(DIST_CONFIDENT))
    client = make_client(state=state)
    with patch.object(jev, "_http_post", transport):
        a = client.adjudicate(question_id="edge:1", prompt="p", day=DAY)
        b = client.adjudicate(question_id="edge:2", prompt="p", day=DAY)
    assert a.status == b.status == "ok"
    assert state.model_anchor(DAY) == "jev-2026-09-01"
    assert len(calls) == 2


# --- §2.9 gates ------------------------------------------------------------------

def test_key_absent_refuses_without_network():
    transport, calls = make_transport(lambda b, n: ok_response(DIST_CONFIDENT))
    client = make_client(api_key=None)
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    assert adj.status == "key_absent"
    assert calls == []
    assert adj.records[0]["status"] == "key_absent"  # refusal is recorded


def test_flag_off_refuses_without_network():
    transport, calls = make_transport(lambda b, n: ok_response(DIST_CONFIDENT))
    client = make_client(flag_on=False)
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    assert adj.status == "flag_off"
    assert calls == []


def test_daily_cap_refuses_after_max_calls():
    state = jev.MemoryClientState()
    transport, calls = make_transport(lambda b, n: ok_response(DIST_CONFIDENT))
    client = make_client(state=state, max_calls=1)
    with patch.object(jev, "_http_post", transport):
        first = client.adjudicate(question_id="edge:1", prompt="p", day=DAY)
        second = client.adjudicate(question_id="edge:2", prompt="p", day=DAY)
    assert first.status == "ok"
    assert second.status == "budget_refused"
    assert len(calls) == 1
    assert state.calls_today(DAY) == 1


# --- §2.8 failure handling -----------------------------------------------------------

def _http_error(code):
    return urllib.error.HTTPError(
        url=jev.ENDPOINT, code=code, msg="err", hdrs=None, fp=None)


def test_retry_once_on_429_then_ok():
    def responder(body, n):
        if n == 1:
            raise _http_error(429)
        return ok_response(DIST_CONFIDENT)
    slept = []
    transport, calls = make_transport(responder)
    client = make_client(sleeper=slept.append)
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    assert adj.status == "ok"
    assert len(calls) == 2
    assert slept == [jev.RETRY_BACKOFF_S]


def test_second_429_fails_closed():
    transport, calls = make_transport(lambda b, n: (_ for _ in ()).throw(_http_error(429)))
    client = make_client()
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    assert adj.status == "http_error"
    assert adj.records[0]["http_status"] == 429
    assert len(calls) == 2  # exactly one retry


def test_no_retry_on_500():
    transport, calls = make_transport(lambda b, n: (_ for _ in ()).throw(_http_error(500)))
    client = make_client()
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    assert adj.status == "http_error"
    assert len(calls) == 1


def test_socket_timeout_status():
    def boom(body, n):
        raise socket.timeout("timed out")
    transport, calls = make_transport(boom)
    client = make_client()
    with patch.object(jev, "_http_post", transport):
        adj = client.adjudicate(question_id="edge:test", prompt="p", day=DAY)
    assert adj.status == "timeout"
    assert len(calls) == 1


# --- record shape (§2.5) ------------------------------------------------------------

def test_record_carries_all_contract_fields():
    adj, _ = adjudicate_with(DIST_CONFIDENT)
    rec = adj.records[0]
    for field_name in jev.CALL_RECORD_FIELDS:
        assert field_name in rec, f"missing §2.5 field {field_name}"
    assert rec["campaign"] == "jev-dual-shadow-v0"
    assert rec["component"] == "labelwatch.semantic_shadow"
    assert rec["model_requested"] == "jev-latest"
    assert rec["model_resolved"] == "jev-2026-09-01"
    assert rec["confidence_observed"] == 0.9  # observational only
    assert rec["est_cost_usd"] == pytest.approx(700 * 0.042 / 1_000_000)
    assert rec["typesafe_request_id"] == "req_test"


def test_preflight_estimate_is_worst_case():
    est = jev.estimate_cost_usd(600)
    assert est["est_input_tokens_worst_case"] == 600 * jev.EST_INPUT_TOKENS_PER_CALL
    assert est["est_cost_usd_worst_case"] == pytest.approx(
        600 * jev.EST_INPUT_TOKENS_PER_CALL * 0.042 / 1_000_000)
    assert "unverified" in est["rate_basis"]
