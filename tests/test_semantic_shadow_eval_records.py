"""Durable §2.5 per-call records for `eval-fixtures --live` (offline).

The transport is mocked at `_http_post`. The confident response is the
verbatim recorded Labelwatch live response (2026-09-17); other responses use
the same recorded shape with fabricated values (non-evidence). No network.
"""
import json
import os
import socket
import urllib.error
from pathlib import Path
from unittest.mock import patch

import pytest

from labelwatch import cli
from labelwatch import semantic_shadow as shadow
from labelwatch import semantic_shadow_jev as jev

HERE = Path(__file__).parent
FIXTURES = str(HERE / "fixtures" / "semantic_relations.jsonl")
RECORDED = next(
    r for r in json.loads(
        (HERE / "fixtures" / "typesafe_systemone_recorded.json").read_text()
    )["responses"] if r["decision_family"] == "edge_relation")
SECRET = "sk-test-SECRET-do-not-persist-8841"
AMBIGUOUS = [
    {"dependent_direct": 0.55, "dependent_indirect": 0.30,
     "independent_lexical": 0.05, "unrelated": 0.05, "underdetermined": 0.05},
    {"dependent_direct": 0.45, "dependent_indirect": 0.40,
     "independent_lexical": 0.05, "unrelated": 0.05, "underdetermined": 0.05},
    {"dependent_direct": 0.60, "dependent_indirect": 0.25,
     "independent_lexical": 0.05, "unrelated": 0.05, "underdetermined": 0.05},
]


# The validator requires the whole frozen file (>= 10 pairs per class), so
# every test evaluates it and bounds spend with max_calls instead.
FIXTURE_RECORDS = [json.loads(l) for l in Path(FIXTURES).read_text().splitlines()]
N = len(FIXTURE_RECORDS)


def shaped(probs, *, model="jev-1.13.0", choice=None, req="req_fab"):
    body = {"model": model,
            "answers": {"edge_relation": {
                "type": "choice", "confidence": 0.5,
                "choice": choice or max(probs, key=probs.get),
                "probabilities": probs}},
            "usage": {"input_tokens": 900, "output_tokens": 60}}
    return 200, json.dumps(body).encode(), {jev.REQUEST_ID_HEADER: req}


def recorded():
    return (RECORDED["http_status"], json.dumps(RECORDED["body"]).encode(),
            dict(RECORDED["headers"]))


def scripted(*items):
    """Transport playing items in order; the last one repeats."""
    sent = []
    queue = list(items)

    def transport(url, headers, body, timeout_s):
        sent.append({"headers": dict(headers), "body": json.loads(body)})
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if callable(item):
            item = item()
        if isinstance(item, BaseException):
            raise item
        return item

    return transport, sent


def run_eval(tmp_path, *items, max_calls=500, api_key=SECRET, flag_on=True,
             axolotl_mod=None):
    fixtures = FIXTURE_RECORDS
    transport, sent = scripted(*items)
    with patch.object(jev, "_http_post", transport):
        receipt = shadow.eval_fixtures(
            FIXTURES, live=True, api_key=api_key, flag_on=flag_on,
            max_calls=max_calls, records_dir=str(tmp_path / "receipts"),
            axolotl_mod=axolotl_mod)
    log = shadow.read_eval_call_log(receipt["call_records_path"])
    return receipt, log, sent, fixtures


# 1 + 4: a single live-style response persists the complete record ----------

def test_single_recorded_response_persists_complete_record(tmp_path):
    receipt, log, sent, fixtures = run_eval(tmp_path, recorded())
    assert len(sent) == N
    assert log["complete"] is True and log["problems"] == []
    assert len(log["calls"]) == len(log["pair_results"]) == N
    call = log["calls"][0]
    rec = call["record"]
    assert set(rec) == set(jev.CALL_RECORD_FIELDS)
    answer = RECORDED["body"]["answers"]["edge_relation"]
    assert call["fixture_id"] == fixtures[0]["fixture_id"]
    assert call["call_ordinal"] == 1 and call["sent_ordinal"] == 1
    assert call["repeat_ordinal"] == rec["replicate_index"] == 1
    assert call["network_sent"] is True and call["error_category"] is None
    assert rec["ts"] and len(rec["request_digest"]) == 64
    assert rec["typesafe_request_id"] == RECORDED["headers"]["x-typesafe-request-id"]
    assert rec["model_requested"] == "jev-latest"
    assert rec["model_resolved"] == "jev-1.13.0"
    assert rec["choice_returned"] == answer["choice"]
    assert rec["probabilities"] == answer["probabilities"]
    assert rec["argmax_recomputed"] == "dependent_direct"
    assert call["choice_matches_argmax"] is True
    assert rec["confidence_observed"] == answer["confidence"]
    assert "observational only" in call["confidence_note"]
    assert rec["input_tokens"] == 670 and rec["output_tokens"] == 68
    assert rec["latency_s"] is not None
    assert rec["est_cost_usd"] == pytest.approx(670 * 0.042 / 1e6)
    assert rec["status"] == "ok" and rec["http_status"] == 200
    assert call["repeat_trigger"] is None

    pair = log["pair_results"][0]
    assert pair["fixture_id"] == call["fixture_id"]
    assert pair["call_ids"] == [rec["call_id"]]
    assert pair["argmax"] == "dependent_direct"
    assert pair["median_probabilities"] == answer["probabilities"]
    assert pair["gate_state"] == "axolotl_unavailable"
    assert pair["semantic_channel"] is True
    assert pair["repeat_spent"] is False and pair["repeat_basis"] is None

    close = log["closeout"]
    assert close["evaluation_complete"] is True
    assert close["stop_reason"] == "completed"
    assert close["calls_sent"] == N and close["input_tokens"] == 670 * N
    assert receipt["live_summary"]["evaluation_complete"] is True
    header = log["header"]
    assert header["fixture_count"] == N and header["max_calls"] == 500
    assert header["axolotl"] == "unavailable"
    assert header["record_fields"] == list(jev.CALL_RECORD_FIELDS)


def test_record_alone_reconstructs_the_result(tmp_path):
    receipt, log, _, _ = run_eval(
        tmp_path, *[shaped(p) for p in AMBIGUOUS], max_calls=3)
    ok = [c["record"] for c in log["calls"] if c["record"]["status"] == "ok"]
    medians = jev.replicate_medians(ok, shadow.RELATION_OPTIONS)
    pair = log["pair_results"][0]
    assert medians == pair["median_probabilities"]
    assert jev.recompute_argmax(medians, shadow.RELATION_OPTIONS) == \
        pair["argmax"] == receipt["results"][0]["argmax"]
    assert jev.ambiguity_set(medians) == pair["ambiguity_set"]


# 2: repeats are separate, ordered records --------------------------------------

def test_ambiguous_repeats_are_separate_ordered_records(tmp_path):
    _, log, sent, _ = run_eval(
        tmp_path, *[shaped(p, req=f"req_{i}") for i, p in enumerate(AMBIGUOUS)],
        max_calls=3)
    assert len(sent) == 3
    calls = log["calls"][:3]
    assert [c["repeat_ordinal"] for c in calls] == [1, 2, 3]
    assert [c["call_ordinal"] for c in calls] == [1, 2, 3]
    assert [c["sent_ordinal"] for c in calls] == [1, 2, 3]
    recs = [c["record"] for c in calls]
    assert len({r["replicate_set_id"] for r in recs}) == 1
    assert len({r["request_digest"] for r in recs}) == 1
    assert len({r["call_id"] for r in recs}) == 3
    assert [r["typesafe_request_id"] for r in recs] == ["req_0", "req_1", "req_2"]
    assert [r["probabilities"] for r in recs] == AMBIGUOUS  # all preserved
    assert calls[0]["repeat_trigger"] is None
    for c in calls[1:]:
        trig = c["repeat_trigger"]
        assert trig["basis"] == "threshold_policy_2_6"
        assert trig["p_top1"] == 0.55 and trig["gap"] == 0.25
        assert trig["reasons"] == ["p_top1 0.55 <= 0.7"]
    pair = log["pair_results"][0]
    assert pair["repeat_spent"] is True and pair["calls_sent"] == 3
    assert pair["repeat_basis"]["basis"] == "threshold_policy_2_6"
    # Median view, not a vote: 0.55 is the median, not a 2-of-3 count.
    assert pair["median_probabilities"]["dependent_direct"] == 0.55


def test_axolotl_gate_result_is_recorded_when_invoked(tmp_path):
    class FakeGate:
        pass

    def fake_decision(mod, plausible):
        return shadow.GATE_LOAD_BEARING, {"plausible_set": plausible,
                                          "axolotl_status": "fake"}
    with patch.object(shadow, "axolotl_repeat_decision", fake_decision):
        _, log, sent, _ = run_eval(
            tmp_path, *[shaped(p) for p in AMBIGUOUS], max_calls=3,
            axolotl_mod=FakeGate())
    assert len(sent) == 3
    pair = log["pair_results"][0]
    assert pair["gate_state"] == shadow.GATE_LOAD_BEARING
    assert pair["gate_detail"]["axolotl_status"] == "fake"
    assert pair["gate_plausible_set"] == ["dependent_direct", "dependent_indirect"]
    assert log["calls"][1]["repeat_trigger"]["basis"] == "axolotl_gate"
    assert log["header"]["axolotl"] == "available"


# 3: raw choice and recomputed argmax both retained ------------------------------

def test_disagreeing_choice_is_retained_beside_argmax(tmp_path):
    confident = {"dependent_direct": 0.82, "dependent_indirect": 0.08,
                 "independent_lexical": 0.04, "unrelated": 0.03,
                 "underdetermined": 0.03}
    _, log, _, _ = run_eval(tmp_path, shaped(confident, choice="unrelated"),
                            max_calls=1)
    call = log["calls"][0]
    assert call["record"]["choice_returned"] == "unrelated"
    assert call["record"]["argmax_recomputed"] == "dependent_direct"
    assert call["record"]["choice_argmax_discrepancy"] is True
    assert call["choice_matches_argmax"] is False
    assert log["pair_results"][0]["argmax"] == "dependent_direct"


# 5: sent failures vs refusals before the network ------------------------------

def http_error(code):
    return urllib.error.HTTPError(jev.ENDPOINT, code, "err", None, None)


@pytest.mark.parametrize("item,category,http_status", [
    (lambda: http_error(500), "sent:http_error", 500),
    (lambda: (200, b"<html>", {}), "sent:contract_error", 200),
    (lambda: socket.timeout("t"), "sent:timeout", None),
    (lambda: (200, json.dumps({"model": "jev-1.13.0", "answers": {}}).encode(),
              {jev.REQUEST_ID_HEADER: "req_bad"}), "sent:contract_error", 200),
])
def test_sent_failures_get_durable_failure_records(tmp_path, item, category,
                                                   http_status):
    _, log, sent, _ = run_eval(tmp_path, item)
    assert len(sent) == N  # each pair fails once; failures never repeat
    call = log["calls"][0]
    assert call["network_sent"] is True
    assert call["error_category"] == category
    assert call["record"]["http_status"] == http_status
    assert call["record"]["probabilities"] is None
    assert log["closeout"]["evaluation_complete"] is False
    assert log["closeout"]["stop_reason"] == "pairs_unavailable"
    assert log["complete"] is False


@pytest.mark.parametrize("kw,category", [
    ({"flag_on": False}, "refused_before_network:flag_off"),
    ({"api_key": None}, "refused_before_network:key_absent"),
    ({"max_calls": 0}, "refused_before_network:budget_refused"),
])
def test_refusals_before_network_are_distinguishable(tmp_path, kw, category):
    _, log, sent, _ = run_eval(tmp_path, recorded(), **kw)
    assert sent == []
    assert [c["error_category"] for c in log["calls"]] == [category] * N
    assert all(c["network_sent"] is False and c["sent_ordinal"] is None
               for c in log["calls"])
    assert all(c["record"]["latency_s"] is None for c in log["calls"])
    assert log["closeout"]["calls_sent"] == 0
    assert log["closeout"]["calls_refused_before_network"] == N


def test_model_version_change_then_stop(tmp_path):
    confident = RECORDED["body"]["answers"]["edge_relation"]["probabilities"]
    _, log, sent, _ = run_eval(
        tmp_path, shaped(confident, model="jev-1.13.0"),
        shaped(confident, model="jev-1.14.0"))
    assert len(sent) == 2  # every later pair refused before the wire
    cats = [c["error_category"] for c in log["calls"]]
    assert cats == [None, "sent:model_version_change"] + \
        ["refused_before_network:model_version_stop"] * (N - 2)
    statuses = [c["record"]["status"] for c in log["calls"]]
    assert statuses == ["ok"] + ["contract_error"] * (N - 1)
    assert [p["model_stop"] for p in log["pair_results"]][:3] == \
        [False, True, False]
    close = log["closeout"]
    assert close["stop_reason"] == "model_version_stop"
    assert close["model_anchor"] == "jev-1.13.0"
    assert close["evaluation_complete"] is False


# partial runs cannot masquerade as complete --------------------------------------

def test_budget_truncated_run_is_marked_incomplete(tmp_path):
    receipt, log, sent, _ = run_eval(tmp_path, recorded(), max_calls=1)
    assert len(sent) == 1
    assert log["closeout"]["run_finished"] is True
    assert log["closeout"]["stop_reason"] == "max_calls_reached"
    assert log["closeout"]["evaluation_complete"] is False
    assert log["complete"] is False
    assert receipt["live_summary"]["evaluation_complete"] is False


def test_crash_mid_run_leaves_an_incomplete_log(tmp_path):
    transport, _ = scripted(recorded(), RuntimeError("boom"))
    records_dir = tmp_path / "receipts"
    with patch.object(jev, "_http_post", transport), \
            pytest.raises(RuntimeError):
        shadow.eval_fixtures(FIXTURES, live=True, api_key=SECRET, flag_on=True,
                             max_calls=10, records_dir=str(records_dir))
    (log_path,) = records_dir.glob("*.jsonl")
    log = shadow.read_eval_call_log(str(log_path))
    assert log["complete"] is False
    assert log["closeout"]["run_finished"] is False
    assert log["closeout"]["stop_reason"] == "exception:RuntimeError"
    # The call that did reach TypeSafe was already durable.
    assert log["calls"][0]["record"]["status"] == "ok"


def test_truncated_file_is_flagged(tmp_path):
    _, log, _, _ = run_eval(tmp_path, recorded())
    assert log["complete"] is True
    path = Path(log["path"])
    lines = path.read_text().splitlines()
    path.write_text("\n".join(lines[:-1]) + "\n")  # drop the closeout
    again = shadow.read_eval_call_log(str(path))
    assert again["complete"] is False
    assert "closeout missing (interrupted run)" in again["problems"]


# 6 + 7: no secrets, no text ----------------------------------------------------

def test_no_secret_header_or_text_is_persisted(tmp_path):
    receipt, log, sent, fixtures = run_eval(
        tmp_path, *[shaped(p) for p in AMBIGUOUS], max_calls=6)
    assert sent and all(s["headers"]["Authorization"] == f"Bearer {SECRET}"
                        for s in sent)  # it did go on the wire...
    written = "".join(p.read_text() for p in (tmp_path / "receipts").iterdir())
    written += json.dumps(receipt)
    assert SECRET not in written  # ...and nowhere else
    assert "Bearer" not in written and "Authorization" not in written
    for fx in fixtures:
        for text in (fx["text_a"], fx["text_b"]):
            if text:
                assert text[:30] not in written


def test_text_hashes_and_tripwire_match_the_offline_path(tmp_path):
    offline = shadow.eval_fixtures(FIXTURES, live=False)
    _, log, _, _ = run_eval(tmp_path, recorded(), max_calls=1)
    assert len(offline["results"]) == len(log["pair_results"]) == N
    for off, pair in zip(offline["results"], log["pair_results"]):
        for k in ("packet_sha256", "text_a_sha256", "text_b_sha256",
                  "privacy_tripwire_hits"):
            assert off[k] == pair[k]


# 8: caps count only sent calls; unchanged -----------------------------------------

def test_cap_truncates_a_repeat_set_and_counts_only_sent_calls(tmp_path):
    _, log, sent, _ = run_eval(
        tmp_path, *[shaped(p) for p in AMBIGUOUS], max_calls=2)
    assert len(sent) == 2
    cats = [(c["repeat_ordinal"], c["network_sent"], c["error_category"])
            for c in log["calls"][:4]]
    assert cats == [
        (1, True, None), (2, True, None),
        (3, False, "refused_before_network:budget_refused"),
        (1, False, "refused_before_network:budget_refused"),
    ]
    assert log["closeout"]["calls_sent"] == 2
    assert log["header"]["preflight"] == jev.estimate_cost_usd(2)


def test_cost_estimate_uses_the_measured_planning_figure():
    est = jev.estimate_cost_usd(3)
    assert est["est_input_tokens_per_call"] == 950  # measured L2 2026-09-17
    assert est["est_cost_usd_worst_case"] == round(3 * 950 * 0.042 / 1e6, 6)
    assert "measured" in est["est_input_tokens_per_call_status"]


# 9: offline evaluation unchanged -------------------------------------------------

def test_offline_evaluation_writes_no_call_log(tmp_path):
    receipts = tmp_path / "receipts"
    receipt = shadow.eval_fixtures(FIXTURES, live=False,
                                   records_dir=str(receipts))
    assert "call_records_path" not in receipt
    assert "live_summary" not in receipt
    assert not receipts.exists()


def test_live_evaluation_refuses_without_a_records_dir():
    with pytest.raises(ValueError):
        shadow.eval_fixtures(FIXTURES, live=True, api_key="k", flag_on=True,
                             max_calls=1)


# CLI wiring --------------------------------------------------------------------

def test_cli_live_eval_writes_the_call_log_to_the_receipts_dir(
        tmp_path, monkeypatch, capsys):
    cfg = tmp_path / "config.toml"
    cfg.write_text(f'db_path = "{tmp_path / "main.db"}"\n'
                   "semantic_shadow_enabled = true\n")
    monkeypatch.setenv(jev.ENV_API_KEY, SECRET)
    monkeypatch.delenv(shadow.KILL_SWITCH_ENV, raising=False)
    transport, sent = scripted(recorded())
    receipts = tmp_path / "receipts"
    with patch.object(jev, "_http_post", transport):
        cli.main(["--config", str(cfg), "semantic-shadow", "eval-fixtures",
                  "--fixtures", FIXTURES, "--live", "--max-calls", "1",
                  "--confirm-live-spend", "--receipts-dir", str(receipts)])
    out = json.loads(capsys.readouterr().out)
    log = shadow.read_eval_call_log(out["call_records_path"])
    assert Path(out["call_records_path"]).parent == receipts
    assert log["problems"] == [] and len(sent) == 1
    # One call against a 60-pair file: finished, but not a complete evaluation.
    assert log["closeout"]["run_finished"] is True
    assert log["closeout"]["evaluation_complete"] is False
    assert log["complete"] is False
    assert not (tmp_path / "main.db").exists()
    assert SECRET not in "".join(p.read_text() for p in receipts.iterdir())
