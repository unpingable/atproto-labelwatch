"""`eval-holdout --live` runner: data separation, durable receipts, bounds
(Amendment 2026-09-17-C §6). Offline: `_http_post` is mocked and the recorded
live response shapes are reused. No network, no holdout content, no labels.
"""
import json
import socket
import urllib.error
from pathlib import Path
from unittest.mock import patch

import pytest

from labelwatch import cli
from labelwatch import semantic_shadow as shadow
from labelwatch import semantic_shadow_holdout as ho
from labelwatch import semantic_shadow_jev as jev
from labelwatch import semantic_shadow_v01 as v01

from test_semantic_shadow_holdout import (agreed_annotations, make_item,
                                          synthetic_corpus)

ROOT = Path(__file__).resolve().parents[1]
RECORDED = next(r for r in json.loads(
    (ROOT / "tests" / "fixtures" / "typesafe_systemone_recorded.json").read_text()
)["responses"] if r["decision_family"] == "edge_relation")
OPTS = shadow.RELATION_OPTIONS
SECRET = "sk-test-SECRET-holdout-live-7731"
L2 = str(ROOT / "tests" / "fixtures" / "semantic_relations.jsonl")

# Forbidden values planted on items; none may reach a request.
PLANTED = {
    "author_ref": "author-deadbeef",
    "author_intended": {"explicit_reference": "yes", "relation": "dependent_direct"},
    "author_note": "PLANTED-AUTHOR-NOTE",
    "categories": ["irony"],
}


def dist(top, p):
    out = {o: 0.0 for o in OPTS}
    out[top] = p
    other = "unrelated" if top != "unrelated" else "dependent_direct"
    out[other] = round(1 - p, 2)
    return out


CONFIDENT = dist("dependent_direct", 0.95)
AMBIGUOUS = dist("dependent_direct", 0.55)


def response(probs, model="jev-1.13.0", choice=None, req="req_live"):
    body = {"model": model,
            "answers": {"edge_relation": {
                "type": "choice", "confidence": 0.5,
                "choice": choice or max(probs, key=probs.get),
                "probabilities": probs}},
            "usage": {"input_tokens": 700, "output_tokens": 60}}
    return 200, json.dumps(body).encode(), {jev.REQUEST_ID_HEADER: req}


def transport_for(answer):
    """answer(n_calls, request_body) -> transport item or exception."""
    sent = []

    def transport(url, headers, body, timeout_s):
        req = json.loads(body.decode("utf-8"))
        sent.append({"url": url, "headers": dict(headers), "body": req})
        item = answer(len(sent), req)
        if isinstance(item, BaseException):
            raise item
        return item

    return transport, sent


def small_corpus(n=6):
    """A few synthetic plumbing items (not a valid Part H corpus: the
    composition validator is exercised elsewhere)."""
    items = [make_item(i, ["irony"], anchored=i < 3) for i in range(n)]
    report = ho.validate_corpus(items)
    assert not [e for e in report["errors"] if "corpus size" not in e
                and "category" not in e and "long-text" not in e
                and "multilingual" not in e and "anchor-bearing" not in e], report
    report["valid"] = True  # composition is not what this module tests
    report["corpus_digest"] = ho.hash_sha256(ho.stable_json(
        sorted((it["item_id"], ho.item_digest(it)) for it in items)))
    report["item_digests"] = {it["item_id"]: ho.item_digest(it) for it in items}
    return items, report


def run(tmp_path, items, report, answer, **kw):
    transport, sent = transport_for(answer)
    kw.setdefault("api_key", SECRET)
    kw.setdefault("flag_on", True)
    kw.setdefault("max_calls", 100)
    with patch.object(jev, "_http_post", transport):
        receipt = ho.run_holdout_eval(items, corpus_report=report,
                                      records_dir=str(tmp_path / "receipts"), **kw)
    return receipt, sent, shadow.read_eval_call_log(receipt["call_records_path"])


# --- 1. preflight makes no network calls ---------------------------------------

def test_preflight_is_pure_and_bounded(tmp_path):
    items, _ = small_corpus()
    for it in items:
        it.update(PLANTED)
    pre = ho.preflight(items, max_calls=10, codebook_version="cb/v1")
    assert pre["item_count"] == 6
    assert pre["worst_case_calls_under_repeat_policy"] == 18
    assert pre["calls_bounded_by_max_calls"] == 10
    assert pre["repeat_policy"] == {"k": 3, "top1_max": 0.70, "gap_max": 0.15}
    assert pre["estimate"]["est_input_tokens_per_call"] == 950  # planning figure
    assert "estimate only" in pre["estimate"]["basis"]
    assert pre["rule_digest"] == v01.RULE_DIGEST
    assert "PLANTED-AUTHOR-NOTE" not in json.dumps(pre)


def test_cli_dry_run_makes_no_calls_and_writes_nothing(tmp_path, capsys):
    items, _ = small_corpus()
    path = tmp_path / "items.jsonl"
    path.write_text("".join(json.dumps(it) + "\n" for it in items))
    records = tmp_path / "receipts"

    def boom(*a, **k):  # any network use fails the test
        raise AssertionError("network used during dry run")

    real_validate = ho.validate_corpus
    with patch.object(jev, "_http_post", boom), \
            patch.object(ho, "validate_corpus",
                         lambda its, refs=(): _valid(real_validate, its)):
        cli.main(["semantic-holdout", "eval-holdout", "--items", str(path),
                  "--l2-fixtures", L2, "--records-dir", str(records),
                  "--max-calls", "5", "--dry-run"])
    out = json.loads(capsys.readouterr().out)
    assert out["dry_run"] is True and out["network_used"] is False
    assert out["preflight"]["calls_bounded_by_max_calls"] == 5
    assert not records.exists()


def _valid(real_validate, items):
    report = real_validate(items)
    report["valid"] = True
    report["corpus_digest"] = report["corpus_digest"] or "0" * 64
    report["item_digests"] = {it["item_id"]: ho.item_digest(it) for it in items}
    return report


# --- 2, 3, 13. data separation ---------------------------------------------------

def test_projection_is_allow_listed():
    item = make_item(1, ["irony"])
    item.update(PLANTED)
    proj = ho.evaluation_projection(item)
    assert set(proj) == set(ho.PROJECTION_FIELDS)
    assert "item_id" not in ho.projection_to_candidate(proj)
    packet = ho.projection_packet(item)
    assert set(packet) == {"packet_kind", "relation_type", "text_a", "text_b",
                           "dt_seconds", "langs_a", "langs_b", "shared_url_domain",
                           "shared_hashtag", "lexical_jaccard", "thread_depth_b"}


def test_no_forbidden_field_reaches_the_request(tmp_path):
    items, report = small_corpus()
    gold_like = {"explicit_reference": "yes", "relation": "dependent_direct",
                 "annotator_ref": "annotator-aaaaaaaa", "adjudication": "yes",
                 "m1": 0.0}
    for i, it in enumerate(items):
        it.update(PLANTED)
        it.update({f"PLANTED_{k}": v for k, v in gold_like.items()})
        it["item_id_note"] = "PLANTED-CATEGORY-irony"
    _, sent, log = run(tmp_path, items, report, lambda n, req: response(CONFIDENT))
    blob = json.dumps([s["body"] for s in sent])
    # "irony" itself appears in the frozen option descriptions, so the planted
    # category is marked to be distinguishable from that legitimate text.
    for needle in ("author-deadbeef", "PLANTED", "annotator-aaaaaaaa",
                   "adjudication", "explicit_reference", "author_intended",
                   "anchor_bearing_negative", "categories", "declared_long_text",
                   "author_ref", "item_id"):
        assert needle not in blob, needle
    for s in sent:
        assert set(s["body"]) == {"state", "model", "questions"}
        assert set(s["body"]["state"]) == {
            "packet_kind", "relation_type", "text_a", "text_b", "dt_seconds",
            "langs_a", "langs_b", "shared_url_domain", "shared_hashtag",
            "lexical_jaccard", "thread_depth_b"}
        assert list(s["body"]["questions"]) == ["edge_relation"]
    # The opaque item id is local only: it never appears in a request.
    assert all(it["item_id"] not in blob for it in items)


def test_runner_takes_no_gold_or_annotation_input():
    import inspect
    params = set(inspect.signature(ho.run_holdout_eval).parameters)
    assert not params & {"gold", "gold_report", "annotations", "annotation_view"}
    src = ""
    for fn in (ho.run_holdout_eval, ho.evaluation_projection,
               ho.projection_to_candidate, ho.projection_packet):
        body = inspect.getsource(fn)
        doc = fn.__doc__ or ""
        src += body.replace(doc, "")  # code only; docstrings may mention them
    for needle in ("gold", "annotation", "annotator", "adjudicator",
                   "adjudication", "author_intended", "categories",
                   "author_ref", "anchor_bearing_negative"):
        assert needle not in src, needle
    assert "adjudicate_packet" in src  # the qualified client path is reused


# --- 4, 5, 6, 11. durable evidence ------------------------------------------------

def test_single_call_item_persists_complete_evidence(tmp_path):
    items, report = small_corpus(1)
    receipt, sent, log = run(tmp_path, items, report,
                             lambda n, req: response(CONFIDENT, req="req_abc"))
    assert len(sent) == 1 and receipt["evaluation_complete"] is True
    header = log["header"]
    assert header["holdout_corpus_digest"] == report["corpus_digest"]
    assert header["rule_digest"] == v01.RULE_DIGEST
    assert header["item_count"] == 1 and header["max_calls"] == 100
    assert header["model_requested"] == "jev-latest"
    assert header["relation_options"] == list(OPTS)
    assert header["repeat_policy"]["k"] == 3
    assert header["preflight"]["estimate"]["est_input_tokens_per_call"] == 950
    assert header["projection_fields"] == list(ho.PROJECTION_FIELDS)

    (call,) = log["calls"]
    rec = call["record"]
    assert set(rec) == set(jev.CALL_RECORD_FIELDS)
    assert call["fixture_id"] == items[0]["item_id"]
    assert call["call_ordinal"] == 1 and call["repeat_ordinal"] == 1
    assert call["network_sent"] is True and call["error_category"] is None
    assert rec["typesafe_request_id"] == "req_abc"
    assert rec["model_requested"] == "jev-latest" and rec["model_resolved"] == "jev-1.13.0"
    assert rec["probabilities"] == CONFIDENT
    assert rec["argmax_recomputed"] == "dependent_direct"
    assert rec["choice_returned"] == "dependent_direct"
    assert call["choice_matches_argmax"] is True
    assert rec["confidence_observed"] == 0.5  # observational only
    assert rec["input_tokens"] == 700 and rec["latency_s"] is not None
    assert rec["est_cost_usd"] == pytest.approx(700 * 0.042 / 1e6)

    (item_result,) = log["pair_results"]
    assert item_result["item_id"] == items[0]["item_id"]
    assert item_result["argmax"] == "dependent_direct"
    assert item_result["median_probabilities"]["dependent_direct"] == 0.95
    assert item_result["ambiguity_set"] == ["dependent_direct"]
    assert item_result["item_digest"] == report["item_digests"][items[0]["item_id"]]
    assert len(item_result["text_a_sha256"]) == 64 and len(item_result["packet_sha256"]) == 64
    v = item_result["v01"]
    assert v["disposition"] == v01.QUALIFIED_EXPLICIT  # anchored item 0
    assert v["anchors"]["shared_hashtag"] is True
    assert "shared_distinctive_token" in v["validation_only"]
    close = log["closeout"]
    assert close["evaluation_complete"] is True and close["stop_reason"] == "completed"
    assert close["items_total"] == close["items_evaluated"] == 1
    assert close["calls_sent"] == 1 and close["input_tokens"] == 700


def test_no_raw_text_in_the_receipt(tmp_path):
    items, report = small_corpus(2)
    receipt, _, _ = run(tmp_path, items, report, lambda n, req: response(CONFIDENT))
    blob = Path(receipt["call_records_path"]).read_text()
    for it in items:
        assert it["text_a"][:40] not in blob and it["text_b"][:40] not in blob
    assert SECRET not in blob and "Bearer" not in blob


def test_ambiguous_item_records_three_calls(tmp_path):
    items, report = small_corpus(1)
    _, sent, log = run(tmp_path, items, report,
                       lambda n, req: response(AMBIGUOUS, req=f"req_{n}"))
    assert len(sent) == 3
    calls = log["calls"]
    assert [c["repeat_ordinal"] for c in calls] == [1, 2, 3]
    assert len({c["record"]["call_id"] for c in calls}) == 3
    assert len({c["record"]["request_digest"] for c in calls}) == 1
    assert [c["record"]["typesafe_request_id"] for c in calls] == ["req_1", "req_2", "req_3"]
    for c in calls[1:]:
        assert c["repeat_trigger"]["basis"] == "threshold_policy_2_6"
    (result,) = log["pair_results"]
    assert result["calls_sent"] == 3
    # Repeats feed the unchanged V0.1 rule: spent replicates downgrade.
    assert result["v01"]["disposition"] == v01.UNDERDETERMINED
    assert v01.ROUTE_AMBIGUITY in result["v01"]["review_routes"]


def test_repeat_outcome_matches_the_rule_module(tmp_path):
    items, report = small_corpus(3)
    _, _, log = run(tmp_path, items, report,
                    lambda n, req: response(AMBIGUOUS if n % 2 else CONFIDENT))
    adjs = ho.adjudications_from_log(log)
    for result in log["pair_results"]:
        item = next(i for i in items if i["item_id"] == result["item_id"])
        expected = v01.evaluate_packet(ho.projection_packet(item), adjs[result["item_id"]])
        assert result["v01"]["disposition"] == expected["disposition"]
        assert result["v01"]["review_routes"] == expected["review_routes"]


def test_choice_argmax_mismatch_survives(tmp_path):
    items, report = small_corpus(1)
    _, _, log = run(tmp_path, items, report,
                    lambda n, req: response(CONFIDENT, choice="unrelated"))
    rec = log["calls"][0]["record"]
    assert rec["choice_returned"] == "unrelated"
    assert rec["argmax_recomputed"] == "dependent_direct"
    assert rec["choice_argmax_discrepancy"] is True
    assert log["calls"][0]["choice_matches_argmax"] is False


# --- 7, 8, 9, 10. incomplete runs -------------------------------------------------

def test_crash_leaves_an_incomplete_log(tmp_path):
    items, report = small_corpus(4)

    def answer(n, req):
        return RuntimeError("boom") if n == 2 else response(CONFIDENT)

    transport, _ = transport_for(answer)
    with patch.object(jev, "_http_post", transport), pytest.raises(RuntimeError):
        ho.run_holdout_eval(items, corpus_report=report,
                            records_dir=str(tmp_path / "r"), api_key=SECRET,
                            flag_on=True, max_calls=50)
    (path,) = (tmp_path / "r").glob("*.jsonl")
    log = shadow.read_eval_call_log(str(path))
    assert log["complete"] is False
    assert log["closeout"]["run_finished"] is False
    assert log["closeout"]["stop_reason"] == "exception:RuntimeError"
    assert log["calls"][0]["record"]["status"] == "ok"  # the first call is durable


def test_max_calls_stop_is_incomplete(tmp_path):
    items, report = small_corpus(6)
    receipt, sent, log = run(tmp_path, items, report,
                             lambda n, req: response(CONFIDENT), max_calls=2)
    assert len(sent) == 2
    assert receipt["evaluation_complete"] is False
    assert receipt["stop_reason"] == "max_calls_reached"
    assert log["complete"] is False
    assert log["closeout"]["items_evaluated"] == 2 < log["closeout"]["items_total"]


def test_model_version_change_halts_and_marks_incomplete(tmp_path):
    items, report = small_corpus(5)
    receipt, sent, log = run(
        tmp_path, items, report,
        lambda n, req: response(CONFIDENT, model="jev-1.13.0" if n == 1 else "jev-1.14.0"))
    assert receipt["evaluation_complete"] is False
    assert receipt["stop_reason"] == "model_version_stop"
    assert log["complete"] is False
    cats = [c["error_category"] for c in log["calls"]]
    assert cats[0] is None and "model_version_change" in cats[1]
    assert len(sent) == 2  # the rest are refused before the wire


@pytest.mark.parametrize("failure,category", [
    (lambda: urllib.error.HTTPError(jev.ENDPOINT, 500, "e", None, None), "sent:http_error"),
    (lambda: socket.timeout("t"), "sent:timeout"),
    (lambda: (200, b"<html>", {}), "sent:contract_error"),
])
def test_api_and_contract_failures_are_recorded_and_stop_the_run(tmp_path, failure, category):
    items, report = small_corpus(4)
    value = failure()
    receipt, _, log = run(tmp_path, items, report, lambda n, req: value)
    assert log["calls"][0]["error_category"] == category
    assert receipt["evaluation_complete"] is False
    assert receipt["stop_reason"].startswith("semantic_unavailable:")
    assert log["pair_results"][0]["v01"]["disposition"] == v01.SEMANTIC_UNAVAILABLE


# --- 12, 14, 15, 16. evaluator binding and the end-to-end flow ----------------------

def full_corpus_run(tmp_path, answer, **kw):
    items = synthetic_corpus()
    report = ho.validate_corpus(items)
    assert report["valid"]
    kw.setdefault("max_calls", 500)  # above the worst case for 150 items
    receipt, sent, log = run(tmp_path, items, report, answer, **kw)
    gold = ho.assemble_gold(report, agreed_annotations(items, report))
    return items, report, gold, log, receipt


def good_probs(i):
    if i < 60:
        return dist("dependent_direct", 0.95)
    if i < 90:
        return dist("unrelated", 0.9)
    if i < 140:
        return dist("dependent_indirect", 0.8)
    return dist("unrelated", 0.9)


def positional_answer():
    state = {"i": 0}

    def answer(n, req):
        probs = good_probs(state["i"])
        state["i"] += 1
        return response(probs, req=f"req_{n}")

    return answer


def test_mock_end_to_end_part_h_flow_without_network(tmp_path):
    items, report, gold, log, receipt = full_corpus_run(tmp_path, positional_answer())
    assert receipt["evaluation_complete"] is True
    assert receipt["items_evaluated"] == 150 and receipt["calls_sent"] == 150
    assert gold["grade"] == "qualification" and gold["agreement"]["x_gate_passed"]
    result = ho.evaluate_part("H", items, report, gold, log)
    assert result["disposition"] == "PASSED", result.get("failed") or result.get("blockers")
    assert result["metrics"]["m1_false_positive_qualification"]["k"] == 0
    assert result["metrics"]["m1_false_positive_qualification"]["n"] == 60
    assert result["disposition_counts"][v01.QUALIFIED_EXPLICIT] == 60
    assert ho.overall_decision(result, None)["decision"] == "NO_GO_PENDING_PART_R"
    # Nothing in the artefacts carries the key or raw text.
    blob = Path(receipt["call_records_path"]).read_text() + json.dumps(result, default=str)
    assert SECRET not in blob and "Bearer" not in blob
    assert all(it["text_a"][:40] not in blob for it in items)


def test_corpus_and_rule_digest_mismatch_block_evaluation(tmp_path):
    items, report, gold, log, _ = full_corpus_run(tmp_path, positional_answer())
    other = dict(report, corpus_digest="f" * 64)
    blocked = ho.evaluate_part("H", items, other, gold, log)
    assert blocked["disposition"] == "BLOCKED"
    assert "call_log_corpus_digest_mismatch" in blocked["blockers"]
    assert "gold_corpus_digest_mismatch" in blocked["blockers"]
    tampered = dict(log, header=dict(log["header"], rule_digest="0" * 64))
    assert "call_log_rule_digest_mismatch" in \
        ho.evaluate_part("H", items, report, gold, tampered)["blockers"]


def test_incomplete_annotation_blocks_qualification(tmp_path):
    items, report, _, log, _ = full_corpus_run(tmp_path, positional_answer())
    partial = [a for a in agreed_annotations(items, report)
               if not (a["role"] == "B" and a["item_id"] == items[2]["item_id"])]
    gold = ho.assemble_gold(report, partial)
    assert gold["grade"] == "exploratory"
    blocked = ho.evaluate_part("H", items, report, gold, log)
    assert blocked["disposition"] == "BLOCKED"
    assert "annotation_not_qualification_grade" in blocked["blockers"]


def test_low_kappa_halts_before_metrics(tmp_path):
    items, report, _, log, _ = full_corpus_run(tmp_path, positional_answer())
    noisy = lambda i, x, r: ({"yes": "no", "no": "unclear", "unclear": "yes"}[x], r) \
        if i % 2 else (x, r)
    gold = ho.assemble_gold(report, agreed_annotations(items, report, noisy))
    assert not gold["agreement"]["x_gate_passed"]
    halted = ho.evaluate_part("H", items, report, gold, log)
    assert halted["disposition"] == "HALTED_X_AGREEMENT" and "metrics" not in halted


def test_incomplete_run_blocks_evaluation(tmp_path):
    items = synthetic_corpus()
    report = ho.validate_corpus(items)
    receipt, _, log = run(tmp_path, items, report, positional_answer(), max_calls=10)
    gold = ho.assemble_gold(report, agreed_annotations(items, report))
    blocked = ho.evaluate_part("H", items, report, gold, log)
    assert blocked["disposition"] == "BLOCKED"
    assert "evaluation_log_incomplete" in blocked["blockers"]


# --- 17. CLI gating (no network) ----------------------------------------------------

def test_cli_refuses_without_flag_key_or_confirmation(tmp_path, monkeypatch, capsys):
    items = synthetic_corpus()
    path = tmp_path / "items.jsonl"
    path.write_text("".join(json.dumps(it) + "\n" for it in items))
    cfg_on = tmp_path / "on.toml"
    cfg_on.write_text(f'db_path = "{tmp_path / "main.db"}"\nsemantic_shadow_enabled = true\n')
    cfg_off = tmp_path / "off.toml"
    cfg_off.write_text(f'db_path = "{tmp_path / "main.db"}"\n')
    monkeypatch.delenv(shadow.KILL_SWITCH_ENV, raising=False)
    base = ["semantic-holdout", "eval-holdout", "--items", str(path),
            "--l2-fixtures", L2, "--records-dir", str(tmp_path / "r"),
            "--max-calls", "5"]

    def call(argv, cfg):
        with patch.object(jev, "_http_post", lambda *a, **k: pytest.fail("network")):
            with pytest.raises(SystemExit) as e:
                cli.main(["--config", str(cfg)] + argv)
        return json.loads(capsys.readouterr().out), e.value.code

    monkeypatch.delenv(jev.ENV_API_KEY, raising=False)
    out, code = call(base + ["--confirm-live-spend"], cfg_off)
    assert code == 2 and out["refused"] == "flag_off"
    out, code = call(base + ["--confirm-live-spend"], cfg_on)
    assert code == 2 and out["refused"] == "key_absent"
    monkeypatch.setenv(jev.ENV_API_KEY, SECRET)
    out, code = call(base, cfg_on)
    assert code == 2 and out["refused"] == "confirmation_required"
    assert SECRET not in json.dumps(out)
    monkeypatch.setenv(shadow.KILL_SWITCH_ENV, "1")
    out, code = call(base + ["--confirm-live-spend"], cfg_on)
    assert code == 2 and out["refused"] == "kill_switch"
    assert not (tmp_path / "r").exists()
