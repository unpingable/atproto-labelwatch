"""Wire-contract tests for the Jev client against RECORDED System One shapes.

`tests/fixtures/typesafe_systemone_recorded.json` holds verbatim live
responses (typesafe characterization 2026-09-16; Weatherwatch live smoke
2026-09-17). They pin the parse path to what the API actually returns. Where a
test needs Labelwatch's own option set, it uses the same recorded shape with
fabricated values (non-evidence). The transport is mocked at `_http_post`; no
network.
"""
import json
import urllib.error
from email.message import Message
from pathlib import Path
from unittest.mock import patch

import pytest

from labelwatch import semantic_shadow as shadow
from labelwatch import semantic_shadow_jev as jev

DAY = "2026-09-17"
FIXTURE = Path(__file__).parent / "fixtures" / "typesafe_systemone_recorded.json"
RECORDED = json.loads(FIXTURE.read_text())["responses"]
FAMILY = shadow.DECISION_FAMILY
OPTIONS = shadow.RELATION_OPTIONS
UNCLEAR = {"dependent_direct": 0.10, "dependent_indirect": 0.08,
           "independent_lexical": 0.04, "unrelated": 0.03,
           "underdetermined": 0.75}
CONFIDENT = {"dependent_direct": 0.82, "dependent_indirect": 0.08,
             "independent_lexical": 0.04, "unrelated": 0.03,
             "underdetermined": 0.03}
HDRS = {"content-type": "application/json",
        jev.REQUEST_ID_HEADER: "req_01fabricated"}


def recorded_shape(probs, *, family=FAMILY, model="jev-1.13.0",
                   choice=None, confidence=0.7):
    """The recorded response structure with fabricated values."""
    return {"model": model,
            "answers": {family: {"type": "choice",
                                 "choice": choice or max(probs, key=probs.get),
                                 "confidence": confidence,
                                 "probabilities": probs}},
            "usage": {"input_tokens": 1500, "output_tokens": 60}}


def transport_of(*responses):
    """Each item: (status, body_obj_or_bytes, headers) or an exception."""
    calls = []
    queue = list(responses)

    def transport(url, headers, body, timeout_s):
        calls.append({"url": url, "headers": dict(headers),
                      "body": json.loads(body.decode("utf-8"))})
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        status, payload, hdrs = item
        if not isinstance(payload, bytes):
            payload = json.dumps(payload).encode("utf-8")
        return status, payload, hdrs

    return transport, calls


def client(family=FAMILY, options=OPTIONS, **kw):
    kw.setdefault("state", jev.MemoryClientState())
    return jev.JevContractClient(
        decision_family=family, options=options, api_key="k", flag_on=True,
        max_calls=50, sleeper=lambda _s: None, **kw)


def ask(c, *responses, **kw):
    transport, calls = transport_of(*responses)
    with patch.object(jev, "_http_post", transport):
        adj = c.adjudicate(question_id="edge:e1", prompt="instructions",
                           day=DAY, **kw)
    return adj, calls


def http_error(code, request_id=None):
    hdrs = Message()
    if request_id:
        hdrs[jev.REQUEST_ID_HEADER] = request_id
    return urllib.error.HTTPError(jev.ENDPOINT, code, "err", hdrs, None)


# --- request shape --------------------------------------------------------------

def test_request_body_is_the_documented_shape():
    packet = shadow.build_packet({
        "relation_type": "quote", "a": {"text": "alpha words", "langs": ["en"]},
        "b": {"text": "beta words", "langs": ["en"]}, "dt_seconds": 5,
        "thread_depth_b": None})
    transport, calls = transport_of((200, recorded_shape(CONFIDENT), HDRS))
    with patch.object(jev, "_http_post", transport):
        shadow.adjudicate_packet(
            packet, edge_id="e1", day=DAY, api_key="k", flag_on=True,
            max_calls=50, seed_state=(0, None, False))
    (call,) = calls
    body = call["body"]
    assert call["url"] == "https://api.typesafe.ai/v1/systemone"
    assert call["headers"]["Content-Type"] == "application/json"
    assert set(body) == {"state", "model", "questions"}
    assert body["model"] == "jev-latest"
    assert list(body["questions"]) == [FAMILY]
    q = body["questions"][FAMILY]
    assert set(q) == {"type", "instructions", "criteria"}
    assert q["type"] == "choice"
    # Canonical (sorted-key) serialization on the wire; the frozen option
    # order is the local argmax tie-break, not a wire property.
    assert list(q["criteria"]) == sorted(OPTIONS)
    assert q["criteria"] == shadow.RELATION_CRITERIA
    # Evidence travels as state, verbatim; instructions carry none of it.
    assert body["state"] == packet
    assert "alpha words" not in q["instructions"]
    assert "B quotes A" in q["instructions"]
    # Local labels never go on the wire.
    raw = json.dumps(body)
    for local in ("edge:e1", "jev-dual-shadow-v0", "labelwatch.semantic_shadow"):
        assert local not in raw


def test_criteria_cover_options_and_default_to_option_name():
    body = jev.build_request_body(
        decision_family=FAMILY, instructions="i", options=OPTIONS,
        criteria={"underdetermined": "u", "dependent_direct": "d"})
    crit = body["questions"][FAMILY]["criteria"]
    assert list(crit) == list(OPTIONS)
    assert crit["dependent_direct"] == "d"
    assert crit["unrelated"] == "unrelated"


def test_repeats_serialize_identically():
    unsure = {"dependent_direct": 0.55, "dependent_indirect": 0.30,
              "independent_lexical": 0.05, "unrelated": 0.05,
              "underdetermined": 0.05}
    adj, calls = ask(client(), (200, recorded_shape(unsure), HDRS),
                     state={"packet_kind": "x"})
    assert len(calls) == 3
    assert calls[0]["body"] == calls[1]["body"] == calls[2]["body"]
    assert len({r["request_digest"] for r in adj.records}) == 1


# --- verbatim recorded responses --------------------------------------------------

@pytest.mark.parametrize("rec", RECORDED, ids=[r["decision_family"]
                                               for r in RECORDED])
def test_recorded_live_response_parses(rec):
    c = client(family=rec["decision_family"], options=tuple(rec["options"]))
    adj, _ = ask(c, (rec["http_status"], rec["body"], rec["headers"]))
    answer = rec["body"]["answers"][rec["decision_family"]]
    r = adj.records[0]
    assert r["status"] == "ok"
    assert r["http_status"] == 200
    assert r["model_requested"] == "jev-latest"
    assert r["model_resolved"] == rec["body"]["model"] == "jev-1.13.0"
    assert r["typesafe_request_id"] == rec["headers"]["x-typesafe-request-id"]
    assert r["probabilities"] == {o: float(answer["probabilities"][o])
                                  for o in rec["options"]}
    assert r["choice_returned"] == answer["choice"]
    assert r["confidence_observed"] == answer["confidence"]
    assert r["input_tokens"] == rec["body"]["usage"]["input_tokens"]
    assert r["output_tokens"] == rec["body"]["usage"]["output_tokens"]
    expected = jev.recompute_argmax(answer["probabilities"],
                                    tuple(rec["options"]))
    assert r["argmax_recomputed"] == expected
    assert r["choice_argmax_discrepancy"] is (answer["choice"] != expected)


def test_recorded_off_grid_float_is_in_contract():
    # 0.6900000000000001 was returned live; it must not trip validation.
    rec = next(r for r in RECORDED if r["decision_family"] == "window_signature")
    probs = rec["body"]["answers"]["window_signature"]["probabilities"]
    assert probs["deletion_cascade"] == 0.6900000000000001
    assert jev.validate_probabilities(probs, rec["options"]) is None


def test_recorded_no_match_option_is_an_ordinary_answer():
    # The characterization question carried an explicit no-match option.
    rec = next(r for r in RECORDED if r["decision_family"] == "failure_class")
    assert "unclear" in rec["options"]
    body = json.loads(json.dumps(rec["body"]))
    body["answers"]["failure_class"].update(
        choice="unclear", probabilities={o: 0.0 for o in rec["options"]})
    body["answers"]["failure_class"]["probabilities"].update(
        unclear=0.9, dependency=0.1)
    c = client(family="failure_class", options=tuple(rec["options"]))
    adj, _ = ask(c, (200, body, rec["headers"]))
    assert adj.status == "ok" and adj.argmax == "unclear"


def test_underdetermined_argmax_is_kept_and_carries_no_semantic_weight():
    adj, _ = ask(client(), (200, recorded_shape(UNCLEAR), HDRS))
    assert adj.status == "ok"
    assert adj.argmax == "underdetermined"
    assert adj.records[0]["argmax_recomputed"] == "underdetermined"
    assert shadow.semantic_channel(adj) is False


def test_raw_choice_preserved_when_it_disagrees():
    adj, _ = ask(client(), (200, recorded_shape(CONFIDENT, choice="unrelated"),
                            HDRS))
    r = adj.records[0]
    assert r["status"] == "ok"
    assert r["choice_returned"] == "unrelated"
    assert r["argmax_recomputed"] == "dependent_direct"
    assert r["choice_argmax_discrepancy"] is True
    assert adj.argmax == "dependent_direct"


def test_confidence_is_read_from_the_answer_not_the_top_level():
    body = recorded_shape(CONFIDENT, confidence=0.33)
    body["confidence"] = 0.99
    body["choice"] = "unrelated"
    adj, _ = ask(client(), (200, body, HDRS))
    r = adj.records[0]
    assert r["confidence_observed"] == 0.33
    assert r["choice_returned"] == "dependent_direct"


# --- malformed / missing answers ------------------------------------------------------

@pytest.mark.parametrize("mutate,why", [
    (lambda b: b["answers"].pop(FAMILY), "answer key absent"),
    (lambda b: b["answers"].update({FAMILY: "dependent_direct"}), "answer not object"),
    (lambda b: b.update(answers=[1, 2]), "answers not object"),
    (lambda b: b.pop("answers"), "answers absent"),
    (lambda b: b.pop("model"), "model absent"),
    (lambda b: b.update(model=""), "model empty"),
    (lambda b: b["answers"][FAMILY].pop("probabilities"), "probabilities absent"),
    (lambda b: b["answers"].update(
        other=b["answers"].pop(FAMILY)), "answer under another key"),
])
def test_malformed_answer_is_a_contract_error(mutate, why):
    body = recorded_shape(CONFIDENT)
    mutate(body)
    adj, _ = ask(client(), (200, body, HDRS))
    assert adj.status == "contract_error", why
    r = adj.records[0]
    assert r["probabilities"] is None and r["argmax_recomputed"] is None
    assert r["typesafe_request_id"] == "req_01fabricated"


def test_pre_fix_flat_shape_is_rejected():
    flat = {"model": "jev-1.13.0", "probabilities": CONFIDENT,
            "choice": "dependent_direct", "confidence": 0.9,
            "request_id": "req_body", "model_resolved": "jev-x"}
    adj, _ = ask(client(), (200, flat, {}))
    r = adj.records[0]
    assert adj.status == "contract_error"
    # Neither invented field is honoured.
    assert r["typesafe_request_id"] is None
    assert r["model_resolved"] == "jev-1.13.0"


@pytest.mark.parametrize("payload", [b"", b"not json", b"[1,2]", b"null"])
def test_unparseable_body_is_a_contract_error(payload):
    adj, _ = ask(client(), (200, payload, HDRS))
    assert adj.status == "contract_error"


# --- HTTP / API failures --------------------------------------------------------------

@pytest.mark.parametrize("code", [401, 422, 500])
def test_api_error_is_http_error_without_retry(code):
    c = client()
    adj, calls = ask(c, http_error(code, request_id="req_err"))
    r = adj.records[0]
    assert adj.status == "http_error"
    assert r["http_status"] == code
    assert r["typesafe_request_id"] == "req_err"
    assert len(calls) == 1
    assert c.state.calls_today(DAY) == 1  # the attempt spent the network


def test_overloaded_529_retries_once_then_parses():
    adj, calls = ask(client(), http_error(529),
                     (200, recorded_shape(CONFIDENT), HDRS))
    assert adj.status == "ok" and len(calls) == 2


def test_connection_refused_is_http_error():
    adj, _ = ask(client(), urllib.error.URLError(ConnectionRefusedError()))
    assert adj.status == "http_error"
    assert adj.records[0]["http_status"] is None


def test_non_200_success_status_is_http_error():
    adj, _ = ask(client(), (202, recorded_shape(CONFIDENT), HDRS))
    assert adj.status == "http_error"
    assert adj.records[0]["http_status"] == 202


def test_failure_leaves_no_semantic_receipt():
    adj, _ = ask(client(), http_error(500))
    assert shadow.semantic_channel(adj) is None


# --- model receipts ---------------------------------------------------------------

def test_model_alias_and_resolved_version_are_recorded_and_anchor_the_day():
    c = client()
    ask(c, (200, recorded_shape(CONFIDENT, model="jev-1.13.0"), HDRS))
    assert c.state.model_anchor(DAY) == "jev-1.13.0"
    adj, calls = ask(c, (200, recorded_shape(CONFIDENT, model="jev-1.14.0"),
                         HDRS))
    assert adj.status == "contract_error"
    assert adj.records[0]["model_requested"] == "jev-latest"
    assert adj.records[0]["model_resolved"] == "jev-1.14.0"
    assert c.state.is_model_stopped(DAY)
    later, later_calls = ask(c, (200, recorded_shape(CONFIDENT), HDRS))
    assert later.status == "contract_error" and later_calls == []


# --- no identity/content persistence beyond the ephemeral request ----------------

def test_records_carry_digests_not_request_content():
    text_a = "an unmistakable fixture sentence about lighthouses"
    packet = shadow.build_packet({
        "relation_type": "reply", "a": {"text": text_a, "langs": ["en"]},
        "b": {"text": "reply about lighthouses", "langs": ["en"]},
        "dt_seconds": 30, "thread_depth_b": 1})
    unsure = {"dependent_direct": 0.55, "dependent_indirect": 0.30,
              "independent_lexical": 0.05, "unrelated": 0.05,
              "underdetermined": 0.05}
    transport, calls = transport_of((200, recorded_shape(unsure), HDRS))
    with patch.object(jev, "_http_post", transport):
        adj, mem, _ = shadow.adjudicate_packet(
            packet, edge_id="e1", day=DAY, api_key="k", flag_on=True,
            max_calls=50, seed_state=(0, None, False))
    # The text did go out (the approved ephemeral path) ...
    assert text_a in json.dumps(calls[0]["body"])
    # ... but nothing retained carries it, or the key.
    retained = json.dumps([adj.records, mem.records], default=str)
    assert "lighthouses" not in retained
    assert "Bearer" not in retained and '"k"' not in retained
    for r in adj.records:
        assert set(r) == set(jev.CALL_RECORD_FIELDS)
        assert len(r["request_digest"]) == 64
