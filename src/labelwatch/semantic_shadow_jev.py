"""Thin JEV-CONTRACT-v0 client (jev-dual-shadow-v0/DESIGN.md §2).

Stdlib ``urllib`` only. Choice questions over a frozen option order;
probabilities are observations. TypeSafe ``confidence`` is recorded
observationally and used in no decision. The selected option is always
the recomputed argmax over returned probabilities (tie-break: first
option in the frozen order); the raw returned choice is preserved
separately and a discrepancy is flagged, never fatal.

Network gating (§2.9): the adapter refuses the network unless all of
API key present, component feature flag on, daily call cap not reached.
Refusals still emit per-call records with the refusal status — the
gates themselves are qualification evidence.

Wire shape is the documented System One API, as recorded on live 200
responses by the typesafe characterization harness
(``/data/git/typesafe/ts_harness``, 2026-09-16) and exercised live by the
Weatherwatch component (2026-09-17)::

    request : {"state": packet, "model": "jev-latest",
               "questions": {family: {"type": "choice", "instructions": str,
                                      "criteria": {option: description}}}}
    response: {"model": str, "answers": {family: {"type", "choice",
               "confidence", "probabilities"}}, "usage": {...}}

with the request id in the ``x-typesafe-request-id`` response header. One
question per request, keyed by the decision family; ``question_id`` and the
campaign/component labels stay on the local record and are never sent.

No Jev output has enforcement authority anywhere in this campaign.
"""
from __future__ import annotations

import json
import logging
import socket
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import timezone
from typing import Callable, Dict, List, Optional, Sequence

from .utils import format_ts, hash_sha256, now_utc, stable_json

log = logging.getLogger(__name__)

CAMPAIGN = "jev-dual-shadow-v0"
COMPONENT = "labelwatch.semantic_shadow"
ENDPOINT = "https://api.typesafe.ai/v1/systemone"
MODEL_REQUESTED = "jev-latest"
ENV_API_KEY = "TYPESAFE_API_KEY"
REQUEST_ID_HEADER = "x-typesafe-request-id"

TIMEOUT_S = 15  # §2.8
RETRYABLE_HTTP_STATUSES = (429, 529)  # §2.8: one retry with backoff, only these
RETRY_BACKOFF_S = 2.0

# §2.5 cost rate. Unverified cookbook figure; flagged as such. Used only
# for observational cost estimates, never in any decision.
EST_COST_PER_MTOK_INPUT_USD = 0.042
# §2.9 pre-flight planning estimate; live entry points print it.
# Measured on this component (L2 live fixture evaluation, 2026-09-17, 102
# calls, jev-1.13.0): input tokens mean 635 / median 638 / min 608 / max
# 670, fitting ~593 + 0.31 per char of packet text (residual <= 19). The
# fixtures carry 66-248 chars; a maximum packet (500 + 500 chars) projects
# to ~903. The planning value is the upper practical figure covering that
# maximum. It replaces the pre-live 800 and is independent of Weatherwatch's
# ~1,900 (different payload). Estimates only — never used for caps or gating.
EST_INPUT_TOKENS_PER_CALL = 950
EST_INPUT_TOKENS_STATUS = (
    "measured 2026-09-17 (L2, 102 calls): mean 635, max 670 on fixture "
    "text; 950 covers a maximum 1,000-char packet (~903 projected)"
)

STATUS_OK = "ok"
STATUS_TIMEOUT = "timeout"
STATUS_HTTP_ERROR = "http_error"
STATUS_CONTRACT_ERROR = "contract_error"
STATUS_BUDGET_REFUSED = "budget_refused"
STATUS_KEY_ABSENT = "key_absent"
STATUS_FLAG_OFF = "flag_off"

PROB_SUM_TOLERANCE = 0.02  # §2.4; transport grid is ~0.01
REPEAT_TOP1_MAX = 0.70  # §2.6: repeat if p_top1 <= this
REPEAT_GAP_MAX = 0.15  # §2.6: repeat if (p_top1 - p_top2) <= this
REPEAT_K = 3  # §2.6: identical serialized repeats, trigger call is replicate 1
AMBIGUITY_MIN_MEDIAN_P = 0.10  # §2.6: ambiguity set = replicate-median p >= this

CALL_RECORD_FIELDS = (
    "call_id", "ts", "campaign", "component", "decision_family",
    "question_id", "request_digest", "model_requested", "model_resolved",
    "probabilities", "argmax_recomputed", "choice_returned",
    "choice_argmax_discrepancy", "confidence_observed", "latency_s",
    "input_tokens", "output_tokens", "est_cost_usd", "replicate_set_id",
    "replicate_index", "status", "http_status", "typesafe_request_id",
)


def network_attempted(record: dict) -> bool:
    """True iff the record is for a call that reached the network. Every
    network attempt stamps ``latency_s`` before any other outcome is set;
    refusals (flag, key, budget, model-version stop) never do."""
    return record.get("latency_s") is not None


def utc_day(dt=None) -> str:
    """UTC day stamp (YYYY-MM-DD) anchoring caps and the model anchor."""
    return (dt or now_utc()).astimezone(timezone.utc).strftime("%Y-%m-%d")


def build_request_body(
    *,
    decision_family: str,
    instructions: str,
    options: Sequence[str],
    criteria: Optional[Dict[str, str]] = None,
    state: Optional[dict] = None,
) -> dict:
    """Canonical choice-question request body in the documented shape. The
    request digest is the sha256 of the canonical serialization of exactly
    this dict (§2.5), so k=3 repeats serialize identically by construction
    (§2.6). ``criteria`` is rebuilt in the frozen option order; an option
    without a description is described by its own name."""
    criteria = criteria or {}
    return {
        "state": state if state is not None else {},
        "model": MODEL_REQUESTED,
        "questions": {
            decision_family: {
                "type": "choice",
                "instructions": instructions,
                "criteria": {o: criteria.get(o, o) for o in options},
            },
        },
    }


def extract_answer(resp: dict, decision_family: str) -> Optional[dict]:
    """The answer object for the asked question, or None when absent."""
    answers = resp.get("answers")
    if not isinstance(answers, dict):
        return None
    answer = answers.get(decision_family)
    return answer if isinstance(answer, dict) else None


def request_digest(body: dict) -> str:
    return hash_sha256(stable_json(body))


def validate_probabilities(probabilities, options: Sequence[str]) -> Optional[str]:
    """§2.4 contract validation. Returns an error string or None."""
    if not isinstance(probabilities, dict):
        return "probabilities is not an object"
    if set(probabilities.keys()) != set(options):
        return "probabilities keys do not equal the option set"
    for opt in options:
        v = probabilities[opt]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return f"probability for {opt!r} is not numeric"
        if not (0.0 <= float(v) <= 1.0):
            return f"probability for {opt!r} outside [0,1]"
    total = sum(float(probabilities[o]) for o in options)
    if abs(total - 1.0) > PROB_SUM_TOLERANCE:
        return f"probability sum {total:.4f} outside 1±{PROB_SUM_TOLERANCE}"
    return None


def recompute_argmax(probabilities: Dict[str, float], options: Sequence[str]) -> str:
    """§2.3 recomputed argmax; ties break to the first option in the frozen
    option order of the family (strictly-greater scan in frozen order)."""
    best = options[0]
    best_p = float(probabilities[best])
    for opt in options[1:]:
        p = float(probabilities[opt])
        if p > best_p:
            best, best_p = opt, p
    return best


# Probabilities arrive on the 0.01 transport grid; 1e-9 epsilon keeps float
# noise (e.g. 0.42-0.27 == 0.15000000000000002) from flipping a boundary
# decision, while staying far below the grid step.
_GRID_EPS = 1e-9


def repeat_triggered(probabilities: Dict[str, float], options: Sequence[str]) -> bool:
    """§2.6 frozen repeat policy: repeat iff p_top1 <= 0.70 OR
    (p_top1 - p_top2) <= 0.15 (both multiples of the 0.01 transport grid)."""
    ps = sorted((float(probabilities[o]) for o in options), reverse=True)
    p_top1 = ps[0]
    p_top2 = ps[1] if len(ps) > 1 else 0.0
    return (
        p_top1 <= REPEAT_TOP1_MAX + _GRID_EPS
        or (p_top1 - p_top2) <= REPEAT_GAP_MAX + _GRID_EPS
    )


def _median(values: Sequence[float]) -> float:
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def replicate_medians(
    records: Sequence[dict], options: Sequence[str]
) -> Optional[Dict[str, float]]:
    """Median p per option across the ok records of a replicate set.
    No majority-vote collapse (§2.6); all distributions stay on their
    per-call records — this is only the channel-computation view."""
    ok = [r for r in records if r.get("status") == STATUS_OK and r.get("probabilities")]
    if not ok:
        return None
    return {
        opt: _median([float(r["probabilities"][opt]) for r in ok]) for opt in options
    }


def ambiguity_set(medians: Dict[str, float]) -> List[str]:
    """§2.6: options with replicate-median p >= 0.10, in frozen option order
    as iterated by the caller's dict construction (medians is built in
    frozen order)."""
    return [o for o, p in medians.items() if p >= AMBIGUITY_MIN_MEDIAN_P]


def estimate_cost_usd(max_calls: int) -> dict:
    """§2.9 pre-flight cost estimate, mandatory on every live entry point.
    Worst case assumes every call spends the k=3 replicate set at the
    unverified planning token figure."""
    worst_calls = max_calls
    est_tokens = worst_calls * EST_INPUT_TOKENS_PER_CALL
    return {
        "max_calls": max_calls,
        "est_input_tokens_per_call": EST_INPUT_TOKENS_PER_CALL,
        "est_input_tokens_per_call_status": EST_INPUT_TOKENS_STATUS,
        "est_input_tokens_worst_case": est_tokens,
        "est_cost_usd_worst_case": round(
            est_tokens * EST_COST_PER_MTOK_INPUT_USD / 1_000_000, 6
        ),
        "rate_basis": (
            "$0.042 / 1M input tokens — unverified cookbook figure "
            "(JEV-CONTRACT-v0 §2.5), planning use only"
        ),
    }


def _lower_headers(headers) -> Dict[str, str]:
    return {k.lower(): v for k, v in (headers.items() if headers else ())}


def _http_post(url: str, headers: dict, body: bytes, timeout_s: float):
    """Single network seam. Tests patch this; nothing else in the module
    touches the network. Returns (http_status, body_bytes, headers) with
    lower-cased header names; non-2xx raises ``urllib.error.HTTPError``."""
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.status, resp.read(), _lower_headers(resp.headers)


class ClientState:
    """Persistence the client needs (§2.6/§2.7/§2.9): daily call counter,
    daily model anchor + stop, and the per-call record sink. In-memory
    default; the sidecar store provides a durable implementation with the
    same interface."""

    def calls_today(self, day: str) -> int:
        raise NotImplementedError

    def set_calls_today(self, day: str, n: int) -> None:
        raise NotImplementedError

    def model_anchor(self, day: str) -> Optional[str]:
        raise NotImplementedError

    def set_model_anchor(self, day: str, model: str) -> None:
        raise NotImplementedError

    def is_model_stopped(self, day: str) -> bool:
        raise NotImplementedError

    def set_model_stopped(self, day: str) -> None:
        raise NotImplementedError

    def record_call(self, record: dict) -> None:
        raise NotImplementedError


class MemoryClientState(ClientState):
    """In-memory state. ``sink``, when given, receives each per-call record
    the moment the client produces it (durable per-call persistence for
    callers that need it; the daemon merges records into the sidecar
    instead and passes none)."""

    def __init__(self, sink: Optional[Callable[[dict], None]] = None):
        self.counters: Dict[str, int] = {}
        self.anchors: Dict[str, str] = {}
        self.stopped: set = set()
        self.records: List[dict] = []
        self.sink = sink

    def calls_today(self, day: str) -> int:
        return self.counters.get(day, 0)

    def set_calls_today(self, day: str, n: int) -> None:
        self.counters[day] = n

    def model_anchor(self, day: str) -> Optional[str]:
        return self.anchors.get(day)

    def set_model_anchor(self, day: str, model: str) -> None:
        self.anchors[day] = model

    def is_model_stopped(self, day: str) -> bool:
        return day in self.stopped

    def set_model_stopped(self, day: str) -> None:
        self.stopped.add(day)

    def record_call(self, record: dict) -> None:
        self.records.append(record)
        if self.sink is not None:
            self.sink(record)


@dataclass
class Adjudication:
    """One question's adjudication: the trigger call plus any repeats.
    ``records`` preserves every distribution (§2.6); the median view is
    derived, never collapsed back over the records."""

    status: str
    question_id: str
    replicate_set_id: str
    records: List[dict] = field(default_factory=list)
    median_probabilities: Optional[Dict[str, float]] = None
    argmax: Optional[str] = None
    ambiguity_set: List[str] = field(default_factory=list)
    calls_spent: int = 0
    repeat_spent: bool = False


class JevContractClient:
    """JEV-CONTRACT-v0 adapter. Constructed per component per run; the
    injected ``state`` carries the daily counter/anchor so restarts
    within a UTC day cannot exceed the cap or miss a model stop."""

    def __init__(
        self,
        *,
        decision_family: str,
        options: Sequence[str],
        component: str = COMPONENT,
        api_key: Optional[str] = None,
        flag_on: bool = False,
        max_calls: int = 0,
        state: Optional[ClientState] = None,
        endpoint: str = ENDPOINT,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if not options:
            raise ValueError("options must be a non-empty frozen option order")
        self.decision_family = decision_family
        self.options = tuple(options)
        self.component = component
        self.api_key = api_key
        self.flag_on = flag_on
        self.max_calls = max_calls
        self.state = state or MemoryClientState()
        self.endpoint = endpoint
        self.sleeper = sleeper

    # -- gates ------------------------------------------------------------

    def _refusal(self, status: str, question_id: str, day: str) -> Adjudication:
        rec = self._new_record(question_id, "", uuid.uuid4().hex, 1)
        rec["status"] = status
        self.state.record_call(rec)
        return Adjudication(
            status=status,
            question_id=question_id,
            replicate_set_id=rec["replicate_set_id"],
            records=[rec],
        )

    def _gate_status(self, day: str) -> Optional[str]:
        """§2.9 + §2.7 network gates. Returns None when the network may be
        touched, else the refusal status."""
        if not self.flag_on:
            return STATUS_FLAG_OFF
        if not self.api_key:
            return STATUS_KEY_ABSENT
        if self.state.calls_today(day) >= self.max_calls:
            return STATUS_BUDGET_REFUSED
        if self.state.is_model_stopped(day):
            return STATUS_CONTRACT_ERROR
        return None

    # -- records ----------------------------------------------------------

    def _new_record(
        self, question_id: str, digest: str, replicate_set_id: str, replicate_index: int
    ) -> dict:
        return {
            "call_id": uuid.uuid4().hex,
            "ts": format_ts(now_utc()),
            "campaign": CAMPAIGN,
            "component": self.component,
            "decision_family": self.decision_family,
            "question_id": question_id,
            "request_digest": digest,
            "model_requested": MODEL_REQUESTED,
            "model_resolved": None,
            "probabilities": None,
            "argmax_recomputed": None,
            "choice_returned": None,
            "choice_argmax_discrepancy": False,
            "confidence_observed": None,
            "latency_s": None,
            "input_tokens": None,
            "output_tokens": None,
            "est_cost_usd": None,
            "replicate_set_id": replicate_set_id,
            "replicate_index": replicate_index,
            "status": None,
            "http_status": None,
            "typesafe_request_id": None,
        }

    # -- transport ----------------------------------------------------------

    def _call_once(
        self, body: dict, digest: str, question_id: str,
        replicate_set_id: str, replicate_index: int, day: str,
    ) -> dict:
        rec = self._new_record(question_id, digest, replicate_set_id, replicate_index)
        body_bytes = stable_json(body).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = None
        resp_headers: Dict[str, str] = {}
        attempts = 0
        while True:
            attempts += 1
            t0 = time.monotonic()
            try:
                http_status, payload, resp_headers = _http_post(
                    self.endpoint, headers, body_bytes, TIMEOUT_S
                )
                rec["latency_s"] = round(time.monotonic() - t0, 3)
                rec["http_status"] = http_status
            except urllib.error.HTTPError as e:
                rec["latency_s"] = round(time.monotonic() - t0, 3)
                rec["http_status"] = e.code
                rec["typesafe_request_id"] = _lower_headers(
                    e.headers).get(REQUEST_ID_HEADER)
                if e.code in RETRYABLE_HTTP_STATUSES and attempts == 1:
                    self.sleeper(RETRY_BACKOFF_S)
                    continue
                rec["status"] = STATUS_HTTP_ERROR
            except (TimeoutError, socket.timeout):
                rec["latency_s"] = round(time.monotonic() - t0, 3)
                rec["status"] = STATUS_TIMEOUT
            except urllib.error.URLError as e:
                rec["latency_s"] = round(time.monotonic() - t0, 3)
                reason = getattr(e, "reason", None)
                if isinstance(reason, (TimeoutError, socket.timeout)):
                    rec["status"] = STATUS_TIMEOUT
                else:
                    rec["status"] = STATUS_HTTP_ERROR
            except OSError:
                rec["latency_s"] = round(time.monotonic() - t0, 3)
                rec["status"] = STATUS_HTTP_ERROR
            break

        # Every network attempt counts against the daily call cap (§2.9),
        # including failed ones — the cap bounds spend, not success.
        self.state.set_calls_today(day, self.state.calls_today(day) + 1)

        if rec["status"] is None and rec["http_status"] != 200:
            # urlopen raises for non-2xx; any other non-200 success code is
            # outside the contract's single success status.
            rec["status"] = STATUS_HTTP_ERROR
        rec["typesafe_request_id"] = (
            resp_headers.get(REQUEST_ID_HEADER) or rec["typesafe_request_id"])
        if rec["status"] is not None:
            self.state.record_call(rec)
            return rec

        try:
            resp = json.loads(payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            rec["status"] = STATUS_CONTRACT_ERROR
            self.state.record_call(rec)
            return rec
        if not isinstance(resp, dict):
            rec["status"] = STATUS_CONTRACT_ERROR
            self.state.record_call(rec)
            return rec

        model = resp.get("model")
        rec["model_resolved"] = model if isinstance(model, str) and model else None
        usage = resp.get("usage") if isinstance(resp.get("usage"), dict) else {}
        rec["input_tokens"] = usage.get("input_tokens")
        rec["output_tokens"] = usage.get("output_tokens")
        if isinstance(rec["input_tokens"], (int, float)):
            rec["est_cost_usd"] = (
                rec["input_tokens"] * EST_COST_PER_MTOK_INPUT_USD / 1_000_000
            )

        # §2.7 model-version stop: anchor the day's resolved model on first
        # sight; a change refuses further calls that day. This call's record
        # carries contract_error; later calls are refused before the network.
        if rec["model_resolved"]:
            anchor = self.state.model_anchor(day)
            if anchor is None:
                self.state.set_model_anchor(day, rec["model_resolved"])
            elif rec["model_resolved"] != anchor:
                rec["status"] = STATUS_CONTRACT_ERROR
                self.state.set_model_stopped(day)
                self.state.record_call(rec)
                log.error(
                    "model_resolved changed within UTC day %s (%s -> %s); "
                    "refusing further calls today",
                    day, anchor, rec["model_resolved"],
                )
                return rec

        answer = extract_answer(resp, self.decision_family)
        if rec["model_resolved"] is None:
            err = "resolved model string missing"
        elif answer is None:
            err = f"answer for {self.decision_family!r} missing"
        else:
            rec["confidence_observed"] = answer.get("confidence")  # observational only
            err = validate_probabilities(answer.get("probabilities"), self.options)
        if err is not None:
            log.warning("contract validation failed for %s: %s", question_id, err)
            rec["status"] = STATUS_CONTRACT_ERROR
            self.state.record_call(rec)
            return rec

        probs = {o: float(answer["probabilities"][o]) for o in self.options}
        rec["probabilities"] = probs
        rec["argmax_recomputed"] = recompute_argmax(probs, self.options)
        rec["choice_returned"] = answer.get("choice")
        rec["choice_argmax_discrepancy"] = bool(
            rec["choice_returned"] is not None
            and rec["choice_returned"] != rec["argmax_recomputed"]
        )
        rec["status"] = STATUS_OK
        self.state.record_call(rec)
        return rec

    # -- adjudication -------------------------------------------------------

    def adjudicate(
        self,
        *,
        question_id: str,
        prompt: str,
        day: Optional[str] = None,
        repeat_decider: Optional[Callable[[List[dict]], bool]] = None,
        state: Optional[dict] = None,
        criteria: Optional[Dict[str, str]] = None,
    ) -> Adjudication:
        """Run one question under the contract.

        ``prompt`` is the question's instructions; the evidence travels as
        ``state`` and option descriptions as ``criteria``. Neither is
        persisted: records carry only the request digest.

        ``repeat_decider`` (§5.6 hook) receives the replicate records so far
        and returns True to spend the k=3 repeat set. When None, the frozen
        §2.6 threshold policy applies directly.
        """
        day = day or utc_day()
        refusal = self._gate_status(day)
        if refusal is not None:
            return self._refusal(refusal, question_id, day)

        body = build_request_body(
            decision_family=self.decision_family,
            instructions=prompt,
            options=self.options,
            criteria=criteria,
            state=state,
        )
        digest = request_digest(body)
        replicate_set_id = uuid.uuid4().hex

        records = [
            self._call_once(body, digest, question_id, replicate_set_id, 1, day)
        ]
        first = records[0]
        if first["status"] != STATUS_OK:
            return Adjudication(
                status=first["status"],
                question_id=question_id,
                replicate_set_id=replicate_set_id,
                records=records,
                calls_spent=1,
            )

        if repeat_decider is not None:
            spend = bool(repeat_decider(records))
        else:
            spend = repeat_triggered(first["probabilities"], self.options)

        if spend:
            for idx in range(2, REPEAT_K + 1):
                gate = self._gate_status(day)
                if gate is not None:
                    rec = self._new_record(question_id, digest, replicate_set_id, idx)
                    rec["status"] = gate
                    self.state.record_call(rec)
                    records.append(rec)
                    break
                rec = self._call_once(body, digest, question_id, replicate_set_id, idx, day)
                records.append(rec)
                if rec["status"] != STATUS_OK:
                    break

        medians = replicate_medians(records, self.options)
        ok_records = [r for r in records if r["status"] == STATUS_OK]
        status = STATUS_OK if ok_records else records[-1]["status"]
        return Adjudication(
            status=status,
            question_id=question_id,
            replicate_set_id=replicate_set_id,
            records=records,
            median_probabilities=medians,
            argmax=recompute_argmax(medians, self.options) if medians else None,
            ambiguity_set=ambiguity_set(medians) if medians else [],
            calls_spent=len(records),
            repeat_spent=spend and len(ok_records) > 1,
        )
