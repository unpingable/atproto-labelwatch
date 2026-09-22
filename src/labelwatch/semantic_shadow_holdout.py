"""Part H holdout substrate for the V0.1 rule (Amendment 2026-09-17-C §6).

Offline only: schema and composition validation for independently authored
holdout items, the blind annotation view, annotation ingest and gold
assembly, agreement gates, and the pre-registered evaluation. No network.

Nothing here chooses a threshold from data. Every tolerance below is copied
from the frozen amendment, and the evaluator only reports against them.
This module contains no holdout content; items arrive from blind authors
(see ``specs/semantic_shadow/part_h/AUTHORING_PROTOCOL.md``).
"""
from __future__ import annotations

import json
import math
import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import semantic_shadow as shadow
from . import semantic_shadow_jev as jev
from . import semantic_shadow_v01 as v01
from .utils import format_ts, get_git_commit, hash_sha256, now_utc, stable_json

AUTHORING_PROTOCOL_VERSION = "lw-part-h-authoring/v1"
ITEM_RECORD_KIND = "labelwatch.semantic_shadow_holdout_item.v1"
ANNOTATION_RECORD_KIND = "labelwatch.semantic_shadow_holdout_annotation.v1"

ITEM_ID_RE = re.compile(r"^h-[0-9a-f]{12}$")
AUTHOR_REF_RE = re.compile(r"^author-[0-9a-f]{8}$")
ANNOTATOR_REF_RE = re.compile(r"^annotator-[0-9a-f]{8}$")
LANG_RE = re.compile(r"^[a-z]{2,3}(-[A-Za-z0-9]{1,8})*$")

L2_CATEGORIES = tuple(shadow.FIXTURE_CLASSES)
EXTRA_CATEGORIES = ("common_source_convergence", "explicit_reference_with_stance",
                    "quote_no_comment")
CATEGORIES = L2_CATEGORIES + EXTRA_CATEGORIES

# §6.1 composition (frozen). "~150" is implemented as 150 ± 10 %.
TARGET_SIZE = 150
SIZE_MIN, SIZE_MAX = 135, 165
CATEGORY_MINIMA = {**{c: 15 for c in L2_CATEGORIES},
                   "common_source_convergence": 15,
                   "explicit_reference_with_stance": 15,
                   "quote_no_comment": 10}
LONG_TEXT_MIN_ITEMS = 15
LONG_TEXT_CHARS = (300, 500)
MULTILINGUAL_MIN_ITEMS = 10
ANCHOR_BEARING_NEGATIVE_MIN_SHARE = 0.20

ITEM_REQUIRED = ("record_kind", "item_id", "authoring_protocol_version",
                 "author_ref", "relation_type", "text_a", "text_b",
                 "dt_seconds", "langs_a", "langs_b", "categories",
                 "anchor_bearing_negative", "declared_long_text",
                 "declared_multilingual")
ITEM_OPTIONAL = ("author_intended", "author_note")
AUTHOR_ONLY_FIELDS = ("author_ref", "categories", "anchor_bearing_negative",
                      "declared_long_text", "declared_multilingual",
                      "author_intended", "author_note")

EXPLICIT_LABELS = ("yes", "no", "unclear")
RELATION_LABELS = tuple(shadow.RELATION_OPTIONS)
ROLES = ("A", "B", "adjudicator")
ANNOTATION_REQUIRED = ("record_kind", "item_id", "item_digest", "corpus_digest",
                       "annotator_ref", "role", "explicit_reference",
                       "relation", "unresolved", "codebook_version",
                       "annotated_at", "seconds_spent", "blind_attestation")
BLINDNESS_KEYS = ("saw_jev_outputs", "saw_anchor_features",
                  "saw_distinctive_token_features", "saw_other_annotator",
                  "saw_author_intent")

# §6.3 / §6.4 (frozen).
X_KAPPA_MIN = 0.60
R_KAPPA_HALT = 0.40
R_KAPPA_UPPER = 0.60
M1_POINT_MAX = 0.05
M1_WILSON_UPPER_MAX = 0.12
M1_MIN_QUALIFIED = 50
M2_POINT_MIN = 0.90
M2_WILSON_LOWER_MIN = 0.80
M3_USEFUL_MIN = 0.50
M4_MIN = 0.95
M5_MAX_ITEMS = 2
M6_RECALL_MARGIN = 0.05
M10_MIN_SLICE_QUALIFIED = 15
M10_M1_MAX = 0.12
WILSON_Z = 1.959963984540054

FROZEN_GATES = {
    "x_kappa_min": X_KAPPA_MIN, "r_kappa_halt": R_KAPPA_HALT,
    "r_kappa_upper": R_KAPPA_UPPER, "m1_point_max": M1_POINT_MAX,
    "m1_wilson_upper_max": M1_WILSON_UPPER_MAX,
    "m1_min_qualified": M1_MIN_QUALIFIED, "m2_point_min": M2_POINT_MIN,
    "m2_wilson_lower_min": M2_WILSON_LOWER_MIN,
    "m3_useful_min": M3_USEFUL_MIN, "m4_min": M4_MIN,
    "m5_max_items": M5_MAX_ITEMS, "m6_recall_margin": M6_RECALL_MARGIN,
    "m10_min_slice_qualified": M10_MIN_SLICE_QUALIFIED,
    "m10_m1_max": M10_M1_MAX,
}


# --- items -------------------------------------------------------------------

def item_to_candidate(item: dict) -> dict:
    return {
        "relation_type": item["relation_type"],
        "a": {"text": item["text_a"], "langs": item["langs_a"]},
        "b": {"text": item["text_b"], "langs": item["langs_b"]},
        "dt_seconds": item["dt_seconds"],
        "thread_depth_b": None,
    }


def item_packet(item: dict) -> dict:
    return shadow.build_packet(item_to_candidate(item))


def item_digest(item: dict) -> str:
    return hash_sha256(stable_json(item))


def derived_long_text(item: dict) -> bool:
    lo, hi = LONG_TEXT_CHARS
    return all(lo <= len(item[k]) <= hi for k in ("text_a", "text_b"))


def derived_multilingual(item: dict) -> bool:
    langs = set(item["langs_a"]) | set(item["langs_b"])
    return any(l.split("-")[0] != "en" for l in langs)


def validate_item(item: Any) -> List[str]:
    """Per-item schema check (mirrors holdout_item.schema.json) plus the
    derived-metadata and structural checks. Returns errors."""
    if not isinstance(item, dict):
        return ["record is not an object"]
    errors = [f"missing required field {f!r}" for f in ITEM_REQUIRED if f not in item]
    if errors:
        return errors
    unknown = set(item) - set(ITEM_REQUIRED) - set(ITEM_OPTIONAL)
    if unknown:
        errors.append(f"unknown fields {sorted(unknown)} (gold, Jev or class "
                      "labels never belong in an authored item)")
    if item["record_kind"] != ITEM_RECORD_KIND:
        errors.append("record_kind mismatch")
    if not isinstance(item["item_id"], str) or not ITEM_ID_RE.match(item["item_id"]):
        errors.append("item_id must match ^h-[0-9a-f]{12}$ (opaque)")
    if item["authoring_protocol_version"] != AUTHORING_PROTOCOL_VERSION:
        errors.append("authoring_protocol_version mismatch")
    if not isinstance(item["author_ref"], str) or not AUTHOR_REF_RE.match(item["author_ref"]):
        errors.append("author_ref must match ^author-[0-9a-f]{8}$ (pseudonymous)")
    if item["relation_type"] not in ("reply", "quote", "repost"):
        errors.append("relation_type must be reply|quote|repost")
    for k in ("text_a", "text_b"):
        v = item[k]
        if not isinstance(v, str):
            errors.append(f"{k} must be a string")
        elif len(v) > shadow.TEXT_MAX_CHARS:
            errors.append(f"{k} exceeds {shadow.TEXT_MAX_CHARS} chars")
    if isinstance(item["text_a"], str) and not item["text_a"].strip():
        errors.append("text_a must be non-empty")
    dt = item["dt_seconds"]
    if not isinstance(dt, int) or isinstance(dt, bool) or dt < 0:
        errors.append("dt_seconds must be a non-negative integer")
    for k in ("langs_a", "langs_b"):
        langs = item[k]
        if not isinstance(langs, list) or not langs \
                or not all(isinstance(l, str) and LANG_RE.match(l) for l in langs):
            errors.append(f"{k} must be a non-empty list of language tags")
    cats = item["categories"]
    if not isinstance(cats, list) or not cats or len(set(cats)) != len(cats) \
            or any(c not in CATEGORIES for c in cats):
        errors.append("categories must be a non-empty list of distinct frozen categories")
        cats = []
    for k in ("anchor_bearing_negative", "declared_long_text", "declared_multilingual"):
        if not isinstance(item[k], bool):
            errors.append(f"{k} must be a boolean")
    if "author_intended" in item:
        ai = item["author_intended"]
        if not isinstance(ai, dict) or set(ai) - {"explicit_reference", "relation"} \
                or ai.get("explicit_reference", "yes") not in EXPLICIT_LABELS \
                or ai.get("relation", "unrelated") not in RELATION_LABELS:
            errors.append("author_intended must hold only explicit_reference/relation "
                          "from the frozen vocabularies")
    if "author_note" in item and not isinstance(item["author_note"], str):
        errors.append("author_note must be a string")
    if errors:
        return errors

    text_b_empty = not item["text_b"].strip()
    if "quote_no_comment" in cats:
        if item["relation_type"] != "quote" or not text_b_empty:
            errors.append("quote_no_comment requires relation_type quote and empty text_b")
    elif text_b_empty and item["relation_type"] != "repost":
        errors.append("text_b may be empty only for repost or quote_no_comment items")
    if "explicit_reference_with_stance" in cats and item["relation_type"] == "repost":
        errors.append("explicit_reference_with_stance requires reply or quote")
    if item["declared_long_text"] != derived_long_text(item):
        errors.append("declared_long_text does not match text lengths "
                      f"({LONG_TEXT_CHARS[0]}-{LONG_TEXT_CHARS[1]} chars per text)")
    if item["declared_multilingual"] != derived_multilingual(item):
        errors.append("declared_multilingual does not match language tags")
    packet = item_packet(item)
    if shadow.packet_privacy_scan(packet):
        errors.append("privacy tripwire: identifier-shaped content in packet")
    if packet["text_a"] != item["text_a"] or packet["text_b"] != item["text_b"]:
        errors.append("text contains identifiers that packetization would redact")
    if item["anchor_bearing_negative"] and not v01.anchors_from_packet(packet).present:
        errors.append("anchor_bearing_negative item carries no admitted V0.1 anchor")
    return errors


def _norm(text: str) -> str:
    return " ".join(text.casefold().split())


def validate_corpus(items: Sequence[dict],
                    reference_texts: Iterable[str] = ()) -> dict:
    """Validate a Part H corpus: per-item checks, duplicates, freshness
    against ``reference_texts`` (the L2 corpus texts), and §6.1 composition.
    Returns a report; ``valid`` is True only when there are no errors."""
    errors: List[str] = []
    ids = Counter()
    pairs = Counter()
    ok_items = []
    for idx, item in enumerate(items, 1):
        errs = validate_item(item)
        errors.extend(f"item {idx}: {e}" for e in errs)
        if isinstance(item, dict):
            ids[item.get("item_id")] += 1
            if isinstance(item.get("text_a"), str) and isinstance(item.get("text_b"), str):
                pairs[(_norm(item["text_a"]), _norm(item["text_b"]))] += 1
        if not errs:
            ok_items.append(item)
    errors.extend(f"duplicate item_id {i!r}" for i, n in ids.items() if n > 1)
    dup_pairs = sum(n - 1 for n in pairs.values() if n > 1)
    if dup_pairs:
        errors.append(f"{dup_pairs} duplicate text pair(s)")
    ref = {_norm(t) for t in reference_texts if t}
    reused = [it["item_id"] for it in ok_items
              if _norm(it["text_a"]) in ref or (it["text_b"] and _norm(it["text_b"]) in ref)]
    if reused:
        errors.append(f"{len(reused)} item(s) reuse L2 corpus text: {reused[:5]}")

    n = len(items)
    counts = Counter(c for it in ok_items for c in it["categories"])
    long_n = sum(1 for it in ok_items if it["declared_long_text"])
    multi_n = sum(1 for it in ok_items if it["declared_multilingual"])
    abn_n = sum(1 for it in ok_items if it["anchor_bearing_negative"])
    if not SIZE_MIN <= n <= SIZE_MAX:
        errors.append(f"corpus size {n} outside {SIZE_MIN}-{SIZE_MAX} "
                      f"(target {TARGET_SIZE})")
    for cat, minimum in CATEGORY_MINIMA.items():
        if counts.get(cat, 0) < minimum:
            errors.append(f"category {cat!r}: {counts.get(cat, 0)} < {minimum}")
    if long_n < LONG_TEXT_MIN_ITEMS:
        errors.append(f"long-text items {long_n} < {LONG_TEXT_MIN_ITEMS}")
    if multi_n < MULTILINGUAL_MIN_ITEMS:
        errors.append(f"multilingual items {multi_n} < {MULTILINGUAL_MIN_ITEMS}")
    if n == 0 or abn_n / n < ANCHOR_BEARING_NEGATIVE_MIN_SHARE:
        errors.append(f"anchor-bearing negatives {abn_n}/{n} below "
                      f"{ANCHOR_BEARING_NEGATIVE_MIN_SHARE:.0%}")
    digests = sorted((it["item_id"], item_digest(it)) for it in ok_items)
    return {
        "valid": not errors,
        "errors": errors,
        "size": n,
        "category_counts": dict(sorted(counts.items())),
        "long_text_items": long_n,
        "multilingual_items": multi_n,
        "anchor_bearing_negatives": abn_n,
        "item_digests": dict(digests),
        "corpus_digest": hash_sha256(stable_json(digests)) if not errors else None,
        "frozen_parameters": v01.FROZEN_PARAMETERS,
    }


def load_jsonl(path: str) -> List[dict]:
    out = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if line.strip():
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise ValueError(f"{path}:{lineno}: invalid JSON: {e}") from e
    return out


def l2_reference_texts(path: str = shadow.DEFAULT_FIXTURES_PATH) -> List[str]:
    """L2 corpus texts, used only for the freshness (no-reuse) check."""
    return [t for r in load_jsonl(path) for t in (r["text_a"], r["text_b"]) if t]


def annotation_view(items: Sequence[dict], corpus_digest: str) -> List[dict]:
    """What annotators see: text and context only. Author identity, author
    intent, categories, anchor designations and all derived features are
    removed; order is by item digest, not by authoring order or category."""
    view = []
    for it in items:
        view.append({
            "item_id": it["item_id"],
            "item_digest": item_digest(it),
            "corpus_digest": corpus_digest,
            "relation_type": it["relation_type"],
            "text_a": it["text_a"],
            "text_b": it["text_b"],
            "dt_seconds": it["dt_seconds"],
            "langs_a": it["langs_a"],
            "langs_b": it["langs_b"],
        })
    return sorted(view, key=lambda v: v["item_digest"])


# --- annotations and gold ---------------------------------------------------------

def validate_annotation(rec: Any) -> List[str]:
    if not isinstance(rec, dict):
        return ["record is not an object"]
    errors = [f"missing required field {f!r}" for f in ANNOTATION_REQUIRED if f not in rec]
    if errors:
        return errors
    if set(rec) - set(ANNOTATION_REQUIRED):
        errors.append(f"unknown fields {sorted(set(rec) - set(ANNOTATION_REQUIRED))}")
    if rec["record_kind"] != ANNOTATION_RECORD_KIND:
        errors.append("record_kind mismatch")
    if not isinstance(rec["annotator_ref"], str) or not ANNOTATOR_REF_RE.match(rec["annotator_ref"]):
        errors.append("annotator_ref must match ^annotator-[0-9a-f]{8}$")
    if rec["role"] not in ROLES:
        errors.append("role must be A|B|adjudicator")
    if not isinstance(rec["unresolved"], bool):
        errors.append("unresolved must be a boolean")
    elif rec["unresolved"]:
        if rec["role"] != "adjudicator":
            errors.append("only the adjudicator may mark an item unresolved")
        if rec["explicit_reference"] is not None or rec["relation"] is not None:
            errors.append("an unresolved adjudication carries no labels")
    else:
        if rec["explicit_reference"] not in EXPLICIT_LABELS:
            errors.append("explicit_reference must be yes|no|unclear")
        if rec["relation"] not in RELATION_LABELS:
            errors.append("relation must be one of the five §5.4 values")
    if not isinstance(rec["codebook_version"], str) or not rec["codebook_version"]:
        errors.append("codebook_version must be a non-empty string")
    if not isinstance(rec["annotated_at"], str) or not rec["annotated_at"]:
        errors.append("annotated_at must be a timestamp string")
    s = rec["seconds_spent"]
    if not isinstance(s, (int, float)) or isinstance(s, bool) or s < 0:
        errors.append("seconds_spent must be a non-negative number")
    ba = rec["blind_attestation"]
    if not isinstance(ba, dict) or set(ba) != set(BLINDNESS_KEYS):
        errors.append(f"blind_attestation must have exactly {list(BLINDNESS_KEYS)}")
    elif any(ba[k] is not False for k in BLINDNESS_KEYS):
        errors.append("blind_attestation: every key must be false (blindness broken)")
    return errors


def cohen_kappa(pairs: Sequence[Tuple[str, str]]) -> Optional[float]:
    n = len(pairs)
    if n == 0:
        return None
    po = sum(a == b for a, b in pairs) / n
    ca = Counter(a for a, _ in pairs)
    cb = Counter(b for _, b in pairs)
    pe = sum(ca[k] * cb[k] for k in set(ca) | set(cb)) / (n * n)
    if pe >= 1.0:
        return None  # undefined: no variation; the gate cannot pass on it
    return (po - pe) / (1 - pe)


def assemble_gold(corpus_report: dict, annotations: Sequence[dict]) -> dict:
    """Two annotators + adjudicator (mandatory) -> gold per item, plus the
    pre-adjudication agreement gates. Never uses Jev output."""
    errors: List[str] = []
    for i, rec in enumerate(annotations, 1):
        errors.extend(f"annotation {i}: {e}" for e in validate_annotation(rec))
    if not corpus_report.get("valid"):
        errors.append("corpus report is not valid")
    if errors:
        return {"valid": False, "errors": errors, "grade": None}
    digests = corpus_report["item_digests"]
    by_item: Dict[str, Dict[str, dict]] = {}
    for rec in annotations:
        if rec["corpus_digest"] != corpus_report["corpus_digest"]:
            errors.append(f"{rec['item_id']}: corpus_digest mismatch")
        if digests.get(rec["item_id"]) != rec["item_digest"]:
            errors.append(f"{rec['item_id']}: unknown item or item_digest mismatch")
        slot = by_item.setdefault(rec["item_id"], {})
        if rec["role"] in slot:
            errors.append(f"{rec['item_id']}: duplicate {rec['role']} record")
        slot[rec["role"]] = rec

    gold: Dict[str, dict] = {}
    x_pairs, r_pairs = [], []
    exploratory = []
    for item_id in digests:
        slot = by_item.get(item_id, {})
        a, b, adj = slot.get("A"), slot.get("B"), slot.get("adjudicator")
        if a is None or b is None:
            exploratory.append(item_id)
            continue
        refs = [r["annotator_ref"] for r in (a, b, adj) if r is not None]
        if len(set(refs)) != len(refs):
            errors.append(f"{item_id}: A, B and adjudicator must be different people")
        x_pairs.append((a["explicit_reference"], b["explicit_reference"]))
        r_pairs.append((v01.collapse(a["relation"]), v01.collapse(b["relation"])))
        agree_x = a["explicit_reference"] == b["explicit_reference"]
        agree_r = a["relation"] == b["relation"]
        if agree_x and agree_r:
            gold[item_id] = {"explicit_reference": a["explicit_reference"],
                             "relation": a["relation"], "source": "agreement"}
            continue
        if adj is None:
            errors.append(f"{item_id}: A/B disagree and no adjudicator record")
            continue
        if adj["unresolved"]:
            gold[item_id] = {"explicit_reference": "unclear",
                             "relation": "underdetermined",
                             "source": "unresolved"}
            continue
        gold[item_id] = {
            "explicit_reference": a["explicit_reference"] if agree_x
            else adj["explicit_reference"],
            "relation": a["relation"] if agree_r else adj["relation"],
            "source": "adjudicated",
        }
    if exploratory:
        errors.append(f"{len(exploratory)} item(s) lack two independent annotations; "
                      "the study is exploratory only, not qualification evidence")
    x_kappa = cohen_kappa(x_pairs)
    r_kappa = cohen_kappa(r_pairs)
    if r_kappa is None or r_kappa < R_KAPPA_HALT:
        inferred = "halted_for_v0x"
    elif r_kappa < R_KAPPA_UPPER:
        inferred = "stays_underdetermined_by_rule"
    else:
        inferred = "no_inferred_automation_in_v0.1"
    return {
        "valid": not errors,
        "errors": errors,
        "grade": "qualification" if not exploratory and not errors else "exploratory",
        "gold": gold,
        "agreement": {
            "x_kappa": x_kappa, "x_gate_passed": x_kappa is not None and x_kappa >= X_KAPPA_MIN,
            "r_collapsed_kappa": r_kappa, "inferred_relations": inferred,
            "n_pairs": len(x_pairs),
            "note": "Cohen's kappa over role A vs role B, pre-adjudication",
        },
        "corpus_digest": corpus_report["corpus_digest"],
    }


# --- evaluation -------------------------------------------------------------------

def wilson(k: int, n: int, z: float = WILSON_Z) -> Tuple[Optional[float], Optional[float]]:
    if n == 0:
        return None, None
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return max(0.0, centre - half), min(1.0, centre + half)


def _rate(k: int, n: int) -> dict:
    lo, hi = wilson(k, n)
    return {"k": k, "n": n, "point": (k / n) if n else None,
            "wilson_lower": lo, "wilson_upper": hi}


def adjudications_from_log(log: dict) -> Dict[str, jev.Adjudication]:
    """Rebuild one Adjudication per item from an eval call log, using the
    persisted §2.5 records only (medians and argmax recomputed here)."""
    records = {c["record"]["call_id"]: c["record"] for c in log["calls"]}
    out = {}
    options = shadow.RELATION_OPTIONS
    for pair in log["pair_results"]:
        recs = [records[cid] for cid in pair["call_ids"]]
        meds = jev.replicate_medians(recs, options)
        ok = any(r["status"] == jev.STATUS_OK for r in recs)
        out[pair["fixture_id"]] = jev.Adjudication(
            status=jev.STATUS_OK if ok else recs[-1]["status"],
            question_id=f"holdout:{pair['fixture_id']}",
            replicate_set_id=recs[0]["replicate_set_id"],
            records=recs, median_probabilities=meds,
            argmax=jev.recompute_argmax(meds, options) if meds else None,
            ambiguity_set=jev.ambiguity_set(meds) if meds else [],
        )
    return out


def _false_positive(g: dict) -> bool:
    return g["explicit_reference"] != "yes" or g["relation"] in ("unrelated", "independent_lexical")


def _qualifier_metrics(rows: List[dict], qualifies) -> dict:
    q = [r for r in rows if qualifies(r)]
    xyes = [r for r in rows if r["gold"]["explicit_reference"] == "yes"]
    return {
        "m1_false_positive_qualification": _rate(sum(_false_positive(r["gold"]) for r in q), len(q)),
        "m2_explicit_precision": _rate(sum(r["gold"]["explicit_reference"] == "yes" for r in q), len(q)),
        "m3_explicit_recall": _rate(sum(1 for r in xyes if qualifies(r)), len(xyes)),
    }


def _m1_ok(m: dict) -> bool:
    r = m["m1_false_positive_qualification"]
    return (r["n"] >= M1_MIN_QUALIFIED and r["point"] is not None
            and r["point"] <= M1_POINT_MAX and r["wilson_upper"] <= M1_WILSON_UPPER_MAX)


def _m2_ok(m: dict) -> bool:
    r = m["m2_explicit_precision"]
    return (r["point"] is not None and r["point"] >= M2_POINT_MIN
            and r["wilson_lower"] >= M2_WILSON_LOWER_MIN)


def _length_bucket(item: dict) -> str:
    m = max(len(item["text_a"]), len(item["text_b"]))
    return "short_lt150" if m < 150 else "medium_150_299" if m < 300 else "long_300_500"


def evaluate_part(part: str, items: Sequence[dict], corpus_report: dict,
                  gold_report: dict, log: dict) -> dict:
    """Pre-registered §6.4/§6.5 evaluation for one part. ``log`` is a
    ``semantic_shadow.read_eval_call_log`` result whose pair ids are the
    item ids. Returns metrics, gates and the part disposition."""
    blockers: List[str] = []
    header = log.get("header") or {}
    if not corpus_report.get("valid"):
        blockers.append("corpus_invalid")
    if gold_report.get("grade") != "qualification" or not gold_report.get("valid"):
        blockers.append("annotation_not_qualification_grade")
    if gold_report.get("corpus_digest") != corpus_report.get("corpus_digest"):
        blockers.append("gold_corpus_digest_mismatch")
    if not log.get("complete"):
        blockers.append("evaluation_log_incomplete")
    if header.get("fixture_count") != len(items):
        blockers.append("evaluation_log_item_count_mismatch")
    # The log must bind to this exact corpus and this exact frozen rule.
    if header.get("holdout_corpus_digest") != corpus_report.get("corpus_digest"):
        blockers.append("call_log_corpus_digest_mismatch")
    if header.get("rule_digest") != v01.RULE_DIGEST:
        blockers.append("call_log_rule_digest_mismatch")
    missing_gold = [it["item_id"] for it in items
                    if it["item_id"] not in (gold_report.get("gold") or {})]
    if missing_gold:
        blockers.append(f"gold_missing_items:{len(missing_gold)}")
    slice_fields = ("relation_type", "text_a", "text_b", "langs_a", "langs_b")
    if any(f not in it for it in items for f in slice_fields):
        blockers.append("slice_metadata_missing")
    result: Dict[str, Any] = {
        "part": part, "rule": v01.FROZEN_PARAMETERS, "gates": FROZEN_GATES,
        "corpus_digest": corpus_report.get("corpus_digest"),
        "agreement": gold_report.get("agreement"),
    }
    if blockers:
        result.update(disposition="BLOCKED", blockers=blockers)
        return result
    agreement = gold_report["agreement"]
    if not agreement["x_gate_passed"]:
        result.update(disposition="HALTED_X_AGREEMENT",
                      blockers=["x_kappa_below_0.60: rule not evaluated"])
        return result

    adjs = adjudications_from_log(log)
    models = sorted({c["record"]["model_resolved"] for c in log["calls"]
                     if c["network_sent"] and c["record"]["model_resolved"]})
    by_id = {it["item_id"]: it for it in items}
    missing = sorted(set(by_id) - set(adjs))
    if missing or len(models) != 1:
        result.update(disposition="BLOCKED",
                      blockers=(["items_missing_from_log"] if missing else [])
                      + (["model_version_not_single"] if len(models) != 1 else []))
        return result

    rows = []
    for item_id, item in by_id.items():
        packet = item_packet(item)
        adj = adjs[item_id]
        rec = v01.evaluate_packet(packet, adj)
        channels = shadow.compute_channels(packet, adj)
        first_argmax = rec["semantic"]["first_argmax"]
        draws = [r["argmax_recomputed"] for r in adj.records
                 if r["status"] == jev.STATUS_OK]
        rows.append({
            "item_id": item_id, "item": item, "gold": gold_report["gold"][item_id],
            "v01": rec, "old_rule_qualified": shadow.shadow_qualified(channels),
            "first_argmax": first_argmax, "draw_argmax": draws,
        })
    unavailable = [r["item_id"] for r in rows
                   if r["v01"]["disposition"] == v01.SEMANTIC_UNAVAILABLE]
    if unavailable:
        result.update(disposition="BLOCKED",
                      blockers=[f"semantic_unavailable_items:{len(unavailable)}"])
        return result

    qualified = lambda r: r["v01"]["disposition"] == v01.QUALIFIED_EXPLICIT
    main = _qualifier_metrics(rows, qualified)
    amb = [r for r in rows if r["gold"]["relation"] == "underdetermined"
           or r["gold"]["explicit_reference"] == "unclear"]
    m4 = _rate(sum(r["v01"]["disposition"] in (v01.UNDERDETERMINED, v01.NOT_QUALIFIED)
                   for r in amb), len(amb))
    m5_items = [r["item_id"] for r in rows if r["gold"]["relation"] == "dependent_indirect"
                and qualified(r) and r["gold"]["explicit_reference"] != "yes"]
    indirect_n = sum(r["gold"]["relation"] == "dependent_indirect" for r in rows)

    anchor = lambda r: r["v01"]["anchor_present"]
    tok = lambda part_: (lambda r: r["v01"]["validation_only"]["shared_distinctive_token"][part_])
    jx = lambda r: r["v01"]["j_explicit"]
    baselines = {
        "anchor_only": _qualifier_metrics(rows, anchor),
        "token_numeric_only": _qualifier_metrics(rows, tok("numeric")),
        "token_long_word_only": _qualifier_metrics(rows, tok("long_word")),
        "anchor_or_token_numeric": _qualifier_metrics(rows, lambda r: anchor(r) or tok("numeric")(r)),
        "anchor_or_token_long_word": _qualifier_metrics(rows, lambda r: anchor(r) or tok("long_word")(r)),
        "v01_plus_token_numeric": _qualifier_metrics(
            rows, lambda r: jx(r) and (anchor(r) or tok("numeric")(r))),
        "v01_plus_token_long_word": _qualifier_metrics(
            rows, lambda r: jx(r) and (anchor(r) or tok("long_word")(r))),
        "note": "distinctive-token rows are V0.2 evidence only; they change nothing here",
    }
    ao = baselines["anchor_only"]
    v_recall = main["m3_explicit_recall"]["point"] or 0.0
    a_recall = ao["m3_explicit_recall"]["point"] or 0.0
    jev_adds_no_value = _m1_ok(ao) and _m2_ok(ao) and a_recall >= v_recall - M6_RECALL_MARGIN

    m7 = _qualifier_metrics(rows, lambda r: r["old_rule_qualified"])["m1_false_positive_qualification"]
    m8 = _rate(sum(r["v01"]["review_route"] != v01.ROUTE_NONE for r in amb), len(amb))
    first_err = [r for r in rows if r["first_argmax"] != r["gold"]["relation"]]
    m9 = {
        "first_call_errors": len(first_err),
        "with_replicates": sum(len(r["draw_argmax"]) > 1 for r in first_err),
        "draws_disagreed": sum(len(set(r["draw_argmax"])) > 1 for r in first_err),
        "note": "reported only; not used for any decision",
    }

    slice_keys = {
        "relation_type": lambda r: r["item"]["relation_type"],
        "text_length": lambda r: _length_bucket(r["item"]),
        "language": lambda r: "non_en_present" if derived_multilingual(r["item"]) else "en_only",
        "anchor_type": lambda r: r["v01"]["anchors"]["anchor_type"],
    }
    slices, blocking = {}, []
    for dim, key in slice_keys.items():
        groups: Dict[str, List[dict]] = {}
        for r in rows:
            groups.setdefault(key(r), []).append(r)
        slices[dim] = {}
        for val, grp in sorted(groups.items()):
            m1 = _qualifier_metrics(grp, qualified)["m1_false_positive_qualification"]
            blocks = m1["n"] >= M10_MIN_SLICE_QUALIFIED and m1["point"] > M10_M1_MAX
            slices[dim][val] = {"n_items": len(grp), "m1": m1, "blocks": blocks}
            if blocks:
                blocking.append(f"{dim}={val}")

    criteria = {
        "x_agreement_gate": True,
        "m1": _m1_ok(main),
        "m2": _m2_ok(main),
        "m4": m4["n"] > 0 and m4["point"] >= M4_MIN,
        "m5": len(m5_items) <= M5_MAX_ITEMS,
        "m10_no_blocking_slice": not blocking,
    }
    result.update(
        disposition="PASSED" if all(criteria.values()) else "FAILED",
        criteria=criteria,
        failed=[k for k, v in criteria.items() if not v],
        model_resolved=models[0],
        n_items=len(rows),
        disposition_counts=dict(Counter(r["v01"]["disposition"] for r in rows)),
        review_route_counts=dict(Counter(r["v01"]["review_route"] for r in rows)),
        metrics={
            **main,
            "m3_useful": (main["m3_explicit_recall"]["point"] or 0.0) >= M3_USEFUL_MIN,
            "m4_ambiguity_preservation": m4,
            "m5_indirect_qualified_without_explicit": {
                "items": m5_items, "count": len(m5_items), "indirect_n": indirect_n},
            "m6_baselines": baselines,
            "m6_jev_adds_no_qualification_value": jev_adds_no_value,
            "m7_old_rule_false_positive_qualification": m7,
            "m8_routing_yield": m8,
            "m9_repeat_informativeness": m9,
            "m10_slices": slices,
            "m10_blocking_slices": blocking,
        },
        per_item=[{"item_id": r["item_id"], "gold": r["gold"],
                   "disposition": r["v01"]["disposition"],
                   "review_routes": r["v01"]["review_routes"],
                   "anchor_type": r["v01"]["anchors"]["anchor_type"],
                   "j_explicit": r["v01"]["j_explicit"],
                   "old_rule_qualified": r["old_rule_qualified"]} for r in rows],
    )
    return result


def overall_decision(part_h: Optional[dict], part_r: Optional[dict]) -> dict:
    """§6.5: GO for a bounded qualification shadow needs BOTH parts passed."""
    h = (part_h or {}).get("disposition")
    r = (part_r or {}).get("disposition")
    if h == "PASSED" and r == "PASSED":
        decision = "GO_BOUNDED_QUALIFICATION_SHADOW"
    elif h == "PASSED" and part_r is None:
        decision = "NO_GO_PENDING_PART_R"
    else:
        decision = "NO_GO"
    return {"decision": decision, "part_h": h, "part_r": r,
            "note": "no re-tuning on the same data; a revised rule needs a "
                    "new amendment and a new holdout"}


# --- live evaluation runner (Part H / Part R item corpora) -------------------------

HOLDOUT_RUN_RECEIPT_KIND = "labelwatch.semantic_shadow_holdout_eval_run.v1"

#: The ONLY fields that may reach the Jev request path. The projection is
#: built field by field from a validated item; nothing else is copied, so
#: author identity, author intent, categories, the anchor-bearing-negative
#: designation, annotations, gold and evaluation metrics cannot leak even if
#: an item (or a future schema) carries them.
PROJECTION_FIELDS = ("item_id", "relation_type", "text_a", "text_b",
                     "dt_seconds", "langs_a", "langs_b")
#: Of those, the fields that reach the packet. ``item_id`` stays local: it
#: associates the receipt, and is never sent as semantic state.
PACKET_FIELDS = tuple(f for f in PROJECTION_FIELDS if f != "item_id")


def evaluation_projection(item: dict) -> dict:
    """Narrow, allow-listed projection of one holdout item."""
    return {
        "item_id": item["item_id"],
        "relation_type": item["relation_type"],
        "text_a": item["text_a"],
        "text_b": item["text_b"],
        "dt_seconds": item["dt_seconds"],
        "langs_a": list(item["langs_a"]),
        "langs_b": list(item["langs_b"]),
    }


def projection_to_candidate(proj: dict) -> dict:
    """Candidate for packetization. ``item_id`` is deliberately absent."""
    return {
        "relation_type": proj["relation_type"],
        "a": {"text": proj["text_a"], "langs": proj["langs_a"]},
        "b": {"text": proj["text_b"], "langs": proj["langs_b"]},
        "dt_seconds": proj["dt_seconds"],
        "thread_depth_b": None,
    }


def projection_packet(item: dict) -> dict:
    return shadow.build_packet(projection_to_candidate(evaluation_projection(item)))


def preflight(items: Sequence[dict], *, max_calls: int,
              codebook_version: Optional[str] = None) -> dict:
    """Bounds and cost estimate. Pure: no network, no key, no files."""
    worst_calls = min(len(items) * jev.REPEAT_K, max_calls) if max_calls else len(items) * jev.REPEAT_K
    est = jev.estimate_cost_usd(worst_calls)
    return {
        "item_count": len(items),
        "max_calls": max_calls,
        "worst_case_calls_under_repeat_policy": len(items) * jev.REPEAT_K,
        "calls_bounded_by_max_calls": worst_calls,
        "repeat_policy": {"k": jev.REPEAT_K, "top1_max": jev.REPEAT_TOP1_MAX,
                          "gap_max": jev.REPEAT_GAP_MAX},
        "model_requested": jev.MODEL_REQUESTED,
        "relation_options": list(shadow.RELATION_OPTIONS),
        "estimate": {**est, "basis": "estimate only; the planning figure is not "
                                     "a cap and not a billing figure"},
        "codebook_version": codebook_version,
        "rule_digest": v01.RULE_DIGEST,
        "rule_version": v01.RULE_VERSION,
    }


def run_holdout_eval(items: Sequence[dict], *, corpus_report: dict, records_dir: str,
                     api_key: Optional[str], flag_on: bool, max_calls: int,
                     codebook_version: Optional[str] = None, axolotl_mod=None,
                     day: Optional[str] = None) -> dict:
    """Run Jev over a frozen item corpus and write the durable call log.

    Takes items only: no gold, no annotations, no author metadata reaches
    this function's request path (see ``evaluation_projection``). Failures,
    the cap and a model-version change all stop the run and leave the log
    marked incomplete; a partial run is never complete evidence.
    """
    if not corpus_report.get("valid"):
        raise ValueError("refusing to run: the corpus report is not valid")
    day = day or jev.utc_day()
    pre = preflight(items, max_calls=max_calls, codebook_version=codebook_version)
    log = shadow.EvalCallLog(records_dir, kind=shadow.HOLDOUT_CALLS_RECEIPT_KIND, header={
        "generated_at": format_ts(now_utc()),
        "git_commit": get_git_commit(),
        "campaign": jev.CAMPAIGN,
        "component": jev.COMPONENT,
        "holdout_corpus_digest": corpus_report["corpus_digest"],
        "rule_digest": v01.RULE_DIGEST,
        "rule_version": v01.RULE_VERSION,
        "amendment": v01.AMENDMENT,
        "frozen_parameters": v01.FROZEN_PARAMETERS,
        "item_count": len(items),
        "fixture_count": len(items),  # read_eval_call_log/evaluate_part compatibility
        "max_calls": max_calls,
        "day": day,
        "model_requested": jev.MODEL_REQUESTED,
        "relation_options": list(shadow.RELATION_OPTIONS),
        "repeat_policy": pre["repeat_policy"],
        "preflight": pre,
        "codebook_version": codebook_version,
        "axolotl": "available" if axolotl_mod is not None else "unavailable",
        "projection_fields": list(PROJECTION_FIELDS),
        "packet_fields": list(PACKET_FIELDS),
    })
    calls_spent, model_anchor, model_stopped = 0, None, False
    evaluated, stop_reason = 0, "completed"
    try:
        for item in items:
            if model_stopped:
                stop_reason = "model_version_stop"
                break
            if max_calls and calls_spent >= max_calls:
                stop_reason = "max_calls_reached"
                break
            item_id = item["item_id"]
            packet = projection_packet(item)
            hits = shadow.packet_privacy_scan(packet)
            if hits:
                stop_reason = "privacy_tripwire"
                break
            first: Dict[str, Any] = {}

            def sink(rec, _iid=item_id, _first=first):
                if rec["replicate_index"] == 1 and rec["probabilities"]:
                    _first["p"] = rec["probabilities"]
                log.call(rec, fixture_id=_iid, edge_id=_iid,
                         axolotl_available=axolotl_mod is not None,
                         first_probabilities=_first.get("p"))

            adj, mem, gate = shadow.adjudicate_packet(
                packet, edge_id=hash_sha256(f"holdout|{item_id}"), day=day,
                api_key=api_key, flag_on=flag_on, max_calls=max_calls,
                seed_state=(calls_spent, model_anchor, model_stopped),
                axolotl_mod=axolotl_mod, record_sink=sink)
            calls_spent = mem.calls_today(day)
            model_anchor = mem.model_anchor(day) or model_anchor
            model_stopped = model_stopped or mem.is_model_stopped(day)
            outcome = v01.evaluate_packet(packet, adj)
            log.pair_result({
                "fixture_id": item_id,          # reader compatibility
                "item_id": item_id,
                "relation_type": item["relation_type"],
                "call_ids": [r["call_id"] for r in adj.records],
                "calls_sent": sum(1 for r in adj.records if jev.network_attempted(r)),
                "status": adj.status,
                "median_probabilities": adj.median_probabilities,
                "argmax": adj.argmax,
                "ambiguity_set": adj.ambiguity_set,
                "gate_state": gate["gate_state"],
                "packet_sha256": hash_sha256(stable_json(packet)),
                "text_a_sha256": hash_sha256(packet["text_a"]),
                "text_b_sha256": hash_sha256(packet["text_b"]),
                "item_digest": corpus_report["item_digests"][item_id],
                "v01": outcome,
            })
            evaluated += 1
            if adj.status != jev.STATUS_OK and not model_stopped:
                stop_reason = f"semantic_unavailable:{adj.status}"
                break
    except BaseException as e:
        log.closeout({"run_finished": False,
                      "stop_reason": f"exception:{type(e).__name__}",
                      "items_total": len(items), "items_evaluated": evaluated,
                      "pairs_evaluated": evaluated, "evaluation_complete": False,
                      "model_anchor": model_anchor})
        log.close()
        raise
    complete = evaluated == len(items) and stop_reason == "completed"
    log.closeout({
        "run_finished": True, "stop_reason": stop_reason,
        "items_total": len(items), "items_evaluated": evaluated,
        "pairs_evaluated": evaluated, "evaluation_complete": complete,
        "model_anchor": model_anchor, "model_version_stopped": model_stopped,
        "holdout_corpus_digest": corpus_report["corpus_digest"],
        "rule_digest": v01.RULE_DIGEST,
    })
    log.close()
    return {
        "receipt_kind": HOLDOUT_RUN_RECEIPT_KIND,
        "call_records_path": log.path,
        "evaluation_complete": complete,
        "stop_reason": stop_reason,
        "items_total": len(items),
        "items_evaluated": evaluated,
        "calls_sent": log.sent,
        "calls_recorded": log.call_ordinal,
        "input_tokens": log.input_tokens,
        "output_tokens": log.output_tokens,
        "est_cost_usd": round(log.est_cost_usd, 8),
        "model_anchor": log.model_anchor,
        "corpus_digest": corpus_report["corpus_digest"],
        "rule_digest": v01.RULE_DIGEST,
        "preflight": pre,
    }
