"""Part H substrate (Amendment 2026-09-17-C §6): schema, composition, blind
annotation view, gold assembly, agreement gates, and frozen evaluation.

All items here are SYNTHETIC PLUMBING generated in memory ("synthetic item N
..."); they are not holdout content and are never written as a holdout file.
Jev records are fabricated §2.5 shapes. No network.
"""
import hashlib
import json
from pathlib import Path

import pytest

from labelwatch import cli
from labelwatch import semantic_shadow as shadow
from labelwatch import semantic_shadow_holdout as ho
from labelwatch import semantic_shadow_jev as jev
from labelwatch import semantic_shadow_v01 as v01

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "specs" / "semantic_shadow" / "part_h"
OPTS = shadow.RELATION_OPTIONS
BLIND = {k: False for k in ho.BLINDNESS_KEYS}

# Synthetic words: a short numeric-free vocabulary per item index.
WORDS = ["amber", "birch", "cobalt", "dune", "ember", "fjord", "garnet", "heron",
         "iris", "juniper", "kelp", "lagoon", "moss", "nectar", "onyx", "prairie"]
# Disjoint vocabulary for B so that only the explicit hashtag creates an anchor.
WORDS_B = ["quokka", "zephyr", "wombat", "xylem", "vortex", "tundra", "sphinx",
           "pumice", "yucca", "gecko", "cumulus", "bayou", "lynx", "mauve", "plover", "wharf"]


def _hex(n, width):
    return hashlib.sha256(str(n).encode()).hexdigest()[:width]


def _words(i, k=6, vocab=WORDS):
    return " ".join(vocab[(i * 7 + j * 3) % len(vocab)] for j in range(k))


def make_item(i, categories, *, relation="reply", anchored=False,
              anchor_negative=False, long=False, lang_b="en", empty_b=False):
    tag = f" #synth{_hex(i, 6)}" if anchored or anchor_negative else ""
    text_a = f"synthetic item {_hex(i, 10)} {_words(i)}{tag}"
    text_b = "" if empty_b else f"plumbing {_hex(i + 1000, 10)} {_words(i, vocab=WORDS_B)}{tag}"
    if long:
        text_a = (text_a + " " + " ".join(_words(i + k) for k in range(12)))[:320]
        text_b = (text_b + " " + " ".join(_words(i + k, vocab=WORDS_B)
                                          for k in range(12)))[:330]
    return {
        "record_kind": ho.ITEM_RECORD_KIND,
        "item_id": f"h-{_hex(i, 12)}",
        "authoring_protocol_version": ho.AUTHORING_PROTOCOL_VERSION,
        "author_ref": f"author-{_hex(i % 5, 8)}",
        "relation_type": relation,
        "text_a": text_a,
        "text_b": text_b,
        "dt_seconds": 30 + i,
        "langs_a": ["en"],
        "langs_b": [lang_b],
        "categories": categories,
        "anchor_bearing_negative": anchor_negative,
        "declared_long_text": long,
        "declared_multilingual": lang_b != "en",
    }


def synthetic_corpus():
    """150 synthetic items meeting every §6.1 composition minimum.

    Plan (by index): 0-59 anchored "positives" (explicit_direct_reference /
    explicit_reference_with_stance), 60-89 anchor-bearing negatives
    (unrelated_similarity / common_source_convergence), 90-139 others,
    140-149 quote_no_comment. Long texts on 0-14, multilingual on 90-99.
    """
    items = []
    for i in range(150):
        if i < 60:
            cats = ["explicit_direct_reference"] if i < 30 else ["explicit_reference_with_stance"]
            items.append(make_item(i, cats, anchored=True, long=i < 15))
        elif i < 90:
            cats = ["unrelated_similarity"] if i < 75 else ["common_source_convergence"]
            items.append(make_item(i, cats, anchor_negative=True))
        elif i < 140:
            four = ("subtweet", "irony", "vaguebooking",
                    "structurally_linked_semantically_unrelated")
            cats = sorted({four[(i - 90) % 4], four[(i - 89) % 4]})
            items.append(make_item(i, cats, lang_b="es" if i < 100 else "en"))
        else:
            items.append(make_item(i, ["quote_no_comment"], relation="quote", empty_b=True))
    return items


@pytest.fixture()
def corpus():
    items = synthetic_corpus()
    report = ho.validate_corpus(items)
    assert report["valid"], report["errors"]
    return items, report


# --- schema -------------------------------------------------------------------

def test_synthetic_corpus_meets_composition(corpus):
    items, report = corpus
    for i, it in enumerate(items):
        anchored = v01.anchors_from_packet(ho.item_packet(it)).present
        assert anchored == (i < 90), (i, it["item_id"])
    assert report["size"] == 150
    assert report["long_text_items"] >= 15 and report["multilingual_items"] >= 10
    assert report["anchor_bearing_negatives"] / 150 >= 0.20
    assert all(report["category_counts"][c] >= m for c, m in ho.CATEGORY_MINIMA.items())
    assert len(report["corpus_digest"]) == 64
    assert set(report["item_digests"]) == {it["item_id"] for it in items}
    assert report["frozen_parameters"] == v01.FROZEN_PARAMETERS


@pytest.mark.parametrize("field", ["gold_relation", "explicit_reference", "relation",
                                   "acceptable_answers", "fixture_class", "jev_argmax"])
def test_gold_jev_and_class_fields_are_rejected(field):
    item = make_item(1, ["irony"])
    item[field] = "dependent_direct"
    assert any("unknown fields" in e for e in ho.validate_item(item))


@pytest.mark.parametrize("mutate,needle", [
    (lambda it: it.update(item_id="irony-001"), "item_id"),
    (lambda it: it.update(author_ref="alice"), "author_ref"),
    (lambda it: it.pop("langs_b"), "missing required field"),
    (lambda it: it.update(langs_a=[]), "langs_a"),
    (lambda it: it.update(categories=["sarcasm"]), "categories"),
    (lambda it: it.update(categories=[]), "categories"),
    (lambda it: it.update(dt_seconds=-1), "dt_seconds"),
    (lambda it: it.update(text_a=""), "text_a"),
    (lambda it: it.update(text_b="x" * 501), "exceeds"),
    (lambda it: it.update(text_b=""), "text_b may be empty"),
    (lambda it: it.update(declared_long_text=True), "declared_long_text"),
    (lambda it: it.update(declared_multilingual=True), "declared_multilingual"),
    (lambda it: it.update(anchor_bearing_negative=True), "no admitted V0.1 anchor"),
    (lambda it: it.update(text_b="hi @someone.bsky.social"), "redact"),
    (lambda it: it.update(text_b="see did:plc:abc123"), "redact"),
    (lambda it: it.update(record_kind="other"), "record_kind"),
    (lambda it: it.update(author_intended={"relation": "vibes"}), "author_intended"),
])
def test_item_validation_errors(mutate, needle):
    item = make_item(3, ["subtweet"])
    mutate(item)
    errors = ho.validate_item(item)
    assert any(needle in e for e in errors), errors


def test_quote_no_comment_constraints():
    ok = make_item(4, ["quote_no_comment"], relation="quote", empty_b=True)
    assert ho.validate_item(ok) == []
    wrong_rel = dict(ok, relation_type="reply")
    assert any("quote_no_comment" in e for e in ho.validate_item(wrong_rel))
    with_text = dict(ok, text_b="some words")
    assert any("quote_no_comment" in e for e in ho.validate_item(with_text))
    repost_empty = make_item(5, ["subtweet"], relation="repost", empty_b=True)
    assert ho.validate_item(repost_empty) == []


def test_author_intent_is_optional_and_valid():
    item = make_item(6, ["irony"])
    item["author_intended"] = {"explicit_reference": "unclear", "relation": "underdetermined"}
    item["author_note"] = "coordinator only"
    assert ho.validate_item(item) == []


# --- composition ------------------------------------------------------------------

def test_duplicates_are_rejected(corpus):
    items, _ = corpus
    dup = items + [dict(items[0])]
    errors = ho.validate_corpus(dup)["errors"]
    assert any("duplicate item_id" in e for e in errors)
    assert any("duplicate text pair" in e for e in errors)


def test_l2_text_reuse_is_rejected(corpus):
    items, _ = corpus
    l2 = ho.l2_reference_texts(str(ROOT / "tests" / "fixtures" / "semantic_relations.jsonl"))
    items = [dict(it) for it in items]
    items[95]["text_a"] = "  " + l2[0].upper() + " "
    report = ho.validate_corpus(items, l2)
    assert not report["valid"]
    assert any("reuse L2 corpus text" in e for e in report["errors"])
    assert report["corpus_digest"] is None


@pytest.mark.parametrize("drop,needle", [
    (lambda its: its[:120], "corpus size"),
    (lambda its: its + [make_item(1000 + k, ["irony"]) for k in range(20)], "corpus size"),
    (lambda its: [it for it in its if "quote_no_comment" not in it["categories"]]
     + [make_item(2000 + k, ["irony"]) for k in range(10)], "'quote_no_comment'"),
    (lambda its: [dict(it, anchor_bearing_negative=False) if i < 85 else it
                  for i, it in enumerate(its)], "anchor-bearing negatives"),
])
def test_composition_shortfalls(corpus, drop, needle):
    items, _ = corpus
    errors = ho.validate_corpus(drop(items))["errors"]
    assert any(needle in e for e in errors), errors


def test_long_and_multilingual_minima(corpus):
    items, _ = corpus
    short = [make_item(3000 + i, it["categories"], anchored=i < 60,
                       anchor_negative=it["anchor_bearing_negative"])
             if it["declared_long_text"] else it for i, it in enumerate(items)]
    assert any("long-text items" in e for e in ho.validate_corpus(short)["errors"])
    mono = [dict(it, langs_b=["en"], declared_multilingual=False) for it in items]
    assert any("multilingual items" in e for e in ho.validate_corpus(mono)["errors"])


def test_composition_minima_are_the_frozen_values():
    assert ho.TARGET_SIZE == 150 and (ho.SIZE_MIN, ho.SIZE_MAX) == (135, 165)
    assert ho.CATEGORY_MINIMA == {
        "explicit_direct_reference": 15, "subtweet": 15, "irony": 15,
        "vaguebooking": 15, "unrelated_similarity": 15,
        "structurally_linked_semantically_unrelated": 15,
        "common_source_convergence": 15, "explicit_reference_with_stance": 15,
        "quote_no_comment": 10}
    assert ho.LONG_TEXT_MIN_ITEMS == 15 and ho.LONG_TEXT_CHARS == (300, 500)
    assert ho.MULTILINGUAL_MIN_ITEMS == 10
    assert ho.ANCHOR_BEARING_NEGATIVE_MIN_SHARE == 0.20


def test_no_l2_outputs_are_encoded():
    src = Path(ho.__file__).read_text() + Path(v01.__file__).read_text()
    protocol = (SPEC / "AUTHORING_PROTOCOL.md").read_text()
    for needle in ("8fd20b88", "lw-l2-eval", "analysis.json", "irony-00",
                   "vaguebooking-00", "unrelated_similarity-00"):
        assert needle not in src and needle not in protocol


# --- blind annotation view ---------------------------------------------------------

def test_annotation_view_strips_author_and_design_fields(corpus):
    items, _ = corpus
    items[0]["author_intended"] = {"relation": "dependent_direct"}
    report = ho.validate_corpus(items)
    view = ho.annotation_view(items, report["corpus_digest"])
    assert len(view) == len(items)
    for row in view:
        assert set(row) == {"item_id", "item_digest", "corpus_digest", "relation_type",
                            "text_a", "text_b", "dt_seconds", "langs_a", "langs_b"}
    assert [r["item_digest"] for r in view] == sorted(r["item_digest"] for r in view)
    assert [r["item_id"] for r in view] != [it["item_id"] for it in items]


# --- annotations and gold ------------------------------------------------------------

def ann(item_id, report, role, x, r, who, unresolved=False, **over):
    rec = {
        "record_kind": ho.ANNOTATION_RECORD_KIND, "item_id": item_id,
        "item_digest": report["item_digests"][item_id],
        "corpus_digest": report["corpus_digest"],
        "annotator_ref": f"annotator-{who}", "role": role,
        "explicit_reference": None if unresolved else x,
        "relation": None if unresolved else r, "unresolved": unresolved,
        "codebook_version": "codebook-test/v0", "annotated_at": "2026-09-18T00:00:00Z",
        "seconds_spent": 40, "blind_attestation": dict(BLIND),
    }
    rec.update(over)
    return rec


A_REF, B_REF, J_REF = "aaaaaaaa", "bbbbbbbb", "cccccccc"


def gold_plan(i):
    """Synthetic gold per index, matching synthetic_corpus()."""
    if i < 60:
        return "yes", "dependent_direct"
    if i < 90:
        return "no", "independent_lexical"
    if i < 140:
        return "unclear", "underdetermined"
    return "no", "unrelated"


def agreed_annotations(items, report, b_override=None):
    out = []
    for i, it in enumerate(items):
        x, r = gold_plan(i)
        out.append(ann(it["item_id"], report, "A", x, r, A_REF))
        bx, br = (b_override(i, x, r) if b_override else (x, r))
        out.append(ann(it["item_id"], report, "B", bx, br, B_REF))
        if (bx, br) != (x, r):
            out.append(ann(it["item_id"], report, "adjudicator", x, r, J_REF))
    return out


def test_gold_from_agreement(corpus):
    items, report = corpus
    g = ho.assemble_gold(report, agreed_annotations(items, report))
    assert g["valid"] and g["grade"] == "qualification"
    assert g["agreement"]["x_kappa"] == pytest.approx(1.0)
    assert g["agreement"]["x_gate_passed"]
    assert g["gold"][items[0]["item_id"]] == {
        "explicit_reference": "yes", "relation": "dependent_direct", "source": "agreement"}


def test_disagreement_requires_adjudicator_and_unresolved_becomes_underdetermined(corpus):
    items, report = corpus
    iid = items[0]["item_id"]
    anns = [a for a in agreed_annotations(items, report) if a["item_id"] != iid]
    anns += [ann(iid, report, "A", "yes", "dependent_direct", A_REF),
             ann(iid, report, "B", "no", "unrelated", B_REF)]
    missing = ho.assemble_gold(report, anns)
    assert not missing["valid"]
    assert any("no adjudicator" in e for e in missing["errors"])
    resolved = ho.assemble_gold(report, anns + [ann(iid, report, "adjudicator", None, None,
                                                    J_REF, unresolved=True)])
    assert resolved["valid"]
    assert resolved["gold"][iid] == {"explicit_reference": "unclear",
                                     "relation": "underdetermined", "source": "unresolved"}


def test_adjudicator_rules_only_on_the_disputed_construct(corpus):
    items, report = corpus
    iid = items[0]["item_id"]
    anns = [a for a in agreed_annotations(items, report) if a["item_id"] != iid]
    anns += [ann(iid, report, "A", "yes", "dependent_direct", A_REF),
             ann(iid, report, "B", "yes", "dependent_indirect", B_REF),
             ann(iid, report, "adjudicator", "no", "dependent_indirect", J_REF)]
    g = ho.assemble_gold(report, anns)
    assert g["gold"][iid] == {"explicit_reference": "yes",
                              "relation": "dependent_indirect", "source": "adjudicated"}


def test_single_annotator_is_exploratory_only(corpus):
    items, report = corpus
    anns = [a for a in agreed_annotations(items, report)
            if not (a["item_id"] == items[3]["item_id"] and a["role"] == "B")]
    g = ho.assemble_gold(report, anns)
    assert g["grade"] == "exploratory" and not g["valid"]


@pytest.mark.parametrize("over,needle", [
    ({"blind_attestation": {**BLIND, "saw_jev_outputs": True}}, "blindness broken"),
    ({"blind_attestation": {"saw_jev_outputs": False}}, "blind_attestation"),
    ({"annotator_ref": "bob"}, "annotator_ref"),
    ({"role": "C"}, "role"),
    ({"relation": "vibes"}, "relation"),
    ({"explicit_reference": "maybe"}, "explicit_reference"),
    ({"unresolved": True}, "only the adjudicator"),
    ({"seconds_spent": -1}, "seconds_spent"),
    ({"codebook_version": ""}, "codebook_version"),
    ({"jev_argmax": "x"}, "unknown fields"),
])
def test_annotation_validation(corpus, over, needle):
    items, report = corpus
    rec = ann(items[0]["item_id"], report, "A", "yes", "dependent_direct", A_REF)
    rec.update(over)
    assert any(needle in e for e in ho.validate_annotation(rec))


def test_same_person_in_two_roles_is_rejected(corpus):
    items, report = corpus
    anns = agreed_annotations(items, report)
    for a in anns:
        if a["role"] == "B" and a["item_id"] == items[0]["item_id"]:
            a["annotator_ref"] = f"annotator-{A_REF}"
    g = ho.assemble_gold(report, anns)
    assert any("different people" in e for e in g["errors"])


def test_digest_mismatch_is_rejected(corpus):
    items, report = corpus
    anns = agreed_annotations(items, report)
    anns[0]["item_digest"] = "0" * 64
    assert any("item_digest mismatch" in e for e in ho.assemble_gold(report, anns)["errors"])


def test_cohen_kappa_hand_values():
    assert ho.cohen_kappa([("yes", "yes"), ("no", "no")]) == pytest.approx(1.0)
    # po = 0.5, pe = 0.5 -> 0
    assert ho.cohen_kappa([("yes", "yes"), ("yes", "no"), ("no", "yes"), ("no", "no")]) == 0.0
    assert ho.cohen_kappa([("yes", "yes")] * 4) is None  # undefined
    assert ho.cohen_kappa([]) is None


def test_r_kappa_gate_labels(corpus):
    items, report = corpus
    flip = lambda i, x, r: (x, "unrelated" if r == "dependent_direct" and i % 2 else r)
    g = ho.assemble_gold(report, agreed_annotations(items, report, flip))
    assert g["agreement"]["inferred_relations"] in (
        "halted_for_v0x", "stays_underdetermined_by_rule", "no_inferred_automation_in_v0.1")
    assert g["agreement"]["r_collapsed_kappa"] < 1.0


# --- evaluation ------------------------------------------------------------------

def dist(top, p):
    out = {o: 0.0 for o in OPTS}
    out[top] = p
    other = "unrelated" if top != "unrelated" else "dependent_direct"
    out[other] = round(1 - p, 2)
    return out


def write_log(tmp_path, items, answer, model=lambda i: "jev-1.13.0", complete=True):
    """A fabricated holdout call log, bound to the corpus and the frozen rule
    the same way `run_holdout_eval` binds a real one."""
    report = ho.validate_corpus(items)
    log = shadow.EvalCallLog(str(tmp_path), kind=shadow.HOLDOUT_CALLS_RECEIPT_KIND,
                             header={"fixture_count": len(items),
                                     "item_count": len(items),
                                     "holdout_corpus_digest": report["corpus_digest"],
                                     "rule_digest": v01.RULE_DIGEST})
    for i, it in enumerate(items):
        probs = answer(i)
        cid = f"call-{i}"
        record = {k: None for k in jev.CALL_RECORD_FIELDS}
        record.update(
            call_id=cid, ts="2026-09-18T00:00:00Z", campaign=jev.CAMPAIGN,
            component=jev.COMPONENT, decision_family="edge_relation",
            question_id=f"edge:{it['item_id']}", request_digest="0" * 64,
            model_requested="jev-latest", model_resolved=model(i),
            probabilities=probs, argmax_recomputed=jev.recompute_argmax(probs, OPTS),
            choice_returned=jev.recompute_argmax(probs, OPTS),
            choice_argmax_discrepancy=False, confidence_observed=0.5, latency_s=0.3,
            input_tokens=700, output_tokens=60, est_cost_usd=0.0,
            replicate_set_id=f"set-{i}", replicate_index=1, status="ok",
            http_status=200, typesafe_request_id=f"req_{i}")
        log.call(record, fixture_id=it["item_id"], edge_id=it["item_id"],
                 axolotl_available=False, first_probabilities=probs)
        log.pair_result({"fixture_id": it["item_id"], "call_ids": [cid], "status": "ok"})
    if complete:
        log.closeout({"run_finished": True, "stop_reason": "completed",
                      "pairs_total": len(items), "pairs_evaluated": len(items),
                      "evaluation_complete": True})
    log.close()
    return shadow.read_eval_call_log(log.path)


def good_answer(i):
    if i < 60:
        return dist("dependent_direct", 0.95)
    if i < 90:
        return dist("unrelated", 0.9)
    if i < 140:
        return dist("dependent_indirect", 0.8)
    return dist("unrelated", 0.9)


@pytest.fixture()
def gold(corpus):
    items, report = corpus
    return ho.assemble_gold(report, agreed_annotations(items, report))


def test_passing_part(tmp_path, corpus, gold):
    items, report = corpus
    res = ho.evaluate_part("H", items, report, gold, write_log(tmp_path, items, good_answer))
    assert res["disposition"] == "PASSED", res.get("failed") or res.get("blockers")
    m = res["metrics"]
    assert m["m1_false_positive_qualification"]["k"] == 0
    assert m["m1_false_positive_qualification"]["n"] == 60
    assert m["m2_explicit_precision"]["point"] == 1.0
    assert m["m3_explicit_recall"]["point"] == 1.0 and m["m3_useful"]
    assert m["m4_ambiguity_preservation"]["point"] == 1.0
    assert m["m5_indirect_qualified_without_explicit"]["count"] == 0
    assert res["disposition_counts"][v01.QUALIFIED_EXPLICIT] == 60
    assert res["rule"] == v01.FROZEN_PARAMETERS and res["gates"] == ho.FROZEN_GATES
    # The old §5.5 rule qualifies nearly everything on the same items.
    assert m["m7_old_rule_false_positive_qualification"]["k"] > 0
    assert ho.overall_decision(res, None)["decision"] == "NO_GO_PENDING_PART_R"
    assert ho.overall_decision(res, res)["decision"] == "GO_BOUNDED_QUALIFICATION_SHADOW"


def test_false_positive_qualification_fails_m1(tmp_path, corpus, gold):
    items, report = corpus
    bad = lambda i: dist("dependent_direct", 0.95) if i < 90 else good_answer(i)
    res = ho.evaluate_part("H", items, report, gold, write_log(tmp_path, items, bad))
    assert res["disposition"] == "FAILED"
    assert "m1" in res["failed"] and "m2" in res["failed"]
    assert res["metrics"]["m1_false_positive_qualification"]["k"] == 30
    assert ho.overall_decision(res, res)["decision"] == "NO_GO"


def test_too_few_qualified_items_cannot_pass_m1(tmp_path, corpus, gold):
    items, report = corpus
    few = lambda i: dist("dependent_direct", 0.95) if i < 40 else (
        dist("dependent_direct", 0.80) if i < 60 else good_answer(i))
    res = ho.evaluate_part("H", items, report, gold, write_log(tmp_path, items, few))
    assert res["metrics"]["m1_false_positive_qualification"]["n"] == 40
    assert "m1" in res["failed"]


def test_x_agreement_below_gate_halts_before_evaluation(tmp_path, corpus):
    items, report = corpus
    noisy = lambda i, x, r: ({"yes": "no", "no": "unclear", "unclear": "yes"}[x], r) if i % 2 else (x, r)
    g = ho.assemble_gold(report, agreed_annotations(items, report, noisy))
    assert not g["agreement"]["x_gate_passed"]
    res = ho.evaluate_part("H", items, report, g, write_log(tmp_path, items, good_answer))
    assert res["disposition"] == "HALTED_X_AGREEMENT"
    assert "metrics" not in res


def test_blockers(tmp_path, corpus, gold):
    items, report = corpus
    log = write_log(tmp_path / "a", items, good_answer, complete=False)
    assert ho.evaluate_part("H", items, report, gold, log)["blockers"] == \
        ["evaluation_log_incomplete"]
    mixed = write_log(tmp_path / "b", items, good_answer,
                      model=lambda i: "jev-1.14.0" if i == 7 else "jev-1.13.0")
    assert "model_version_not_single" in \
        ho.evaluate_part("H", items, report, gold, mixed)["blockers"]
    exploratory = dict(gold, grade="exploratory")
    log_ok = write_log(tmp_path / "c", items, good_answer)
    assert "annotation_not_qualification_grade" in \
        ho.evaluate_part("H", items, report, exploratory, log_ok)["blockers"]


def test_ambiguity_collapsed_into_qualification_fails_m4(tmp_path, corpus, gold):
    items, report = corpus
    # Give underdetermined-gold items an anchor-free explicit answer: no
    # qualification (no anchor), so M4 still passes -- anchors gate it.
    res = ho.evaluate_part("H", items, report, gold, write_log(
        tmp_path / "a", items,
        lambda i: dist("dependent_direct", 0.95) if 90 <= i < 140 else good_answer(i)))
    assert res["metrics"]["m4_ambiguity_preservation"]["point"] == 1.0
    # With anchors on those items, the collapse would qualify them -> M4 fails.
    anchored = [make_item(i, it["categories"], anchored=True,
                          lang_b=it["langs_b"][0]) if 90 <= i < 140 else it
                for i, it in enumerate(items)]
    rep2 = ho.validate_corpus(anchored)
    g2 = ho.assemble_gold(rep2, agreed_annotations(anchored, rep2))
    res2 = ho.evaluate_part("H", anchored, rep2, g2, write_log(
        tmp_path / "b", anchored,
        lambda i: dist("dependent_direct", 0.95) if 90 <= i < 140 else good_answer(i)))
    assert "m4" in res2["failed"]


def test_evaluation_reports_token_baselines_without_changing_decision(tmp_path, corpus, gold):
    items, report = corpus
    res = ho.evaluate_part("H", items, report, gold, write_log(tmp_path, items, good_answer))
    base = res["metrics"]["m6_baselines"]
    assert set(base) >= {"anchor_only", "token_numeric_only", "token_long_word_only",
                         "anchor_or_token_numeric", "anchor_or_token_long_word",
                         "v01_plus_token_numeric", "v01_plus_token_long_word"}
    # Anchor-only qualifies the anchor-bearing negatives too, so it fails M1.
    assert base["anchor_only"]["m1_false_positive_qualification"]["k"] == 30
    assert res["metrics"]["m6_jev_adds_no_qualification_value"] is False
    assert res["disposition"] == "PASSED"


def test_slices_are_reported(tmp_path, corpus, gold):
    items, report = corpus
    res = ho.evaluate_part("H", items, report, gold, write_log(tmp_path, items, good_answer))
    s = res["metrics"]["m10_slices"]
    assert set(s) == {"relation_type", "text_length", "language", "anchor_type"}
    assert "long_300_500" in s["text_length"] and "non_en_present" in s["language"]
    assert res["metrics"]["m10_blocking_slices"] == []


def test_wilson():
    lo, hi = ho.wilson(0, 60)
    assert lo == 0.0 and hi == pytest.approx(0.0602, abs=1e-3)
    lo, hi = ho.wilson(3, 60)
    assert hi == pytest.approx(0.1370, abs=2e-3)
    assert ho.wilson(0, 0) == (None, None)


def test_frozen_gates_are_the_amendment_values():
    assert ho.FROZEN_GATES == {
        "x_kappa_min": 0.60, "r_kappa_halt": 0.40, "r_kappa_upper": 0.60,
        "m1_point_max": 0.05, "m1_wilson_upper_max": 0.12, "m1_min_qualified": 50,
        "m2_point_min": 0.90, "m2_wilson_lower_min": 0.80, "m3_useful_min": 0.50,
        "m4_min": 0.95, "m5_max_items": 2, "m6_recall_margin": 0.05,
        "m10_min_slice_qualified": 15, "m10_m1_max": 0.12}


# --- specs and CLI ------------------------------------------------------------------

def test_json_schemas_match_validators():
    item = json.loads((SPEC / "holdout_item.schema.json").read_text())
    annotation = json.loads((SPEC / "annotation_record.schema.json").read_text())
    assert item["required"] == list(ho.ITEM_REQUIRED)
    assert set(item["properties"]) == set(ho.ITEM_REQUIRED) | set(ho.ITEM_OPTIONAL)
    assert item["properties"]["categories"]["items"]["enum"] == list(ho.CATEGORIES)
    assert annotation["required"] == list(ho.ANNOTATION_REQUIRED)
    assert annotation["properties"]["relation"]["enum"][:-1] == list(OPTS)
    assert set(annotation["properties"]["blind_attestation"]["required"]) == set(ho.BLINDNESS_KEYS)


def test_authoring_protocol_states_the_boundaries():
    text = (SPEC / "AUTHORING_PROTOCOL.md").read_text()
    for needle in ("no access to per-item outputs", "Two independent annotators plus one adjudicator are mandatory",
                   "Authors never provide gold labels", "Annotators never see Jev output",
                   "implementation session that built this tooling"):
        assert needle in text


def test_cli_validate_and_view(tmp_path, corpus, capsys):
    items, _ = corpus
    path = tmp_path / "synthetic_items.jsonl"
    path.write_text("".join(json.dumps(it) + "\n" for it in items))
    l2 = str(ROOT / "tests" / "fixtures" / "semantic_relations.jsonl")
    cli.main(["semantic-holdout", "validate", "--items", str(path), "--l2-fixtures", l2])
    assert json.loads(capsys.readouterr().out)["valid"] is True
    view = tmp_path / "view.jsonl"
    cli.main(["semantic-holdout", "annotation-view", "--items", str(path),
              "--l2-fixtures", l2, "--out", str(view)])
    rows = [json.loads(l) for l in view.read_text().splitlines()]
    assert len(rows) == 150 and "author_ref" not in rows[0] and "categories" not in rows[0]
    bad = tmp_path / "bad.jsonl"
    bad.write_text(json.dumps(items[0]) + "\n")
    with pytest.raises(SystemExit) as e:
        cli.main(["semantic-holdout", "validate", "--items", str(bad), "--l2-fixtures", l2])
    assert e.value.code == 2
