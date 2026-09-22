# Part H holdout — authoring and annotation protocol (`lw-part-h-authoring/v1`)

Normative source: `/data/git/jev-dual-shadow-v0/DESIGN.md`, Amendment
2026-09-17-C §6 (Labelwatch V0.1 qualification rule). This protocol governs
how the Part H holdout is produced. It is not a result and contains no
evaluation outputs.

## 1. Roles and separation

| Role | Receives | Must not receive | Produces |
|---|---|---|---|
| **Coordinator** | everything below | — | pseudonymous refs, the annotation view, assembled files |
| **Author** | this protocol, `holdout_item.schema.json`, the composition targets (§3) | any earlier evaluation output (see §2), Jev outputs, annotations | item records |
| **Annotator A / B** | the annotation view only, the codebook | Jev outputs, anchor or token features, author identity, author intent, item categories, the other annotator's labels | annotation records |
| **Adjudicator** | the annotation view, A's and B's labels for disputed items, the codebook | Jev outputs, anchor or token features, author identity, author intent, item categories | adjudication records |

- Authors, annotators and the adjudicator are different people for any
  given item. Each item has exactly one annotator A and one annotator B,
  and they are different people.
- **Two independent annotators plus one adjudicator are mandatory.** With
  fewer, the output is exploratory fixtures only and is never used as
  qualification evidence (the tooling marks it `exploratory`).
- **Authors never provide gold labels.** An author may record their intended
  reading in `author_intended`. It is a secondary field, it is stripped from
  the annotation view, and it is never used as gold.
- **Annotators never see Jev output.** Each annotation record carries a
  blind attestation, and any `true` value invalidates the record.

## 2. Blind-author boundary

- Part H authors must have had **no access to per-item outputs** of any
  earlier Labelwatch semantic evaluation. That includes the 60-pair L2
  fixture evaluation of 2026-09-17: its eval receipt, call log, analysis,
  summary and channel files, and any discussion of which items or classes
  the model got wrong.
- Authors must not reuse or paraphrase items from
  `tests/fixtures/semantic_relations.jsonl`. The validator rejects exact
  text reuse (case- and whitespace-normalized).
- **Ineligible:** the implementation session that built this tooling, and
  any agent or person who ran or read the L2 analysis. They have seen L2
  results and may not author, edit, select, filter or label Part H items.
  They may build and run the schema and composition validators, the
  annotation-view export, and the evaluation tooling.
- The coordinator records each author's eligibility attestation outside
  this repository, keyed by `author_ref`.

## 3. What authors write

One JSON object per line, validated by
`labelwatch semantic-holdout validate --items FILE`. Fields:

| Field | Meaning |
|---|---|
| `record_kind` | `labelwatch.semantic_shadow_holdout_item.v1` |
| `item_id` | opaque `h-` + 12 lowercase hex, assigned by the coordinator; carries no category information |
| `authoring_protocol_version` | `lw-part-h-authoring/v1` |
| `author_ref` | pseudonymous `author-` + 8 hex, assigned by the coordinator; never shown to annotators |
| `relation_type` | `reply`, `quote` or `repost` (the structural link from B to A) |
| `text_a`, `text_b` | post texts, ≤ 500 chars each. `text_a` is non-empty. `text_b` may be empty only for a repost or a `quote_no_comment` item. No handles, DIDs or `at://` URIs. |
| `dt_seconds` | seconds between A and B (non-negative integer) |
| `langs_a`, `langs_b` | non-empty lists of language tags (e.g. `en`, `es`, `pt-BR`) |
| `categories` | one or more of the categories below |
| `anchor_bearing_negative` | `true` if the item is meant to share surface material with A (see anchors below) without B depending on A |
| `declared_long_text` | `true` iff both texts are 300–500 chars (checked) |
| `declared_multilingual` | `true` iff any language tag is not English (checked) |
| `author_intended` | optional `{explicit_reference, relation}`; secondary; hidden from annotators |
| `author_note` | optional free text for the coordinator; hidden from annotators |

**Categories** (authoring targets, not gold):
- `explicit_direct_reference`: B plainly responds to, restates or
  continues specific content of A.
- `subtweet`: B appears to be about A's content without saying so.
- `irony`: B's relation to A depends on irony or sarcasm.
- `vaguebooking`: B is vague enough that any relation to A is hard to tell.
- `unrelated_similarity`: A and B share wording or topic, but B does not
  depend on A.
- `structurally_linked_semantically_unrelated`: B is linked to A but its
  content is unrelated to A.
- `common_source_convergence`: A and B share vocabulary or a link because
  both respond to the same outside event or source, not to each other.
- `explicit_reference_with_stance`: B references specific content of A
  while taking a sarcastic or critical stance toward it.
- `quote_no_comment`: B quotes A and adds no text (`relation_type` quote,
  empty `text_b`).

**Anchors** (so authors can write anchor-bearing negatives). An item
carries an anchor if any of these holds after identifier redaction:
- A and B link to the same registered domain, not on the generic-host list
  (link shorteners, big platforms, multi-part public suffixes such as
  `co.uk`; list frozen in `labelwatch/semantic_shadow_v01.py`);
- A and B share a hashtag;
- their character 5-gram Jaccard overlap is ≥ 0.20.

The validator rejects an `anchor_bearing_negative` item that carries no
anchor.

**Composition** (enforced for the whole file):
- 135–165 items (target 150);
- at least 15 items in each of the six categories from
  `explicit_direct_reference` to
  `structurally_linked_semantically_unrelated`, 15
  `common_source_convergence`, 15 `explicit_reference_with_stance` and 10
  `quote_no_comment`;
- at least 15 long-text items and at least 10 multilingual items;
- at least 20 % `anchor_bearing_negative`;
- no duplicate ids or text pairs, and no missing metadata.

An item may count toward several categories.

## 4. Annotation

- The coordinator exports the blind view with
  `labelwatch semantic-holdout annotation-view --items FILE --out VIEW`.
  The view holds id, digests, relation type, texts, dt and languages only,
  ordered by digest.
- Codebook: derived from `dispute-edge-pilot/CODEBOOK.md`, edge-scored;
  `unclear` and `underdetermined` are real answers. Its version string goes
  in every record.
- Two constructs per item:
  - **explicit reference** (`yes` / `no` / `unclear`): does B's text
    reference identifiable content of A?
  - **relation**: `dependent_direct`, `dependent_indirect`,
    `independent_lexical`, `unrelated` or `underdetermined`.
- Records follow `annotation_record.schema.json`: roles `A`, `B`,
  `adjudicator`, plus pseudonymous `annotator_ref`, time spent and the blind
  attestation.
- **Adjudication.**
  - The adjudicator rules only on items where A and B disagree.
  - For a disputed construct, the adjudicator's label is gold. Agreed
    constructs keep the A/B label.
  - If the adjudicator cannot resolve an item, the record has
    `unresolved: true` and no labels. Gold is then `unclear` /
    `underdetermined`, a legitimate result, not a failure.
- **Agreement gates** (pre-adjudication, Cohen's κ, A vs B):
  - explicit reference: κ ≥ 0.60, or the study halts before the rule is
    evaluated;
  - collapsed relation: κ < 0.40 halts automation of inferred relations
    for V0.x;
  - collapsed relation, 0.40–0.60: inferred relations stay
    `UNDERDETERMINED` by rule.
  - A halt is a result. Jev is never used to settle annotator
    disagreement.

## 5. Ordering (Jev never influences annotation)

1. Authors submit the item file. 2. `semantic-holdout validate` passes.
3. The corpus is frozen and digested. 4. The blind annotation view is
exported. 5. Annotators A and B, then the adjudicator, complete and freeze
labels without any Jev output. 6. Gold is assembled and frozen.
7. `semantic-holdout eval-holdout --live` runs Jev over the frozen corpus;
it needs no gold and receives none. 8. `semantic-holdout evaluate` compares
the V0.1 outputs against frozen gold and refuses to issue a disposition
unless the corpus digest, rule digest, complete run, gold completeness and
agreement gates all hold.

Step 7 may precede steps 5-6 only if the annotation team has no access to
the run output; the tooling keeps Jev results out of the annotation view in
either order.

## 6. After annotation

The evaluation (`semantic-holdout evaluate --part H …`) applies the frozen
V0.1 rule and the pre-registered tolerances. No threshold, anchor,
stop-list or gate is changed after items exist; a failed rule needs a new
amendment and a new holdout.
