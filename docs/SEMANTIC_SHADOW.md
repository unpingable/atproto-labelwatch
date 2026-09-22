# Semantic Shadow (LW-JEV-SHADOW-v0)

Part of campaign JEV-DUAL-SHADOW-V0. The frozen spec is
`/data/git/jev-dual-shadow-v0/DESIGN.md` §5; the shared client contract is its
§2 (JEV-CONTRACT-v0). This file is the operator-facing summary.

## Named analytic question (frozen)

> For candidate post-relation edges that already have structural evidence
> (reply/quote/repost lineage), does author-blind semantic adjudication agree
> with the structural channel, at what ambiguity rate, and at what apparent
> false-attribution rate — as qualification evidence only?

Nothing here is enforcement. The sidecar produces shadow qualification
evidence and nothing else.

## What it is

A CLI-run daemon (`labelwatch semantic-shadow run`) that consumes Jetstream
(`app.bsky.feed.post`, `app.bsky.feed.repost`), extracts candidate post-relation
edges from **structural record fields only** (`record.reply.parent/root`,
`embed.record.uri`, `subject.uri`), holds post text ephemerally in a bounded
in-memory cache, deterministically samples ≤ `semantic_shadow_daily_cap`
edges/day, and adjudicates each sampled edge through a thin JEV-CONTRACT-v0
client. Author-blindness: the evidence packet carries no DIDs, handles, display
names, follower counts, account history, or Labelwatch dials; identifiers inside
text are redacted before packetization and a privacy tripwire scans every
serialized outbound packet. Receipts and the sidecar store `sha256` digests of
texts and URIs, never text.

Wire shape (documented System One API, pinned by
`tests/test_semantic_shadow_jev_contract.py` against verbatim recorded live
responses in `tests/fixtures/typesafe_systemone_recorded.json`): the packet is
sent as `state`; one `choice` question keyed `edge_relation` carries fixed
instructions and the five relation descriptions as `criteria`; the answer is
read from `answers.edge_relation`, the resolved model from `model`, and the
request id from the `x-typesafe-request-id` response header. The edge id,
campaign, and component labels stay on the local record and are never sent.

Fixture evaluation evidence (`semantic-shadow eval-fixtures --live`): every
per-call record is persisted as it is produced to
`<receipts-dir>/labelwatch.semantic_shadow_fixture_eval_calls.v0.<UTC>.<id>.jsonl`
(the path is also in the eval receipt as `call_records_path`). The file has
one `header` line; one `call` line per record, holding the unmodified §2.5
record plus the fixture id, call/sent/repeat ordinals, `network_sent`,
`error_category` (`sent:<status>`, `sent:model_version_change`,
`refused_before_network:<status|model_version_stop>`),
`choice_matches_argmax`, and the repeat trigger; one `pair_result` line per
pair (median distribution, argmax, ambiguity set, gate state and detail,
repeat decision, text/packet digests); and a final `closeout` line. A file
without a closeout is an interrupted run, and `evaluation_complete` is true
only when every pair was adjudicated. `read_eval_call_log()` checks all of
this. The file holds no text, no headers, and no key material.

Channels per edge (§5.5): `structural_lineage` (always true — it is the
prefilter), `lexical` (5-gram shingle Jaccard ≥ 0.20), `semantic`
(replicate-median argmax ∈ dependent_* with median p ≥ 0.50), `temporal`
(dt ≤ 3600s). An edge is shadow-qualified iff ≥2 channels agree; leave-one-
channel-out fragility flags are recorded per edge. An optional axolotl gate
(§5.6) decides whether the k=3 repeat spend is load-bearing; when axolotl is
not resolvable the gate reports `axolotl_unavailable` and the frozen §2.6
repeat policy applies directly.

## Hard V0 boundaries (DESIGN §8)

- **No post text persisted** — digests only, in the sidecar DB and receipts.
- No account-level semantic score, no dossiers, no person/motive/ideology
  classification.
- No semantic result into suppression/mute/quench/moderation/admission/
  collection policy.
- **No public output**: `report.py`, `server.py`, `frontdoor.py` are
  untouched; a test (`test_http_surface_has_no_semantic_shadow_references`)
  asserts the HTTP surface stays unchanged.
- No correlated probabilities summed as if independent; no majority-vote
  collapse of replicate sets.

## Kill switches and gates

- `semantic_shadow_enabled = false` (config; default off). Unsetting/disabling
  returns the system to bit-identical current behavior — no migration or data
  repair needed (the sidecar is a separate file; delete it to remove all
  shadow state).
- `LABELWATCH_SEMANTIC_SHADOW_DISABLED=1` — env kill switch, overrides the
  config flag (precedent: `CLIMATE_API_DISABLED`).
- `TYPESAFE_API_KEY` — absent by design in V0 qualification; **every live path
  refuses closed without it** (status `key_absent`).
- Every live entry point prints a §2.9 pre-flight cost estimate and requires
  `--max-calls` and `--confirm-live-spend`.
- Daily caps: `semantic_shadow_daily_cap` adjudications/UTC day (hard counter
  in the sidecar meta) and the run's `--max-calls` call cap. Model-version
  stop: if `model_resolved` changes within a UTC day, further calls that day
  are refused (§2.7).
- Axolotl is optional: resolved via `LABELWATCH_AXOLOTL_PATH`, never vendored,
  never a pip dependency.

## Commands

```bash
labelwatch semantic-shadow status                 # sidecar state, gates, counters
labelwatch semantic-shadow run --max-calls 600 --confirm-live-spend
labelwatch semantic-shadow eval-fixtures          # offline validation + plumbing
labelwatch semantic-shadow eval-fixtures --live --max-calls 200 --confirm-live-spend  # Phase 4 only
labelwatch semantic-shadow export-receipts [--day YYYY-MM-DD] [--out calls.jsonl]
```

Every invocation (any state) writes a
`labelwatch.semantic_shadow_activation.v0` receipt; runs write a
`labelwatch.semantic_shadow_run_manifest.v0` (code digests, config hash, caps,
thresholds, `implemented_by`) and a
`labelwatch.semantic_shadow_closeout.v0` (counts, tokens, cost, stop reason).
Receipts land in `semantic-shadow-receipts/` next to the sidecar DB unless
`--receipts-dir` is given. The sidecar is `labelwatch_semantic_shadow.db` next
to the main DB; a 30-day retention prune runs hourly in the daemon.

`deploy/labelwatch-semantic-shadow.service` is an **uninstalled template**;
this campaign installs no systemd unit and nothing references it.

## Failure degradation (§5.8)

Any Jev failure/timeout/model-version change/contract discrepancy → **no
semantic receipt for that edge** (`channel_semantic` NULL, disposition
`semantic_unavailable`); structural candidate recording continues; existing
Labelwatch behavior is untouched (the sidecar shares no tables and only WAL-
citizen discipline with the main DB).

## V0.1 qualification rule and Part H (Amendment 2026-09-17-C)

The accepted amendment (`/data/git/jev-dual-shadow-v0/DESIGN.md`, Amendment
2026-09-17-C) replaces the §5.5 "two agreeing channels" rule for future runs.
It is implemented **offline only** in `labelwatch.semantic_shadow_v01`; the
daemon still runs the original rule, and the qualification daemon is NO-GO
until fresh validation passes.

- Context (structure, dt, relation type, depth, languages) never qualifies;
  it can only downgrade.
- `QUALIFIED_EXPLICIT` requires an admitted anchor (shared non-generic URL
  domain, shared hashtag, or 5-gram Jaccard ≥ 0.20) **and** a first Jev call
  with recomputed argmax `dependent_direct`, p ≥ 0.90, and no repeat
  trigger or spend.
- Otherwise the result is `UNDERDETERMINED` (indirect/underdetermined argmax,
  or replicates spent), `NOT_QUALIFIED`, or `SEMANTIC_UNAVAILABLE`.
- Review routes are orthogonal to the disposition: `semantic_unavailable`,
  `ambiguity`, `anchor_semantic_conflict`, `inferred_claim`.
- `shared_distinctive_token` (numeric and long-word parts) is recorded under
  `validation_only` and cannot reach the rule.

Part H fresh validation, in order: `labelwatch semantic-holdout validate`
(schema + composition) → `annotation-view` (blind export) → annotators and
adjudicator work independently of Jev → `gold` → `eval-holdout --live`
(Jev over the frozen corpus) → `evaluate` → `semantic-holdout-decide`.

`eval-holdout` sends a narrow allow-listed projection of each item
(relation type, both texts, dt, languages); the opaque item id stays local,
and author identity, author intent, categories, anchor designations,
annotations and gold never reach the request. It requires the config flag,
`TYPESAFE_API_KEY`, `--max-calls` and `--confirm-live-spend`; `--dry-run`
prints the preflight (item count, worst-case calls under the repeat policy,
estimate at the 950-token planning figure) with no network use. It writes a
`labelwatch.semantic_shadow_holdout_eval_calls.v1` JSONL log in the same
header/call/item/closeout shape as the L2 log, readable by
`read_eval_call_log`, bound to the corpus and rule digests. A run that hits
the cap, loses the model version, or fails is marked incomplete, and
`evaluate` refuses incomplete or unbound runs.
The authoring and annotation protocol, and both JSON schemas, are in
`specs/semantic_shadow/part_h/`. Holdout items come only from blind authors;
gold comes only from two annotators plus an adjudicator.

## Fixture evaluation (§5.9)

`tests/fixtures/semantic_relations.jsonl` holds ≥10 authored synthetic pairs
per class (`explicit_direct_reference`, `subtweet`, `irony`, `vaguebooking`,
`unrelated_similarity`, `structurally_linked_semantically_unrelated`), each
with gold relation (§5.4 collapse), an authored ambiguity note, and an
acceptable-answer set. Several classes legitimately gold `underdetermined`
(subtweet, vaguebooking, bare reposts). The schema is
`tests/fixtures/semantic_relations.schema.json`; the normative validator is
`labelwatch.semantic_shadow.validate_fixture_record`. **No accuracy claim may
be made before the live fixture evaluation runs** (Phase 4).
