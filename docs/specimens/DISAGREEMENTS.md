# Specimen DISAGREEMENTS + FINDINGS log

> **The doctrine block immediately below is FROZEN.** It is the
> load-bearing claim cited by `docs/findings/operator-maturity/`
> (the public findings page) and by F-001, F-004, F-007, F-008.
> Edit only for correctness — not for vibes, not for shorter prose,
> not for "I have a better word now." If you genuinely need to
> change it: open the change with a one-line rationale, leave the
> prior wording as a strikethrough or footnote so a future reader
> can audit the drift, and update the findings page in the same
> commit.

Three kinds of entries:

**D-NNN — disagreements.** The classifier's verdict on a real-data
evidence packet differs from what the operator expected, OR an
explicit operator self-discipline note about a classifier/analysis
choice that should not be over-interpreted. Each entry resolves to one of:

- **classifier_wrong** — code has a bug; fix in `classifier.py`
- **schema_incomplete** — evidence vocabulary doesn't capture the
  distinction the operator was making; fix the schema (`*.evidence.json`
  shape) and update `derive_evidence.py` + `classifier.py` together
- **operator_wrong** — the operator's pre-judgment was the laundering
  shape; classifier is honest; resolution is a write-up explaining why
  the operator's intuition was off
- **discipline_note** — no disagreement per se; recorded to prevent a
  later reader from treating heuristic output as normative

**F-NNN — findings.** Non-disagreement observations worth recording —
operator intuitions the schema correctly refuses to encode, coverage
gaps surfaced by a batch run, or ecosystem patterns that surface in
analysis lanes (per `docs/analysis/`) and feed back into the
admissibility frame.

**T-NNN — technical hygiene.** Items about Labelwatch's own classifiers,
scanners, or apparatus that need a review pass. Distinct from F-NNN
(which observe the ECOSYSTEM) and D-NNN (which resolve specific
disagreements). T-items are tool debt: they don't change the doctrine,
they fix our own measurement instruments.

Per the methodology: "the first real success is not agreement with the
operator. The first real success is a schema-grounded derived verdict
that disagrees with the operator and survives audit." Empty log = the
mechanization isn't yet paying for itself.

The canonical admissibility hook from F-007's headline (worth keeping
visible at the top of this file):

> **Do not let "labeler exists" silently convert into "moderation
> service exists."** Label emission, declared semantics, and
> operational liveness are separate properties. The observed ATProto
> labeler ecosystem contains all three failure modes at scale.
>
> The core triad:
>
>   - **emission ≠ declaration** (F-007)
>   - **declaration ≠ liveness** (F-008)
>   - **liveness ≠ authority** (F-001, F-004, consumer-conversion census)

"Operational liveness" is the right term for this canonical doctrine
because it tracks the admissibility claim. "Operational maturity" is
heuristic / SRE-shape (see D-002) — fine for analysis tables, wrong
for doctrinal statements.

---

## F-001 — Reference-labeler status does not imply default-client conversion

**Recorded:** Bundle A close, 2026-06-08.
**Batch:** stratified Bundle A run (10 packets, 0 disagreements).

**Observation.** Reference labelers (`skywatch.blue`, `label.haus`) and
unknown-classifier third-party labelers (`xblock.aendra.dev`,
`labeler.plural.host`) classify identically under the current schema
when their labels are not in `@atproto/api`'s global `LABELS` map. Both
strata yield `conversion_witness_gap_no_consumer` for the
`bsky.app-default-client` consumer.

**Why this is correct (not a disagreement).** Reference-labeler status
is a *Labelwatch calibration choice* — Labelwatch designates a small
curated set as reference anchors for measuring other labeler behavior.
It is NOT a claim about consumer adoption. From the default Bluesky
client's perspective, `skywatch.blue/fringe-media` requires the viewer
to opt in to skywatch as a labeler service exactly the same way
`xblock.aendra.dev/twitter-screenshot` does.

**Operator-frame correction.** If the operator's intuition was
"reference labels should classify differently because they're
important," this batch is the empirical rejection. The conversion
gap is determined by whether the *named consumer* documents a policy
for the label_value — not by whether Labelwatch happens to use the
labeler as a calibration reference.

**Implications for next runs.** A third-party label converts only when
some consumer's policy explicitly honors it. Bundle B's target 5 —
"find a third-party labeler consumed by some non-default consumer" —
is the adversarial test of this finding. If found, the schema must
support documenting the opt-in consumer's policy artifact, and the
classifier must produce `execution_gap_policy_present(...)` for that
consumer while remaining `conversion_witness_gap_no_consumer` for the
default consumer. Same label, different consumer, different verdict.

**Status:** recorded.

**Status update (2026-06-08, consumer-conversion census):** The
`docs/analysis/consumer-conversion-census.md` empirically tested
whether any production Bluesky client converts third-party labelers
into default visibility behavior WITHOUT explicit user adoption.
7/7 sampled clients hardcoded ZERO third-party labelers as defaults;
the 3 that hardcode any labeler all hardcode `mod.bsky`. F-001's
operator-frame correction stands ("reference status doesn't imply
default-client conversion"), but the urgency framing is demoted: the
opt-in machinery this finding motivated is a **dormant guardrail**,
not an active wildfire perimeter. Re-promote on observed third-party
default conversion, runtime config-fetch evidence, or closed-client
behavioral proof.

---

## F-002 — `GLOBAL_LABELS` hardcode was 21 entries; upstream has 8

**Recorded:** Bundle B target 1 audit, 2026-06-08.

**Observation.** During Bundle B's KNOWN_LABEL_SURFACE audit, a direct
grep over `@atproto/api/packages/api/src/moderation/const/labels.ts` at
HEAD `7b8c5d60a` revealed that the global `LABELS` map contains only
**8 keys**: `!hide`, `!warn`, `!no-unauthenticated`, `porn`, `sexual`,
`nudity`, `graphic-media`, `gore`. The deriver's `GLOBAL_LABELS`
hardcode contained **21 entries** — 13 of which (`intolerant`,
`self-harm`, `sensitive`, `threat`, `spam`, `rude`, `sexual-figurative`,
`impersonation`, `illicit`, `security`, `misleading`, `unsafe-link`,
`inauthentic`) **do not exist in the upstream policy map**. `gore` was
also missing from my list.

**Impact (had it gone undetected).** Any detection-lane packet for one
of the 13 fictional entries would have been classified as
`execution_gap_policy_present(client_render)` — implying the default
consumer documents a render-side rule — when in fact the consumer has
no rule for that label_value at all. The honest classification is
`conversion_witness_gap_no_consumer`.

**Diagnosis: operator_wrong + schema_drift.** The hardcoded list was
seeded from operator intuition about plausible label categories rather
than from reading the upstream file. The schema's behavior under the
correct list is fine; the operator's input was wrong.

**Patch applied (this commit).**
1. `GLOBAL_LABELS` reduced to the authoritative 8 keys.
2. `KNOWN_LABEL_SURFACE` purged of the 13 fictional entries; `gore`
   added with audit metadata.
3. Detection-lane re-derived; `gore` now classifies correctly as
   `execution_gap_policy_present(client_render)`; the 13 fictional
   labels would now classify as `conversion_witness_gap_no_consumer`
   if they ever appear (none did in the Bundle A batch).

**Forward note.** The deriver should ideally re-validate `GLOBAL_LABELS`
against the live upstream file on each run, or at least at deploy.
Manual audit-on-bump is fine for v1 (the file changes infrequently);
when it changes, `DEFAULT_POLICY_HEAD` bumps and the table needs a
review pass with `reviewed_at` refresh.

**Status:** patched.

---

## F-003 — `!takedown` is policy-documented but not via `@atproto/api` LABELS

**Recorded:** Bundle B target 1+4 cross-finding, 2026-06-08.

**Observation.** F-002's grep also revealed that `!takedown` is **not**
in the `@atproto/api` global LABELS map. Yet `!takedown` is a real
atproto label (per protocol spec) that the default bsky client honors
operationally: when a takedown is applied, the PDS removes the record
and the client never sees it (the takedown effect is upstream of any
client-side rendering rule).

**Pre-patch state.** The D-001 patch placed `!takedown` in
`KNOWN_LABEL_SURFACE` as `pds_hosting` with a source citation pointing
at the LABELS const. With the F-002 correction shrinking `GLOBAL_LABELS`
to upstream-true, `!takedown` would have classified as
`conversion_witness_gap_no_consumer` — wrong; the label IS documented,
just by a different artifact (the protocol spec, not the client library).

**Diagnosis: schema_incomplete.** The deriver's `policy_documented`
check tested only `label_value in GLOBAL_LABELS`. Documented-via-protocol
labels were structurally invisible.

**Patch applied (this commit).**
1. Added `PROTOCOL_DOCUMENTED_LABELS` set, currently `{"!takedown"}`,
   for labels documented at the protocol/PDS layer rather than in the
   client-library LABELS map.
2. `_policy_documented(label_value)` returns True if the label is in
   either set.
3. `PolicyDocumentation.policy_artifact` now carries an
   `artifact_kind` field: `"atproto_api_labels_const"` for the
   client-library case, `"atproto_protocol_spec"` for the protocol case.
4. `!takedown`'s `source` citation in `KNOWN_LABEL_SURFACE` updated to
   correctly cite protocol behavior, not the LABELS const.

**Verified.** `!takedown` re-derived under the patched schema still
classifies as `execution_gap_policy_present(pds_hosting)` with the
correct artifact citation; `gap` and `surface` unchanged.

**Forward note.** Other protocol-level labels may exist (admin-action
labels emitted by Ozone, `!warn`-variants for specific actions). The
`PROTOCOL_DOCUMENTED_LABELS` set will need periodic review against
protocol spec changes.

**Status:** patched.

---

## F-004 — Third-party labelers DO publish `labelValueDefinitions`; opt-in consumers honor them

**Recorded:** Bundle B target 5, 2026-06-08.

**Observation.** Query over `discovery_events.record_json` shows that
three of the third-party labelers in the Bundle A batch publish
`app.bsky.labeler.service` records with `labelValueDefinitions`:

| labeler | labelValueDefinitions count (visible labels) |
|---|---|
| `skywatch.blue` (`did:plc:e4elbtctnfqocyfcml6h2lf7`) | ~34 (alf, alt-tech, fringe-media, fundraising-link, ...) |
| `label.haus` (`did:plc:6ebfnuunfngxfw3rth3ewojw`) | 2 (fucked-up-replyref, doesnt-know-how-replyrefs-work) |
| `xblock.aendra.dev` (`did:plc:newitj5jo3uel7o4mnf3vj2o`) | 11 (twitter-screenshot, bluesky-screenshot, ...) |

These service-record definitions ARE policy artifacts. For a consumer
who has explicitly subscribed to the labeler service in their Bluesky
moderation settings, the bsky client honors the labeler's own
`labelValueDefinitions` — same conversion path as for global LABELS,
just per-labeler instead of platform-wide.

**Adversarial test of F-001.** F-001 said third-party reference labels
classify as `conversion_witness_gap_no_consumer` for the default
consumer. F-004 confirms the missing distinction: for an *opt-in*
consumer (e.g., `bsky.app-with-skywatch-subscribed`), the same labels
would classify as `execution_gap_policy_present(...)` with policy
artifact pointing at skywatch's service record. **Same label, different
consumer, different verdict — exactly the parameterized-consumer
discipline F-001 forecast.**

**Diagnosis: schema_incomplete.** The deriver currently models only
the default consumer (`bsky.app-default-client` with no third-party
labelers subscribed). It has no mechanism to:
1. take a `--consumer-id` flag that names a non-default consumer with
   a documented subscription set;
2. look up the relevant labeler service record from
   `discovery_events.record_json`;
3. populate `PolicyDocumentation` with the service-record artifact and
   the per-label `labelValueDefinitions` rule.

**First-party variant (worth flagging).** Even `moderation.bsky.app`
emits labels not in the global LABELS map: `needs-review` (27k events
in 7d), `extremist`, `corpse`, `misinformation`, `scam`, `rumor`,
`vip-protection`, `vip`. These presumably ARE documented in
moderation.bsky.app's own service record's `labelValueDefinitions`,
and the default consumer DOES subscribe to moderation.bsky.app by
default — so the conversion path exists, just not via the
`@atproto/api` LABELS const. Same schema gap, applied to first-party.

**Patch NOT applied this commit.** Implementing multi-consumer +
service-record artifact lookup is a substantial schema extension
that touches the deriver, the classifier (gap branching may need to
distinguish "documented via global LABELS" from "documented via
labeler service record"), and the consumer terminology. Per the
Bundle B charter ("do not add Lean theory unless a disagreement
forces a missing distinction"), F-004 records the missing
distinction and parks the patch.

**Forward path.** When this lands:
- Deriver `--consumer-id <name>` with a registry mapping consumer
  ids to subscription sets.
- For each subscribed labeler, the deriver pulls the most recent
  `app.bsky.labeler.service` record from `discovery_events` and
  extracts `policies.labelValueDefinitions`.
- `PolicyDocumentation.policy_artifact.artifact_kind` gains a third
  value: `"labeler_service_record"`.
- Classifier behavior unchanged at the `ConversionGap` level — the
  artifact_kind affects what's CITED in admissible claims, not what
  the gap discriminator returns.

**Status (updated by Bundle G):** **end-to-end mechanized for one named consumer (Driftwatch).**

Bundle C added `LabelerEmitterDocumentation` and the `emitter_declared`
scope. Bundle G closes the F-004 loop with a real opt-in consumer
adoption path, end-to-end:

  - **Schema:** `ConsumerAdoption` + `ConsumerActionObservation` evidence
    fields (Bundle G stage 1 — `960be9d`).
  - **Classifier:** `opt_in_consumer_observed` is now a real branch of
    `_classify_consumer_scope`. New gap path: complete_path on the
    `consumer_local_state` surface when adoption + receipt are both
    observed; `execution_gap_policy_present(consumer_local_state)` when
    adoption is documented but no receipt.
  - **Exporter:** `consumer_scope_effective` formats as
    `opt_in:<consumer_id>` (e.g., `opt_in:driftwatch`) — string-shape
    difference prevents accidental field-equality with `global_platform`
    or `emitter_declared`. `consumer_local_scope_only` caveat plus
    inherited `non_global_provenance`.
  - **Driftwatch policy artifact:** real, with version, allowlist,
    refusal vocabulary, idempotent roster, per-attempt receipts
    (driftwatch repo commit `b4a8e3e` —
    `scripts/consumer_policy/policy.py` + `README.md` +
    `data/consumer_policy/state/external_advisory_caveats.json` + 3
    receipts).
  - **End-to-end specimen:** `specimen-003-driftwatch-opt-in-fringe-media`
    cites a real receipt by sha (3a6bb004a53d…) referencing a real input
    packet hash (ddaf736f…). Verified via classifier + exporter +
    fixture verify.

**Counterexample to the inverse rule "third-party → no conversion":**
the same `LabelObservation` (skywatch.blue, fringe-media, on the
specific post) now appears in two evidence bundles in the corpus that
differ in their consumer-side evidence:

  - derived packet — has LabelerEmitterDocumentation, no
    ConsumerAdoption → exports as `emitter_declared`
  - specimen-003 — has LabelerEmitterDocumentation, AND
    ConsumerAdoption=driftwatch, AND ConsumerActionObservation with a
    real receipt → exports as `opt_in:driftwatch`

The precise shape (this is the whole point):

  **same testimony**            (identical LabelObservation —
                                 same labeler, same label_value,
                                 same target)
  **different consumer evidence** (the second bundle adds
                                 ConsumerAdoption +
                                 ConsumerActionObservation)
  **different admissible conclusion** (the second bundle's
                                 consumer-side evidence supports a
                                 named-consumer-local conversion
                                 claim; the first bundle's does not)

The classifier reaches different admissible conclusions because the
evidence bundles ARE different — the second simply contains more
consumer-side material than the first. Adoption is a verb requiring
receipts; receipts are evidence.

**Discipline preserved:** opt_in adoption NEVER promotes to
`global_platform` (verified by `test_opt_in_does_not_promote_to_global`).
`non_global_provenance` from the underlying emitter_declared is
INHERITED, not erased (verified by
`test_opt_in_consumer_exports_with_local_scope_caveat`). Adoption by
Driftwatch does NOT entail adoption by any other consumer (always-fired
inadmissible `no_cross_consumer_inference`).

**Still parked:** Multi-consumer subscription graph (one
`ConsumerAdoption` field today; a list would be next). Live wiring of
Driftwatch's downstream consumer code (cluster-report annotation
reading the roster). Lean promotion (Bundle G's distinctions are
mechanized in the classifier; no Lean theorem yet).

**Status update (2026-06-08, consumer-conversion census):** Bundle G
machinery is correct but its threat model is currently speculative.
The `consumer-conversion-census.md` survey of 7 production clients
found ZERO third-party labelers hardcoded as defaults. Driftwatch's
synthetic adoption (the only "real" opt-in specimen) is something we
wrote ourselves; no third-party Bluesky client in the sampled corpus
is converting third-party labelers into default visibility behavior
without explicit user adoption. Bundle G is reclassified:

  - **Status:** dormant guardrail
  - **Evidence class:** future-compatible / counterfactual
  - **Urgency:** low
  - **Promotion trigger:** observed third-party consumer conversion,
    runtime config-fetch evidence, or closed-client behavioral proof

The machinery stays — it correctly models what the protocol allows,
and the synthetic Driftwatch specimen demonstrates the distinction.
Future work motivated by "what if a client does X" must cite an
actual observation first.

The cleaner doctrinal line that emerges from the census:

> **Consumer conversion is not assumed from labeler publication.
> Consumer conversion is not assumed from protocol affordance.
> Consumer conversion requires observed client behavior, explicit
> user preference, or a named synthetic specimen.**

---

## F-005 — moderation.bsky.app's service record is not in labelwatch's `discovery_events`

**Recorded:** Bundle C target-3 byproduct, 2026-06-08.

**Observation.** Querying `discovery_events` for moderation.bsky.app's
labeler service record returns **zero rows**. Same for the
`labelValueDefinitions` projection. The other queried labelers
(skywatch.blue, label.haus, xblock.aendra.dev) all have service
records on file with labelValueDefinitions; moderation.bsky.app
does not.

**Why this is a finding (not a disagreement).** moderation.bsky.app
emits 27k+ events of `needs-review` in 7d, plus `extremist`, `corpse`,
`misinformation`, `scam`, `rumor`, etc. — labels that are NOT in
`@atproto/api`'s upstream LABELS const but operationally ARE honored
by the default client (since it auto-subscribes to mod.bsky). Their
documented behavior presumably lives in mod.bsky's own
`app.bsky.labeler.service` record's `labelValueDefinitions`. But
labelwatch hasn't ingested that record (or mod.bsky doesn't publish
one in the standard place).

**Implication.** When the deriver runs on a `(mod.bsky, needs-review)`
packet, the classifier honestly reports:
- `PolicyDocumentation.status = absent_for_consumer` (no upstream LABELS entry, no protocol_doc entry)
- `LabelerEmitterDocumentation.status = absent` (no service record observed)
- `ConversionGap = {name: conversion_witness_gap_no_consumer, surface: None, consumer_scope: unknown}`

That's honest given current evidence. But it's also operationally
misleading: the default client likely DOES render `needs-review`
according to some rule (because mod.bsky is default-subscribed and
either has a service record we haven't ingested, or has builtin
behavior for these labels we haven't documented).

**Diagnosis: ingestion_gap, not schema_gap.** The schema (Bundle C
shape) can represent emitter-declared provenance correctly; the data
to populate it is missing for the most operationally important
first-party labeler.

**Patch path (deferred).**
1. Check whether moderation.bsky.app publishes an `app.bsky.labeler.service` record at all (DID:plc resolution → service endpoint → record listing).
2. If yes: confirm labelwatch's discovery pipeline knows to fetch it, and look at why it isn't in `discovery_events`.
3. If no: moderation.bsky.app's labels are documented somewhere else (atproto-internal admin definitions, perhaps embedded in the bsky appview). That's a different artifact_kind candidate; possibly extends `PROTOCOL_DOCUMENTED_LABELS`.

**Status (updated by Bundle F):** **bytes-level resolved.**

Bundle F added `docs/specimens/tools/backfill_service_record_via_appview.py`
and ran it once for moderation.bsky.app. The 18-entry
labelValueDefinitions block now lives in `labelwatch.db /
discovery_events` as a row with `source='appview_backfill'`,
`operation='create'`, synthetic
`commit_rev='appview-backfill-<utc-ts>'`. The deriver's primary
discovery_events lookup finds it on the first pass; the snapshot
fallback no longer fires for mod.bsky.

Externally-visible effect: every emitter_declared packet now reports
`service_record_provenance.source_table = 'labelwatch.db /
discovery_events'`. The snapshot file in
`service_record_snapshots/` remains as a true fallback for labelers
whose records haven't been backfilled (none currently in scope).

The Bundle F regression test (`test_source_table_is_classification_invariant`)
proves the source path does not change classification: two packets
differing only in `source_table` yield identical gap, scope, exporter
decision, and caveats. The discover-path change moves provenance bits,
not doctrine.

**What remains parked.** Modifying labelwatch's `discover.py` /
`discovery_stream.py` to auto-backstop appview records on a schedule
is its own architectural change in the live labelwatch package; it
belongs to a labelwatch infrastructure cycle, not the specimens
track. The current Bundle F state is: "the record is in the right
table; refreshing it later is a separate ops question."

**Status (D.5 historical, preserved):** mostly worked around via
snapshot fallback path.

Bundle D.5 confirmed via direct appview probe
(`app.bsky.labeler.getServices?dids=did:plc:ar7c4by46qjdydhdevvrndac&detailed=true`)
that mod.bsky **does** publish a service record with 24 `labelValues`
and 18 `labelValueDefinitions`. labelwatch's discovery pipeline simply
hasn't ingested it — the underlying ingestion gap is real, but it's
not the same thing as "the labeler doesn't declare the rule."

Workaround landed in D.5: a `service_record_snapshots/<did>.json`
fallback. The deriver now consults this directory after
`discovery_events` returns nothing, so labelers with missing ingest
but available appview data still resolve emitter-declared provenance.
Snapshot for `did:plc:ar7c4by46qjdydhdevvrndac` (moderation.bsky.app)
captured 2026-06-08.

Effect: labels previously misdiagnosed as ingestion gaps now resolve:
  - `extremist`, `intolerant` (and the others in mod.bsky's
    labelValueDefinitions) → `emitter_declared` + EXPORTED.
  - `needs-review` (genuinely not in mod.bsky's service record at all)
    → still BLOCKED, but with a different blocker per D.5
    refinement: `emitter_does_not_declare_label`, not the misleading
    `ingestion_gap_surface_unresolved`.

The underlying ingestion gap (labelwatch's discovery pipeline missing
mod.bsky's service record) is **not patched** — only worked around
via the snapshot path. Forward fix would belong to labelwatch's
discover module proper.

---

## F-006 — `needs-review` is emitter-undeclared, not ingestion-gap-shaped

**Recorded:** Bundle D.5, 2026-06-08.

**Observation.** After landing the F-005 snapshot workaround, the
deriver can now distinguish "service record absent" from "service
record present but doesn't declare this label." Re-running on
moderation.bsky.app's `needs-review` shows the latter: the service
record IS in the snapshot (24 labelValues, 18 definitions), and
`needs-review` is in NEITHER list.

**Why this matters.** Three distinct shapes that previously all
classified as `ingestion_gap_surface_unresolved`:
  1. real ingestion gap (no service record on file at all)
  2. service record present but the label is genuinely undeclared
  3. service record present, label declared but with surface=unknown
     (caught earlier by `unknown_surface_not_specimen`)

Shapes 1 and 2 carry very different operational meaning. Shape 2
means the labeler is emitting events for a label its own published
policy doesn't define — operationally honored only by an
implementation rule that doesn't appear in any source-backed artifact
the deriver can consult.

**Diagnosis: not a bug; a refinement.** F-006 is the schema-refinement
finding that follows from D.5 wiring. No D-NNN disagreement; the
classifier and exporter were doing the right thing on the available
evidence, and now have richer evidence to distinguish two cases that
look identical from the outside.

**Patch applied (D.5):**
  - `LabelerEmitterDocumentation.labeler_service_record_present`
    boolean (true if a service record was found, regardless of
    whether it declares this particular label).
  - New exporter blocker `emitter_does_not_declare_label` for the
    case (first-party + consumer_scope=unknown + service record
    present + label not declared).
  - Existing `ingestion_gap_surface_unresolved` retained for the
    true ingestion-gap shape (no service record found anywhere).

**Detection-lane outcome:**
  - `needs-review` (mod.bsky) → BLOCKED with
    `emitter_does_not_declare_label`. Honest blocker per source-backed
    evidence.
  - `extremist`, `intolerant` (mod.bsky) → EXPORTED with
    `emitter_declared` via snapshot path.

**Forward note.** A label being emitted by an official_platform
labeler without a declared definition in the labeler's own published
service record is itself an interesting governance observation —
worth surfacing if more first-party labels show this pattern. v1
records it as a blocker; future work may produce a separate "emitter-
undeclared first-party label" report.

**Status:** patched in D.5.

---

## F-007 — Population-scale publish-without-declare

**Recorded:** 2026-06-08, from
[`docs/analysis/labeler-operator-maturity-001.md`](../analysis/labeler-operator-maturity-001.md).

**Observation:** In the operator-maturity scan of 150 observed
labelers, **14 high-volume labelers emitted labels over the last 30
days while publishing zero `labelValueDefinitions`** in their service
record.

Top of the cohort (events_30d):

| labeler | events / 30d | defs |
|---|---:|---:|
| `antiantiai.bsky.social` | 908,000 | 0 |
| `labeler.plural.host` | 906,500 | 0 |
| `oracle.posters.rip` | 698,575 | 0 |
| `uspol-labeler.bsky.social` | 446,585 | 0 |
| `bottags.bsky.social` | 121,267 | 0 |
| (… 9 more from 12k to 73k events/30d) | | |

Two of these emit at moderation.bsky.app's own volume (~908k
events/30d each).

**Why this matters:** Under the ATProto stackable-moderation
subscription model, client-side honoring via `labelersPref` depends on
a declared label vocabulary. A labeler with no definitions presents
no standard consumer-side semantic surface to subscribe to, even if
it emits high-volume labels.

**Generalizes F-006.** F-006 was one specimen: `needs-review` from
moderation.bsky.app. F-007 generalizes the same shape into an
**ecosystem pattern**: label publication is operationally separable
from declared consumer semantics, and the separation occurs at
non-trivial scale across many independent operators.

**Impact:** Consumers, auditors, and downstream tools cannot infer
that high-volume emitted labels correspond to an admissible
moderation surface. Emission volume is therefore not evidence of
usable protocol participation. A label-events firehose view of the
ecosystem systematically overstates the size of subscribable
moderation infrastructure.

**Refusal (the admissibility hook):**

> **Do not treat an observed label stream as subscribable
> moderation infrastructure unless the labeler also declares the
> consumed label values in its service record.**
>
> **Do not let "labeler exists" silently convert into "moderation
> service exists."** Label emission, declared semantics, and
> operational liveness are separate properties. The observed ATProto
> labeler ecosystem contains all three failure modes at scale.
>
> Core triad:
>
>   - **emission ≠ declaration** (this finding, F-007)
>   - **declaration ≠ liveness** (F-008)
>   - **liveness ≠ authority** (F-001, F-004, consumer-conversion census)

**Diagnostic class (per the user's framing — none of these is "bug
in labeler"):**
1. **Protocol affordance gap** — emitting labels is operationally
   easier than declaring usable consumer semantics.
2. **Client reality gap** — clients may rely on definitions; emitters
   may not provide them.
3. **Ecosystem measurement gap** — raw label volume overstates
   meaningful moderation infrastructure.
4. **Anthropology gap** — the "federated moderation ecosystem" is
   mostly not a mature operator field.

**Status:** recorded. No schema patch — the existing classifier
already produces the right verdict for any individual packet from
these labelers (`emitter_declares_no_rule_for_label` if a packet ever
got that far). F-007 is a population-level framing claim, not a
classifier bug.

**Forward path:**
- Maintain the operator-maturity scan as a periodic check; F-007
  cohort size + composition over time is the natural metric.
- Decide whether the per-labeler exporter should ALSO carry a
  population-context caveat ("this labeler's emission volume is in
  the publish-without-declare cohort"). Deferred — would couple
  per-packet exports to ecosystem state.

---

## F-008 — Stale-service / abandoned declared-scope no-op subscriptions

**Recorded:** 2026-06-08, from
[`docs/analysis/labeler-operator-maturity-001.md`](../analysis/labeler-operator-maturity-001.md).

**Observation:** Of the 150 observed labelers, **65 are abandoned**
(43%) — had service record on file, zero events in last 30 days.
Of those, **28 had substantial declared scope** (≥ 6
`labelValueDefinitions`) before going silent:

| labeler | defs |
|---|---:|
| `sonasky.app` | 684 |
| `stemlabels.xyz` | 461 |
| `pokemon.sonasky.app` | 161 |
| `label.wol.blue` | 119 |
| `cons.fyi` | 108 |
| (… 23 more 6–75 defs each) | |

**Why this matters:** A Bluesky user who subscribes to any of these
labelers via `labelersPref` would receive zero label events from
that subscription. The labeler is discoverable via the service
record and may even appear in client UIs as available, but the
subscription is operationally a no-op.

**Distinct from F-007 (sharper mirror framing):**

- **F-007:** label emission without declared consumer semantics.
- **F-008:** declared consumer semantics without operational label
  emission.

Same admissibility hook applies in both directions: declaration and
liveness travel separately, and the user-facing service-readiness
signal needs to know about both.

Together, F-007 + F-008 cover the two big failure modes the
operator-maturity scan surfaced at population scale:

- **F-007:** ~9% of observed labelers (14/150) emit substantially
  without declaring.
- **F-008:** 43% of observed labelers (65/150) appear abandoned; at
  least ~19% (28/150) retain substantial declared scope despite
  operational silence.

(Earlier draft conflated "43% abandoned" with "43% declare without
emitting" — the two numbers are different. 65 labelers are abandoned;
28 of those 65 had substantial declared scope before going silent.
Tightened on operator review.)

**Pathological subcase — definition churn without emission:**
`vocalabeller.kanshen.click` published **106,000 service-record
revisions** of a single labelValueDefinition with zero events in
the scan window. `cons.fyi`: 1,034 revisions, 108 defs, 0 events.
`labeler-bot-tan.suibari.com`: 866 revisions, 18 defs, 0 events.
Operationally pathological; likely stuck redeploy loops. The
abandoned set is not just "stopped" — some of it is "stuck."

**Refusal:**

> **Do not treat the presence of a service record as evidence of
> a live moderation service.** Subscribing to a labeler with
> declared scope and zero recent emissions is a no-op until the
> labeler resumes. A maturity / activity check should accompany
> any client-side discovery surface.

**Status:** recorded. Same diagnostic class as F-007 (protocol
affordance / client reality / measurement / anthropology gaps).
The operator-maturity table is the per-labeler artifact;
F-007 + F-008 are the population-level claims it supports.

---

## D-002 — Operator-maturity taxonomy is heuristic, not normative

**Recorded:** 2026-06-08 as a discipline note (no actual
operator-vs-classifier disagreement — a self-correction about how
the maturity table should be read).

**Resolution:** `discipline_note`.

**The note:** The `maturity_class` column in
`docs/analysis/labeler-operator-maturity-001.md` is a heuristic
categorization, not measurement. The classes (`experimental` /
`personal/reputational` / `community-service` /
`moderation-infrastructure` / `abandoned` / `unknown` /
`platform-root`) are operational cohorts with arbitrary thresholds
(`events_30d ≥ 100`, `events_30d ≥ 10000`, etc.). The boundaries
will shift on threshold tuning; specific cutoffs are defensible
but not principled.

**Do not:**
- Cite a specific labeler's `maturity_class` as a normative judgment
  about that labeler.
- Use the table as input to admissibility apparatus (the goblin
  math) — the maturity_class is descriptive, not provenance.
- Treat "experimental" / "abandoned" / etc. as accusation-shaped.
  They are SRE-shape, not moral-shape.

**Do:**
- Cite the table as aggregate signal about the ecosystem
  (histograms, distributional claims, cohort framing — F-007, F-008).
- Reproduce by running
  `docs/analysis/tools/operator_maturity_scan.py` against fresh data
  and noting threshold sensitivity.
- Re-tune thresholds if they stop matching operator intuition; record
  the tuning in this entry's revision history.

**Status:** discipline note, no patch required.

---

## T-001 — `likely_test_dev` heuristic mis-flags `xblock.aendra.dev` + `recordcollector.edavis.dev`

**Recorded:** 2026-06-08, from operator-maturity-001 Pattern 4.

**Item type:** technical hygiene (Labelwatch upstream classifier
debt). Distinct from F-NNN: this is a tool problem, not an ecosystem
finding.

**Observation:** Labelwatch's `labelers.likely_test_dev` column flags
two real high-volume third-party labelers as test/dev:

| handle | events_30d | declared defs | likely_test_dev |
|---|---:|---:|---|
| `xblock.aendra.dev` | 908,000 | 13 | 1 |
| `recordcollector.edavis.dev` | 50,727 | 67 | 1 |

`xblock.aendra.dev` is the labeler Bundle G's specimen-003 cites as
a real third-party consumer-adoption case — there is nothing
test/dev about it operationally.

**Impact:** The operator-maturity-001 scan used `likely_test_dev` as
an override that downgrades any flagged labeler to "experimental"
regardless of activity volume or declared scope. This silently
loses signal for at least two real labelers; the maturity table's
cohort sizes are slightly off as a consequence.

**Likely root cause (speculation, not yet confirmed):** the
`likely_test_dev` heuristic in labelwatch is probably matching on
handle pattern — `*.dev` TLDs, perhaps `recordcollector` or
`xblock` substrings. Worth a review pass on the upstream
classification code.

**What needs doing:**
1. Locate the `likely_test_dev` classification logic in labelwatch
   (likely `discover.py` or a derive pass).
2. Identify the false-positive pattern.
3. Either tighten the heuristic or carve out an allowlist for
   labelers like xblock.
4. Re-derive labelers + re-run operator-maturity scan; verify the
   two labelers reclassify out of `experimental`.

**Not blocking:** the cohort framing in F-007/F-008 doesn't depend
on these two labelers' specific classes. The maturity table is
descriptive aggregate; the mis-flags are noise at the
high-volume tail.

**Status:** recorded. Not patched — touches labelwatch's live
package code (not specimens-track scope). Pick up when the
`labelers` table classifications get their next review pass.

---

## T-002 — `docs/findings/` was not on the served Labelwatch surface

**Recorded:** 2026-06-08, immediately after pushing the operator-
maturity findings page (commit `078ff80`).

**Item type:** technical hygiene. Meta-instance of F-007's
publish-without-declare shape, applied to our own publication
pipeline:

  > "artifact exists in repo" silently converted into "artifact
  > exists on served public surface."

The push message could have read as "this is live." It wasn't.
`docs/findings/operator-maturity/index.md` was committed and
reproducible, but Labelwatch's live `report.py` publisher did not
copy or render anything under `docs/findings/` into the output
directory Caddy serves.

**Fix shape (the user picked option 1 — report.py-driven):**

Two published surfaces, intentionally separate URLs, both written
by `report.py`'s `generate_report()` each run:

  - `/findings/<topic>/` — **frozen** historical findings,
    rendered from `docs/findings/<topic>/index.md` with all
    `artifacts/` and `regression/` subdirectories copied verbatim.
    Reproducible from the repo; pinned receipts; admissible
    historical claim.
  - `/operator-maturity/` — **live** operational scan, regenerated
    from the current DB each run. Current measurements with
    provenance + caveat banner. Not a historical claim.

Both pages cross-link explicitly. Frozen page points at live; live
page points at frozen. Neither silently overwrites the other.

**Patch applied** (this commit):

- `pyproject.toml`: added `markdown>=3` dependency.
- New module `src/labelwatch/findings_pages.py`:
  - `install_frozen_findings(out_dir, layout_fn)` — copies
    `docs/findings/<topic>/` into `out_dir/findings/<topic>/`,
    renders `index.md` → `index.html` wrapped in the standard
    `_layout`, preserves `artifacts/` + `regression/` verbatim,
    writes `/findings/index.html` listing all topics.
  - `install_live_operator_maturity(out_dir, conn, layout_fn)` —
    queries the live `labelers` + `discovery_events` tables (same
    SQL shape as `docs/analysis/tools/operator_maturity_scan.py`),
    classifies via the same heuristic, renders `/operator-maturity/
    index.html` with provenance dl + the doctrine triad table
    populated from current data + maturity histogram + top-20 table.
- `report.py`: imports both functions and invokes them inside the
  `generate_report()` Static-prose-pages section, wrapped in
  `try/except` so a publication failure can't crash the main
  report.
- Homepage gets a `findings_callout` block — quiet pointer to the
  findings index, the named frozen finding, and the live page.
- Both rendered pages carry the discipline banner; the LIVE banner
  explicitly says "current measurements, not historical claim" with
  a link to the frozen one.

**Refusal (the discipline this T-item enforces):**

> Do not describe a findings page as "published" unless it is
> reachable from the served Labelwatch surface (`curl -fsS
> https://labelwatch.neutral.zone/findings/<topic>/` returns 200).

**Verification** (post-deploy):

```bash
curl -fsS https://labelwatch.neutral.zone/findings/ \
  >/dev/null && echo OK
curl -fsS https://labelwatch.neutral.zone/findings/operator-maturity/ \
  >/dev/null && echo OK
curl -fsS https://labelwatch.neutral.zone/operator-maturity/ \
  >/dev/null && echo OK
```

Receipts captured below the deploy.

**Status:** patched in code; reachability verified post-deploy
(see below or follow-up commit).

---

## D-001 — `!takedown` is render-layer per LABELS but hosting-layer in practice

**Packet:** `derived/derived-39516736-did-plc-ar7c4by46qjdydhdevvrndac-takedown.evidence.json`
**Specimen:** moderation.bsky.app emits `!takedown` on `did:plc:cthunuhvp2n3fgw7s7jdwp2n` at 2026-06-08T15:00:25Z.

**Classifier verdict:** `execution_gap_policy_present`
- `!takedown` is in the global `LABELS` map in `@atproto/api`
  (`defaultSetting: 'hide'`, `severity: 'alert'`, `flags: ['no-override', 'no-self']`)
- LabelObservation present, PolicyDocumentation status='documented', RenderObservation absent
- Classifier applies the rule "documented policy + no render witness = execution_gap_policy_present"

**Operator expectation:** the operator (labelwatch-claude during fixture
design) treated `!takedown` as a **hosting-layer constraint** — the
post is removed from the PDS by an admin action; the bsky client just
shows the absence (or a tombstone). The render-layer "blur" rule in
`LABELS` is downstream of, and predicated on, a removal that already
happened at the server. Treating `!takedown` as render-layer alone
loses that distinction.

**Diagnosis: schema_incomplete.**
The current `PolicyDocumentation` shape carries `policy_artifact +
extracted_rule + documented_expected_action` but has no field for
**which architectural layer the constraint operates at**. A label
whose primary effect is `app.bsky.feed.post` removal at the PDS
should not classify with the same `ConversionGap` value as a label
whose primary effect is client-side blur on a fully-present record.

**Patch required:**
1. Add `policy_layer` to `PolicyDocumentation`. Vocabulary candidates:
   `render` | `hosting` | `mixed`. (For `!takedown` it's `mixed`: the
   PDS removal is hosting-layer; the residual client render is
   render-layer.)
2. `derive_evidence.py` should populate `policy_layer` from a small
   table of known global labels. `!takedown`, `!hide`-on-account →
   `hosting`/`mixed`; `porn`, `sexual`, `nudity`, `graphic-media` →
   `render`. Unknown → `render` default with note.
3. `classifier.py` should emit a more specific gap value for
   `policy_layer=hosting`: e.g.
   `execution_gap_hosting_layer_constraint`. Render-layer remains
   `execution_gap_policy_present`.
4. Goldens for fixture 001 (`porn`) are unaffected (still render).
   No fixture for `!takedown` yet; D-001's resolution would create
   one as part of the patch.

**Audit result:** schema_incomplete (operator was right; classifier
honestly reported what the schema allowed it to see).

**Patch applied (this commit):**
1. Added `execution_surface` field to `PolicyDocumentation`. Vocabulary:
   `client_render | pds_hosting | mixed | unknown`. SOURCED FROM the
   policy artifact + a known-label semantics table in the deriver;
   describes WHERE the documented conversion acts; does NOT encode
   whether the conversion gap exists.
2. Added `HostingObservation` as a peer to `RenderObservation`.
   Surface-aware: `not_applicable` when the documented surface does
   not act on hosting; `absent` when it does but we have no
   hosting-side probe yet; `observed` when we do.
3. `derive_evidence.py` now populates `execution_surface` from
   `KNOWN_LABEL_SURFACE` (small hand-maintained table) and frames
   `RenderObservation` / `HostingObservation` to match the surface.
4. `classifier.py` `_classify_gap` now returns a struct
   `{name, surface}` and uses `_execution_witnessed_on_surface` to
   pick the right observation (Render for client_render; Hosting for
   pds_hosting; either for mixed).
5. Inadmissible claim set is now surface-aware: render-side claims
   fire when render_relevant + render absent; hosting-side claims
   (`no_individual_hosting_claim`, `no_population_hosting_claim`)
   fire when hosting_relevant + hosting absent.

**Patch verified against the original packet:**
- Re-derived `derived-NNNNN-...-takedown.evidence.json` shows
  `PolicyDocumentation.execution_surface = "pds_hosting"`,
  `RenderObservation.status = "not_applicable"`,
  `HostingObservation.status = "absent"`.
- Classifier output:
  `ConversionGap = {name: "execution_gap_policy_present", surface: "pds_hosting"}`.
- Inadmissible claim set now correctly includes
  `no_individual_hosting_claim` + `no_population_hosting_claim`
  instead of the render-side claims.

**Status:** patched-in-current-commit. Resolved.

**Forward note:** The patch does NOT yet ship specimen 003 as a
hand-authored fixture. The detection-lane `!takedown` packet
demonstrates the classifier behaves correctly under the patched
schema; a fixture-lane specimen 003 should follow once we have a
canonical hand-authored shape worth pinning.

---

## D-000 — (template, no entry)

When recording a new disagreement, copy this template:

```
## D-NNN — short title

**Packet:** path/to/packet.evidence.json
**Specimen:** description of the actual data row.

**Classifier verdict:** what classifier.py output.

**Operator expectation:** what the operator thought it should output, and why.

**Diagnosis:** classifier_wrong | schema_incomplete | operator_wrong (with one-sentence reasoning).

**Patch required:** concrete steps (changes to classifier.py / schema / fixtures / docs).

**Status:** open | patched-in-<commit> | wontfix (with reason).
```
