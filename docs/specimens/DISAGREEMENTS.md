# Specimen DISAGREEMENTS + FINDINGS log

Two kinds of entries:

**D-NNN — disagreements.** The classifier's verdict on a real-data
evidence packet differs from what the operator expected. Each disagreement
resolves to one of:

- **classifier_wrong** — code has a bug; fix in `classifier.py`
- **schema_incomplete** — evidence vocabulary doesn't capture the
  distinction the operator was making; fix the schema (`*.evidence.json`
  shape) and update `derive_evidence.py` + `classifier.py` together
- **operator_wrong** — the operator's pre-judgment was the laundering
  shape; classifier is honest; resolution is a write-up explaining why
  the operator's intuition was off

**F-NNN — findings.** Non-disagreement observations from the detection
lane that are worth recording even when classifier and operator agree.
Typical shape: an operator intuition that the schema correctly refuses
to encode, or a coverage gap surfaced by a batch run.

Per the methodology: "the first real success is not agreement with the
operator. The first real success is a schema-grounded derived verdict
that disagrees with the operator and survives audit." Empty log = the
mechanization isn't yet paying for itself.

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

**Status (updated by Bundle C):** **partially mechanized.** Service-record provenance now flows into the evidence packet as `LabelerEmitterDocumentation`; the classifier reads it and adds `consumer_scope=emitter_declared` to the gap struct. Three Bundle B bite points migrated from `consumer_scope=unknown` to `consumer_scope=emitter_declared` (fringe-media, twitter-screenshot, fucked-up-replyref). The invariant test confirms service-record labels are NEVER silently promoted to `global_platform`. Full multi-consumer subscription modeling (opt-in evidence → `opt_in_consumer_observed`) remains deferred to Bundle D+.

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
