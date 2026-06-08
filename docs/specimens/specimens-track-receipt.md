# Specimens track — closure receipt

Closing receipt for the Labelwatch specimens / exporter ladder.
This file is the institutional memory anchor for the D / D.5 / E / F
arc. After this commit, the branch pauses unless an explicit
disagreement hunt or consumer-adoption track is started.

## 1. Purpose

Test whether real Labelwatch packets can be classified without
laundering testimony into constraint. The mechanization had to:

- distinguish label *observation* from any claim about label *effect*
- distinguish documented policy from inferred policy
- distinguish emitter declaration from consumer adoption
- distinguish snapshot/discovery/protocol bytes as evidence kinds
- refuse to export packets where any of the above is unresolved

The hypothesis under test was that a deterministic classifier +
exporter could uphold the doctrine
"display ≠ constraint, annotation ≠ adoption, adoption requires a
policy and an effect" on a real corpus without smuggling claims past
its own refusal gates. The hypothesis held — the gates fired
correctly on each surfaced bite point.

## 2. Ladder

The four-rung ladder, in commit order:

| rung | commit  | summary |
|------|---------|---------|
| D    | `d763733` | Exporter skeleton with refusal gates. Hard discipline: unknown is not a specimen; preserve ambiguity rather than laundering it. Refusal vocabulary: `no_label_observation`, `unknown_surface_not_specimen`, `ingestion_gap_surface_unresolved`, `provenance_unresolved`, `missing_required_basis`. |
| D.5  | `828f430` | Resolved F-005 at the ingestion boundary via `service_record_snapshots/` fallback. Added the refined blocker `emitter_does_not_declare_label` to distinguish "service record exists but does not declare this label" from "no service record at all." needs-review correctly diagnosed. |
| E    | `2f918bf` | State-basis freshness gate. Evidence packets gain a `StateBasis` block; exporter classifies `current_basis` / `stale_basis` / `unknown_basis` / `missing` and blocks freshness-lane packets that fail to name their basis. Freshness preserves, caveats, stales, or blocks — never promotes. |
| F    | `c15f079` | Discover repair via `backfill_service_record_via_appview.py`. mod.bsky's service record now lives in canonical `discovery_events`; snapshot fallback retained as a true fallback only. Regression test asserts the source path is classification-invariant. |

Lead-up commits (Bundles A/B/C and the prior specimen 001/002 + fixture/classifier setup) are at `4c49db4`, `8993969`, `cad5763`, `7867c8c`, `6b81dd8`, `02929ca`, `fa71fe5`, `7a6669c`.

## 3. Findings

Recorded in `DISAGREEMENTS.md`. Two kinds: **D-NNN** are disagreements
that resolved as schema_incomplete (the classifier/operator delta
forced a schema refinement). **F-NNN** are non-disagreement findings
surfaced by the detection lane.

| id    | kind | status                                  | summary |
|-------|------|------------------------------------------|---------|
| D-001 | schema_incomplete | patched in `6b81dd8`     | `!takedown` is render-layer per LABELS but hosting-layer in practice. Forced the addition of `execution_surface` (`client_render | pds_hosting | mixed | unknown`) and `HostingObservation` as a peer to `RenderObservation`. |
| F-001 | recorded         | Bundle A close, `02929ca`| Reference-labeler status does not imply default-client conversion. Skywatch's `fringe-media` and unknown third-party labels both classify as `conversion_witness_gap_no_consumer` for the default consumer. Reference status is Labelwatch calibration choice, not consumer adoption claim. |
| F-002 | patched in `fa71fe5` | corrected | `GLOBAL_LABELS` hardcode was 21 entries; upstream `@atproto/api` LABELS has 8. 13 fictional entries removed; `gore` added. |
| F-003 | patched in `fa71fe5` | corrected | `!takedown` is documented at protocol level, not in `@atproto/api` LABELS. Added `PROTOCOL_DOCUMENTED_LABELS` and `artifact_kind` field (`upstream_const` / `protocol_doc`). |
| F-004 | partially mechanized in `7a6669c` | refined further by F | Third-party labelers publish `labelValueDefinitions` in service records; opt-in consumers honor them. Added `LabelerEmitterDocumentation` field and `consumer_scope` (`global_platform` / `emitter_declared` / `opt_in_consumer_observed` / `unknown`). |
| F-005 | bytes-level resolved in `c15f079` | refined | mod.bsky's service record is not in labelwatch's `discovery_events`. Bundle F's appview backfill tool ingests it into the canonical table; snapshot fallback retained for other gaps. |
| F-006 | patched in `828f430` (diagnostic refinement) | recorded | `needs-review` is emitter-undeclared, not ingestion-gap-shaped. moderation.bsky.app emits 27k events/7d of `needs-review` but its own published service record declares no rule for it. |

Total: one disagreement + six findings across the arc, all routed
into either a schema patch or an explicit recorded refinement. None
silently absorbed.

## 4. Current guarantees

The exporter, with the deriver + classifier behind it, guarantees:

1. **Unknown/unresolved does not export as global.** Surface-unknown
   evidence emits `execution_gap_surface_unknown`; first-party
   `consumer_scope=unknown` blocks with `ingestion_gap_surface_unresolved`
   or `emitter_does_not_declare_label`; third-party
   `consumer_scope=unknown` blocks with `provenance_unresolved`.

2. **`emitter_declared` remains non-global.** Service-record provenance
   is preserved as scoped, caveated evidence. The classifier's
   `_classify_consumer_scope` MAY emit `emitter_declared` only when no
   `global_platform` `PolicyDocumentation` exists; precedence is
   `global_platform > emitter_declared > unknown`. Exported
   `emitter_declared` candidates carry the `non_global_provenance` caveat.

3. **`global_platform` comes only from upstream/protocol basis.**
   `upstream_const` and `protocol_doc` are the only `artifact_kind`
   values that yield `global_platform`. Service-record content cannot
   be silently promoted.

4. **Freshness never upgrades authority.** Bundle E added a state-basis
   discriminator (`current_basis` / `stale_basis` / `unknown_basis` /
   `missing`) that runs independently of `consumer_scope`. `stale_basis`
   and `unknown_basis` always carry a caveat; `missing` always blocks
   the freshness lane. A freshness lane is gated; an authority lane
   is informational.

5. **Source path is classification-invariant.** Bundle F's regression
   test (`test_source_table_is_classification_invariant`) proves that
   two evidence packets differing only in
   `service_record_provenance.source_table` produce identical gap,
   scope, exporter decision, and caveats. The classifier does not
   worship a table name.

6. **Schema refusals are typed.** Every refusal has a `blocker` name +
   `reason` + `what_would_unblock`. Downstream consumers (Lean,
   fixtures, future exporters) can refuse by name, not by guess.

## 5. Explicit non-guarantees

The track does NOT guarantee any of the following. These remain
honest absences, not failures.

1. **No render execution receipts.** ATProto publishes no per-render
   receipts in the wire protocol. `RenderObservation.status='absent'`
   is the architectural baseline; `render_execution_unwitnessed`
   caveat is on every `client_render` export.

2. **No hosting execution receipts.** Same shape for pds_hosting
   surfaces. v1 has no hosting probes; `HostingObservation.status='absent'`
   is the baseline; `hosting_execution_unwitnessed` caveat is on every
   `pds_hosting` export.

3. **No population claims.** Per the inadmissible set:
   `no_individual_render_claim` / `no_individual_hosting_claim` /
   their `_population_` counterparts. The exporter does not produce
   "all renders of this post did X" claims.

4. **No live freshness horizon.** The deriver always sets
   `freshness_horizon='unknown'`. The `current_basis` and `stale_basis`
   code paths exist and are test-covered, but no real packet exercises
   them yet. Live fetch is not magically current forever.

5. **No multi-consumer subscription graph.** `opt_in_consumer_observed`
   is a reserved `consumer_scope` value with no current code path that
   populates it. Bundle G (if started) would be the place.

6. **No `PolicyWitness` constructor consumed by Lean.** The formal
   layer accepted the derivation frame in spring 2026 but no specimen
   has been promoted into Lean. The classifier currently emits
   `PolicyWitness.status='partial_documentary_not_receipted'` on
   every documented-policy export, and the
   `policy_witnessed_documentary_only` caveat flows accordingly.

7. **No "specimen 003" hand-authored fixture.** Roadmap held; the
   detection lane's `!takedown` packet exercises the protocol_doc /
   pds_hosting path and a fixture has not been forced. Will follow
   when a canonical shape becomes worth pinning.

## 6. Parked work

In rough order of "next likely surface if the track resumes":

- **Live `discover.py` / `discovery_stream.py` scheduled backstop.**
  Bundle F got the bytes into `discovery_events` once; auto-refresh
  on a schedule is live-package refactor work that belongs to a
  labelwatch infrastructure cycle, not the specimens track. When done,
  delete the `service_record_snapshots/` workaround.

- **Opt-in consumer adoption — Driftwatch as documented consumer
  (proposed Bundle G).** The current schema collapses
  `consumer_scope=emitter_declared` against
  `consumer_scope=opt_in_consumer_observed` because no consumer has
  documented adoption. Driftwatch is a viable candidate: a third-party
  consumer that could publish a policy artifact mapping
  `emitter_declared` labels of class X from labeler L into a local
  Driftwatch action. The acceptance criteria would be:
    1. real Driftwatch policy artifact, versioned, with effect
    2. real receipt of execution (state transition, quarantine,
       routing, exclusion — not display)
    3. classifier produces `consumer_scope=opt_in_consumer_observed`
       with `consumer_scope_effective` scoped to Driftwatch, never
       promoted to `global_platform`
    4. non-global provenance + emitter caveats inherited, not erased
  Discipline: "display ≠ constraint, annotation ≠ adoption."

- **Mixed-execution-surface labels.** `KNOWN_LABEL_SURFACE` has no
  `mixed` entries yet because no global label documentably acts on
  both client_render and pds_hosting. The classifier's mixed branch
  exists and is test-covered. Wait for real data; do not invent.

- **Real render / hosting probes.** Bundle E's stale/current paths
  would have something to bite on if external probes started landing
  in evidence. Synthetic users + appview reads is a tractable
  starting point; v1 has none.

- **Lean promotion.** Reserved for when the exporter coughs up
  repeated real specimens showing the same bad inference more than
  once. The current artifact distinctions (
  `unknown / emitter_declared / global_platform / unknown_basis /
  emitter_does_not_declare_label`) are sufficient for the current
  corpus. The next formal pressure most likely arrives in one of:

    - "policy artifact captured after label emission"
    - "service record current now but unknown at label time"
    - "snapshot-derived declaration with unknown validity horizon"
    - "stale policy doc still being used to export global claim"
    - "live fetch says declared, snapshot says undeclared"

  Until then, exporter-side gating is enough.

## 7. Disposition

This receipt closes the D / D.5 / E / F arc. The exporter is
deterministic, the classifier is pure, the refusal gates are typed,
the regression coverage holds, and every refusal carries enough
provenance to re-enter the system later without losing its
diagnosis.

The next move is either Option A (stop here and switch projects) or
Option C (Bundle G consumer-adoption via Driftwatch). Either is fine.
A wider disagreement-hunt (Option B / Bundle G as field-scan) would
also be in scope. Whichever is chosen, this receipt is the anchor
point.

The little goblin has learned timestamps but not confidence. That is
the right amount of progress to ship.
