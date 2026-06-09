# Observation export custody

**Status**: candidate doctrine. Spine ratified; refusals load-bearing; extensions and implementation consequences staged.
**Companion**: [labelers-as-testimony.md](labelers-as-testimony.md) · [authority-failure-modes.md](authority-failure-modes.md) · [DISAGREEMENTS.md](specimens/DISAGREEMENTS.md) (no-laundering theorem)

## The spine

> Labelwatch may export observation custody.
> Labelwatch must not export authority.
>
> No export may imply completeness, ordering, adoption, endorsement, or judgment beyond what a single observer, using its own clock, resolver, cache, and log, can directly attest.

That paragraph is the ratified part of this doc. Everything below it elaborates the spine into specific refusals, into extensions implied by the spine but not yet equally hard-forced, and into implementation consequences. Candidate-not-binding for any clause not under "Refusals."

Compressed form, for the doctrine block:

> Integrity is not completeness. Sequence is not network order. Verification failure is not labeler fault. Observation is not adoption.

## Scope and promotion rule

This doctrine is written for Labelwatch — the wound is here, the forcing cases are here, and the export surfaces it constrains are Labelwatch's. The doctrine generalizes naturally to any observatory or exporter that emits machine-consumable claims about externally originated artifacts (driftwatch consumer-policy v1.0.0 is the obvious next candidate), but **promotion to workbench-level doctrine is gated on a second observer or exporter hitting the same boundary**. Until that happens, this doc stays where the failure lives.

The telescope writes in its own logbook first. The observatory union can form later.

## Refusals (load-bearing now)

These six refusals are what "must not export authority" means in concrete terms. They are mechanically distinct failure modes, each of which has been identified at synthesis time, each of which would otherwise let Labelwatch quietly become an authority by accident.

### 1. Integrity is not completeness

A Merkle root over a receipt bundle commits to *integrity and membership* — "these N receipts, untampered, here is the root." It does not commit to *completeness*. But completeness is the entire rhetorical force of a hash-rooted bundle the moment it is published: every consumer reads "receipts A..N under root R, generated at T" as "this is what Labelwatch saw in window W" — *the* set, not *a* set.

The integrity guarantee launders the completeness claim. The cryptographic seal makes the completeness implication feel earned. Of the available export shapes, the bundle is the most dangerous precisely because it is the most disciplined-looking.

The safe claim of a bundle is:

> This artifact contains these receipts, and they have not been altered.

The unsafe implied claim, which the bundle's shape silently asserts unless the type explicitly refuses it, is:

> This artifact contains the receipts for that period / labeler / network slice.

A bundle therefore must carry, in its type and at the surface of its payload:

- `completeness: non_complete_subset`
- `absence_semantics: absence_implies_nothing`
- `coverage_claim: none`

A bundle may not carry a bare `window_start` / `window_end` pair unless those fields are explicitly typed as **export construction window**, not as observed-world coverage window. Otherwise the bundle has invented a notarized census.

### 2. Sequence is not network order

`observed_at` — even when scoped as `observed_at_by_labelwatch_clock` — is too tempting an ordering primitive to leave unsigned. Two receipts with timestamps and somebody sorts by them. Then NTP steps, VM migrations, backwards clock adjustments, daemon restarts, and any number of other quiet sources of monotonicity loss smuggle network-ordering error back into the export under Labelwatch's own unreliable narration. The timestamp is the observer's clock attesting to itself — and not even that, beyond what an undisciplined clock can honestly attest.

The export must use the local log sequence (or export position) as the **sole** ordering primitive. `observed_at` is demoted to advisory, non-ordering metadata.

Required schema:

- `log_sequence: <integer>` — the sole admissible ordering field
- `observed_at_labelwatch_clock: <ISO8601>` — non-ordering, advisory only
- `timestamp_semantics: advisory_non_ordering` — explicit at the type level

Doctrine:

> Receipts are ordered only by log position / export sequence. Timestamp comparison is inadmissible as an ordering claim.

Clocks lie. Worse, clocks lie politely in ISO 8601. Logbook page numbers order the log; the times written inside the entries do not.

### 3. Verification failure is not labeler fault

A verification-failure receipt — "failed under key K, reason R" — carries a heavier custody burden than its success counterpart. "Y emitted an invalid signature" *looks* like an observation but is in fact an accusation whose truth depends on a key-resolution context Labelwatch may not actually hold cleanly. DID documents rotate. Resolver caches stale. A failure receipt that does not bind the resolution context can become a false accusation when the real story is Labelwatch's own stale cache missing a rotation. Exporting that as an observation is laundering a judgment dressed in a checksum.

A failure receipt may only say:

> Labelwatch failed to verify artifact X under resolution context C at local time T.

The resolution context C must be nailed down at the type level:

- `did_doc_hash`
- `resolved_at_labelwatch_clock`
- `resolver_id` / `resolver_version`
- `cache_status`
- `key_id`
- `key_material_hash`
- `verification_algorithm`
- `failure_reason`

Otherwise stale DID resolution becomes defamation with a checksum.

### 4. Absence implies nothing

Absence of a receipt in any export is silent over every world claim that could be inferred from it. Specifically: a receipt's absence does not entail that the underlying event did not occur, that Labelwatch did not see it, that any consumer can rely on its non-occurrence, or that an aggregate count of present receipts is the count of events. This refusal is the dual of #1 — it removes the "by elimination" path that would otherwise let a careful reader reconstruct a completeness claim out of repeated bundle observation.

Doctrine: every export must explicitly carry `absence_semantics: absence_implies_nothing` at the bundle level, and consumers must be unable to obtain any export shape that contradicts this.

### 5. observed_at is non-ordering metric

Restates #2 from the field-shape side. `observed_at` may appear as a metric (rate, freshness signal, age indicator) but never as a sort key, never as an admissibility criterion for downstream filtering, and never as a binding for "before/after" comparison between any two receipts. Schema-level enforcement: any export pipeline that exposes `observed_at` as a sortable column violates the doctrine and must be refused at the type layer.

### 6. Receipt bundle / root is membership and integrity only, not coverage

Restates #1 from the bundle-payload side. The receipt root attests that the named receipts have not been tampered with and were jointly committed to. It does not attest that those are the receipts for any time window, labeler, target population, or network slice. Bundle metadata fields that imply coverage — `window_start`, `window_end`, `subject_population`, `slice_definition` — are forbidden unless they are explicitly typed as construction parameters of the export, not as descriptions of the observed world.

## Extensions (implied by spine, not yet forced equally hard)

These are clauses the spine logically implies but which lack independent forcing cases sharp enough to make them load-bearing today. They are filed here so the doctrine doesn't drift into pretending they're optional and so the next operator knows what's been seen-but-not-yet-ratified.

### Derived symmetry: success receipts inherit the failure-context binding

A *failure* receipt carries the visible accusation; a *success* receipt carries an implicit "key material K, resolution context C, resolved cleanly" claim that is symmetrically false-positive-able if the resolution was stale-but-coincidentally-matching. The custody fix in refusal #3 — binding the full resolution context — applies to **both** verdicts, not just failure. Otherwise the success path quietly attests to a custody chain Labelwatch does not actually hold. The accusation risk is lower on the success side, but the laundering shape is the same.

Status: **derived symmetry**. Promote to load-bearing on the first incident where a success receipt is read as endorsement of resolution provenance Labelwatch did not actually possess.

### Generalization boundary: observatory family

Driftwatch consumer-policy v1.0.0 (`b4a8e3e`) is the first declared consumer of Labelwatch's published surfaces and writes its own receipts when adopting third-party labels. Those receipts are the same shape of export, and the same laundering risks apply. The doctrine generalizes here naturally; the surface area expands to wherever this family writes machine-consumable claims about externally originated artifacts.

Status: **generalization boundary**. Promote to workbench-level doctrine on the first concrete forcing case in driftwatch (or any new observer) where one of refusals #1–#6 fails in a different shape. Until then, repo-local with explicit scope clause is the right altitude.

## Implementation consequences (not current implementation mandate)

These are the consequences the doctrine entails for any future implementation work. They are not a build mandate — they describe what the future gap-spec is allowed to mean.

### Schema-level enforcement

Doctrine that lives only in prose drifts back. The mechanism for keeping these refusals alive is that the export schema *cannot represent* the laundering shapes:

- A bundle type cannot be constructed without `completeness: non_complete_subset` and `absence_semantics: absence_implies_nothing` — these are required fields with a single allowed value, not optional fields with safe defaults.
- A receipt's `observed_at` field must be typed such that the export pipeline (and any consumer schema-checker) treats sort-by-`observed_at` as a schema violation, not merely as discouraged practice.
- A verification-result type must require the full resolution context (`did_doc_hash`, `resolved_at`, `resolver_id`, `cache_status`, `key_id`, `key_material_hash`, `verification_algorithm`) for both `verified` and `failed` outcomes. Either branch must be unconstructable without it.
- Bundle metadata field names that imply coverage (`window_start`, `window_end`, `subject_population`, `slice_definition`) are reserved and must be typed as construction parameters, never as world descriptions.

The next gap-spec for export work treats schema-level enforcement of these refusals as the acceptance criterion. Prose enforcement is insufficient. If the type system allows the laundering shape, the laundering shape will eventually be written.

### Tests as receipts of refusal

Each refusal in this doc should be expressed as a test that asserts the type system rejects the laundering shape — not as a test that asserts well-formed exports pass. The asymmetry is intentional: the value of the test is the receipt that the laundering shape was tried and refused.

## The metaphor

> Labelwatch may publish signed log entries. It may not publish a navigational chart.

The telescope can absolutely have a logbook. It just cannot emboss the logbook with a seal and a page count and leave it on the harbormaster's desk — because that is how a logbook becomes Greenwich without anyone deciding it should.
