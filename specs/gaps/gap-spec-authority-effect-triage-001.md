# Gap spec: authority-effect triage 001 — rank unprofiled volume, propose receipted candidates

**Status:** ratified next-slice spec. Filed 2026-06-10 immediately after
`gap-spec-authority-effect-inference-v0.md`.

**Inherits the receipt shape from:**
`gap-spec-authority-effect-inference-v0.md` (the `labelwatch.authority_effect_inference.v0`
schema and tier stack).

**Companion:**
- `forward-note-authority-effect-report-lenses.md` (report-lens follow-ons
  that become useful once this slice closes the 61.1% gap)
- `docs/labelers-as-testimony.md` (parent doctrine)
- `docs/authority-failure-modes.md` (the seven failure modes this slice
  helps surface)

## Architecture sentence

> The dashboard already knows where the volume sits. This slice writes
> the queue and the proposals; humans ratify, the machine never
> auto-promotes a reputational claim.

## Why this slice now

The dashboard says: **1,355,787 events** (61.1% of 7-day network volume)
are `authority_effect = unprofiled`. The bucket split:

```
top-10 values     →  88.8% of unprofiled volume
top-5  labelers   →  85.6% of unprofiled volume
of top 20 values: 17 have a usable emitter description (Tier 2)
                   1 matches a pattern profile (Tier 3)
                   2 remain raw fallback (Tier 4)
```

That is not taxonomy debt. That is an instrument staring at the answer
and politely declining to read the label on the box. The corpus where
the proposal can be generated already exists in the database, and the
top-20 review is small enough to land without a cathedral.

Forcing-case status: **kernel-shaped surface — the classifier is the
substrate other lenses depend on.** Forward-note lenses 1, 4, 6, 8 all
materially improve when the classifier stops returning `unknown` for
61% of events. Forcing-case discipline applies.

## Target (brutal, on purpose)

> Reduce 7-day `authority_effect = unprofiled` volume from **61.1% to
> under 20%** using only top-20 value triage, **without** adding any
> unified trust score or subject adjudication.

20% is the ceiling because the forward-note rejection of social-function
and the weather-not-verdict constraint together rule out aggressive
auto-promotion. Anything below 20% is gravy.

## What gets built

### CLI

```
labelwatch authority-effect triage [--window 7d] [--top-values 20] [--top-labelers 10] [--out receipts/authority_effect_inference/]
```

Behavior:

1. Pull top-N label values + top-M labelers by `authority_effect = unknown`
   event count in the window.
2. For each candidate (labeler, value) pair, gather evidence:
   - emitter description from `labelers` row + cached labelValueDefinition
   - observed values (from `label_events`)
   - attachment loci (account / post / profile / list — extract from
     `label_events.uri`)
   - event_count, time_window
   - sample_targets (≤ 5, for citation)
   - co_occurring_labels (label values appearing on the same `target_did`
     within the window — citations only, not corroboration)
3. Run the tier stack from `authority-effect-inference-v0.md`:
   tier 1 registry → tier 2 emitter_described → tier 3 pattern_profile →
   tier 4 raw_fallback.
4. Emit a `labelwatch.authority_effect_inference.v0` receipt for each
   candidate with the `promotion_status` set per the rules below.
5. Emit a single `labelwatch.authority_effect_triage.v0` index receipt
   summarizing the run.

### Promotion rules

| Tier | Default promotion_status | Rationale |
|---|---|---|
| `registry` | (not emitted; tier 1 means already promoted) | n/a |
| `emitter_described` (confidence=high) | `needs_human_review` | High-stakes axis; even high-confidence emitter inferences want one set of human eyes before the registry adopts the framing |
| `emitter_described` (confidence=medium) | `needs_human_review` | obvious |
| `emitter_described` (confidence=low) | `refused_insufficient_evidence` | description was ambiguous; do not propose |
| `pattern_profile` (safe class: spam/scam/malware/phishing) | `auto_pattern_matched` | These have decades of precedent + low political surface. Safe to populate `AUTHORITY_EFFECT_MAP` without review |
| `pattern_profile` (reputational class: nazi/racist/terf/...) | `needs_human_review` | Reputational classifications are exactly where the operator must own the call |
| `pattern_profile` (visibility class: hide/mute/adult/...) | `needs_human_review` | Same reason: control-surface effects |
| `raw_fallback` | `proposed` (with `candidate_authority_effect=null`) | surfaces to unknown-label watchlist (forward-note lens 5) |

The `auto_pattern_matched` row is the **only** promotion path that
bypasses human review in v0. It is narrow: the safe class only.

### Index receipt — labelwatch.authority_effect_triage.v0

```json
{
  "receipt_kind": "labelwatch.authority_effect_triage.v0",
  "generated_at": "2026-06-..T..Z",
  "window": "7d",
  "params": {
    "top_values": 20,
    "top_labelers": 10
  },
  "input_state": {
    "total_events_in_window": 2219123,
    "unprofiled_events": 1355787,
    "unprofiled_share": 0.611,
    "top_values_share_of_unprofiled": 0.888,
    "top_labelers_share_of_unprofiled": 0.856
  },
  "queue": [
    {
      "labeler_did": "did:plc:...",
      "label_value": "substack",
      "unprofiled_event_count": 312044,
      "network_share_7d": 0.1407,
      "attachment_loci": ["post"],
      "tier_fired": "emitter_described",
      "candidate_authority_effect": "reputational",
      "confidence": "medium",
      "promotion_status": "needs_human_review",
      "receipt_path": "receipts/authority_effect_inference/did:plc:.._substack__20260610T....json"
    }
  ],
  "tier_breakdown": {
    "emitter_described": 17,
    "pattern_profile": 1,
    "raw_fallback": 2
  },
  "auto_pattern_matched_count": 1,
  "needs_human_review_count": 17,
  "refused_insufficient_evidence_count": 0,
  "proposed_unknown_count": 2,
  "config_hash": "sha256:..."
}
```

### Promotion CLI (separate from triage)

```
labelwatch authority-effect promote --from <triage-receipt> [--accept-status auto_pattern_matched]
```

Default behavior: apply the `auto_pattern_matched` rows to
`AUTHORITY_EFFECT_MAP` (writes a code change as a generated overlay file,
NOT a runtime mutation — keeps the registry under source control).
Surfaces the `needs_human_review` rows as a checklist for the operator to
hand-edit.

Hand-editing is the v0 path for `needs_human_review` rows. No interactive
prompt UI. The receipts are the queue; the operator reads the queue,
makes the call, edits `label_family.py`, commits. The promotion CLI is a
diff-helper, not an oracle.

## Dashboard / UI change

Replace today's flat unprofiled bucket with:

```
Authority-effect unprofiled: 1.36M events / 61.1%

Recoverable now:
  17/20 top values have emitter descriptions   [needs_human_review]
   1/20 matches known pattern profile          [auto_pattern_matched]
   2/20 raw fallback                            [proposed / unknown-watchlist]

[Review triage queue]   [Generate triage now]
```

The dashboard does not auto-trigger promotion. "Generate triage now" runs
the CLI; "Review triage queue" links to the receipt index for the most
recent run.

When clicked, individual candidates render per the rules in
`authority-effect-inference-v0.md § UI rendering rules`. Inferred is
visually distinct from registry. No camouflaging.

## Acceptance for this slice

1. `labelwatch authority-effect triage` runs against a fixture database
   and produces:
   - one `labelwatch.authority_effect_inference.v0` receipt per
     (labeler, value) candidate
   - one `labelwatch.authority_effect_triage.v0` index receipt
2. Top-20 values + top-10 labelers are accurately identified by
   unprofiled event count in the configured window.
3. Tier resolution follows `gap-spec-authority-effect-inference-v0.md`
   precedence: registry > emitter_described > pattern_profile >
   raw_fallback. Verified by fixture with one value covered at each tier.
4. Promotion-status defaults match the rules table above. Verified by
   fixture covering: safe-class pattern, reputational-class pattern,
   high-confidence emitter, low-confidence emitter, ambiguous emitter,
   raw fallback.
5. `auto_pattern_matched` is the ONLY status that bypasses human review;
   asserted in a test that scans the rules table.
6. `labelwatch authority-effect promote --from <triage-receipt>` applies
   `auto_pattern_matched` rows as a code overlay (a written file under
   source control), NOT a runtime registry mutation.
7. Dashboard surfaces the recoverable-now breakdown with the three
   counts (emitter / pattern / raw).
8. The promotion CLI surfaces `needs_human_review` rows as a checklist;
   does not auto-apply them.
9. After a real triage run + human ratification cycle on the top-20,
   7-day unprofiled share drops below 20%. (Operational acceptance, not
   a unit test.)

## Out of scope for triage 001

- LLM stage. Forcing-case gate stands; tier 4 `raw_fallback` candidates
  surface to the unknown-label watchlist (forward-note lens 5), not to
  an LLM.
- Per-target receipts. Inference is scoped to (labeler, value); targets
  appear only as citation samples.
- Auto-promotion outside the safe pattern class.
- Boundary-conflict matrix and the rest of the forward-note lenses. They
  become cheaper after the registry is populated; staffing each is a
  separate decision.
- A unified trust score. Reaffirmed: the four-dial constraint stands;
  this slice neither adds a dial nor collapses one.
- Subject adjudication. The triage acts on (labeler, value), never on
  per-target classification.

## Doctrine composition checks

| Constraint | Check |
|---|---|
| Detect-only structural | Receipts written; no labels emitted. ✓ |
| Aggregate-first | Scope is (labeler, value); target population is not addressed. ✓ |
| Weather, never verdict | Output is "we read this labeler's testimony as X"; subject is never the finding. ✓ |
| Co-presence is not corroboration | `co_occurring_labels` is citation evidence, NOT rationale support. Validated by review of rationale strings. ✓ |
| Four dials, not one trust score | Triage does not modify regime/auditability/inference-risk/coherence; it populates the classifier substrate those dials read from. ✓ |
| No LLM in the loop | v0 has no LLM stage; doctrinal placeholder only. ✓ |

## What this slice does NOT change

- `AUTHORITY_EFFECT_MAP` row format (still `dict[str, str]`).
- `LABELER_DEFAULT_EFFECT` row format.
- `classify_authority_effect()` signature.
- Boundary analysis, hosting-locus, regime detection, or any of the four
  dials.
- The detect-only structural constraint.

## Forward-link readiness check

After this slice ships AND the top-20 hand-ratification cycle completes:

- Forward-note **lens 1** (authority-effect conflict matrix) — becomes
  cheap to build because the matrix denominators are no longer 61%
  `unknown`.
- Forward-note **lens 4** (target blast-radius by authority_effect) —
  the per-class concentration numbers stop being lies.
- Forward-note **lens 5** (unknown-label watchlist) — wired directly to
  the `proposed` / `raw_fallback` rows from triage.
- Forward-note **lens 6** (reputational/enforcement coupling) — usable
  once `enforcement_instruction` and `reputational` have honest
  populations to coupling-analyze.

None of those lenses are authorized here. Staffing is a separate
decision when an operator surface needs one of them.

## Acceptance for this spec (not implementation)

- [x] CLI shape named.
- [x] Receipt outputs enumerated (per-candidate + index).
- [x] Promotion rules tabled with status defaults.
- [x] `auto_pattern_matched` narrowness called out + tested-in-spec.
- [x] Brutal target (61.1% → <20%) preserved.
- [x] Dashboard copy concrete.
- [x] Doctrine composition table present.
- [x] Out-of-scope list distinct from the inference-v0 out-of-scope list.

## Suggested commit name when implemented

```
authority-effect-triage: labelwatch.authority_effect_triage.v0 — rank unprofiled volume, propose receipted candidates
```
