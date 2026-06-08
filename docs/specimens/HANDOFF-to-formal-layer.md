# Handoff to formal layer (paper / Lean Claude)

**From:** labelwatch-claude
**Date:** 2026-06-08
**Specimens:** [001](specimen-001-moderation-bsky-app-porn.md), [002](specimen-002-xblock-twitter-screenshot.md)
**Lean target:** NQ-side admissibility / ConsequencePartition kernel (evidence-backed claims authorizing transitions, receipt replay, witness/testimony boundaries, refusal of laundering).
**Explicitly NOT the target:** driftwatch's `formal-claim-admissibility` gap spec (`439100c`, "Z3 first, Lean later") — wrong altitude; that asks *is this claim well-shaped?* not *when may testimony become constraint?*

## The demand

Given the specimens below, formalize the minimum distinction between:

1. **testimony observed**
2. **policy / documented conversion rule**
3. **constraint applied** (would-apply, given a context)
4. **render / hosting execution witnessed**

The calculus must **refuse any rule that derives constraint directly from label issuance.** That refusal is the load-bearing theorem; everything else is scaffolding.

## The specimen pair, in calculus shape

The empirical layer reduces every conversion autopsy to three claims:

```
A. label_observed(labeler, target_uri, label_value, ts)
B. policy_declares(consumer_config, label_value, action, policy_artifact)
C. render_observed(consumer_config, target_uri, viewer_context, action, observed_at)
```

| | 001 | 002 |
|---|---|---|
| issuer | moderation.bsky.app (first-party) | xblock.aendra.dev (third-party) |
| label_value | `porn` | `twitter-screenshot` |
| A: label_observed | **witnessed** by Labelwatch | **witnessed** by Labelwatch |
| B: policy_declares (default consumer) | **documented** — pinned to `@atproto/api` v0.19.17 `packages/api/src/moderation/const/labels.ts` (HEAD `7b8c5d60a`); `defaultSetting: 'hide'`, `blurs: 'media'`, configurable | **absent** — no entry for `twitter-screenshot` in global `LABELS` map |
| C: render_observed | **absent** — no per-render receipt exists in atproto today | **absent** (and B-conditional) |
| admissible | "documented policy would apply under stated context" | "no constraint follows under default consumer" |
| classification | execution gap | no-conversion-rule (clean) |

## What the formal layer should produce

The minimum slate, in priority order:

1. **The smallest Lean vocabulary needed** for the three-claim schema.
   - Suggested predicates: `label_observed`, `policy_declares`, `render_observed`, plus a consumer-configuration term so `policy_declares` is parameterized by *which consumer*.
   - A render context type (client × viewer settings × adult-content gate × label-setting overrides) is needed for any honest B → expected-action mapping in 001.
   - A `policy_artifact` term so policy declarations cite a specific pinned source (package version + file path; the live consumer running a particular version is a separate claim).

2. **The admissible conclusions for each specimen.** Stated explicitly:
   - **001 admissible:** A holds; B holds for the default consumer pinned to artifact P; therefore "under render_context CTX, the default consumer would apply action `blur(media)` to target_uri TGT" is a deducible *conditional* statement. No statement about actual rendering.
   - **002 admissible:** A holds; B is absent for the default consumer; therefore "no constraint follows from this label under the default consumer's policy" is deducible. The deduction is over the *absence* of B, not over a positive policy claim.

3. **The inadmissible conclusions** (the no-laundering boundary):
   - **001 inadmissible:** "this post was blurred for user U at time T" without a render-side witness; "all renders of this post are blurred" (population claim from a single inferential rule); "the label's existence entails the constraint's application."
   - **002 inadmissible:** "this label produces no constraint anywhere" (silent over opt-in consumers); "the label is unimportant" (the calculus is silent on importance).

4. **At least one no-laundering theorem.** Suggested shape:
   ```
   ¬ (label_observed(L, T, V, _) ⊢ render_observed(C, T, _, _, _))
   ```
   That is: from issuance alone (claim A) you cannot derive an execution witness (claim C). The arrow only exists via B + a render context + (for honest production claims) a render-side witness.

   A stronger version that may or may not be reachable in v1:
   ```
   render_observed(C, T, CTX, A, _) ⊢ ∃ artifact. policy_declares(C, V, A, artifact)
                                       ∧ label_observed(L, T, V, _)
                                       ∧ CTX consistent_with artifact
   ```
   Reading: any honestly-witnessed render that names a label-derived action must trace back to a documented policy that names that action *and* a label whose value triggers it *and* a render context the policy was meant to apply to.

5. **A gap classification type.** Discriminated union over the four states:
   ```
   inductive ConversionGap
     | observability_gap         -- A is absent
     | conversion_witness_gap    -- A holds; B is partial (consumer named, policy known, but binding "consumer X applies policy version Y" is documentary not receipt-bearing)
     | execution_gap             -- A holds; B holds (or documented); C absent
     | no_conversion_rule        -- A holds; B explicitly absent for the consumer in question
     | complete_path             -- A, B, C all witnessed (does not arise in the wild yet)
   ```
   The discriminator is exhaustive over the (A, B, C) presence pattern AND the "documented vs witnessed" axis on B.

## Constraints on the formalization

- **Brutalist register required.** Vocabulary must match the spec: `label_observed`, `policy_declares`, `render_observed`, `policy_artifact`, `consumer_config`, `render_context`. Compression to "moderation," "applied," "user saw," etc. without surface preservation is a register violation.
- **Refusal is first-class.** When a claim cannot be made (e.g., C absent), the calculus must produce a typed *refusal* — not just a missing proposition. Reuse NQ's basis_state vocabulary if it fits (`witnessed | documented | absent | unknown`).
- **Consumer is parameterized, not global.** "The bsky client" is not a uniform object. Default-consumer and opt-in-to-xblock are two configurations of the same client; they have different policy maps. The calculus must take consumer configuration as an argument.
- **Co-presence is not corroboration.** If a future specimen has two labelers both labeling the same post with the same value, the calculus must refuse the trivial composition unless independence + commensurability are proven.

## What the empirical layer will and won't provide

**Will:**
- More A claims (label_observed events) at any volume needed; Labelwatch ingests ~2M events/week from ~500 labelers.
- More B claims (policy_declares) for labelers whose service records are observable — including service-defined `labelValueDefinitions` from `app.bsky.labeler.service` records on labelers' PDSes.
- Per-cohort lifetime stats (see `lifetime.py`) for adversarial-testing the "labels are persistent" half of the inheritance discipline argument.

**Won't (without new instrumentation):**
- C claims (render_observed). atproto publishes no per-render receipts. Producing C requires either a protocol change OR external probes (puppeteer-loaded synthetic-user campaigns).
- Receipts for "consumer X is running policy version Y at time T." Possible to infer from client version reports if anyone publishes them, but not currently observable.

## Suggested first formal artifact

Three short Lean files, none of them ambitious:

1. `Calculus/Claims.lean` — the three-predicate schema + consumer/context/artifact types.
2. `Calculus/Refusal.lean` — the no-laundering theorem and one or two adjacent ones (inheritance, absent-B handling).
3. `Specimens/Spec001.lean` and `Specimens/Spec002.lean` — the two specimens encoded as concrete instances, exercising the schema and verifying the admissible/inadmissible conclusions.

If those three compile and the no-laundering theorem proves, the pair has bitten the formal layer cleanly. Anything beyond that is bonus.

## Open questions for the formal layer to answer

1. Does the NQ basis_state vocabulary (`live | stale | retired | invalidated | unknown`) fit `policy_declares` as well? If yes, does "documented but the live client may run a different version" become `basis_state = stale` or `basis_state = unknown`? The answer shapes how 001's B claim is rendered.
2. Should `render_context` be a record/struct or a free-form proposition the calculus has to manipulate? Suggests record + structural equality.
3. Is the conversion-witness gap (documentary-but-not-receipted) a permanent feature of atproto, or a temporary instrumentation gap? If permanent, the calculus must support reasoning that explicitly never reaches a *witnessed* B for any default-client conversion — a structurally honest finding.

## Next specimens (queued in labelwatch, not started)

- **003 — `!takedown`**: hosting/removal-layer constraint, not render-layer. Useful for a second axis after the render-layer formalization lands. Likely smaller execution gap (the post genuinely disappears from a host that publishes the takedown receipt), but the consumer surface is different (PDS, not client).
- **002b — opt-in xblock subscriber**: same `twitter-screenshot` label, different consumer configuration (xblock subscribed). Tests whether the parameterized-consumer machinery actually distinguishes the two configurations.

## Pointer back

This handoff sits in `labelwatch/docs/specimens/`. The labelwatch repo has the empirical instruments; the formal layer lives outside this repo (per `labelers-as-testimony.md` §"Where this connects to other work" — the Admissibility / ConsequencePartition annex). Any clarification needed on the empirical side, fire questions back here.
