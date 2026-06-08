# Specimen 002 — xblock.aendra.dev / `twitter-screenshot` / one post

**Status**: working autopsy, 2026-06-08.
**Lean target**: same as 001 (NQ-side admissibility kernel).
**Companion**: [`specimen-001-moderation-bsky-app-porn.md`](specimen-001-moderation-bsky-app-porn.md) — the positive case.

## Why this specimen

Specimen 001 walked a first-party label whose default-client conversion is documented but whose execution is unwitnessed (an execution-gap path). 002 walks the contrastive negative: a third-party label that the default Bluesky client **does not convert into any constraint** because no entry for that label exists in the default consumer's policy.

> label observed ≠ constraint applied

That sentence is the theorem-shape this specimen targets. Pairing 001 + 002 prevents the formal layer from accidentally learning the laundering rule `label_observed → constraint_applied`. The negative case is load-bearing.

## The ten-item autopsy

| # | field | value |
|---|---|---|
| 1 | **issuer** | `did:plc:newitj5jo3uel7o4mnf3vj2o` — xblock.aendra.dev ("XBlock Screenshot Labeller", third_party class) |
| 2 | **target_uri / kind** | `at://did:plc:rzwzv7yzjbuybw7beim3clda/app.bsky.feed.post/3mcoaxxulgb2q` — single post record |
| 3 | **label_value** | `twitter-screenshot` |
| 4 | **observed ts** | `2026-06-08T14:28:48.159566Z` |
| 5 | **authority_effect** | `unknown` (per Labelwatch `AUTHORITY_EFFECT_MAP`; the label value has no entry in the map and the labeler is not in `LABELER_DEFAULT_EFFECT`, so resolution falls through to unknown) |
| 6 | **consumer surface (default)** | Bluesky's official appview + clients via `@atproto/api`'s moderation library — **without** xblock subscribed as a labeler service |
| 7 | **policy artifact (default consumer)** | `bluesky-social/atproto`, `@atproto/api` v0.19.17, `packages/api/src/moderation/const/labels.ts` (HEAD `7b8c5d60a`): **no entry for `twitter-screenshot`** in the global `LABELS` map (verified by grep; only `!hide`, `!warn`, `!takedown`, `porn`, `sexual`, `nudity`, `graphic-media`, etc. are global). |
| 8 | **render_context** | client: bsky.app default web/iOS/Android · viewer_state: any · xblock subscribed as labeler service: **no (default)** · expected action: **none** (label is not recognized by the default client's moderation pipeline; renders as if absent) |
| 9 | **observed downstream behavior** | **No conversion observable, none documented for default consumer.** The label is published in the protocol stream, ingested by Labelwatch, and ignored by any consumer that hasn't subscribed to xblock's labeling service. |
| 10 | **gap** | **No conversion path exists at all in the default consumer.** This is not an execution gap (cannot observe execution) — it is the *absence of a conversion rule* for this consumer/label pair. Conversion-witness gap is *vacuously* clean: there is no policy to witness because no policy exists. |

## The three claims

| claim | shape | state for this specimen |
|---|---|---|
| **A** | `label_observed(labeler, target_uri, label_value, ts)` | **witnessed by Labelwatch** — item 4 above |
| **B** | `policy_declares(consumer, label_value, action, policy_artifact)` | **ABSENT** for the default consumer. The global `LABELS` map has no `twitter-screenshot` entry; the default client's moderation pipeline has no rule to apply. |
| **C** | `render_observed(consumer, target_uri, viewer_context, action, observed_at)` | **ABSENT** (the same architectural limitation as 001 — atproto publishes no per-render receipts), but here the absence is also a *consequence of* B's absence: there is no expected action whose application could be witnessed. |

The contrast with 001 is precise: 001 had B documented (just not witnessed live); 002 has B absent. The negative theorem follows:

> A alone does not entail any constraint. Conversion requires a non-absent B for the consumer in question. Without B, no claim about constraint may be made — not even an inferential one.

## Conditional conversion path (out of scope for default-consumer claim)

The xblock labeling service can be subscribed to by a user. If subscribed, the bsky client honors xblock's `app.bsky.labeler.service` record — specifically its `policies.labelValueDefinitions` array — for any label values defined there. This means:

- For an opt-in subscriber, a `policy_declares(bsky.app + xblock, twitter-screenshot, action_X, xblock.service.record)` claim may exist (claim B is *conditionally* available, with the consumer being a *different* configuration of the same client).
- For the default consumer (no xblock subscription), B remains absent.

This specimen claims **nothing about the opt-in case**. Documenting that would be specimen 002b — useful follow-up, but conflating it with 002 would let "the label MIGHT be converted under SOMEONE's settings" launder back into "the label IS converted." Keep separate.

## The four gaps, applied

| gap | this specimen |
|---|---|
| **observability gap** — can't see the label | NO. Labelwatch has the event. |
| **conversion-witness gap** — can see label + consumer but not policy | **vacuously NO for the default consumer:** no policy exists to witness. The gap shape doesn't apply. |
| **execution gap** — can see label + policy but not actual effect | **vacuously NO for the default consumer:** no policy → no expected effect → nothing whose execution could be missed. |
| **complete path** | NO. Path is intentionally short: testimony observed, no conversion possible in the default-consumer context. |

The diagnostic still classifies this as a complete autopsy — just one where the autopsy's conclusion is *no constraint follows*. That conclusion is itself the finding.

## Admissible / inadmissible conclusions

**Admissible:**
- A third-party label with `label_value=twitter-screenshot` exists on `at://did:plc:rzwzv7yzjbuybw7beim3clda/app.bsky.feed.post/3mcoaxxulgb2q`, issued by xblock.aendra.dev at `2026-06-08T14:28:48.159566Z`.
- The default Bluesky client's published moderation policy has **no entry** for this label value. No conversion rule from this label to a render action exists in that consumer's pipeline.
- A reader of Labelwatch may therefore claim: *under the default consumer's policy, this label produces no constraint on this post.*

**Inadmissible:**
- That this label produces no constraint *anywhere*. Users who have opted into xblock's labeling service may see different behavior governed by xblock's own service-record policy definitions. Specimen 002 makes no claim about that population.
- That the label is "noise" or "irrelevant." Labelwatch observes; whether the testimony is meaningful is a separate question.
- That xblock's labeling activity does not affect anyone. Adoption is a free variable; this specimen documents the default-render zero-conversion case, not the labeler's reach.

## What Lean cannot currently express without lying

Same root issues as 001, with the negative case adding two more:

1. **Absent-B is not the same as "label unimportant."** Lean must distinguish `policy_declares(...)` returning *no rule* from `policy_declares(...)` returning a *null/no-op rule*. The first is the consumer not having this label in scope; the second would be the consumer explicitly choosing to no-op on it. Different epistemic objects.
2. **Consumer is parameterized.** B is a function of `(consumer_config, label_value)`. The default consumer is one configuration; an opt-in-to-xblock consumer is a different configuration of the same client. Lean's `policy_declares` predicate must take the consumer configuration as an explicit argument, not assume a global "the bsky client" object.

## What this specimen does NOT do

- Make any claim about whether `twitter-screenshot` is correctly applied to the post.
- Pass judgment on xblock.aendra.dev.
- Claim no consumer anywhere converts this label.
- Adjudicate whether twitter-screenshot is a useful claim category.

## Pair with specimen 001

Together, 001 and 002 give Lean both signs of the partition:

| | 001 (positive-ish) | 002 (negative) |
|---|---|---|
| A: label_observed | witnessed | witnessed |
| B: policy_declares | documented | absent |
| C: render_observed | absent | absent (and B-conditional) |
| admissible inference | "documented policy would apply under stated context" | "no constraint follows under default consumer" |
| classification | execution gap | no-conversion-rule (clean) |

The handoff to the formal layer is now well-founded: it has at minimum one case where some conversion path is documentary but unwitnessed, and one case where no conversion path exists for the default consumer. The negative case prevents the formal layer from accidentally over-claiming.

`!takedown` (specimen 003, queued) extends to hosting-layer constraint, which is a second axis. Save for after the formalization bite on the render axis lands.
