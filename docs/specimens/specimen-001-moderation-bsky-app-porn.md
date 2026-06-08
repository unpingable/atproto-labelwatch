# Specimen 001 — moderation.bsky.app / `porn` / one post

**Status**: working autopsy, 2026-06-08.
**Lean target**: Admissibility / ConsequencePartition annex (NQ-side calculus kernel — evidence-backed claims authorizing transitions, receipt replay, witness/testimony boundaries, refusal of laundering).
**NOT the target**: driftwatch `formal-claim-admissibility` gap spec (`439100c`, "Z3 first, Lean later"). That sits at the wrong altitude for this test: it asks *is this claim well-shaped?* not *when may testimony become constraint?*

## Specimen framing

This is a methodology specimen. Subject is the **conversion mechanism**, not the labeled account.

> A label is not an intervention until some consumer treats it as one.

That sentence is the theorem-shape. This autopsy walks one real claim from issuer → consumer → execution surface and names which of those steps Labelwatch can witness directly, which require external evidence, and which Lean (in its current shape) cannot express without lying.

Per [`labelers-as-testimony.md`](../labelers-as-testimony.md): labelers are **claim producers**, not moderators. Moderation begins at the conversion step. The first-party case (issuer and consumer institutionally close) is chosen *because* the boundary still matters even when the parties are close — that is part of the specimen's value.

## The ten-item autopsy

| # | field | value |
|---|---|---|
| 1 | **issuer** | `did:plc:ar7c4by46qjdydhdevvrndac` — moderation.bsky.app (first-party Bluesky labeler, official_platform class, reference labeler) |
| 2 | **target_uri / kind** | `at://did:plc:ztlk6b7feflbs4uyhckz6dh5/app.bsky.feed.post/3lqjexu2mik2g` — single post record |
| 3 | **label_value** | `porn` |
| 4 | **observed ts** | `2026-06-08T14:03:01.304298Z` (labelwatch ingest timestamp; matches the label's own `ts` field for first-party labels) |
| 5 | **authority_effect** | `visibility_affecting` (per Labelwatch `AUTHORITY_EFFECT_MAP`) |
| 6 | **consumer surface** | Bluesky's official appview + clients (bsky.app web; iOS / Android apps), via `@atproto/api`'s moderation library |
| 7 | **policy artifact (pinned)** | `bluesky-social/atproto`, package `@atproto/api` v0.19.17, file `packages/api/src/moderation/const/labels.ts` (verified against local mirror at HEAD `7b8c5d60a`). The `porn` definition declares `defaultSetting: 'hide'`, `flags: ['adult']`, `blurs: 'media'`, `configurable: true`; content-view behavior is `contentMedia: 'blur'`. |
| 8 | **render_context (required for any execution claim)** | client: bsky.app default web/iOS/Android · viewer_state: logged-out or fresh logged-in account with default settings · adult_content_enabled: false · per-label porn setting: default (`hide`) · expected action: media blurred behind content-warning placeholder, click-through gated. Each of these is a free variable; a different combination produces different rendering. |
| 9 | **observed downstream behavior** | **Not directly witnessed by Labelwatch.** The conversion's *output* (blur/hide rendering in the client) is not in any stream Labelwatch ingests. Inferred from public policy + label presence, not from a receipt. |
| 10 | **gap** | **Execution gap.** Path is: label witnessed; conversion rule documented; conversion application unwitnessed; render effect unwitnessed. |

## The three claims

This is the specimen's core schema. Every conversion autopsy reduces to whether each of these is **witnessed**, **documented**, or **absent**:

| claim | shape | state for this specimen |
|---|---|---|
| **A** | `label_observed(labeler, target_uri, label_value, ts)` | **witnessed by Labelwatch** — item 4 above; event present in stream with signature and event_hash |
| **B** | `policy_declares(consumer, label_value, action, policy_artifact)` | **documented** — item 7 above; pinned to specific package version + file path; conditional on `render_context` (item 8) |
| **C** | `render_observed(consumer, target_uri, viewer_context, action, observed_at)` | **absent** — no per-render receipt exists in atproto today; would require external probe to produce existence proof; cannot produce population claims |

The asymmetry between B and C matters: public source code is **not** the same thing as a live consumer receipt. The default client *probably* runs some version of `@atproto/api` close to the documented one and *probably* applies the documented behavior, but Labelwatch holds no receipt that this specific render at this specific moment used this specific rule.

## The four gaps, applied

Per chatty's diagnostic ladder:

| gap | this specimen |
|---|---|
| **observability gap** — can't see the label | NO. Labelwatch has the event with millisecond ingest precision. |
| **conversion-witness gap** — can see label + consumer but not policy | PARTIAL. Consumer is named and the policy artifact is pinned (item 7). But the binding "consumer X applies policy version Y at render time T" is *documentary*, not *receipt-bearing*. |
| **execution gap** — can see label + policy but not actual effect | **YES.** Whether *this particular post* is, at the moment of any user's render, actually blurred/hidden is invisible. No per-render receipt is published into any stream. |
| **complete path** | NO. The execution gap blocks completion. |

Completion requires either: (a) consumer-side receipts (a stream of "rendering decision: label X on URI Y triggered action Z at time T for viewer context C") — not part of the atproto wire today; or (b) probes (synthetic users that load known-labeled content and report what they see). Probes give existence proofs but not population claims.

## Admissible / inadmissible conclusions

This is the real output. Not *what happened*, but **what may be said without fraud**.

**Admissible:**
- A first-party label with `label_value=porn` exists on `at://did:plc:ztlk6b7feflbs4uyhckz6dh5/app.bsky.feed.post/3lqjexu2mik2g`, issued by moderation.bsky.app at `2026-06-08T14:03:01.304298Z`.
- The published default-client moderation policy (`@atproto/api` v0.19.17, `packages/api/src/moderation/const/labels.ts`) maps the `porn` label to a visibility-affecting action (`blurs: 'media'`, `defaultSetting: 'hide'`) under the `render_context` specified above.
- A reader of Labelwatch may therefore claim: *under the documented default policy and a viewer whose settings match the default context, this post would be subject to a blur-with-content-warning render.*

**Inadmissible:**
- That this specific post was actually hidden, blurred, or warned for any particular user at any particular time. That claim requires a render-side witness Labelwatch does not have.
- That all renders of this post are blurred. The policy is `configurable: true`; users may override to `warn` or `ignore`. Population claims about render outcome require a probe campaign with a defined sampling frame.
- That the conversion's application is implied by the label's existence. The label is testimony; rendering is interpretation by an interpreter Labelwatch does not directly witness.

## What Lean cannot currently express without lying

Against the NQ-side doctrine I have direct access to (witness contract, basis_state, refusal-first-class, brutalist register — see [continuity NQ scope]); the actual calculus kernel rules are the next thing to compare against.

1. **Partition equality between issued and applied is not provable from the issuer side alone.** A rule of shape `label_observed(L, U) ⊢ constraint_applied(U)` is a laundering rule — it collapses the producer/consumer distinction the doctrine ratifies. Any honest version requires either a render-side witness (claim C) OR an explicit, separately-witnessed `policy_declares` claim plus a render-time deferral marker.

2. **Inheritance discipline.** When a consumer converts, the resulting constraint inherits the testimony's epistemic weaknesses (single-labeler, no quorum, weak appeal — see [`labelers-as-testimony.md`](../labelers-as-testimony.md) §3) without renaming them. The calculus should make this inheritance explicit: a constraint backed by single-source testimony is not strengthened by being executed.

3. **Co-presence vs corroboration.** When two labelers both label the same post `porn`, consumer policy may treat that as more confident grounds for constraint. The calculus must refuse this composition unless the labelers are independent and the testimony forms are commensurable. (Workspace doctrine: co-presence is not corroboration.)

## What this specimen does NOT do

- Verify the actual rendering on bsky.app (would require an external probe).
- Make any claim about whether the `porn` label on this specific post is correct.
- Pass judgment on moderation.bsky.app, Bluesky, or the labeled account.
- Adjudicate whether the consumer policy is well-designed.

The specimen is a methodology autopsy. Its job is to make visible *which steps in the testimony→constraint conversion are witnessed and which are inferred*, so the formal layer can either represent that boundary honestly or refuse to claim coverage it does not have.

## Queued specimens

- **Specimen 002 (contrastive):** a third-party labeler emits a label that the default Bluesky client *does not* convert into constraint (no entry in `@atproto/api`'s `LABELS` map; service-defined label requiring user opt-in to the labeler). Tests the negative case directly: *label observed ≠ constraint applied*. Prevents Lean from accidentally learning the dumb rule `label_observed → constraint_applied`.
- **Specimen 003 (`!takedown`, later):** moderation.bsky.app's `!takedown` shifts the layer from rendering-layer constraint to hosting/removal constraint (PDS-side). Useful, but a second axis; should not pull in before specimen 002 nails the basic witness partition.

## Queued gaps (not addressed in this specimen)

- **Fig-leaf detection** — labels invoked retroactively as cover for decisions made on other grounds. Sharpest probable case in the wild but drags into intent/motive analysis; first teach the scalpel to cut meat, ghosts later.

## Open questions

1. **NQ calculus kernel:** I have not seen the actual kernel rules. This autopsy is written against the *doctrine* surfaced via continuity (witness contract, basis_state, refusal-as-first-class, brutalist register) but cannot claim a specific rule of the kernel is or isn't violated by a given conversion. Pointing at the kernel + walking this specimen through it is the next concrete move.
2. **Per-render receipt:** does the atproto roadmap include anything in this shape, or is it actively excluded? If excluded, the execution gap is permanent and the calculus must treat constraint-side claims as inferential not testimonial — that itself is a load-bearing finding.
