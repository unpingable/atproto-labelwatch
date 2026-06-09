# Specimen 004 — Reference-labeler emission is not default authority

**Status**: working autopsy, 2026-06-09.
**Lean target**: Admissibility / ConsequencePartition annex (same target as 001/002).
**Sister specimens**: 002 (third-party labeler, no default conversion — non-reference case) · 003 (third-party labeler, named opt-in consumer adopts — reference-emitted label adopted by a NAMED consumer, not by the default client).

## Why this specimen exists

[F-001](DISAGREEMENTS.md) records the doctrine: **reference-labeler status is a
Labelwatch calibration choice, not a consumer-adoption claim.** When
`skywatch.blue` (one of the reference set) emits a label whose `label_value`
is not in the default Bluesky client's `LABELS` map, the classifier should
produce the same gap shape as it does for a non-reference third-party
labeler (specimen-002): `conversion_witness_gap_no_consumer` for the
default consumer, with `consumer_scope = emitter_declared`.

That doctrine has so far lived as prose. This specimen bakes it into
apparatus. If a future classifier change treats `is_reference_labeler =
true` as a promoter — directly, or by composition with emission volume,
service-record presence, or directory discoverability — the
`verify.py` regression will FAIL on this packet. The misstatement is
no longer silent.

Load-bearing claim, broader than F-001:

> A labeler being **discoverable**, **referenced**, **popular**, or
> **useful** does not make its labels **platform-enforcing**,
> **client-default**, or **globally adopted**. Consumer adoption must
> be separately witnessed.

The four orthogonal pairs: reference status / emission volume /
discoverability / declared scope vs default-client conversion. None
of them is the conversion witness. PolicyDocumentation or
ConsumerAdoption is.

## Subject

`skywatch.blue` is in Labelwatch's `reference_dids` set
(`did:plc:e4elbtctnfqocyfcml6h2lf7`). It is the current
highest-volume non-platform labeler in the ecosystem
(`fundraising-link` alone has ~1.19M events as of 2026-06-09). It
publishes a service record with `labelValueDefinitions` declaring
each of its labels — including a `defaultSetting`, `severity`, and
`blurs` for `fundraising-link`. By every "soft" signal —
discoverability, volume, reference designation, declared scope — it
is the third-party labeler that an operator's intuition is most
likely to silently promote.

The default Bluesky client does not subscribe to `skywatch.blue`
unless the viewer opts in. The `@atproto/api` global `LABELS` map
([F-002](DISAGREEMENTS.md) — corrected entries: `!hide`, `!warn`,
`!no-unauthenticated`, `porn`, `sexual`, `nudity`, `graphic-media`,
`gore`) does not contain `fundraising-link`. The skywatch
service-record definition is the labeler's own declaration, scoped
to consumers who subscribe to skywatch as a labeler service; it
does not bind the default Bluesky client.

This is the same architectural picture as specimen-002 — the only
delta is `is_reference_labeler: true`. The specimen exists so the
classifier is *required* to ignore that flag for conversion-gap
purposes.

## The ten-item autopsy

| # | field | value |
|---|---|---|
| 1 | **issuer** | `did:plc:e4elbtctnfqocyfcml6h2lf7` — skywatch.blue (third-party labeler, `is_reference_labeler = true`) |
| 2 | **target_uri / kind** | `at://did:plc:s6ktgg7ckbyz6bs66dlag6dj/app.bsky.feed.post/3mntabl3vcs2i` — single post record |
| 3 | **label_value** | `fundraising-link` |
| 4 | **observed ts** | `2026-06-09T03:25:22.950309Z` (live label_events row pulled from prod 2026-06-09) |
| 5 | **authority_effect (Labelwatch heuristic)** | `unknown` (third-party label outside the platform vocabulary; the classification is descriptive, not load-bearing for this specimen) |
| 6 | **consumer surface (named)** | `bsky.app-default-client` — Bluesky's official appview + clients, with no third-party labeler services subscribed beyond the platform defaults |
| 7 | **policy artifact searched** | `bluesky-social/atproto`, package `@atproto/api` v0.19.17, file `packages/api/src/moderation/const/labels.ts`, HEAD `7b8c5d60a`. Search result: **no_entry** for `fundraising-link`. The post-F-002 corrected `LABELS` keys are `!hide`, `!warn`, `!no-unauthenticated`, `porn`, `sexual`, `nudity`, `graphic-media`, `gore`. |
| 8 | **labeler emitter declaration** | `skywatch.blue`'s `app.bsky.labeler.service` record (ingested into labelwatch's `discovery_events`) includes a `labelValueDefinitions[]` entry for `fundraising-link` with `defaultSetting: ignore`, `severity: inform`, `blurs: content`. This is the **emitter's** declared rule, applicable only to consumers who subscribe to skywatch — NOT to the default client. |
| 9 | **observed downstream behavior** | **Not witnessed.** Same architectural absence as 002 — atproto publishes no per-render receipts. Additionally, no render action is *expected* under the named consumer's pipeline because no rule binds the label there. |
| 10 | **gap** | **Conversion-witness gap (no consumer).** Path is: label witnessed; emitter declares a rule; named (default) consumer has no rule; no consumer policy artifact to convert against. |

## The three claims

| claim | shape | state for this specimen |
|---|---|---|
| **A** | `label_observed(labeler, target_uri, label_value, ts)` | **witnessed by Labelwatch** — item 4 above; event present in `label_events` |
| **B** | `policy_declares(consumer, label_value, action, policy_artifact)` | **absent for named consumer** — item 7; no rule for `fundraising-link` in `bsky.app-default-client`'s pipeline. Emitter declaration (item 8) is NOT a B claim — it is a labeler self-description, not a consumer policy binding. |
| **C** | `render_observed(consumer, target_uri, viewer_context, action, observed_at)` | **not applicable / absent** — same architectural absence; additionally moot because there is no B to bind to |

The B / emitter-declaration asymmetry is exactly the point of this
specimen. The emitter publishing a rule does NOT promote the rule to
the consumer's pipeline. **A consumer adopts; an emitter declares.
The two acts are not interchangeable.**

## The four gaps, applied

| gap | this specimen |
|---|---|
| **observability gap** — can't see the label | NO. Labelwatch has the event. |
| **conversion-witness gap** — can see label + consumer but not policy | **YES, for `bsky.app-default-client`.** Default consumer documents no rule for `fundraising-link`; skywatch's emitter-declared rule is scoped to opt-in subscribers, not default. |
| **execution gap** — can see label + policy but not actual effect | N/A under the named consumer (no documented policy to bind to). Becomes possible under a different specimen with a NAMED non-default consumer adopting — see specimen-003 for the driftwatch opt-in shape. |
| **complete path** | NO. The conversion-witness gap blocks completion for the default consumer. |

The conversion-witness gap here is **scoped** to
`bsky.app-default-client`. A separate specimen (or specimen-003-shape
extension) is required to characterize the *opt-in* case. The
evidence is silent over other consumer configurations.

## Admissible / inadmissible conclusions

**Admissible:**

- A label record with `label_value = fundraising-link` exists on
  `at://did:plc:s6ktgg7ckbyz6bs66dlag6dj/app.bsky.feed.post/3mntabl3vcs2i`,
  issued by `skywatch.blue` at `2026-06-09T03:25:22.950309Z`.
- Under `bsky.app-default-client`'s published policy pipeline at the
  pinned artifact, no rule exists that maps `fundraising-link` to any
  render action.
- From this evidence bundle, no constraint may be derived for the
  named consumer from this label record.
- `skywatch.blue`'s own service record declares a rule for
  `fundraising-link`; this rule is consumer-scoped to skywatch's
  subscribers and **does not bind any other consumer** without that
  consumer's own adoption act.

**Inadmissible** (the load-bearing ones for this specimen):

- That `fundraising-link` is "platform-enforced," "client-default,"
  or "globally adopted" by virtue of any of: skywatch's reference
  status; skywatch's emission volume; skywatch's discoverability or
  registry presence; skywatch's declared scope. Reference status is
  Labelwatch's calibration choice; volume is operator activity;
  discoverability is directory infrastructure; declared scope is the
  emitter's self-description. None of these is a consumer-adoption
  claim. **Consumer adoption must be separately witnessed.**
- That this post was rendered with action X for user U at time T.
  Requires `RenderObservation`, absent.
- That `fundraising-link` produced no constraint anywhere. Factual
  non-occurrence is out of scope; the evidence is silent over other
  consumer configurations.
- That the label is meaningless or unimportant. Labelwatch observes;
  whether testimony is meaningful is a different judgment.
- That skywatch's labeling activity has no operational effect.
  Adoption is a free variable across consumers; the evidence
  documents one consumer's zero-conversion case.

## What this specimen does NOT do

- Verify rendering on bsky.app (would require an external probe).
- Pass judgment on `skywatch.blue`, the label, or the labeled
  account.
- Adjudicate whether the consumer policy is well-designed.
- Re-derive F-001's doctrine in prose. F-001 already states it; this
  specimen bakes it into the verifier so future drift becomes
  mechanically visible.

## Cross-references

- **F-001** — Reference-labeler status does not imply default-client
  conversion. Doctrinal source.
- **F-002** — `GLOBAL_LABELS` hardcode correction; the
  `policy_artifact_searched.search_evidence` here uses the corrected
  8-entry list.
- **F-004** — Third-party labelers DO publish `labelValueDefinitions`;
  opt-in consumers honor them. The flip side this specimen guards
  against.
- **F-009** — Discoverable/referenced/popular/useful ≠
  enforcing/default/adopted. The broader generalization this specimen
  is the load-bearing test for.
- **Specimen 002** — same gap shape, non-reference labeler. This
  specimen is the reference-set sister case.
- **Specimen 003** — same emitter (`skywatch.blue`), different label
  (`fringe-media`), with the named opt-in consumer
  (`driftwatch`). The positive case proving that reference labelers
  CAN be adopted — by a named consumer, with a policy artifact and a
  receipt. Specimen 004 is the negative case for the default
  consumer.

## Operational provenance

The label observation row was pulled live from
`/var/lib/labelwatch/labelwatch.db` (prod) at 2026-06-09. The
emitter-declared rule was pulled from the most recent
`discovery_events` record for skywatch.blue at the same time. The
policy-artifact pin matches specimen-001/002 (`@atproto/api`
v0.19.17, HEAD `7b8c5d60a`) so all three specimens read against the
same fixed upstream snapshot.
