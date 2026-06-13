# Gap spec: labelwatch.scope_presentation.v0 — population verdict-band of active labels

**Status:** scoped to build, candidate. Filed 2026-06-13. First instantiation of the
[evaluation-detachment axes](../../docs/evaluation-detachment-axes.md) taxonomy — the
scope-axis. **Not blocked on the agent_gov-side root**; this is the observe-side
population metric and stands alone.

**Inherits:**
- [`docs/evaluation-detachment-axes.md`](../../docs/evaluation-detachment-axes.md) — the parent taxonomy; scope = detachment by authority-presentation.
- [`docs/labelers-as-testimony.md`](../../docs/labelers-as-testimony.md) — anyone may testify; no labeler holds enforcement authority, the subscriber converts. Authority is *uniform*, so the measurable is **self-presentation**, not entitlement.
- `constraint_weather_not_verdict` (workspace) — the band reports the labeler's *own* `defaultSetting`, cited not adopted. Subject is never the finding.
- `gap-spec-authority-effect-inference-v0.md` — reuses its emitter-definition access and its `authority_effect` vocabulary; this spec is the **`defaultSetting` projection** of that work, split at the `hide`/`warn` boundary the population metric needs.

## Architecture sentence

> No labeler can cash enforcement authority — the subscriber converts. So a label
> declaring `defaultSetting: hide` presents at a scope no labeler holds. The scope-axis
> counts that self-presentation across the population. It is the population mirror of
> weather-not-verdict, pointed at the labels — never at the labeled.

## The metric

A label's **scope band** is the projection of the labeler's own `labelValueDefinition.defaultSetting`:

| band | `defaultSetting` | reading |
|---|---|---|
| `verdict` | `hide` | mandatory-hide-by-default — presents as testimony that auto-converts to constraint |
| `nudge` | `warn` | a default action, but soft |
| `weather` | `ignore` / unset | opt-in; descriptive |
| `ungraded` | *no published definition for this `(labeler, val)`* | **warrant-gap, not weather** — see below |

`scope_band` is a pure function of the definition dict. `blurs` and `severity` are
carried in evidence as modifiers but do **not** move the band — `defaultSetting` is the
declared default *action*, which is the thing a subscriber would have to convert.

Two cuts, both reported with **explicit denominators** (CLAUDE.md: coverage never
appears without its denominator):

- **Emission cut (headline)** — over active label events (`neg = 0`) in window *W*,
  aggregated by `(labeler_did, val)`, each cell graded by its band, event counts summed
  per band. Headline = **% of graded active label-events presenting at `verdict`**. This
  is the "active labels" the doctrine names — weighted by what the ecosystem actually
  experiences.
- **Declaration cut (companion)** — over distinct published `(labeler, val)` definitions,
  unweighted, % per band. Answers "what posture do labelers *declare*" vs "what posture
  is *emitted*."

The `ungraded` fraction is reported **alongside** as the warrant-gap coverage shortfall —
it is the denominator's complement, never folded into `weather`. A label with no published
basis is not weather; it is unfalsifiable from outside (warrant-axis, F-007 cohort in
`findings_pages.py`).

### Unset `defaultSetting` — decision (2026-06-13)

A published definition that **omits** `defaultSetting` is mapped to `weather`, same as an
explicit `ignore`. This is an **explicit-only declaration convention**: if a labeler did not
declare a default action, this metric does not infer one. It matches the codebase's existing
convention (`emitter_classifier._authority_from_metadata_only` treats only explicit
`hide`/`warn` as action-bearing).

The ATProto lexicon arguably defaults an omitted `defaultSetting` to `warn` (a client-behavior
fact). v0 deliberately does **not** simulate client behavior — it measures *declared*
self-presentation. Rather than flip the metric under a theoretical ambiguity, v0 makes the
ambiguity **observable before it is policy**:

- output carries an `assumptions` block (`default_setting_omitted: weather_scope_explicit_only`,
  `not_client_behavior_simulation: true`);
- the omitted population is counted on both cuts — `declaration.default_setting_omitted` and
  `emission.weather_from_omitted_default_events` (the exact volume that would reclassify to
  `nudge` under a warn-fallback reading).

A protocol-fallback **sensitivity line** (unset → `nudge`) is deferred until that population is
measured on real data; add it only if the omitted share is material. Make the uncertainty
visible first; legislate second.

**Measured (2026-06-13, first 7d prod run on `de0606c`):** graded coverage 57.2% (headline
published, not suppressed). Verdict-scope presentation share = **1.0%** (nudge 66.4%, weather
32.5%). Ungraded warrant-gap = **857,741 events ≈ 42%** of active volume. Omitted-`defaultSetting`
population: **4,993 of 10,542** declared values (~47%) omit it — yet those defs account for only
**41 weather events** (≈0.003% of graded emission). **Verdict: the protocol-fallback sensitivity
companion is NOT warranted for the emission cut** — immaterial by volume. Caveat retained and now
quantified: the omitted share *is* material to the **declaration** cut (~47% of declared values),
so the unset convention must be revisited there if the declaration-cut headline ever becomes
load-bearing. The 42% warrant-gap is the standout finding and motivates the warrant-axis slice.

## Data & reuse (no new access paths)

- **Definitions**: `discovery_events.record_json` → `$.policies.labelValueDefinitions[]`,
  latest-record-per-labeler via the existing `ROW_NUMBER() OVER (PARTITION BY labeler_did
  ORDER BY discovered_at DESC)` pattern (`findings_pages._LIVE_SCAN_SQL`). Reuse
  `emitter_classifier.lookup_emitter_definition(conn, labeler_did, val)`.
- **Emissions**: `label_events(labeler_did, val, neg, target_did)`, aggregated
  `GROUP BY labeler_did, val WHERE neg = 0` — the distinct-cell set is small even though
  `label_events` is ~40M rows; grade the cells, then sum counts. No per-row enrichment.
- **New code**: a pure `scope_band(definition) -> band` and a population aggregator
  producing both cuts. Smallest landing surface — a module (`scope_axis.py`) + one report
  figure + one test file. **No new per-labeler page in v0** (a ranked "verdict-presenting
  labelers" list edges toward accusation; aggregate-first).

## Boundary discipline (structural, not stylistic)

- The band is the labeler's **own** declared `defaultSetting`, quoted in evidence. We
  report "labeler X declares `hide`-by-default for value Y," never "value Y is wrong" or
  "subject Z is fine." Weather-not-verdict is preserved by *what the number is about*.
- **Aggregate-first**: output carries no `target_did`. The unit is `(labeler, val)` and
  population sums, never a labeled account.
- **Descriptive language**: "verdict-presenting share," not "overreaching labelers."

## Acceptance criteria (testable)

1. `scope_band(def)` is pure and unit-tested across: `hide`→`verdict`, `warn`→`nudge`,
   `ignore`→`weather`, unset→`weather`, `None`/missing def→`ungraded`.
2. Aggregator returns both cuts with denominators attached; `ungraded` is a distinct
   bucket and is **never** summed into `weather`. A test asserts an ungraded cell does not
   move the weather count.
3. Metric output contains **no** `target_did` (aggregate-first); a test asserts the output
   schema has no subject field.
4. Evidence for each graded cell carries the verbatim `defaultSetting`/`severity`/`blurs`
   (testimony, cited not adopted) — asserted in a test.
5. Bang-labels (`val` starting `!`) are excluded from the band cuts and counted on a named
   `protocol_reserved_deferred` line — **not silently dropped** (test asserts a `!hide`
   event lands there, not in `weather`/`ungraded`).
6. Coverage canary: when graded/(graded+ungraded) emission coverage `< 0.5`, the headline
   `verdict` share is annotated/suppressed (mirrors the coverage-watermark discipline),
   not published bare.

## Out of scope for v0 (named, not forgotten)

- **Bang-labels / protocol-reserved** (`!hide`, `!warn`, `!takedown`) — purest verdict-scope
  but hosting-layer; entangled with specimen 003 (held back as the second axis). Counted,
  deferred.
- **Negation & expiry dynamics** (`neg`, `exp`) — freshness-axis. v0 counts `neg = 0`
  active assertions only and cross-links.
- **Warrant-pointer generalization** — moving F-007 from "no defs at all" to "no checkable
  basis pointer" is the warrant-axis, its own slice.
- **Per-labeler ranking surface** — deferred on aggregate-first / accusation-shape grounds.
