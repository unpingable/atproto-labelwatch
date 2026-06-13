# Gap spec: labelwatch.warrant_presence.v0 — population auditability of active labels

**Status:** CANDIDATE handle, **not ratified, not authorized to build.** Filed 2026-06-13 as a
review handle while the shape is fresh. Forcing *evidence* exists (the 42% warrant-gap below);
a forcing *case to implement* does not yet — build only on explicit greenlight, same as scope-axis.

**Inherits:**
- [`docs/evaluation-detachment-axes.md`](../../docs/evaluation-detachment-axes.md) — warrant is the
  **auditability precondition**, orthogonal to the three detachments: it gates whether
  freshness/independence/scope are checkable from outside at all.
- [`gap-spec-scope-axis-v0.md`](gap-spec-scope-axis-v0.md) — sibling; the scope-axis run surfaced
  the population this axis measures.
- `findings_pages.py` F-007 cohort (`events_30d > 1000` with no `labelValueDefinitions`) — the
  shipped seed this axis generalizes from per-labeler to population.

## Forcing evidence (why this is the real story)

First scope-axis prod run (2026-06-13, 7d): **ungraded = 857,741 events ≈ 42% of active volume**,
against a verdict-scope presentation share of only 1.0%. The ecosystem is not silently
verdictizing; it is **missing public basis at scale**. That is exactly the absence the observatory
exists to notice, and it needs no editorial judgment to report.

> Headline: verdict-scope presentation is rare; warrant absence is common.

## Architecture sentence

> Warrant is whether an outsider could check the label at all. We measure the *presence* of a
> publicly checkable basis, mechanically — never whether the basis is *good*. Absence is a fact;
> sufficiency is a judgment we do not make.

## The metric (narrow, mechanical, aggregate-only)

A three-tier auditability ladder over active (`neg = 0`) label volume, each tier a mechanical
test — no editorial read of *quality*:

| tier | mechanical test | reading |
|---|---|---|
| `no_definition` | no `labelValueDefinition` for `(labeler, val)` | pure warrant-gap — the F-007 population |
| `metadata_only` | def exists but **no** locale name/description text | declared, but no human-checkable basis surface |
| `described` | def exists **with** locale name/description | a basis surface exists (floor, not proof of sufficiency) |

`described` is a **floor, not a ceiling**: a locale description is not an appeal path, evidence
locus, or policy pointer. Richer warrant (checkable basis *pointers*) is a later refinement, named
here so the v0 floor is not mistaken for "well-warranted."

Reuse: the `explains` test already exists in `findings_pages._classify_live_row` /
`operator_maturity_scan.py` (any locale name/description present). v0 lifts it from per-labeler
maturity to a population auditability cut. Emission-weighted headline + declaration companion,
explicit denominators — same two-cut discipline as scope-axis.

## Boundary discipline (inherited, non-negotiable)

- **Aggregate-only. No ranked labeler wall of shame.** Warrant absence is a population condition,
  not an accusation. No `target_did`; no per-labeler ranking surface in v0.
- **Absence, not blame.** "no published basis" ≠ "bad labeler." A labeler may testify without
  publishing a warrant; the observatory reports that the warrant is absent, full stop.
- **Mechanical, not editorial.** Every tier is a presence test on published structure, never a
  read of whether the basis is adequate.

## Acceptance criteria (when/if built)

1. Pure tier function over `(definition_present, has_description)` → `no_definition` / `metadata_only`
   / `described`, unit-tested.
2. Emission + declaration cuts with explicit denominators; `no_definition` reconciles with the
   scope-axis `ungraded` count on the same window (cross-check: they must agree).
3. Output carries no `target_did` and no ranked per-labeler list (tested).
4. One F-007 generalization figure (population auditability ladder), aggregate.
5. `described` is labeled a floor in the output, not "warranted."

## Out of scope for v0 (named, not forgotten)

- Checkable-basis **pointers** (policy / appeal / evidence URLs) beyond a locale description — the
  richer warrant definition; its own slice.
- Per-labeler ranking / naming — accusation-shape, deferred.
- Correlating warrant absence with the other axes (does ungraded volume concentrate in high-churn
  or stale labelers?) — interesting, but a cross-axis follow-up, not v0.
