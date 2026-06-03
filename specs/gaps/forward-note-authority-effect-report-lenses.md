# Forward note: authority_effect report lenses

**Status:** forward note, not a gap-spec. Filed 2026-06-02, after the
authority_effect inventory slice landed.
**Sibling:** `rejection-note-social-function-axis.md` (why we did not persist
`social_function`).

## Premise

Now that labels carry an `authority_effect` classification, the next moves
are mostly **report lenses**, not new schema. The axis exists; the question
is what views unlock now that it does.

Listed below in roughly the order they seem most useful. Each is a candidate,
not a commitment. Ratify when a real decision needs the lens.

## 1. Authority-effect conflict matrix

Group boundary/conflict analysis by the authority_effect of the labels
involved on each side of the disagreement. Today boundary analysis answers
"A and B disagree." With this matrix it could answer:

```
reputational vs reputational
reputational vs enforcement_instruction
decorative vs decorative
telemetry vs reputational
unknown vs anything
```

The probable headline finding: most apparent conflicts are
decorative/reputational, not enforcement-bearing. The scarier dual:
reputational labels increasingly co-occurring with visibility-affecting
labels on the same targets.

**Likely first to build.** Boundary analysis is the lens operators
actually consult and the noise floor problem is real today.

## 2. Authority escalation over time

Track whether a label moves up the ladder:
`decorative → reputational → advisory → visibility_affecting → enforcement_instruction`.

A label namespace can start as a joke and become an instrument. The empirical
signature: family X first appears decorative, later appears in reputational
contexts, later in visibility-affecting contexts. Toyification becoming
governance. This needs a temporal join the inventory does not yet do.

## 3. Labeler authority profile

For each labeler, the mix:

```
Labeler A: 72% decorative / 20% reputational / 8% telemetry / 0% enforcement
Labeler B: 4% decorative / 12% reputational / 41% advisory / 43% enforcement_instruction
```

Describes posture without judging content. "This source mostly emits
reputational claims; this one mostly emits enforcement instructions."
Composes with the existing reference-labeler subtype distinction.

## 4. Target blast-radius by authority_effect

Concentration per effect class:

```
reputational:           12,404 targets; top-10 receive 28%
enforcement_instruction:  1,204 targets; top-10 receive 71%
```

The question is not "how many labels?" but "how much authority pressure
lands where?" Composes with existing target_concentration rules.

## 5. Unknown-label watchlist

Unknown is currently a bucket. Make it a watchlist:

```
New unknown labels this period:
  weird-new-thing  — 4,202 events, 3 sources, 3,901 targets
  cursed-badge     —    89 events, 1 source,    87 targets
```

Heuristic readings:

- Unknown with high event count → classifier gap; consider adding to
  `AUTHORITY_EFFECT_MAP`.
- Unknown with high source concentration → labeler-local namespace.
- Unknown with high target concentration → potentially coordinated use.

## 6. Reputational/enforcement coupling

The spiciest detector. Where reputational claims and visibility-affecting /
enforcement labels cluster on the same targets or within the same time
window. Phrased descriptively (mandatory):

> "Reputational and visibility-affecting labels co-occurred on the same
> target population in the observed window."

Not "the reputational label caused enforcement." This is co-presence
reporting; co-presence is not corroboration (see workspace doctrine).

## 7. Decorative noise filter for boundary reports

A toggle on existing boundary analysis:

```
Including decorative labels: 8,204 boundary events
Excluding decorative: 1,116 boundary events
Excluding decorative + telemetry: 482 boundary events
```

Prevents toy-label churn from inflating governance-significance metrics.
Cheapest of all the lenses; would likely land alongside (1).

## 8. Authority asymmetry report

Count of distinct sources emitting each effect class:

```
enforcement_instruction emitted by 3 sources
reputational            emitted by 19 sources
decorative              emitted by 44 sources
telemetry               emitted by 7 sources
```

Read: "Enforcement is concentrated. Reputational is pluralized. Decorative
is diffuse." A governance topology without claiming motive.

## The deeper thing

Labelwatch is not only watching labels. It is watching **which kinds of
symbolic claims are being upgraded into operational surfaces**.

`authority_effect` is the substrate for that observation; the lenses above
are the readouts that make the observation legible.

## Sequencing notes

- Lens 1 (conflict matrix) and lens 7 (decorative noise filter) are the
  obvious near-term wins because they extend an existing operator-facing
  surface (boundary analysis) rather than adding a new page.
- Lens 5 (unknown watchlist) is cheap and directly closes the loop on the
  classifier — every entry is a candidate `AUTHORITY_EFFECT_MAP` row.
- Lenses 2 and 6 are higher-information but require temporal infrastructure
  the inventory does not yet have (per-window historical inventory store).
- None of these are gap-specs. They are candidate report views, ratify when
  a decision hinges on them, otherwise leave them in this note.
