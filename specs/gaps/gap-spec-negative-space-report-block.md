# Gap spec: negative-space report block — "can testify / cannot testify" everywhere

**Status:** CANDIDATE handle, **not ratified, not authorized to build.** Filed
2026-06-29 as a review handle while the shape is fresh (name-early / ratify-lazily).
No forcing *case* to implement yet; build only on explicit greenlight.

**Scope:** cross-observatory (Labelwatch **and** Driftwatch). Filed here; a pointer
lives in the Driftwatch facts-manifest candidate, which already carries the narrow
custody-side instance (`cannot_testify`). This candidate generalizes that one block
into a **report-format discipline** for every public/HTML/JSON artifact.

**Provenance:** the 2026-06-29 hardening pass. The READMEs and design constraints
already state what these systems are NOT (detect-only; weather, never verdict; host
family ≠ operator identity; current PDS ≠ historical-PDS-at-label-time). The gap is
that this discipline lives in READMEs and prose — "where nuance goes to nap" — not in
the generated outputs someone actually screenshots.

## Problem

A report that states only what it found invites over-reading. Without an explicit
negative-space block, "Labeler X labeled DIDs on host family Y" gets screenshotted as
"Labeler X targeted host Y" — the honest sentence and the sexy one are one
misattribution apart.

## Shape (non-binding sketch)

Each major public report carries a compact, machine-and-human-readable block:

```
This report can testify:
- label event volume changed
- boundary overlap/divergence changed
- hosting-locus enrichment under current resolver coverage

This report cannot testify:
- content truth
- moderation correctness
- account intent
- operator identity
- historical host at label time
- coordinated abuse
- user-level culpability
```

The exact "cannot" list is report-specific; the *presence* of the block is the
discipline. Composes with the existing constraints
[[constraint_weather_not_verdict]] and [[constraint_detect_only_structural]].

## Non-goals

- Not a new finding type; it bounds existing ones.
- No emit/enforcement change.
- Not a substitute for the per-artifact custody refusals in the facts manifest — this
  is the *report-rendering* analogue, that is the *data-contract* analogue.

## Why candidate, not build

Cheap and low-risk, but it touches every report renderer, so it wants one ratified
shape before a sweep rather than ad hoc per-report blocks that drift. Promote when the
next public surface ships (e.g. the subject-lookup frontdoor), so the block lands as a
template, not a retrofit.
