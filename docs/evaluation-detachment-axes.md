# Evaluation-detachment axes

**Status**: candidate doctrine, **not ratified**. Filed 2026-06-13.
**Plane**: Signals / observe — descriptive. Not Governor / control. See boundary discipline below.
**Companions**: [labelers-as-testimony.md](labelers-as-testimony.md) · [authority-failure-modes.md](authority-failure-modes.md) · [observation-export-custody.md](observation-export-custody.md) · [specimens/](specimens/) (ConversionGap autopsies)
**Provenance**: governor-side framing (agent_gov, separate session 2026-06-13), pressure-tested across two Claude sessions + a different model (Chatty), then grounded against this repo. The cross-jurisdiction root below is the **pin target** for the AG-side doctrine — AG references this anchor, it does not copy it. One source of truth, two jurisdictions.

---

## Root principle (pinnable anchor: `evaluation-outrunning-reality`)

> A label is a frozen evaluation. The governor's whole job is keeping evaluation tethered to the reality it claims to describe. ATProto's labeling ecosystem is the same problem running **un-governed**: evaluations that outrun the reality they indexed, at labeler scale, with the refusals removed.

Observe and govern are **two jurisdictions of one disease**, not two systems that happen to resemble each other. They share vocabulary because they are the diagnosis and the treatment for the same failure. The relationship is **federation, not merge**: the observatory measures the absence; it does not impose the presence. ATProto is a different sovereign — the operator-fiat root, the caps, the receipt kernel are not ours to install on someone else's protocol.

**Not a control group — a natural experiment.** A control group implies a runnable treatment arm alongside the observed one. Detect-only is structural here (see [observation-export-custody.md](observation-export-custody.md) and the workspace `constraint_detect_only_structural`), so the governed counterfactual is one we can *describe* but not *run in-jurisdiction*. The frame's payoff is therefore not new metrics — it is **earning the refusals already ratified** (detect-only, weather-not-verdict, co-presence-isn't-corroboration) from a single principle.

---

## The axes: three detachments + one precondition

A frozen evaluation detaches from its indexed reality three ways. A fourth axis is orthogonal: it governs whether the detachment is *visible from outside* at all.

| Axis | Detachment | What it measures (observe-side, no verdict) | Build state |
|---|---|---|---|
| **freshness** | temporal | active labels that are stale: negated-but-still-served, or long past any sane horizon | scaffolded, paused |
| **independence** | referential | which labelers correlate / mirror sources → *effective* authority count vs nominal 400+ | shipped in spirit |
| **scope** | by authority-presentation | fraction of active labels whose declared enforcement grade presents at a scope **no** labeler can cash | raw material + 2 primitives shipped; population metric unbuilt |
| **warrant** | *(precondition, not a detachment)* | whether the labeler publishes enough to let an outsider check the other three | seed shipped (F-007) |

### Altitude note — these are not the four dials

`derive.py` already ships **four dials** (regime state, auditability risk, inference risk, temporal coherence) at the *per-labeler* altitude. These axes are the *population/ecosystem* read of the same concerns. They rhyme deliberately:

- **warrant ↔ auditability risk** — same concern, population vs labeler altitude.
- **freshness ↔ temporal coherence** — same concern, two altitudes.

Do not spawn a competing four-tuple. The axes are the ecosystem cut; the dials are the labeler cut. Where a measurement already exists at one altitude, lift it, don't re-derive it.

---

## Two corrections baked in (the record carries its own derivation)

The original governor-side framing had four peer axes named **standing / freshness / independence / witness**. Two were reframed; the record keeps the reasoning so the corrections don't get lost.

### standing → scope (self-presentation, not entitlement)

The original "standing-axis" — *labelers with no entitlement over what they label* — imports a govern-side requirement into the observe-side jurisdiction. In the [testimony frame](labelers-as-testimony.md), **anyone may testify about anyone**; requiring entitlement-to-assert *is* the editorial verdict the observatory refuses. It walks across the exact boundary it was meant to police.

Worse, "asserts at a scope it can't cash" still implies *graded* authority labelers hold in varying amounts. They don't. In ATProto **no labeler holds enforcement authority — the subscriber converts** (the [ConversionGap](specimens/) thesis). So "can't cash" is **uniform** across all labelers. The measurable quantity is therefore not authority-vs-claim; it is **self-presentation**: does the label present as *weather* or as *verdict*?

That is directly observable in the published `labelValueDefinitions` — `severity` / `blurs` / `defaultSetting`. A labeler declaring `defaultSetting: hide` (mandatory-hide) is presenting at a scope no labeler can cash. **The scope-axis is the population mirror of weather-not-verdict, pointed at the labels.**

And it is more built than it looks — the raw material already ships:
- `provenance.py` parses `policies.labelValueDefinitions` → `LabelValueDefinition(severity, blurs, default_setting)` and persists it.
- `emitter_classifier.py` already classifies on `severity / blurs / defaultSetting` alone (metadata-only path).
- `report.py` already quotes those defs as *"testimony, not truth … cited, not adopted. We do not editorialize the emitter's editorializing."* — weather-not-verdict already wired to this exact data.

What is missing is only the **population metric** over data already stored: *fraction of active labels whose `defaultSetting` presents at verdict-scope.* That is a SQL query, not a subsystem.

### witness → warrant (auditability precondition, not a fourth detachment)

"Signed-but-unwitnessed" trivially saturates: ATProto labels carry the labeler signature but essentially never publish a checkable basis, so the population is ~100% and the measurement discriminates nothing. Cryptographic witness is the wrong target.

Refine to **warrant = a publicly checkable basis pointer**: policy, evidence locus, appeal path — enough for an outsider to falsify the label. And warrant is **not a fourth way a label detaches**; it is the **auditability precondition** that gates whether the other three detachments are *visible from outside* at all. Without warrant, freshness/independence/scope are unfalsifiable from the outside. *The warrant is the witness; signed is not witnessed.*

Warrant has a **shipped seed**: `findings_pages.py` F-007 cohort — `events_30d > 1000` with **no** `labelValueDefinitions` = labels active at volume with no published basis. That is the warrant-gap, already counted.

---

## Boundary discipline (so this doesn't collapse into "everything is receipts")

- **Observe, don't govern.** The observatory measures the *absence* of standing/freshness/warrant. It cannot *impose* them. Measurements, not controls.
- **Weather, never verdict.** scope-axis especially: it reports the labeler's *self-presentation*, quoted not adopted. It never adjudicates whether a labeled subject deserves the label.
- **Detect-only is structural.** Running a properly-governed labeler is the prescriptive option and lives in a different sovereign; it is not this repo's to build.
- **Aggregate-first.** All four axes are ecosystem/labeler-level reads, never per-account profiling.

---

## Methodological honesty: the interferometer caveat

This record was sharpened across two Claude sessions. Those are the **same model** — correlated, not independent; **co-presence is not corroboration** (the very independence-axis under discussion, applied to the workflow that produced it). The genuine independence in the loop came from (a) *repo grounding* — testimony doctrine, the specimens track, ConversionGap supplied tool-class evidence one session did not have — and (b) Chatty being a *different model*. The error was caught not because two Claudes are independent witnesses, but because one of them was holding the repo. Trust the catch for that reason, not for the headcount.

---

## Status of each axis as a next-action handle (candidate, non-binding)

- **independence** — shipped in spirit: `boundary.py` edges + JSD orthogonality; the Driftwatch/Labelwatch topology overlap was an independence discovery before it had this name. *Naming, not building.*
- **freshness** — scaffolded, paused: `label_state` sidecar + left-censor flagging + `lifetime.py` (not yet wired through the sidecar). This frame re-motivates the parked slice.
- **scope** — raw material + two primitives shipped (above). Unbuilt piece is the population metric. Smallest real next step of the four — **scoped to build**: [`specs/gaps/gap-spec-scope-axis-v0.md`](../specs/gaps/gap-spec-scope-axis-v0.md) (`labelwatch.scope_presentation.v0`), not blocked on the AG root.
- **warrant** — F-007 seed shipped; the open work is generalizing warrant-gap from "no defs at all" to "no checkable basis pointer."

A record is a handle for review, not authorization to build. Ratify lazily; implement only when a task or acceptance criterion justifies it.
