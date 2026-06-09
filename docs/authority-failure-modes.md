# Authority failure modes

**Status**: candidate doctrine. Draft 2026-06-03.
**Companion**: [labelers-as-testimony.md](labelers-as-testimony.md) — the framing that names these as a coherent family.
**Companion**: [observation-export-custody.md](observation-export-custody.md) — the failure modes this doc predicts in the *labeler* ecosystem have a mirror in Labelwatch's *own export* path (Labelwatch silently becoming an authority through bundle-shape, timestamp-shape, or verification-failure-shape laundering). That mirror is what observation-export-custody refuses.

## What this doc is

This doc names predictable failure modes of the testimony layer Labelwatch observes — failures *in the labeler ecosystem*, not failures of Labelwatch as a system. The system-failure counterpart lives in [`architecture/FAILURE_MODES.md`](architecture/FAILURE_MODES.md) and covers WAL bloat, polling gaps, schema drift, and the like. The two docs should be read as a pair, not merged.

## Capability tags

Each failure mode carries a tag indicating how Labelwatch currently relates to it:

- **Observed by Labelwatch** — the system already surfaces a measurement aligned with this failure mode.
- **Partially observed** — Labelwatch surfaces measurements that touch the failure mode but does not name it as such, or measures only one half of it.
- **Not currently observed** — Labelwatch does not surface a measurement for this failure mode. Documented as a gap; no commitment to implement.

Labelwatch observes; it does not adjudicate. A capability tag describes whether a measurement *exists*, not whether the system has the standing to *call* a failure when it sees one.

---

## Failure modes

### Silent authority decay

**Shape**: a labeler that was once reliable goes stale, dead, or behaviorally degraded, but remains socially relied upon. Downstream consumers keep converting its testimony into constraint as if nothing has changed.

**Why this is a testimony-layer failure**: the producer's authority is implicitly inherited by every consumer who converts. If the producer's reliability drifts and the conversion machinery does not notice, the constraint quietly outlives the warrant.

**Capability**: **Observed by Labelwatch**. Surfaced as:
- Regime state (`stable` / `bursty` / `degraded` / `warming_up`) on every labeler.
- Endpoint reachability (`up` / `down` / `auth_required`) and probe history.
- Warm-up gating (new labelers suppress alerts until sufficient scan history is accumulated, so the failure mode is named-and-bracketed for new entrants rather than tripping on insufficient evidence).

---

### Scope creep

**Shape**: a label namespace that started as safety annotation gradually becomes faction marking or reputation tagging, with no formal renaming event. Consumers who originally subscribed for one purpose now route a different category of testimony into the same constraint.

**Why this is a testimony-layer failure**: the producer's claim shape changed; the consumer's conversion rules did not. The constraint surface absorbs the drift silently.

**Capability**: **Partially observed**. Surfaced as:
- `FAMILY_VERSION` evolution on the label-family classification (versioned vocabulary updates leave an auditable trail).
- `authority_effect` axis distinguishes structural claim-shapes (descriptive vs reputational vs visibility-affecting) per label string.

Not surfaced: a per-labeler measurement of "this labeler's emitted label mix drifted across authority-effect classes over time." The static cross-section is observable; the drift trajectory is not currently tracked.

---

### Single-source capture

**Shape**: one labeler becomes the de facto authority on a category of testimony, and consumer ecosystems converge on its claims without independent corroboration. Disagreement that would have signalled trouble vanishes, not because there was none, but because there was no one else looking.

**Why this is a testimony-layer failure**: an entire category of constraint ends up routing through a single producer's judgment. The composability surface collapses to one decision-maker without ever being formally delegated to.

**Capability**: **Partially observed**. Surfaced as:
- Cross-labeler boundary edges (when two labelers exist on the same category, disagreement edges are computed and surfaced).
- Volume share by labeler (a single labeler dominating active event volume in an authority-effect class is visible).

Not surfaced: a detection rule that fires when a category goes from N producers to one without comment. "Popularity is not standing" — a popular flaky reference should not become a calibration anchor — is recognized as a doctrinal rule in the memory layer but is not yet a Labelwatch-emitted finding.

---

### Consensus masking

**Shape**: no visible cross-labeler conflict on a category, not because the producers actually agree, but because downstream actors have all inherited the same judgment chain and re-emit it. What looks like consensus is upstream homogeneity. The boundary view goes quiet for the wrong reason.

**Why this is a testimony-layer failure**: agreement among consumers tells you about the consumers, not the producers. If the consumers all read from the same producer, their agreement is not corroboration; it is propagation.

**Capability**: **Partially observed**. Surfaced as:
- Cross-labeler disagreement edges: *the presence of* disagreement is named.
- Volume concentration: a category where most events originate from one labeler is computable.

Not surfaced: the dual measurement — *suspicious quiet*. The absence of disagreement on a category where independent producers should plausibly disagree is not flagged. The current view is asymmetric: noise is visible; suspicious silence is not.

---

### Skub / anti-Skub labeling

**Shape**: a label whose function is to route objects into factional camps rather than to describe a property of them. The label-token names a fight, not a feature. Consumers who convert it into constraint inherit the factional reading whether they meant to or not.

**Why this is a testimony-layer failure**: the testimony pretends to be descriptive but is actually positional. A constraint built on it is a constraint that takes a side without saying so.

**Capability**: **Observed by Labelwatch**. Surfaced as:
- `authority_effect` axis distinguishes `descriptive` (claims about properties) from `reputational` (claims that attach normative charge). A label classified as reputational is structurally closer to factional or standing-affecting testimony than to plain description. Labelwatch can observe that shape; it should not infer the faction.
- Per-labeler authority profile shows how much of a labeler's emission volume is reputational vs descriptive.

Reputational ≠ factional by definition. A `known-scammer` label is reputational without being factional; a label that routes objects into camps may be reputational *and* factional. The shape Labelwatch observes is "reputational"; the further reading is downstream of measurement.

---

### Reference capture

**Shape**: a labeler becomes operationally privileged because it is convenient, historically familiar, high-volume, or dashboard-visible, rather than because it remains healthy enough to serve as a calibration reference. Popularity, age, and convenience are mistaken for standing. A dead or degraded reference can keep shaping interpretation after its witness value has collapsed.

**Why this is a testimony-layer failure**: a reference labeler is implicitly converted into constraint by every downstream comparison that uses it as a yardstick. If the reference has degraded but is still treated as a reference, the comparisons inherit the degradation without naming it.

> A reference labeler is a calibration witness, not a memorial plaque.

**Capability**: **Observed by Labelwatch**. Surfaced as endpoint reachability, event volume, anomaly count, regime state, and reference-set health. The component measurements are visible per-labeler. Reference-set membership is editorial — a labeler is or is not in the reference set — and the editorial discipline is what this failure mode is about: retiring references when their witness value has collapsed, rather than leaving them in place because they have always been there.

---

### Social enforcement substitution

**Shape**: a label gets converted into constraint informally — through swarm pressure, off-platform escalation, or social-graph reputation cascades — without any formal client / app / platform conversion. The tooling appears more effective than it is because the constraint is being produced by the social layer downstream.

**Why this is a testimony-layer failure**: the conversion step happens, but outside the parts of the stack that are inspectable. The "moderation" looks performed-by-the-protocol when it is actually performed-by-the-network.

**Capability**: **Not currently observed**. Off-platform pressure and social-graph cascade are out of Labelwatch's view by construction (aggregate-first, motive-blind, sealed lab). This failure mode is documented because pretending it does not exist would be worse than naming a gap.

---

### Appeal invisibility

**Shape**: correction paths for a label — retraction, dispute, counter-evidence — exist socially, privately, or via direct-message appeal, but are not inspectable as protocol history. A label that was retracted is not visibly retracted; a label that was disputed leaves no public trace of the dispute.

**Why this is a testimony-layer failure**: the testimony record looks one-sided not because the case was one-sided but because only one side leaves a protocol footprint. Downstream consumers converting that testimony into constraint cannot see what they are missing.

**Capability**: **Not currently observed**. Negations (`neg=1` label events) are ingested and visible, but explicit retraction-as-correction (with reasoning, with public dispute history) is not a first-class concept in the protocol layer Labelwatch reads. This is a protocol gap as much as a measurement gap.

---

## Historical stress case

### Aegis

Aegis is included as a historical stress case because it made the testimony/consequence boundary visible. The point here is not to relitigate Aegis, characterize its operators, or adjudicate individual labels. The point is the general failure shape: socially consequential label streams can accumulate operational force faster than their appeal, correction and corroboration machinery can carry.

This is a property of the producer–consumer–enforcer asymmetry described in the companion doc. It does not require any individual claim to be right or wrong; it follows from the structure. Aegis is one of several historical demonstrations that the testimony layer is a real layer with real failure modes, separable from any individual case.

---

## What this doc is not

- Not a verdict on any named labeler. Labelwatch observes; it does not adjudicate.
- Not a roadmap for "we should build X to detect Y." The Not-currently-observed tags are gaps, not commitments. Implementation requires its own forcing case.
- Not an architecture failure-modes doc. See [`architecture/FAILURE_MODES.md`](architecture/FAILURE_MODES.md) for that.
- Not exhaustive. New failure modes will be added when they surface; the existing entries may be revised as the framing earns its keep.
