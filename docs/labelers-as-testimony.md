# Labelers as testimony

**Status**: candidate doctrine. Binding distinction ratified; elaboration draft 2026-06-03.
**Companion**: [authority-failure-modes.md](authority-failure-modes.md) — the failure modes this framing predicts.

## The binding distinction

> Labelers are not moderation. Labelers publish classification or reputation testimony. Moderation begins when clients, services, defaults, feeds, communities, or platforms convert testimony into constraint.

That sentence is the ratified part of this doc. Everything below is doctrine elaborating on it, and it is candidate-not-binding until it earns its keep.

## Core claims

### 1. Labelers are claim producers, not moderators

- **Labeler**: claim producer. Publishes a tag — a category, a flag, an interpretation — about a post or an account, in a public stream.
- **Client / feed / app / community**: claim consumer. Receives the published claims and decides what to do with them.
- **Moderation**: enforcement or constraint. Begins at the conversion step, not at the publication step.

A labeler that publishes claims into a stream nobody consumes is testifying into a void. A labeler whose claims are consumed by an app default, a feed filter, or a community norm has had its testimony converted into operational constraint — and that conversion is where moderation actually happens.

The Bluesky moderation stack is explicit about this split: network takedowns, labels from moderation services, and user controls such as mutes and blocks are named as separate layers. Labels are one layer. The system is not the layer.

### 2. Composable moderation is mostly composable testimony consumption

The compositional surface lives downstream of the labeler. The sovereign layers — the ones that actually produce constraint — are:

- App defaults (which labels are on, hidden, warned, content-warning'd by default).
- Client behavior (what the rendering app chooses to do with a label).
- Discoverability mechanics (feeds, search, recommendation).
- Hosting decisions (PDS-level takedowns, account suspensions).
- Official platform policy.
- Social enforcement (swarms, off-platform pressure, reputation cascades).

When people say "composable moderation," they usually mean this consumption surface. The labeler is one input to it. Mistaking the input for the system is the most common reading error.

### 3. Labels are closer to single-source ambient notes than moderation verdicts

The Community Notes machinery — multi-perspective quorum, surfaced reasoning, visible appeal — is a useful comparison precisely because labels are missing most of it. Labels are testimony that has not, by default, been through any of those filters. The differences with Community Notes are load-bearing:

- **Usually no quorum.** One labeler's claim is sufficient to enter the stream. There is no equivalent of the multi-perspective rating mechanism that Community Notes requires.
- **Often weak explanation.** A label is a token (`spam`, `nudity`, `!hide`, `terf-gc`) with optional commentary; the underlying reasoning is rarely visible in the protocol record.
- **Often weak appeal / correction visibility.** Disputes, retractions, and counter-evidence may exist socially or privately, but they are not generally inspectable as protocol history.
- **Can create reputation weather rather than case-specific moderation.** A labeled account does not need to be banned to be operationally penalized. The label changes how the account is perceived, filtered, inferred about, and engaged with — a diffuse social pressure rather than a discrete enforcement event.

### 4. The conversion step matters

> The label is not the moderation. The label is the claim that moderation may later pretend was enough.

The dangerous reading is: "the labeler said X about this account, therefore X is operationally true." That collapses the producer–consumer–enforcer distinction and treats the testimony as if it were the verdict.

The conversion step matters in three ways:

- **Inheritance** — a downstream consumer that converts a label into constraint inherits the testimony's strengths and weaknesses without renaming them. If the testimony was thin, the constraint is thin; that does not stop the constraint from being operationally heavy.
- **Fig leaf risk** — "someone labeled it" can be invoked retroactively as procedural cover for a constraint that was actually decided on other grounds. The label becomes the claim that moderation may later pretend was enough.
- **Compounding** — multiple weak testimonies stacking into one strong-looking constraint is not corroboration. (See [`constraint_copresence_not_corroboration`](../README.md) in the memory layer for the durable version of this rule.)

## What Labelwatch does and does not do under this framing

Labelwatch observes testimony-layer behavior:

- Who is testifying (labeler discovery, registry).
- What kind of authority each testimony attempts to exercise (authority_effect axis: descriptive, advisory, reputational, visibility-affecting, enforcement-instruction, decorative, telemetry, unknown).
- How reliably each labeler is observable, and whether its behavior is regime-stable.
- Where labelers contradict each other on overlapping subjects (boundary instability).
- How an account looks from the receiving end of the testimony stream (per-account climate).

Labelwatch does not:

- Adjudicate the truth of any claim.
- Score labelers as good or bad.
- Aggregate testimony into a verdict.
- Tell consumers whether to convert a given label into constraint.

That is not modesty; it is the structure of the role. Labelwatch is positioned upstream of conversion. Adjudication is a different job that requires different inputs, different accountability, and different durability than what an observatory provides.

## Where this connects to other work

- **[authority-failure-modes.md](authority-failure-modes.md)** — the failure modes this framing predicts in the testimony layer, with capability tags indicating which Labelwatch already measures.
- **[../docs/architecture/FAILURE_MODES.md](architecture/FAILURE_MODES.md)** — system-architecture failure modes (storage, ingest, schema). Distinct doc, distinct domain: that one is about Labelwatch as a system; this pair is about the testimony layer Labelwatch observes.
- The formal sketch — a partition-theoretic version of "the conversion step matters" — lives outside this repo in an Admissibility / ConsequencePartition annex. The empirical hooks named there (an actual pair the platform collapses but the policy separates) are the formal echo of Labelwatch's boundary findings. Pointed at here for completeness; not imported.
