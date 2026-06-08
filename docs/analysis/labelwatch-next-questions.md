# Labelwatch next-questions backlog

> Non-bundle analyses. The specimens-track ladder (D / D.5 / E / F / G)
> built per-event provenance discipline. That work is now closed for
> the single-consumer adoption case. These are the analyses that don't
> come out of the bundle math — most of Labelwatch's actual analytic
> questions live here.
>
> **Operating rule:** Do not start "Bundle H" unless one of these
> analyses produces a finding that forces a new schema/classifier
> distinction. The ladder has a tendency to eat the project like a
> Roomba with tenure.

## Doctrine update from the census

The consumer-conversion census produced one canonical line worth
preserving here as the **anti-wildfire lemma**:

> **Consumer conversion is not assumed from labeler publication.
> Consumer conversion is not assumed from protocol affordance.
> Consumer conversion requires observed client behavior, explicit
> user preference, or a named synthetic specimen.**

Bundle G is reclassified accordingly:

  - **Status:** dormant guardrail
  - **Evidence class:** future-compatible / counterfactual
  - **Urgency:** low
  - **Promotion trigger:** observed third-party consumer conversion,
    runtime config-fetch evidence, or closed-client behavioral proof

The machinery is correct; its threat model is currently speculative.
Don't tear it out; don't motivate new work from it.

## Closed by the census (2026-06-08)

- [`consumer-conversion-census.md`](consumer-conversion-census.md) —
  empirically answered: no sampled production client hardcodes a
  third-party labeler as a default. F-001/F-004's wildfire framing
  is downgraded to fire-code framing. Bundle G machinery is
  defensible but not urgent.

## Inside the goblin math vs outside it

The goblin math is excellent at preventing one specific failure:
**accidental overclaiming.** But Labelwatch also needs to answer
messier questions:

```
Is this ecosystem healthy?
Is it legible?
Is it usable?
Is it concentrating power?
Is it mostly abandoned experiments?
Are users actually protected, informed, confused, or just decorated?
```

Those are not formal-boundary questions. They are product /
research / ecosystem questions. The bundle ladder produces a great
incident recorder; these questions ask "what is this thing FOR, and
who would use it."

Note (correction, 2026-06-08): an earlier draft of this document
promoted **definition drift** to the top of this list, framing it
as "the actually-active problem surface." That was wrong. Definition
drift IS interesting and IS where F-006 lives — but it is still
inside the goblin-math gravity well (later definition ≠ earlier
emission meaning; doctrine with a database). The census was
outside-ish because it asked "is the ecosystem doing the feared thing
at all?" Definition drift pulls back toward native gravity. The
actually-outside analyses are the audience/marketplace/maturity/
topology/UI questions below.

## A. Outside the goblin math (truly outside)

These don't primarily ask "what may we conclude?" They ask "what
is happening, who cares, and what is this thing good for?"

### A1. Labeler operator-maturity analysis  **(next slice)**

Treat labelers like services. Not "is the conclusion admissible?" —
"would I trust this service enough to expose it to users?"

Output: a boring table per labeler:

```
labeler_did | display_name | active_recently | declares_scope
            | explains_labels | has_contact_or_appeal_path
            | has_stable_service_record | label_count_30d
            | distinct_targets_30d | user_visible_consequence_known
            | maturity_class | notes
```

Maturity classes (heuristic, not measurement):

```
experimental         — recent, sparse, possibly one-off
abandoned            — was active, now silent / no recent service record updates
personal/reputational — one operator labeling for personal-style reasons
community-service    — sustained activity, declared scope, public-facing
moderation-infrastructure — high volume, clear scope, treated as infrastructure by clients
platform-root        — the default-subscribed labeler (mod.bsky as of census)
unknown              — insufficient signal
```

**Why this is outside the math:** "Would I trust this thing enough
to expose it to users?" is not an admissibility question. It is an
SRE / service-readiness question wearing a moderation hat. Closer to
"is this service ready for production traffic" than "is this
conclusion derivable from this evidence."

**Boring is the point.** A boring per-labeler operational table that
a Bluesky user could read and decide "yes I'd subscribe / no I
wouldn't" is more useful than another schema refinement.

### A2. Audience / use-case analysis

Who is Labelwatch for, practically? Possible users:

```
Bluesky users choosing labelers
client developers deciding what to expose
labeler operators auditing themselves
researchers studying decentralized moderation
journalists investigating moderation ecosystems
platform governance people watching power drift
```

Each wants a different report. Right now Labelwatch is shaped like
an aircraft incident recorder. The question is whether anyone wants
a consumer guide / operator audit / ecosystem observatory / research
dataset / governance warning system instead — or in addition.

### A3. Labeler marketplace / discoverability

Not "are labels admissible?" — instead:

```
Which labelers are visible?
Which are findable by ordinary users?
Which ones appear in client UIs?
Which ones tell users what subscribing actually does?
```

A labeler can be perfectly admissible and still be unusable.

### A4. Ecosystem topology

Who labels what kinds of things? Aggregate topology only — no
per-account dossiers:

```
labeler clusters
host/PDS concentration
domain-family concentration
language/community concentration
topic concentration (where inferable without creepy enrichment)
overlap between labelers
islands of mutually interacting labelers
```

Tells you whether decentralized moderation is actually decentralized
or whether it's three operators and a cron job.

### A5. UI consequence analysis

For each labeler, ask: what would a user actually SEE?

```
badge | warning | blur | hide | profile interstitial
| feed suppression | nothing visible
```

The protocol labels are abstract. The user experience is concrete.
This maps events to lived UI consequence, not admissible inference.

Output:

```
labeler | label value | likely UX consequence | user-visible explanation
        | reversibility | confusion risk
```

### A6. Legibility ("would normal people understand this?")

Sample label descriptions, classify:

```
clear | ambiguous | inside joke | ideological shorthand
| technical/protocol jargon | overbroad | actionable | non-actionable
```

A label whose description makes sense only to three Bluesky regulars
and a raccoon in a Nix shell has limited public value.

### A7. Labeler lifecycle / abandonment

Not semantic drift. Simpler:

```
active | quiet | dead | burst-only | one-off experiment
| recently revived | high-volume sustained
```

Users may subscribe to effectively abandoned labelers. That's a
stale-service problem, not an admissibility problem.

### A8. Comparative moderation ecology

Compare by labeler type:

```
platform-operated | community-operated | personal/reputational
| topic-specific | anti-abuse | joke/novelty | political/ideological
| spam/scam-focused
```

Ask: which classes are growing / stable / declining / most
visibility-affecting? This is Labelwatch as observatory, not
theorem mill.

## B. Inside the goblin math (doctrine with a database)

Still useful; still has its place. But now correctly framed: these
extend admissibility apparatus and consume the existing schema.
They are NOT primarily about understanding the ecosystem.

### B1. Definition drift / semantic versioning of labelers

(Demoted from a brief mis-promotion to #1 of the overall list.
Belongs in the goblin-math half.)

**Question:** Do labelers change what their labels MEAN over time?

- Did `labelValueDefinitions` change between observed versions of the
  service record?
- Were emitted label values later removed from declarations?
- Did `defaultSetting` shift `ignore → warn/hide` or vice versa?
- Did `blurs` / `severity` change?
- Did a labeler emit values BEFORE declaring them in its service
  record?
- Did it declare values AFTER emitting them?

**Why this matters:** A label fired in March under definition v1
reads differently in June under definition v2. F-006 is one instance
of this shape. There are probably more.

**Method:** Diff `discovery_events.record_json` per labeler over
time; compare against `label_events` timestamps. The data is already
there.

**Output shape:**
```
labeler | label_value | first_emitted | declared_at_emit | later_declared
        | later_removed | semantic_changed | drift_class | caveat
```

`drift_class` taxonomy: `additive | removal | semantic_change |
retroactive_cover | emit_without_declare | stable | unknown`.

**Key refusal:** A later declaration does not discharge
undeclared-at-emission provenance. (The retroactive-laundering
refusal the goblin math trained for, applied one room over.)

### B2. Receipt-reader closure

**Question:** Which reports, dashboards, or exports currently change
behavior when a Labelwatch caveat / refusal / receipt exists?

**Today's answer:** Only the synthetic Driftwatch consumer-policy tool
written for Bundle G stage 2. The full exporter receipt-chain has
exactly one downstream reader, and that reader was written by us.

**Useful next step (cheapest):** Wire Driftwatch's existing cluster
report path to consult the `external_advisory_caveats` roster:
```
cluster_claim: admissible | caveated | narrowed | suppressed
reason: non_global_provenance | undeclared_label_value
       | consumer_not_observed | stale_definition
```
If this surfaces nothing useful, the receipt apparatus is honest
documentation, not enforcement infrastructure. That's still fine —
but knowing the difference matters.

**Why this is highest-value:** Closes the only loop where Labelwatch
produces an output that another system actually USES. Until this
exists, the exporter is admissibility-philosophy-with-a-SQLite-habit.

### 2. Definition drift / semantic versioning of labelers

**Question:** Do labelers change what their labels MEAN over time?
Specifically:

- Did `labelValueDefinitions` change between observed versions of the
  service record?
- Were emitted label values later removed from declarations?
- Did `defaultSetting` shift `ignore → warn/hide` or vice versa?
- Did `blurs` / `severity` change?
- Did a labeler emit values BEFORE declaring them in its service
  record?
- Did it declare values AFTER emitting them?

**Why this matters:** Labels are interpreted by their definitions.
A label fired in March under definition v1 reads differently in June
under definition v2. F-006 (mod.bsky emits `needs-review` without
declaring it) is one instance of this shape. There may be many.

**Method:** Diff `discovery_events.record_json` per labeler over time;
compare against `label_events` timestamps. The data is already
there. Bundle F made the snapshot path available; this analysis
consumes it.

**Output shape:**
```
labeler | label_value | first_emitted | first_declared | last_declared
        | definition_changes | direction (emit-before-declare,
        declare-then-undeclare, etc.) | most_recent_diff
```

**Why this is Labelwatch-native:** This is exactly the kind of
"observatory" question Labelwatch was built for. Aggregate; not
per-account; uses receipts (the service-record diff is the receipt
of definition change).

### 3. Boundary instability between labelers

**Question:** Do labelers disagree about the same target / label class
/ host family?

**Specific shapes worth measuring:**
- Same target, conflicting label classes (one labeler says "spam",
  another says nothing; one labeler hides, another only warns)
- Same label value, different `authority_effect` interpretation by
  different labelers (one uses `intolerant` to mean reputational,
  another to mean enforcement)
- Same target flips between reputational / advisory / enforcement
  classifications over time
- Same host family repeatedly labeled by high-risk labelers
- Labeler A labels accounts that labeler B treats as ordinary (or
  actively un-labels via `neg=1`)

**Aggregation discipline:**
- PDS host family (per existing `hosting.py`)
- Handle domain family
- Labeler class (official_platform / reference / unknown / etc.)
- `authority_effect` (existing axis)
- Inference risk / auditability risk / temporal coherence (the
  existing four-dials)

**Why this is Labelwatch-native:** Per CLAUDE.md this is allegedly
one of Labelwatch's core product powers. It has been weirdly
neglected by the specimens-track work. The data already supports it
(label_events × labelers × cross-labeler joins exist; `boundary.py`
and `boundary_edges` table already exist for some of this — the
question is whether the existing boundary analysis is actually
answering the questions above or just producing JSD-style summary
stats).

**Avoid:** per-account moral dossiers. Aggregate by host/family/class.

### 4. Blast-radius simulation

**Question:** "If this labeler were adopted by every Bluesky client
as a default, what would change?"

**For any labeler L, simulate:**
- Adopted as badge-only: affected target count
- Adopted as warn: affected feeds / clusters
- Adopted as hide: affected feeds / clusters
- Adopted globally as enforce: affected host families

**Discipline mandatory on every row:**
```
simulation_only = true
consumer_observed = false
global_platform = false
counterfactual_basis = <labeler service record version at time T>
```

**Why this is useful even after the census:** the census says no
client currently defaults to third-party labelers. Blast-radius gives
a numerical answer to "what would it cost if one did?" without
making any normative claim. It also surfaces which labelers have
genuinely broad reach (per emission counts) vs which are noisy.

**Why this is NOT a Bundle:** It's a one-shot report, not a schema
extension. Output is a table per (labeler, hypothetical-adoption-mode);
the math is `count(distinct target_did)` filtered by labeler/label;
nothing structural.

### 5. Labeler self-consistency score

**Question:** Per-labeler operational consistency. NOT a moral
judgment. NOT a "good labeler / bad labeler" classifier.

**Dimensions:**
- Declared-value coverage (% of emitted labels that are in the
  labeler's own `labelValueDefinitions`)
- Definition stability (rate of `labelValueDefinitions` change over
  time)
- Negation/removal behavior (how often does the labeler `neg=1` its
  own prior labels?)
- Temporal burstiness (emission spikes / steady-state ratio)
- Auditability surface (does the labeler publish a service record at
  all? Does it list a reason endpoint?)
- Scope discipline (does it label across many domains or stay within
  a declared scope?)
- `authority_effect` drift (does the same label_value's emission
  pattern shift over time?)

**Output:**
```
labeler | declared_values | emitted_values | undeclared_emits
        | definition_changes | caveats | confidence_window
```

**Why boring is good:** A boring self-consistency table is exactly
the kind of artifact that survives contact with anyone who enjoys
saying "well actually." It's also the kind of thing that informs a
reasonable user when deciding whether to subscribe to a third-party
labeler.

### 6. Closed-client behavioral probes

**Question:** What do closed-source clients (Tokimeki, deck.blue,
Bluejeans, Skeets, Sora, etc.) actually DO with labels?

**Method:** subscribe a test account; log into the client; observe
whether labels from a known third-party labeler render. Repeat for
each closed client.

**Why this is in the backlog and not the census:** outside the
goblin-math scope (requires interactive testing, not static
analysis). But the only way to extend the census to clients we
can't read.

**Risk:** test-account ToS issues; possibly account-takedown risk if
clients have anti-automation gating. Manual / sparing.

## Meta-discipline

- **Receipt > schema:** if an analysis produces a useful receipt that
  could be ingested by some other system, prefer it over another
  schema refinement. The schema is mature enough.
- **Boring > clever:** the four-dials, host-family-aggregation,
  declared/emitted-coverage style outputs are valuable specifically
  because they're hard to argue with. Save cleverness for places
  where boring is wrong.
- **Aggregate-first:** the project doctrine. Anything that requires
  per-account narrative is the wrong shape, even if the underlying
  question is interesting.
- **Falsify before fortify:** if an analysis can falsify the urgency
  of existing machinery (like the census just did for Bundle G), do
  THAT analysis before building more machinery. Half the value of
  the census is that it lets us de-prioritize without tearing things
  out.

## When to start a "Bundle H"

A new bundle is justified ONLY when one of these analyses produces a
finding that:

1. Cannot be represented in the current schema, AND
2. Has a concrete operator/operational impact (not "what if some
   hypothetical consumer..."), AND
3. Survives an honest "is this fire code for a building that doesn't
   exist?" check.

Definition drift (#2) is the likeliest source of such a finding,
because it can produce real "stale-but-currently-cited" cases that
the existing schema doesn't quite express. Boundary instability (#3)
and self-consistency (#5) are more likely to produce reports than
schema work — they consume the current schema, not extend it.

## Provenance

- **Compiled:** 2026-06-08, in response to the consumer-conversion
  census' falsification of the F-001/F-004 wildfire framing.
- **Source of question priorities:** operator review of the
  specimens-track closure receipt + the census' implications, plus
  Labelwatch's `CLAUDE.md` (which names anomaly detection / boundary
  instability / hosting-locus as core product capabilities the
  specimens track did not touch).
- **Living document:** add to / strike through as analyses land.
