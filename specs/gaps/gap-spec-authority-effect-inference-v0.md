# Gap spec: labelwatch.authority_effect_inference.v0 — receipted hypothesis generation

**Status:** ratified shape. Filed 2026-06-10 after the dashboard surfaced
that **61.1% of 7d events** sit in the `authority_effect` unprofiled bucket
and **88.8% of that bucket is the top-10 values**, **85.6% of it the top-5
labelers**. Small-head embarrassment, not long-tail drag.

**Inherits:**
- `forward-note-authority-effect-report-lenses.md` (the 8 follow-on report
  lenses; this spec is a different axis — closing the classifier gap, not
  extending readouts)
- `rejection-note-social-function-axis.md` (why we did not adopt motive-
  shaped axes; this spec preserves that discipline by refusing to claim
  emitter intent)
- `docs/labelers-as-testimony.md` (the parent doctrine; emitter
  descriptions are testimony, not truth)

**Companion implementation:** `gap-spec-authority-effect-triage-001.md`
(the next slice that consumes this receipt shape).

## Architecture sentence

> The inference result is itself testimony. Labelwatch classifies what
> *this evidence* warrants saying about a labeler/value, not what the
> labeler *means*. The clerk records the testimony; it does not become
> the bishop.

## The naming question, settled

Not:

```
labelwatch.authority_effect.v0           # implies adjudicated truth
labelwatch.labeler_intent.v0             # mind-reading with YAML
```

Adopted:

```
labelwatch.authority_effect_inference.v0
```

Read as: *the inference about a label's authority effect*, derived from
public evidence, with refusal grounds preserved. Frames the receipt as a
contestable hypothesis, not an oracle ruling.

## Premise

`AUTHORITY_EFFECT_MAP` is the manually-ratified classifier today
(`label_family.py:340`). `LABELER_DEFAULT_EFFECT` covers bespoke namespaces
where a single labeler's whole catalog converges on one effect
(`label_family.py:554`). Both are good. Neither scales to 61.1% of
network volume.

The dashboard already knows where the volume sits — top values, top
labelers, with usable emitter descriptions on 17/20 of them. What it
*does not* do is propose effect classifications from that evidence. So
the operator stares at "unprofiled: 1.36M events" and the system stares
back.

This spec defines the **shape of the proposal**: what evidence we admit,
what classification we emit, what we refuse to claim, what gets promoted
into `AUTHORITY_EFFECT_MAP` by a human, and what stays a candidate.

## What the receipt is (and is not)

It IS:

- A structured hypothesis: "given this evidence packet, the candidate
  effect is X with confidence Y."
- A citation of source evidence (description excerpt, observed label
  values, attachment loci).
- A list of refusal grounds: things this inference does NOT support
  claiming.
- A contestable artifact. A human can override, and the override is
  itself a receipted action.

It is NOT:

- A claim about labeler intent.
- A claim about content truth.
- A claim about downstream moderation effect (unless directly observed
  AND cited).
- A unified trust score, governance verdict, or labeler-rating.
- Authorization to emit labels back into the ecosystem (the detect-only
  structural constraint stands).

## Receipt schema — labelwatch.authority_effect_inference.v0

```json
{
  "receipt_kind": "labelwatch.authority_effect_inference.v0",
  "labeler_did": "did:plc:...",
  "labeler_handle": "labeler.antisubstack.fyi",
  "label_value": "substack",
  "scope": "labeler_value",

  "candidate_authority_effect": "reputational",
  "confidence": "medium",
  "tier": "emitter_described",

  "evidence": {
    "labeler_description": "<excerpt or null>",
    "labeler_description_source": "labelValueDefinition | declaration | profile",
    "observed_values": ["substack"],
    "attachment_loci": ["post"],
    "event_count": 10,
    "sample_targets": ["at://..."],
    "co_occurring_labels": [],
    "time_window": {
      "first_seen": "2026-05-..",
      "last_seen":  "2026-06-.."
    }
  },

  "rationale": [
    "Labeler frames the platform association as adverse testimony.",
    "Raw label value is not merely technical metadata in the labeler's stated context.",
    "Observed attachment is post-level; authority effect is reputational."
  ],

  "refusals": [
    "Cannot infer whether labeled content is true or false.",
    "Cannot infer downstream moderation behavior.",
    "Cannot infer private labeler intent."
  ],

  "promotion_status": "needs_human_review",
  "generated_at": "2026-06-10T..Z",
  "generator_version": "0.1.0",
  "config_hash": "sha256:..."
}
```

### Scopes

| Scope | Meaning |
|---|---|
| `labeler_value` | (labeler, label_value) pair — the default and most common scope |
| `labeler_default` | All values from this labeler converge on this effect (bespoke namespace) |
| `value_global` | This value behaves this way across all labelers that emit it (rare, requires multi-labeler evidence) |

### Tier (evidence layer that fired)

| Tier | What it means | Authority |
|---|---|---|
| `registry` | Already in `AUTHORITY_EFFECT_MAP`; we cite, do not re-propose | Highest |
| `emitter_described` | Inferred from emitter description / declaration | High when language is unambiguous; medium otherwise |
| `pattern_profile` | Regex/keyword match on the value string | Lower than emitter_described |
| `raw_fallback` | No evidence; classified as `unknown` | None |

### Confidence

`high | medium | low`. Coupled to tier, but not 1:1 — a tier-2 inference
on `"label values frame Substack association as adverse"` with the raw
value `"nazi-platforming"` is high-confidence reputational; a tier-2
inference on a vague emitter description with the raw value `"flag"` is
low-confidence anything.

### Promotion statuses

```
proposed                  - generated, awaiting review
auto_pattern_matched      - tier-3 match strong enough to populate without
                            human review (e.g. "spam" → safety/enforcement-adjacent)
needs_human_review        - tier-2 inferences default here
registry_promoted         - operator accepted; AUTHORITY_EFFECT_MAP updated
refused_insufficient_evidence - generator declined to emit a candidate
operator_override         - human classification differs from candidate;
                            cite both
```

## The tier stack

Composed in this order. Higher tiers always win.

### Tier 1 — Registry

`AUTHORITY_EFFECT_MAP[family]`. Already ratified. Cite, do not re-classify.

### Tier 2 — Emitter description (the big prize)

Inputs:
- `labelValueDefinition.locales[...].name + description`
- Labeler service declaration text
- Labeler account profile description (lowest priority of the three)

Heuristics live in `emitter_classifier.py` (already exists; this spec
extends it):

```
"labels accounts that should be hidden/muted"   → visibility_affecting / enforcement-adjacent
"frames X as adverse / harmful / dangerous"     → reputational
"labels posts containing screenshots of X"      → descriptive
"informational only"                            → descriptive / advisory
"for moderation clients"                        → advisory / visibility_affecting candidate
"automated / heuristic / inferred"              → telemetry-adjacent
```

Confidence is medium unless the description language is unambiguous (then
high) or contradictory (then low → refused_insufficient_evidence).

Inferences from this tier MUST cite the emitter excerpt in
`evidence.labeler_description`. No excerpt → no tier-2 emission.

### Tier 3 — Pattern profile

Boring deterministic keyword/regex over `label_value`:

| Pattern | Likely effect |
|---|---|
| `spam`, `scam`, `phishing`, `malware`, `impersonation` | safety / enforcement-adjacent |
| `nazi`, `terf`, `racist`, `abuse`, `harassment` | reputational |
| `bot`, `bridge`, `mirror`, `AI-generated` | descriptive (lean) / reputational (depending on co-evidence) |
| `hide`, `mute`, `block`, `adult`, `sexual`, `graphic` | visibility_affecting |
| `substack` alone (no framing) | descriptive |
| `substack-platforms-X` (descriptive frame) | reputational |

Pattern profiles are *lower* authority than emitter descriptions. Regex
is a golden retriever with a knife. Confidence ceiling: medium.

### Tier 4 — Raw fallback

`unknown`. Honestly so. Promotion status: `proposed` with no candidate
effect, surfaced to the unknown-label watchlist (forward note lens 5).

## The LLM-is-a-clerk-not-a-bishop boundary

The forward-note doctrine on LLMs in labelwatch is unchanged: **no
LLM-in-the-loop for emission, ever**. This spec preserves that.

If an LLM stage is ever introduced (it is NOT in scope for v0; this is a
forcing-case-gated future), the boundary is:

```
Deterministic tier 1-4 stack runs first.
If a tier-2 inference returns "needs_context" (description ambiguous +
no pattern match), AND a forcing case justifies adding an LLM:

  - The LLM is fed a sealed evidence packet (description excerpt + raw
    value + observed loci + co-occurring labels). Nothing else.
  - The LLM is required to output: candidate, confidence, rationale,
    refusals — in the same schema as deterministic tiers.
  - The LLM cannot freeform. No "because it seems bad" rationale.
  - The LLM output is always promotion_status=needs_human_review.
    Auto-promotion is not available to the LLM tier.
  - The LLM tier carries its own `tier: "llm_clerk"` value so audit
    trails distinguish it from deterministic tiers.
```

This is the doctrinal placeholder. v0 ships without it.

## What goes in the registry vs the receipt

A promoted classification adds a row to `AUTHORITY_EFFECT_MAP`:

```python
AUTHORITY_EFFECT_MAP["substack"] = "descriptive"  # default
# or
LABELER_DEFAULT_EFFECT["did:plc:..."] = "reputational"  # bespoke namespace
```

The receipt that triggered the promotion is preserved at
`receipts/authority_effect_inference/<labeler_did>__<label_value>__<gen_at>.json`.
The registry row stores the receipt's path or hash so the rationale is
recoverable on audit.

## UI rendering rules (load-bearing)

Three states, three copy templates:

**Inferred (tier 2/3):**
```
Authority effect: reputational
Basis: inferred from labeler description + observed label values
Confidence: medium
[receipt]
```

**Registry (tier 1):**
```
Authority effect: reputational
Basis: registry classification
```

**Unknown (tier 4 / refused):**
```
Authority effect: unclassified
Reason: <one short phrase from refusal grounds>
```

Do NOT render inferred and registry classifications identically. The
distinction is the whole point: the operator needs to know whether they
are looking at testimony-recorded-as-testimony or a ratified row.

## Composes with existing doctrine

- **Detect-only structural** — receipts are observation; they do not
  emit labels.
- **Weather, never verdict** — candidate framings are weather ("we read
  the labeler's testimony as reputational"), not verdicts about subjects.
- **Co-presence is not corroboration** — co-occurring labels are
  evidence of co-occurrence, not validation. Receipt rationale must not
  read "label X co-occurred with Y therefore X is reputational."
- **Aggregate-first** — receipt scopes are (labeler, value); per-target
  receipts are explicitly not supported. Targets appear only in
  `evidence.sample_targets` as citations.
- **Labelers-as-testimony** — this spec is the operational mechanism by
  which the testimony doctrine becomes legible at scale.

## What this spec does NOT do

- Does not authorize the inference engine. The engine is a separate
  slice (`gap-spec-authority-effect-triage-001.md`).
- Does not change `AUTHORITY_EFFECT_MAP` or `LABELER_DEFAULT_EFFECT`.
- Does not add database tables. Receipts are JSON on disk, like other
  observatory receipts.
- Does not introduce an LLM stage in v0.
- Does not commit to a `receipts/authority_effect_inference/` directory
  layout — the triage spec ratifies that.

## Acceptance for this spec

- [x] Receipt kind named, schema enumerated, scopes/tiers/confidence/
      promotion statuses defined.
- [x] Tier stack ordered with authority ranking.
- [x] LLM boundary stated as forcing-case-gated, not built in v0.
- [x] UI rendering rules give three distinct states.
- [x] Doctrine composition mapped (detect-only, weather, co-presence,
      aggregate-first, labelers-as-testimony).
- [x] Forward link to the implementing slice.

## Forward links

- `gap-spec-authority-effect-triage-001.md` — implementing slice
- `forward-note-authority-effect-report-lenses.md` — separately-scheduled
  report views that consume the populated registry
