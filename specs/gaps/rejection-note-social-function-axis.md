# Rejection note: `social_function` / `moral_boundary` as persisted axis

**Status:** rejected for persistence as a structural axis. Filed 2026-06-02.
**Companion to:** authority_effect (built — `src/labelwatch/label_family.py`,
`src/labelwatch/authority_inventory.py`).

## What was proposed

A `social_function` axis on label families with values:
`governance / telemetry / moral_boundary / toy_badge / identity_boundary /
infrastructure / unknown`, persisted alongside `domain`, `polarity`, and `kind`.

The proposal grouped labels by the sociological role they appear to play:
`spam` as governance, `terf-gc` and `gaza-genocide-supporter` as `moral_boundary`,
identity/pronoun labels as `identity_boundary`, novelty labels as `toy_badge`.

## Why rejected

The map from label string to `social_function` value is itself the editorial
act. Calling some classifications "moral_boundary" and others "governance"
draws a line that:

- is contested at exactly the labels that drive the most heat (`hate` is on
  multiple lists in the original proposal),
- depends on the issuer's framing and the target's context, neither of which
  the schema can see,
- gets read as labelwatch's verdict regardless of how the descriptive copy is
  worded ("appears to function as" vs. "is moralizing" doesn't move the needle
  for anyone reading the JSON artifact a year from now).

Labelwatch's stated posture (CLAUDE.md, README): observation only, aggregate
first, descriptive language, host family is not operator identity. A
`social_function=moral_boundary` enum imports an operator claim into a
structural schema. The classifier map *is* the verdict.

This rhymes with two existing pieces of doctrine:

- *Popularity is not standing* — a popular flaky reference label is an
  interesting subject, not calibration. By analogy, a contested editorial
  framing should not be promoted into a persisted axis.
- *Prose–schema alignment* — if prose distinguishes two roles, the schema
  eventually has to. But the inverse also holds: if the schema persists an
  editorial framing, the prose disclaimer cannot launder it.

## What was built instead

`authority_effect` — classification of what kind of authority a *label, as a
string*, attempts to exercise:

```
enforcement_instruction
visibility_affecting
advisory
reputational
descriptive
telemetry
decorative
unknown
```

This carves a distinction the existing axes do not: `spam` (a policy_claim
attaching normative charge — reputational) is different from `mod-hide`
(an actuator that affects visibility — visibility_affecting) is different
from `mod-takedown` (an actuator that removes — enforcement_instruction).
That distinction is structural, not editorial: it describes the shape of
what the label asks the system to do.

`authority_effect` is strictly explicit. There is no structural fallback.
Labels not in `AUTHORITY_EFFECT_MAP` return `unknown`. Unknown is a valid
finding, surfaced in the report, not silently bucketed into the closest
existing category.

## Forcing case (recorded for the future)

The boundary analysis surface already mixes decorative, telemetry, reputational,
and actuator labels in its conflict counts. The authority_effect inventory
makes that mix visible at the namespace level before operators infer
governance significance from heterogeneous label families. Follow-on report
lenses (authority-effect conflict matrix, authority-effect filters on
boundary analysis, labeler authority profile, unknown watchlist) are tracked
in the next-actions note alongside this rejection.

## What would change this decision

A documented operational need where `authority_effect` alone is insufficient —
specifically, a recurring decision that hinges on distinguishing reputational
labels by their sociological function (e.g., "is this label being used to
adjudicate community membership vs. to attach ideological taint") — would
reopen the question. The reopen would not start from `social_function`; it
would start from the specific decision and work backward to the minimum
schema that supports it.

This is not a moratorium on noticing the sociology. It is a refusal to
persist labelwatch's sociology as schema.
