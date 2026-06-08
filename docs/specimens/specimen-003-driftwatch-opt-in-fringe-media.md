# Specimen 003 — Driftwatch opt-in consumer adoption

```
Input testimony:
  skywatch.blue + fringe-media
  provenance: third-party / emitter_declared

Consumer:
  driftwatch

Policy artifact:
  scripts/consumer_policy/policy.py
  version: external_advisory_caveats-v1.0.0
  commit: b4a8e3e

Consumer receipt:
  input packet hash
  policy artifact/version
  action taken
  timestamp
  inherited caveats

Admissible conclusion:
  Driftwatch locally adopted this third-party label into an
  opt-in consumer-scoped action.

Inadmissible conclusions:
  Bluesky default client converted it.
  The label became global_platform.
  Another consumer adopted it.
  The non_global_provenance caveat was discharged.
```

## Where each line is anchored

| card line | anchored in |
|---|---|
| input testimony | `specimen-003-driftwatch-opt-in-fringe-media.evidence.json` → `LabelObservation` |
| provenance | same file → `LabelerEmitterDocumentation.consumer_scope = emitter_declared` |
| consumer | same file → `ConsumerAdoption.consumer.consumer_id = "driftwatch"` |
| policy artifact | driftwatch repo, commit `b4a8e3e`, file `scripts/consumer_policy/policy.py`, `POLICY_VERSION = external_advisory_caveats-v1.0.0` |
| receipt | driftwatch repo, `data/consumer_policy/receipts/20260608_184805Z-3a6bb004a53d.json`, sha `3a6bb004a53de7c30401daa737da577b76526478e550a6251f29cf1d13519168`; the same fields are mirrored into `specimen-003.ConsumerActionObservation.receipt` |
| admissible conclusion | classifier output: `ConversionGap = {name: complete_path, surface: consumer_local_state, consumer_scope: opt_in_consumer_observed}`; exporter: `consumer_scope_effective = opt_in:driftwatch` |
| "Bluesky default client converted it" — inadmissible | `PolicyDocumentation.status = absent_for_consumer` for `bsky.app-default-client`; nothing in the bundle speaks to default-client behavior |
| "became global_platform" — inadmissible | guarded by `_classify_consumer_scope` precedence + `test_opt_in_does_not_promote_to_global` |
| "another consumer adopted it" — inadmissible | always-fired inadmissible `no_cross_consumer_inference` whenever `ConsumerAdoption` is present |
| "non_global_provenance caveat was discharged" — inadmissible | inherited into `export_caveats`; verified by `test_opt_in_consumer_exports_with_local_scope_caveat` |

## What this specimen is for

The first end-to-end case where a third-party label gets converted
into a real consumer-local constraint with full provenance
inheritance. Forms the counterexample to the inverse rule
"third-party → no conversion": the rule was never that, it was
"third-party + no documented adoption → no conversion." With
adoption + receipt, conversion exists — locally, scoped, inherited.

Same testimony; different consumer evidence; different admissible
conclusion.
