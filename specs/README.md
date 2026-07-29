# labelwatch — Specs

Normative, authoritative documents. The decision rule is borrowed from agent_gov's `DOC_TAXONOMY`:

> **Could implementation be judged wrong by this document?**
>
> If yes → here. If no → `docs/`.

## Layout

```
specs/
  core/         shipped canonical specs (architecture, protocol, invariant)
  gaps/         explicit backlog — things we know are missing
  research/     non-committed lines of inquiry — doctrine/vocabulary parking, not authorization to build
```

## core/

| File | What it specifies |
|------|-------------------|
| `BOUNDARY_PHASE2_SPEC.md` | Label family normalization, contradiction edge filtering, FAMILY_MAP v3 |
| `TEMPORAL_RATIO_SPEC.md` | Temporal ratio computation (paper 22 work; deferred until paper progresses) |
| `SILENCE_ADJUDICATOR_V0.md` | Regime classifier for labeler silence — *why* quiet vs *is* quiet |
| `HOSTING-LOCUS-DATA-CONTRACT.md` | Cross-system data contract: driftwatch ↔ labelwatch hosting-locus reads |

## gaps/

Full index re-synced 2026-07-29 (the table had drifted to 1 of 14 entries).
All are candidate/backlog unless a status line inside says otherwise;
rejection notes are kept as records, not open work.

**Protocol boundaries**

| File | Gap |
|------|-----|
| `KNOWN_GAPS.md` | Protocol-level observability boundaries (e.g. labeler subscriber counts invisible) |

**Authority-effect axis**

| File | Gap |
|------|-----|
| `gap-spec-authority-effect-inference-v0.md` | `authority_effect_inference.v0` — receipted hypothesis generation |
| `gap-spec-authority-effect-triage-001.md` | Triage 001 — rank unprofiled volume, propose receipted candidates |
| `forward-note-authority-effect-report-lenses.md` | Forward note: report lenses over authority_effect |
| `gap-spec-scope-axis-v0.md` | `scope_presentation.v0` — population verdict-band of active labels |
| `gap-spec-warrant-axis-v0.md` | `warrant_presence.v0` — population auditability of active labels |
| `rejection-note-social-function-axis.md` | Rejection record: `social_function`/`moral_boundary` as persisted axis (too editorial) |

**Reference labelers and testimony honesty**

| File | Gap |
|------|-----|
| `gap-spec-reference-labeler-signal-review.md` | Corroboration required before degraded/gone_dark verdicts |
| `reference-role-taxonomy.md` | Candidate taxonomy of reference-labeler roles |
| `gap-spec-negative-space-report-block.md` | "Can testify / cannot testify" blocks on every report surface |
| `gap-spec-resolver-tail-health-surface.md` | Show resolver tail, not just current coverage |

**Workload isolation and witnessing**

| File | Gap |
|------|-----|
| `gap-spec-derive-workload-isolation.md` | Isolate the derive workload from the main writer |
| `gap-spec-report-generation-workload-isolation.md` | Report generation as isolated workload (WAL-pinning scar) |
| `gap-spec-witness-coverage-requirements.md` | What "full" external witness coverage means — host/APM split, scar-mapped acceptance (2026-07-28) |

## research/

| File | What it parks |
|------|---------------|
| `non-sovereign-perimeter-doctrine.md` | Doctrine + candidate vocabulary for a possible future Labelwatch lane watching custodians/lenses. Parked, not built. |

## Adding a new spec

1. Apply the rule. If the doc could be a basis for "the implementation is wrong against this," it's a spec.
2. Place it: `core/` if shipped, `gaps/` if explicit backlog, `research/` if speculative.
3. Update this README's table.

Step 3 drifted for ~3 months (1 of 14 entries indexed by 2026-07-29) because
it is a manual step nothing checks. If it drifts again, prefer a generated
index over another manual re-sync — a stale index is worse than none, since
it reads as an authoritative "these are the gaps" while hiding most of them.

## Architecture vs specs

`docs/architecture/` is the orientation surface — overviews, dataflow, signal model, public surfaces, failure modes. It explains how the system is shaped and why.

`specs/` is the binding contract. Implementation can be judged wrong against a spec; it cannot be "judged wrong" against an explanation.

Both refer to each other. Neither replaces the other.

## Adapted from

agent_gov's `docs/DOC_TAXONOMY.md` (north-star, partially adhered to in source).
