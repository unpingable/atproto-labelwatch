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
  research/     non-committed lines of inquiry — empty for now
```

## core/

| File | What it specifies |
|------|-------------------|
| `BOUNDARY_PHASE2_SPEC.md` | Label family normalization, contradiction edge filtering, FAMILY_MAP v3 |
| `TEMPORAL_RATIO_SPEC.md` | Temporal ratio computation (paper 22 work; deferred until paper progresses) |
| `SILENCE_ADJUDICATOR_V0.md` | Regime classifier for labeler silence — *why* quiet vs *is* quiet |
| `HOSTING-LOCUS-DATA-CONTRACT.md` | Cross-system data contract: driftwatch ↔ labelwatch hosting-locus reads |

## gaps/

| File | Gap |
|------|-----|
| `KNOWN_GAPS.md` | Aggregated known-gaps list across the project |

## Adding a new spec

1. Apply the rule. If the doc could be a basis for "the implementation is wrong against this," it's a spec.
2. Place it: `core/` if shipped, `gaps/` if explicit backlog, `research/` if speculative.
3. Update this README's table.

## Architecture vs specs

`docs/architecture/` is the orientation surface — overviews, dataflow, signal model, public surfaces, failure modes. It explains how the system is shaped and why.

`specs/` is the binding contract. Implementation can be judged wrong against a spec; it cannot be "judged wrong" against an explanation.

Both refer to each other. Neither replaces the other.

## Adapted from

agent_gov's `docs/DOC_TAXONOMY.md` (north-star, partially adhered to in source).
