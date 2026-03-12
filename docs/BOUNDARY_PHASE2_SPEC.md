# Boundary Phase 2: Domain Classification & Fight Cards

**Status:** Spec draft. Phase 1 primitives deployed (schema v18, 725 contradiction
edges, 500 shared targets). This spec covers what's needed to make Phase 2
synthesis actually useful.

## The problem Phase 1 revealed

Phase 1 first-cook finding: JSD=1.0 everywhere. This isn't semantic conflict —
it's **ontological orthogonality**. Novelty labelers applying badge-ecosystem
labels (e.g., `oracle.posters.rip` vs `stechlab-labels.bsky.social`) have
completely disjoint label vocabularies by design. High JSD between them is
correct math but meaningless signal.

Without domain classification, Phase 2's BoundaryFightCard would fire on every
pair of unrelated labelers. The synthesis layer needs to know whether two
labelers *should* overlap before flagging disagreement.

## Domain taxonomy

Six domains, assigned per label family:

| Domain | Examples | Signal when divergent |
|--------|----------|----------------------|
| `moderation` | porn, sexual, nudity, graphic-media, spam | High — real policy conflict |
| `trust` | impersonation, scam, misleading, ai-generated | High — epistemic disagreement |
| `politics` | political, election, activism | High — governance-relevant |
| `identity` | lgbtq, furry, vegan, religion | Medium — community boundary |
| `badge` | custom stickers, achievements, oracle-picks | Low — orthogonal ecosystems |
| `meta` | automation, bot, test, dev | Low — infrastructure labels |

**Default**: unclassified families get `unknown`. Unknown-vs-unknown is treated
as low-signal (same as badge-vs-badge).

### Assignment method

Static mapping in `label_family.py`, versioned. Ship a `FAMILY_DOMAINS` dict
keyed by normalized family string, with a `classify_family_domain()` function
that falls through to keyword heuristics for families not in the dict:

```python
def classify_family_domain(family: str) -> str:
    # 1. Exact match in FAMILY_DOMAINS dict
    # 2. Keyword heuristics (contains "porn" → moderation, etc.)
    # 3. Default: "unknown"
```

No ML. No LLM. Deterministic, auditable, versioned. If the mapping is wrong,
fix the mapping.

## Polarity model

Labels carry implicit polarity — whether they're restrictive, permissive, or
neutral:

| Polarity | Meaning | Examples |
|----------|---------|---------|
| `negative` | Restrictive/warning/removal | spam, porn, misleading, scam |
| `positive` | Endorsement/trust/verification | verified, trusted, safe |
| `cautionary` | Informational warning | ai-generated, satire, sensitive |
| `badge` | Decorative/community | any custom badge, achievement |

Polarity is assigned alongside domain in the same static mapping.

## Orthogonality rule

The key filter for Phase 2 synthesis:

```
def is_meaningful_conflict(edge, domain_a, domain_b, polarity_a, polarity_b):
    # Same domain + opposing polarity → real conflict
    if domain_a == domain_b and polarity_a != polarity_b:
        return True

    # Same domain + same polarity but different values → interesting
    if domain_a == domain_b:
        return True  # worth tracking, lower severity

    # Different domains → orthogonal, not conflict
    return False
```

This single rule would have eliminated most of the 725 contradiction edges from
the Phase 1 first cook, surfacing only edges between labelers that actually
operate in the same space.

## BoundaryFightCard updates

Phase 2 synthesis (from the existing milestone doc) with domain filtering:

### New fields on BoundaryFightCardReceiptV1

```
domain_overlap: float          # Jaccard of domain sets between participants
conflict_domain: str | null    # Primary domain where conflict occurs
orthogonal_fraction: float     # Fraction of edges filtered as orthogonal
```

### Triggering changes

Add to instability gates:
- `domain_overlap >= min_domain_overlap` (new gate)
- At least one contradiction edge must be `is_meaningful_conflict == True`

This prevents fight cards from firing on badge-ecosystem pairs.

### Report cards

**Conflict-only view**: filter contradiction edges to meaningful conflicts only.
Show:
- Pair name (handle or DID)
- Shared domain
- Conflicting families
- JSD (within-domain only, not overall)
- Example shared targets

**Orthogonal pairs**: separate section, collapsed by default. "These labelers
operate in different domains — high JSD is expected."

## Implementation order

1. Add `FAMILY_DOMAINS` dict + `classify_family_domain()` to `label_family.py`
2. Add `FAMILY_POLARITIES` dict + `classify_family_polarity()` to `label_family.py`
3. Add `is_meaningful_conflict()` filter
4. Update `boundary.py` to annotate edges with domain/polarity
5. Filter fight card triggering
6. Update report rendering with conflict-only view
7. Tests: verify novelty-vs-novelty collapses to orthogonal

## Acceptance tests

- [ ] `oracle.posters.rip` vs `stechlab-labels.bsky.social` → orthogonal (badge vs badge)
- [ ] Novelty-vs-novelty pairs stop lighting up as JSD=1.0 conflicts
- [ ] Badge ecosystems mostly collapse to "orthogonal" section
- [ ] Actual contradictory moderation labeling (if present) surfaces cleanly
- [ ] Domain classification is deterministic across runs
- [ ] Unknown families don't generate false-positive conflict edges

## Open questions

- Should domain assignment live in `label_family.py` or a separate `domain.py`?
  Leaning toward keeping it in `label_family.py` since it's tightly coupled to
  family normalization.
- How many families do we need to manually classify before keyword heuristics
  cover the long tail? Probably: audit the top 50 families by edge count from
  Phase 1 data, classify those, let heuristics handle the rest.
- Should we store domain/polarity on `boundary_edges` rows or compute at query
  time? Storing is cheaper for reports but requires backfill. Compute is simpler
  but slower.
