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

## Implementation status

### Done (v3, 2026-03-13)

1. **FAMILY_MAP v3** — synonym collapse for spam/abuse/nsfw/slur variants.
   Eliminates false JSD disagreement when two labelers agree but use different
   terms (e.g., `shopping-spam` vs `spam` → both normalize to `spam`).
2. **DOMAIN_MAP expansion** — explicit routing for ~50 behavioral/stats,
   political, identity, and PDS-identifier families. No more keyword-heuristic
   clowning on `shopping-spam` or `likely-nsfw`.
3. **Word-boundary keyword matching** — regex with separator anchors so
   `ai-hater` doesn't match `hate` and `foamspammer` doesn't match `spam`.
4. **Version-filtered report queries** — `filter_fight_edges()` and
   `boundary_summary_for_report()` filter by `family_version` so stale v2
   edges don't contaminate v3 results.
5. **Domain classification** — `classify_domain()` with 4-step cascade:
   explicit map → `!` prefix → word-boundary keyword → novelty default.
6. **Fight-edge filtering** — moderation-vs-moderation only, 2+ shared targets.
7. **Report rendering** — conflict-only view with collapsed orthogonal section.

### Acceptance tests (verified against live data)

- [x] `oracle.posters.rip` vs `stechlab-labels.bsky.social` → orthogonal (novelty vs metadata)
- [x] Novelty-vs-novelty pairs stop lighting up as JSD=1.0 conflicts
- [x] Badge ecosystems collapse to orthogonal (50k metadata-vs-novelty, 41k metadata-vs-metadata)
- [x] Actual contradictory moderation labeling surfaces cleanly (2 real pairs)
- [x] Domain classification is deterministic across runs
- [x] Unknown families don't generate false-positive conflict edges
- [x] `ai-hater` doesn't match "hate" keyword (word-boundary regex)

### Remaining

1. **Polarity model** — `FAMILY_POLARITIES` + `classify_family_polarity()`
2. **Disagreement type classification** (see below)
3. **BoundaryFightCardReceiptV1 fields** — `domain_overlap`, `conflict_domain`,
   `orthogonal_fraction`

## Disagreement type model

Three buckets for fight-pair annotation. Captured from first v3 observation
(2026-03-13). Do NOT implement rendering until v3 results have cooked and
the pair mix is confirmed stable.

### `taxonomy_shear`

Same rough negative zone, different taxonomy. Both labelers agree the content
is bad, they just carve the object at different joints. Example: skywatch.blue
calls it `inauthenticity`, labeler.hailey.at calls it `spam`. Governance-relevant
because downstream consumers inherit different explanatory frames for the
same content.

### `severity_difference`

Same family, different intensity or scope. Not yet observed in live data.
Would be the polarity model's territory — e.g., one labeler says `warn`,
another says `takedown` for the same content.

### `substantive_disagreement`

Different claims about what the content *is*. Example: `adult-sexual` vs
`misleading` on the same targets (labeler.hailey.at vs labeler-prototype).
Not vocabulary drift — materially different classification. This is the
specimen that makes boundary instability legible to outsiders in one sentence.

### Classification logic (draft)

```python
def classify_disagreement(family_a: str, family_b: str) -> str:
    domain_a = classify_domain(family_a)
    domain_b = classify_domain(family_b)

    if domain_a != domain_b:
        return "substantive_disagreement"

    # Same domain — check if families are in the same "zone"
    # (needs polarity model to distinguish severity from taxonomy)
    # For now: same domain + different families = taxonomy_shear
    return "taxonomy_shear"
```

Severity detection requires the polarity model. Don't implement until
polarity is stable.

## TODO

- [ ] Let v3 cook, inspect top fight pairs after 24h
- [ ] Confirm cleaned distribution supports three-bucket model
- [ ] Add disagreement-type annotation to report rendering
- [ ] Polarity model (`negative`/`positive`/`cautionary`/`badge`)
- [ ] BoundaryFightCardReceiptV1 fields
- [ ] **Account integration**: create `labelwatch.neutral.zone` Bluesky account,
      hook in via app password. Post crisp findings (one example, one explanation,
      one link). Makes infrastructure legible as public signal, not private
      instrumentation.
