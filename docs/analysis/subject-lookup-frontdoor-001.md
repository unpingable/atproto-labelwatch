# Subject-lookup frontdoor — surface contract and audit boundary, 2026-06-09

> **Status: surface contract.** This note freezes the public-surface shape (`whatsonme.frontdoor.v0`), the explicit non-outputs, the refusal grammar, and the query inventory the index audit must certify. It does NOT implement the surface, redesign the homepage, or run the audit. It is the contract the audit (`labelwatch.index_audit.v1`) will verdict against, and the contract the front-door implementation will satisfy if the audit returns admissible.
>
> Publication state: `blocked_pending_index_audit`. Local design and surface-contract work proceeds. Public homepage does not change until the audit verdict exists.

## Why this exists

Labelwatch's current front page is methodology-first: dial counts, graph semantics, classifier-debt ledger. Normal humans arrive with one question — "what does this machinery think of me?" — and the existing `whatsonme` capability already answers it, today, as a buried CLI/API call.

The highest-leverage next move is making the homepage a subject-lookup box. But the lookup's hot read paths run over append-only `label_events` (40+ GB, 7 indexes). A public lookup box whose primary path secretly depends on heroic scans is a denial-of-service attack against itself.

The discipline is therefore:

1. **Define the future public surface first** (this document).
2. **Audit its hot paths before publication** (`labelwatch.index_audit.v1`, scoped to this surface).
3. **Publish only on admissible / admissible-with-debt verdicts**.

This avoids two failure modes: auditing the wrong surface (the current methodology appendix), and publishing a surface that quietly cannot scale.

## The contract: `whatsonme.frontdoor.v0`

### Inputs

```
handle               # bsky.social-style handle or custom-domain handle
did                  # plc: or web: DID; alternative to handle
optional: service    # PDS service URL, used when handle resolution is
                     # ambiguous across services
```

Exactly one of `handle` or `did` is required. If both are supplied, `did` is canonical; `handle` is logged but not authoritative.

### Primary outputs

For an admissible lookup:

```
subject_identity_resolution     # how we resolved the input → DID; provenance
                                #   of the resolution (PLC, DID web fetch, cache)
labelers_touching_subject       # array of labeler DIDs that have emitted at
                                #   least one label against this subject
label_values_touching_subject   # array of {labeler_did, label_value, count,
                                #   first_seen, last_seen}
authority_effect_breakdown      # per-label-value: which authority_effect
                                #   bucket the label attempts to exercise
                                #   (descriptive / advisory / reputational /
                                #   visibility / enforcement / decorative /
                                #   telemetry / unknown)
latest_seen_per_labeler_value   # tuple key (labeler_did, label_value) →
                                #   latest createdAt (or observed_at)
emitter_stability_summary       # per labeler touching subject: regime state
                                #   (warming / steady / churny / stale)
auditability_summary            # per labeler touching subject: easy /
                                #   limited / hard, sourced from the dials
temporal_coherence_summary      # per labeler touching subject: whether
                                #   this labeler has changed its classification
                                #   of the subject over time (and how)
plain_language_labeler_sentence # one template sentence per labeler:
                                #   "This labeler mostly emits {authority_effect}
                                #   labels, appears {stable/churny/stale} over
                                #   time, and is {easy/limited/hard} to audit
                                #   from available evidence."
refusal_or_insufficient_data_state  # if not admissible, the specific reason
                                    #   (see Refusal states below)
```

### Explicit non-outputs

These are NEVER returned, by construction:

```
truth_about_subject            # weather, not verdict
trust_score_for_labeler        # four dials, not one trust score
unified_risk_score             # four dials, not one risk score
moderation_recommendation      # observatory does not advise moderators
adjudication_of_disputes       # right-of-reply gap is not built; no
                               #   first-party adjudication anyway
subject_classification         # the subject is the query, not the finding
```

These exclusions are doctrinal, not implementation choices. They follow from `constraint_detect_only_structural` (observatory does not adjudicate the ecosystem it instruments), `constraint_weather_not_verdict` (publication discipline), and the existing labelwatch architecture rule "four dials, not one trust score."

### Refusal states

```
subject_not_found              # handle/DID resolution returned no PLC record
                               #   or no observed label events
handle_resolution_ambiguous    # handle resolves to multiple DIDs across
                               #   services; caller must specify service
                               #   or supply DID directly
no_observed_labels             # subject resolves, but has zero rows in
                               #   label_events (across all observed labelers)
index_audit_missing            # no fresh labelwatch.index_audit.v1 receipt
                               #   exists; surface is structurally not
                               #   admissible until audit lands
query_shape_unbounded          # audit verdict for this code path is
                               #   refused_query_shape_unbounded; refusing
                               #   to execute until indexed/bounded
insufficient_labeler_profile   # a labeler touching subject lacks enough
                               #   evidence to populate dials (warmup, sparse)
insufficient_temporal_history  # subject has labels but not enough history
                               #   to populate temporal_coherence_summary
```

Refusals are first-class outputs. The frontdoor never silently returns
empty or partial results when one of these states applies — it returns
the refusal kind explicitly, with enough detail for the caller to
understand the boundary.

### Surface posture (composes with publication doctrine)

- The subject is the **query**, never the **finding**. The frontdoor describes which labelers touched the subject and what authority each label attempts; it does not classify the subject.
- All output language uses emitter-describing constructions, not subject-adjudicating ones. "Labeler X emits the Y label against this subject" — never "Labeler X classifies this subject as Y."
- Plain-language card sentences are about **labelers**, not subjects. They render emitter shape (authority_effect, stability, auditability), not subject status.
- Refusal states are receipted: each refusal returned must be reproducible from the receipt audit trail.

## Query inventory (audit target)

The frontdoor depends on the following query shapes. The index audit
(`labelwatch.index_audit.v1`) certifies that each is structurally
admissible before the frontdoor publishes.

For each query, the audit must record: `query_name`, `consumer_surface`,
`sql_fingerprint`, `expected_cardinality`, `observed_runtime_ms`,
`explain_query_plan`, `covering_index_present`, `full_scan_detected`,
`bounded_by_subject_or_time`, `publication_blocking`, `verdict`.

### Q1. Subject identity resolution

**Purpose:** Map handle → DID (and back), determine canonical DID.

**Inputs:** handle (string) OR did (string).

**Boundedness:** Bounded by single subject. Should be a single-row
PLC/DID-web lookup with optional cache.

**Backed by:** PLC mirror / DID web fetch / cache; not directly a
`label_events` query. Audit must still confirm the resolution path
does not fall back to a label_events scan when cache misses.

### Q2. Labelers touching subject

**Purpose:** Return `labelers_touching_subject` and per-labeler row
counts against subject.

**Shape:**
```sql
SELECT labeler_did, COUNT(*) AS event_count
FROM label_events
WHERE target_did = :subject_did
GROUP BY labeler_did;
```

**Expected cardinality:** Tens to low hundreds per subject (number of
distinct labelers observed touching one DID).

**Boundedness:** Bounded by subject. MUST be served by a covering
index on `(target_did, labeler_did)` or `(target_did, labeler_did, ...)`.

**Audit-blocking:** Yes. This is the lookup hot path. A full scan here
is a denial-of-service attack against the homepage.

### Q3. Label values touching subject

**Purpose:** Return `label_values_touching_subject` — per (labeler,
value) tuple with count, first_seen, last_seen.

**Shape:**
```sql
SELECT labeler_did, label_value,
       COUNT(*) AS event_count,
       MIN(observed_at) AS first_seen,
       MAX(observed_at) AS last_seen
FROM label_events
WHERE target_did = :subject_did
GROUP BY labeler_did, label_value;
```

**Expected cardinality:** Low hundreds per subject in the long tail;
small for typical subjects.

**Boundedness:** Bounded by subject. Same covering-index requirement
as Q2; ideally the same index serves both.

**Audit-blocking:** Yes.

### Q4. Latest seen per labeler/value

**Purpose:** Return `latest_seen_per_labeler_value`.

**Shape:** Either folded into Q3 (MAX(observed_at)) or run as separate
windowed query. If separate:

```sql
SELECT labeler_did, label_value, MAX(observed_at) AS latest_seen
FROM label_events
WHERE target_did = :subject_did
GROUP BY labeler_did, label_value;
```

**Boundedness:** Bounded by subject. Folding into Q3 is the cheaper
shape if the index supports it.

**Audit-blocking:** Yes (or folded).

### Q5. Authority-effect breakdown per labeler/value

**Purpose:** Return `authority_effect_breakdown` — which
authority_effect each (labeler_did, label_value) is classified as.

**Shape:** JOIN of `label_events` rollup with the
`authority_effect` axis (per labeler_did + label_value mapping).
Concrete shape depends on where authority_effect lives —
`labelers` table, dedicated lookup, or derived rollup. Audit must
locate the table and confirm the join path.

**Boundedness:** Bounded by the per-subject (labeler, value) set
from Q2/Q3 — i.e., a small lookup over a small candidate set, not
a global scan.

**Audit-blocking:** Yes — but boundedness is naturally tight; the
risk here is wrong join path, not unbounded scan.

### Q6. Emitter stability summary per labeler

**Purpose:** Return `emitter_stability_summary` — per labeler touching
subject, the labeler's current regime state (warming / steady /
churny / stale).

**Shape:** Lookup on `labelers` table by `labeler_did`. Per labeler:
`regime_state`, possibly `volume_stats`, `last_active`.

**Boundedness:** Bounded by labeler DID. Should be a row-per-labeler
lookup against `labelers` PK or unique index on `did`.

**Audit-blocking:** Lower risk (small table, indexed PK), but audit
must still confirm.

### Q7. Auditability summary per labeler

**Purpose:** Return `auditability_summary` — per labeler touching
subject, the auditability bucket (easy / limited / hard) sourced
from the dials.

**Shape:** Same shape as Q6 (lookup on `labelers` by `labeler_did`,
read auditability dial). May be folded into Q6.

**Audit-blocking:** Folded into Q6 if shape allows.

### Q8. Temporal coherence summary per labeler/subject

**Purpose:** Return `temporal_coherence_summary` — per labeler
touching subject, whether the labeler's classification of this
subject has changed over time, and how.

**Shape:**
```sql
SELECT labeler_did, label_value, observed_at, action  -- create/delete/etc
FROM label_events
WHERE target_did = :subject_did
ORDER BY labeler_did, observed_at;
```

Then a sort-pass in code to detect classification flips per labeler.

**Expected cardinality:** Same bounded-by-subject envelope as Q2/Q3.

**Boundedness:** Bounded by subject. Same covering-index requirement.

**Audit-blocking:** Yes.

### Query inventory summary

| Query | Bounded by | Index dependency | Audit-blocking |
|---|---|---|---|
| Q1 subject identity resolution | single subject | PLC/cache, not events | confirm no events-fallback |
| Q2 labelers touching subject | target_did | `(target_did, labeler_did)` covering | yes |
| Q3 label values touching subject | target_did | same as Q2 (or `(target_did, labeler_did, label_value)`) | yes |
| Q4 latest seen per labeler/value | target_did | folded into Q3 if possible | yes-or-folded |
| Q5 authority_effect breakdown | per-subject (labeler, value) set | labelers/authority_effect lookup | yes (low risk) |
| Q6 emitter stability per labeler | labeler_did | `labelers` PK | confirm |
| Q7 auditability per labeler | labeler_did | folded into Q6 | folded |
| Q8 temporal coherence per labeler/subject | target_did | same as Q2 + ordered | yes |

## Audit gate

```
receipt_kind: labelwatch.index_audit.v1
consumer_surface: whatsonme.frontdoor.v0
dataset:
  db_path
  db_size_bytes
  table_counts
  relevant_row_counts
query_inventory:
  - name (Q1..Q8 from this document)
  - sql_fingerprint
  - purpose
  - expected_cardinality
  - observed_runtime_ms
  - explain_query_plan
  - covering_index_present
  - full_scan_detected
  - bounded_by_subject_or_time
  - publication_blocking
verdict:
  - admissible
  - admissible_with_debt
  - refused_index_missing
  - refused_query_shape_unbounded
  - refused_cardinality_unknown
```

A single `refused_*` verdict on any publication-blocking query
blocks the frontdoor. `admissible_with_debt` is acceptable for
publication if and only if the debt is documented and bounded.

Surface state derivation:

```
if no fresh labelwatch.index_audit.v1:
  surface_status = refused_index_audit_missing
elif any publication_blocking query has refused_* verdict:
  surface_status = refused_<reason>
elif any publication_blocking query is admissible_with_debt:
  surface_status = admissible_with_scope
else:
  surface_status = admissible
```

## What this document does NOT do

- Does not commit to an implementation timeline.
- Does not redesign the homepage (that work is downstream of the audit verdict).
- Does not select indexes. The audit's job is to characterize what is and isn't admissible; index work, if needed, comes after the audit.
- Does not freeze the weekly weather digest shape. The weather digest is the next surface after the frontdoor; it has its own contract document at a future `weather-digest-001.md`.
- Does not build the right-of-reply channel. That gap is named and forcing-case gated; see `memory/project_labelwatch_right_of_reply.md`.

## Composes with

- `docs/labelers-as-testimony.md` — the framing that makes a subject-lookup surface coherent: we publish observations of testimony, not adjudication.
- `docs/observation-export-custody.md` — six refusals apply: integrity ≠ completeness, sequence ≠ network order, verification failure ≠ labeler fault, absence implies nothing, observed_at is non-ordering, bundle is membership/integrity only.
- `docs/authority-failure-modes.md` — what we surface (silent authority decay, scope creep, single-source capture) and what we don't (because we can't observe it from outside, or because surfacing requires verdict-mode).
- `docs/analysis/storage-runway-sizing-001.md` — why this audit gate has teeth. `label_events` is 38.4% of the DB; its indexes are another 49%. Any unbounded scan is a marginal-storage operational hazard.

## Next slice

1. Implement `labelwatch.index_audit.v1` as a CLI subcommand
   (`labelwatch index-audit --consumer-surface whatsonme.frontdoor.v0`)
   that emits the receipt for Q1..Q8 above against the live DB.
2. Read verdicts.
3. If admissible / admissible-with-debt → implement frontdoor.
4. If any `refused_*` → index/query work first, then re-audit, then frontdoor.

The copy work for the lookup-first homepage (demote methodology to `/method`, promote per-card "Use this to see / Not for" sentences, generate the plain-language labeler sentence from the four dials) is weekend-scale. The admissibility gate may not be. Honest sequencing prices the audit verdict in before assuming weekend-scale total work.
