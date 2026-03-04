# Milestone: Boundary Instability (B.3 synthesis)

**Goal:** make "moderation boundary fights" observable *without content*, using only label events + existing receipt/evidence plumbing.

**Scope:** Labelwatch (not driftwatch). This is ATPROTO_SEAMS.md **B.3** patterns (contradiction networks + lead/lag + churn/reversal) packaged into a compound event artifact ("BoundaryFightCard").

## Outcomes

### Ships in two phases

* **Phase 1 (primitives):** new cross-labeler rules (queryable standalone, receipt-backed).
* **Phase 2 (synthesis):** `BoundaryFightCard` = deterministic join/aggregation over Phase 1 receipts + existing flip_flop/churn_index/target_concentration/label_rate_spike.

### Non-goals

* No semantic adjudication ("is it a death threat?"). We measure *instability geometry*, not truth.
* No content ingestion.
* No O(n^2) across all labelers globally; pairwise work stays inside "participants of this event."

---

## Definitions

### TargetRef

A "thing being labeled," one of:

* `kind="uri"`: exact post URI
* `kind="author"`: author DID extracted from URI (cluster across posts)

Canonical key:

* `target_key = "uri:" + <uri>` or `target_key = "author:" + <did>`

### Window

A fixed time window for computation:

* `window = [t0, t1)` (UTC, seconds)
* Deterministic windowing only (no "now-ish" fuzz): same inputs -> same outputs.

### LabelAtom normalization

Normalize at boundary:

* `namespace_norm = ns.strip().lower()`
* `value_norm = val.strip().lower().replace(" ", "_")`

### LabelFamily

Coarse label grouping (for early stability):

* `family = namespace_norm + "::" + value_norm` **OR**
* (preferred) a configured mapper that collapses noisy values into a small set (e.g., `porn`, `spam`, `violent`, `politics`, `impersonation`, etc.)

Ship v1 with a minimal mapper:

* default: `family = namespace_norm + "::" + value_norm`
* optional: `family_map.json` can remap to fewer buckets later without changing stored raw.

---

## Data assumptions

Already true:

* `label_events` indexed by `(uri, ts)` and `(labeler_did, ts)`
* Rule framework exists; receipts exist; warmup gating exists.

Required fields at minimum:

* `ts`, `uri`, `labeler_did`, `namespace`, `value`, `op` (apply/remove if present)

If stable `label_event_id` is unavailable, evidence pointers use deterministic fingerprints.

---

## Phase 1: Primitive cross-labeler rules

### Rule 1: CONTRADICTION_EDGES_V1

**Purpose:** emit disagreement/contradiction edges between labelers on the *same target* within a window.

**Inputs**

* `target_key`
* `window [t0, t1)`
* thresholds (below)

**Computation**

1. Select events for target in window.
   * If `kind=uri`: `WHERE uri = ? AND ts BETWEEN t0 AND t1`
   * If `kind=author`: `WHERE uri LIKE "at://<did>/%" AND ts BETWEEN t0 AND t1` (or pre-parsed author column if available)
2. For each `labeler_did`, build distribution `P_labeler(family)` over **applies** (optionally net of removes if you track remove events).
3. For each pair of labelers among participants (top K by event count):
   * compute `jsd(P_a, P_b)` over families
   * identify `top_family_a`, `top_family_b`, and their shares
4. Emit an edge if:
   * `jsd >= contradiction_jsd_min`
   * and both labelers have `events >= min_events_per_labeler`
   * and top family shares are "real" (`>= min_top_share`) to avoid noise

**Receipt: ContradictionEdgeReceiptV1**

* `edge_id`: stable hash of `(target_key, t0, t1, labeler_a, labeler_b, algo_version)`
* `target_key`, `t0`, `t1`
* `labeler_a`, `labeler_b`
* `jsd`
* `top_family_a`, `top_share_a`
* `top_family_b`, `top_share_b`
* `evidence_ref`: list of evidence fingerprints (see Evidence section)

Notes:

* Call it "contradiction" internally, but in UI/CLI label it **"disagreement edge"** unless/until you ship a semantic contradiction map.

---

### Rule 2: LEAD_LAG_EDGES_V1

**Purpose:** who moves first on a target; does labeling propagate.

**Inputs**

* `target_key`, `window`
* `lag_max_s`
* `min_overlap` (family overlap threshold)

**Computation**

1. For each participant labeler, compute:
   * `first_seen_ts` on target in window (first apply)
   * `P_labeler(family)` as above
2. For each pair `(leader, follower)` where `first_seen_leader < first_seen_follower` and `delta_t <= lag_max_s`:
   * compute overlap score (e.g., cosine similarity on family vectors or `1 - jsd`)
3. Emit edge if `overlap >= min_overlap` and both have `min_events_per_labeler`.

**Receipt: LeadLagEdgeReceiptV1**

* `edge_id` stable hash `(target_key, t0, t1, leader, follower, algo_version)`
* `leader`, `follower`
* `delta_s`
* `overlap`
* `leader_top_family`, `follower_top_family`
* `evidence_ref` (first-seen events + distribution summary fingerprint)

---

### Rule 3: DIVERGENCE_JSD_V1

**Purpose:** quantify how non-coherent the crowd is.

**Inputs**

* `target_key`, `window`
* optional sets:
  * `official_labelers` (configured list / tag)

**Computation**

* Build:
  * `P_all(family)` across all labelers
  * `P_official(family)` across official labelers present
  * `P_community(family)` across non-official
* Metrics:
  * `mean_jsd_to_centroid`: average JSD of each labeler to `P_all`
  * `max_jsd_pair`: max from Rule 1 edges (or recompute quickly)
  * `official_vs_community_jsd` if both sets present

**Receipt: DivergenceReceiptV1**

* `target_key`, `t0`, `t1`
* `participants_n`
* `mean_jsd_to_centroid`
* `max_jsd_pair`
* `official_vs_community_jsd` (nullable)
* evidence summary fingerprint

---

### Rule 4: EVENT_PARTICIPANT_CHURN_DELTA_V1

**Purpose:** attach "labeler volatility" around the event, using existing churn_index.

**Inputs**

* participants list from the target/window
* baseline window before event (e.g., `t0 - baseline_span` to `t0`)
* event window (`t0` to `t1`)

**Computation**

* For each participant labeler:
  * `churn_delta = churn_index(event_window) - churn_index(baseline_window)`
  * optionally include `flip_flop_rate` delta on this target (if flip_flop supports target filter; otherwise leave to Phase 2 synthesis)

**Receipt: ParticipantChurnDeltaReceiptV1**

* `target_key`, `t0`, `t1`
* list of `[{labeler_did, churn_delta, baseline_churn, event_churn}]`
* evidence refs: the churn receipts already emitted (by id)

---

## Phase 2: BoundaryFightCard (compound synthesis)

### Triggering

A target/window qualifies for a `BoundaryFightCardV1` if:

Hard gates:

* `distinct_labelers >= min_labelers`
* `label_rate_spike` OR `target_concentration` event gate hit (existing sensors)

Instability gates (any 2 of 3, configurable):

* `mean_jsd_to_centroid >= jsd_centroid_min`
* `contradiction_edges_count >= contradiction_edges_min`
* `leadlag_edges_count >= leadlag_edges_min` AND median `delta_s` under `lag_coherence_max_s`

Amplifiers (optional):

* participant `churn_delta` high
* elevated reversal / flip_flop on target among participants

### Composition (no bespoke logic)

Card is assembled by joining receipts:

* `DIVERGENCE_JSD_V1`
* top K `CONTRADICTION_EDGES_V1` by jsd
* top K `LEAD_LAG_EDGES_V1` by overlap + small delta_t
* `EVENT_PARTICIPANT_CHURN_DELTA_V1`
* existing: flip_flop/churn_index/target_concentration/label_rate_spike receipts for the same window/target

### Receipt: BoundaryFightCardReceiptV1

* `card_id`: stable hash `(target_key, t0, t1, "boundary_fight_v1")`
* `target_key`, `t0`, `t1`
* `participants_n`
* `participants_top`: list of labelers with roles (`official/community/unknown`) + event counts
* `label_summary`: histogram of families across all events (top M)
* `disagreement_summary`:
  * `mean_jsd_to_centroid`, `max_jsd_pair`, `official_vs_community_jsd`
  * `contradiction_edges_count`, top edges
* `dynamics_summary`:
  * lead/lag edges count, top edges, leader set size
* `volatility_summary`:
  * churn_delta highlights
  * flip_flop highlights (if available at target granularity)
* `supporting_receipts`: list of referenced receipt ids (Phase 1 + existing)
* `evidence_ref`: deterministic fingerprint of (target_key, window, supporting_receipts)

---

## Evidence pointers (contentless, replay-stable)

If stable event ids unavailable:

* Evidence item = fingerprint of canonical JSON tuple:
  * `(ts, uri, labeler_did, namespace_norm, value_norm, op)`
* EvidenceRef = list of fingerprints, truncated to max N per receipt, plus:
  * `evidence_truncated: bool`
  * `evidence_count_total: int`

This matches existing "hash/truncation clarity" hygiene.

---

## Thresholds (defaults; tune later)

Keep these in config (`boundary_instability.toml` or config system):

* `min_labelers = 4`
* `min_events_per_labeler = 5`
* `participant_top_k = 25` (cap pairwise work)
* `min_top_share = 0.35`
* `contradiction_jsd_min = 0.25`
* `jsd_centroid_min = 0.20`
* `contradiction_edges_min = 6`
* `leadlag_edges_min = 6`
* `lag_max_s = 6 * 3600`
* `lag_coherence_max_s = 2 * 3600`
* windowing:
  * start from the convergence/spike detector's computed window, not ad-hoc "now - 2h"

All thresholds are *policy knobs*, not invariants. Receipts record which config hash produced them.

---

## Warmup gating & idempotency

* Phase 1 rules require:
  * minimum history to compute churn baselines if used
  * minimum participant event volume
* Phase 2 card requires Phase 1 receipts present for same `(target_key, t0, t1)`.

Idempotency:

* `card_id` and edge ids are stable -> safe upsert (last-write-wins) in the existing receipts store.

---

## CLI surface

Minimal, consistent with existing derive/query patterns:

* `labelwatch derive boundary --since <ts> --until <ts> [--kind uri|author] [--min-labelers N]`
  * runs Phase 1 rules + Phase 2 synthesis
* `labelwatch query boundary --recent [--limit N]`
* `labelwatch show boundary <card_id> [--json]`
* `labelwatch query edges --target <target_key> --window <t0,t1> --type contradiction|leadlag`

---

## Tests

### Unit tests (synthetic fixtures)

1. **Determinism**: same input events -> identical `edge_id` / `card_id` + identical JSON (canonical order)
2. **Symmetry sanity**: contradiction edges: `(a,b)` canonical ordering (e.g., lexicographic) so no duplicates
3. **Noisy labeler suppression**: labelers below `min_events_per_labeler` do not create edges
4. **Lead/lag coherence**: construct fixture with clear leader->followers; assert edges appear and delta_t ordering correct
5. **JSD math**: identical distributions -> JSD = 0; disjoint distributions -> high JSD
6. **Evidence truncation**: ensure truncation flags and counts correct, hash covers full list even if truncated

### Replay tests (integration)

* Take a small frozen slice of real label_events (sanitized) and assert:
  * stable set of cards across repeated runs
  * thresholds produce expected "hit" count within tolerance

---

## Rollout plan

1. Ship Phase 1 rules behind a config flag: `enable_boundary_primitives`.
2. Run on historical windows; inspect:
   * are porn/spam firehoses drowning everything? (if yes: apply namespace whitelist)
   * do cards cluster around known platform drama days? (they should)
3. Ship Phase 2 `BoundaryFightCard` behind `enable_boundary_cards`.
4. Only then wire UI surfaces.

---

## Guardrails

* **Whitelist namespaces** first (e.g., moderation label namespaces you actually care about) so porn/spam firehoses don't dominate.
* **Normalize label space** at the "family" level first; values are where taxonomy fights hide.
* **Rate-limit obvious megaphones** (high-volume labelers) in card participant lists so one actor doesn't crowd out the graph.

---

## Open questions

* Do you want author-cluster windows to merge multiple URIs automatically (event clustering), or strictly windowed per author?
* What's the authoritative "official labeler" set? Config file? Heuristic? (keep it explicit; heuristics can be a second pass)
* Do you treat removes as negative evidence or ignore them for v1 distributions?
* Family mapping: ship raw `namespace::value` first, or require a mapper for noise reduction?
