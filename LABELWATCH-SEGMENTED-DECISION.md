> Historical terminal decision for source 01cb5f6 / closure 872693a.
> The shared custody defect is now repaired and boundedly qualified; see
> [current custody decision](LABELWATCH-CUSTODY-DECISION.md). Architecture adoption
> still requires the three named horizon gates. Original evidence remains at its
> exact prior Git revision; this notice does not revise that historical verdict.

# Labelwatch segmented-storage decision

**REJECT_SEGMENTED_SQLITE_PARQUET_DUCKDB**

This rejects the **current candidate's global-state lifetime and catalog custody
boundaries**. It does not reject SQLite as an engine, prove that every segmented
architecture fails, or automatically recommend PostgreSQL. The candidate fails
the user's explicit hard gate: correctness-required mutable global state grows
with historical event count after payload retirement.

| Gate | Disposition | Evidence |
|---|---|---|
| 40+7-day global-state bound | FAIL: concrete lifetime defect | Ordinary accepted/archived replay creates full mutable per-hash quarantine state indefinitely. Authored-future dedupe keys also outlive the arrival window; dropping one admits duplicate history. |
| Current-volume full 40-day queries | NOT RUN after hard failure | No new capacity/latency/completeness claim. Prior 12M one-week evidence remains valid in its original scope. |
| Long-history catalog admission | FAIL: custody defect; linear read amplification measured | Receipt discovery before global custody commit counts one event twice. New-segment admission rebuilds and hashes all historical artifacts. Tiny-file timing alone is not a product latency rejection. |
| Original missing-history regression | PASS | Exact unchanged failing generator now returns 10, explicitly refuses missing required history, and returns 10 after restoration. |
| Independent acceptance | Hard-failure evidence accepted; adoption rejected | Separate reviewer reproduces ordinary archived replay, future-key substitution and receipt-before-commit duplication; all17 producer-seal hashes match. |

## Precise forcing properties

1. `quarantined_events` is hot-global full historical event storage with exact
   per-hash mutable delivery counts/metadata. Its current subject-completeness
   and operator-count consumers are live source paths. An ordinary cursor replay
   after archive/floor advancement rebuilds historical events in that global
   store. No existing quarantine retirement rule bounds it. Independent96-day
   fixture keeps normal hot keys at160 while quarantine grows28 at day47 to224
   at day96; repeat delivery updates seen_count to 2. Fixed identity cardinality
   rules out growth caused merely by new labelers or sources.
2. `q_hot_keys` expires by authored `ts`, with no maximum future-skew admission.
   It therefore cannot be bounded by an arrival-based40+7-day vessel window.
   Deleting a retired event's still-required key admits its exact replay,
   producing two hot+archive copies. This is a supported finite timestamp
   counterexample, not a claimed production frequency or literal infinite life.
3. A verified receipt is published before global ARCHIVED commit. In that
   durable intermediate state, TierSession includes both SEALED local rows and
   receipt-discovered Parquet, returning two copies of one accepted event.
   Independent qualification uses an exact owned-state substitution matching
   source order; it does not claim an injected process death at that boundary.

A global page ceiling eventually refuses ingestion; that is not an operable
lifetime bound. Raising the ceiling or moving the same mutable history elsewhere
would preserve the defect. PostgreSQL also does not inherently remove this
information-lifetime obligation.

## Why no macro horizon run

The requested47-day current-volume corpus would be about80.57M events at the
prior12M/week margin. The explicit hard failure is established by actual helper
semantics, finite counterexamples and independent replay before that allocation.
A larger specimen cannot turn cumulative quarantine into bounded hot state.
This is **REJECT on evidence**, not NO_DECISION, unavailable external evidence,
or a request for another confidence campaign. No full 40-day p50/p95 or derived
production root/archive capacity bound is claimed.

## Independent identity

Reviewer `/root/independent_acceptance`; exact source
`01cb5f610796fefdf5e2a783b626424001ccb559`.
Acceptance `ACCEPTANCE-d1246d42-b22f-42b6-8032-5ff0f53d55a2.json`,
SHA256 `08e1b7e410fdc50264abd1022757bc7b8c6214e062af6bc7d57cc4ece065a1ef`.
All three independent units terminal; no production contact or source edits.

[Horizon evidence](LABELWATCH-SEGMENTED-HORIZON-QUALIFICATION.md) and
[terminal report](LABELWATCH-SEGMENTED-CAMPAIGN-REPORT.md) contain measurements,
receipt identities, scope and custody.

## Exact next work

Existing [Labelwatch #7](https://github.com/unpingable/atproto-labelwatch/issues/7)
owns a separately admitted bounded state/custody repair:
`lane/labelwatch-segmented-global-lifetime-repair-20261005`.

Define and earn a finite archived-replay/quarantine lifetime and authored-versus-
arrival contract, preserving current completeness/delivery-count semantics or
obtaining an explicit owner-approved contract change. Establish one committed
hot/archive query owner across receipt publication and global commit. Earn
incremental catalog admission without rehashing all historical Parquet to admit
one segment. Reuse the exact preserved counterexamples; do not merely enlarge
caps or restart general storage selection.

Only after that concrete design defect is repaired may the remaining47-day
current-volume state and40-day query conjunction be qualified. Previous 12M
rollover/schema/custody results need not be repeated absent a changed property.
Alternative-engine evaluation is permitted by this rejection under the user's
rule, but no engine-specific forcing property justifies opening it now.
No repair, migration, deployment or retention-policy change occurs in this lane.

Previous `b2b9a416` NO_DECISION is historical and superseded because the missing
capacity gate has become an independently demonstrated correctness/lifetime
failure, rather than an evidence shortage. Its exact evidence remains retained.
