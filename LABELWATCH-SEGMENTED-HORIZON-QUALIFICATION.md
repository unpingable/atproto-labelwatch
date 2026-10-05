# Labelwatch segmented horizon qualification

Lane `lane/labelwatch-segmented-horizon-capacity-20261005`; existing Labelwatch #7.
Starting source `b2b9a416761b6e9daace4c8f4d5beef3ca282f2c`.
Exact executable source `01cb5f610796fefdf5e2a783b626424001ccb559`.
Root run `3fcdf829-e8d8-4a30-b8d7-400775a92d4e`; terminal producer `225859f1-ea73-4ad5-9af5-747229cc8ba0`.

Independent disposition and terminal decision are in
[LABELWATCH-SEGMENTED-DECISION.md](LABELWATCH-SEGMENTED-DECISION.md).
This lane examines only the three remaining gates. It does not repeat the
previous 12M qualification or claim another storage-engine comparison.

## Specimen and scope

The first step is a small deterministic counterexample to the required lifetime
bound. It precedes any current-volume macro allocation. Existing `Store`,
`normalize_label`, event generator, archive/retire/floor machinery, catalog and
coverage regression are reused. Only the exact new campaign fixture paths are
added to the existing path guards; storage semantics remain unchanged.

The lifetime fixture represents 96 synthetic days, including a checkpoint at
47 days, across 13 closed arrival-week segments plus one active segment. One
labeler, one subject and one cursor source stay fixed. Each day offers 256
normal dated events, 256 events authored at `2100-01-01`, and 256 unique events
below the floor. Total accepted before the diagnostic substitution: 49,152;
below-floor offers: 24,576. The authored-time controls are disclosed synthetic
qualification inputs, not claimed production frequencies or valid signatures.

**This is not a 47-day current-volume acceptance specimen.** At the prior
12M/week margin that specimen would require approximately 80.57M accepted
events. No such capacity/query result is claimed. The finite case tests whether
any normal-horizon resource bound can preserve the unchanged candidate contract.
Previous 12M current-volume evidence stays at immutable source `b2b9a416`.

## Gate 1: global-state lifetime

The unchanged source accepts supplied authored `ts` without an upper-skew bound.
Hot replay keys expire only where `ts < live_floor`. Advancing the existing
40-day floor therefore cannot bound the keys by arrival age.

| Simulated days | Accepted | Future-authored keys | Normal dated keys | Full quarantine rows | Global SQLite page bytes |
|---:|---:|---:|---:|---:|---:|
| 7 | 3,584 | 1,792 | 1,792 | 1,792 | 2,220,032 |
| 40 | 20,480 | 10,240 | 10,240 | 10,240 | 11,042,816 |
| 47 | 24,064 | 12,032 | 10,240 | 12,032 | 12,558,336 |
| 80 | 40,960 | 20,480 | 10,240 | 20,480 | 19,574,784 |
| 96 | 49,152 | 24,576 | 10,240 | 24,576 | 22,986,752 |

All 24,576 future-authored keys remain at day96, while ordinary keys settle at
10,240 = 40 × 256. After payload archive/retirement, exact replay with its key
present inserts zero. A local substitution removes one old event's key; exact
replay then inserts one and the cross-tier historical query counts two copies.
The substitution is diagnostic only and is not a proposed repair.

Below-floor delivery calls the actual `quarantine_label_events` helper. Full
historical event columns remain in global SQLite, keyed by event hash/reason;
repeat delivery mutates `seen_count`, last-delivery time and current floor.
Advancing the floor expires no quarantine rows. The fixed subject has 24,576
quarantined events at day96. This is not unused compatibility material:
`retention.quarantined_for_subject` feeds frontdoor history completeness, the
CLI checks it, and `ops_status` reports distinct events and deliveries.

Observed-source evidence also grows (141 rows at day47, 288 at day96), despite
one fixed identity. This is supplementary inherited lifecycle debt, not the
sole disqualifying property.

### Capacity relationship

Let λ be admitted events/day, H the live horizon including admitted catch-up,
F(t) accepted events whose authored timestamps still exceed the moving floor,
Q(t) distinct below-floor event identities retained by quarantine, E(t) retained
evidence rows, and n the archive segment count.

- Normal replay keys can be O(λH); current future keys add O(F(t)), which is not
  bounded by H under the admitted timestamp contract.
- Full mutable quarantine is O(Q(t)); there is no finite existing retirement
  rule. Historical reset/replay can increase it with historical event count.
- Other mutable global tables require their actual owned lifecycle; treating
  their preserved rows as a tiny seed is not a complete capacity qualification.
- Payload vessels are capped at one active plus one closed local vessel;
  this bounds payload by the admitted period volume. It does not bound global
  replay/quarantine state.
- Immutable archive payload appropriately grows with history. Receipts and the
  expected-coverage manifest grow O(n) per current generation.
- Stable cursor/active identity/generation state stays constant for fixed
  source count. Pending acceptance is bounded by the existing 10,000-row page.

An 8 GiB global page ceiling provides eventual refusal, not sustained bounded
operation. Increasing that ceiling or moving the same immortal state file to
another filesystem does not establish the required relationship.

No production policy change, timestamp rejection, quarantine deletion, new
summary contract or storage adapter was introduced to hide this boundary.

## Gate 2: full current-volume horizon queries

**Not qualified in this lane.** Once the hard correctness/lifetime gate is
established, allocating approximately 80.57M events cannot falsify that semantic
counterexample. No new full-40-day p50/p95, dense-query or product usability claim
is made. One-week query evidence remains the previously accepted finite result.
A terminal rejection must be read as a failure of this candidate's state
boundary, not as a measured rejection of historical DuckDB query performance.

## Gate 3: long-history catalog admission

The existing `VerifiedCatalog` is measured directly at 128, 1,024 and 8,192
identities. Each has a separate receipt and a hard link to the same verified
4,192-byte, one-row regression Parquet. This exercises metadata/cardinality and
actual hash-call count; it is not years of event payload or a payload-throughput
benchmark. Metadata is the two fields consumed by this catalog constructor,
so the byte constants are not complete production receipt sizes.

| Existing identities | Receipt metadata after new admission, bytes | Startup s | Cached files p50 / p95 s | New admission s | Payload hashes for new admission |
|---:|---:|---:|---:|---:|---:|
| 128 | 15,351 | 0.0140 | 0.0024 / 0.0025 | 0.0135 | 129 |
| 1,024 | 121,975 | 0.1092 | 0.0191 / 0.0194 | 0.1113 | 1,025 |
| 8,192 | 974,967 | 0.9223 | 0.1541 / 0.1907 | 0.9643 | 8,193 |

At 8,192 identities new admission hashes all 8,193 artifacts, reading
34,345,056 logical payload bytes in this tiny fixture. Peak process RSS is
194,686,976 bytes. All hard-linked payloads share a cacheable inode, so these
tiny-file timings are not independent historical disk throughput. Cached `files()` performs two pathname-stat operations for
every existing entry. It is not an O(log n) time-range lookup. The prototype has
no separate indexed identity/range/retirement lookup or incremental new-segment
admission API. The available direct admission path constructs a fresh catalog
and rehashes all historical payload: O(n + total historical payload bytes).
This observed algorithm, not the sub-second tiny-file result, is the scaling
finding. The archived expected manifest also enumerates all expected entries;
source pruning copies an entire generation. Long-lived generation disposition
is not introduced by this lane.

Catalog cardinality alone has modest metadata at this test scale. The lane does
not claim admission independent of total history, nor measured full-payload
startup/recovery/root-scratch bounds. This is secondary concrete design work,
not proof that another database engine is necessary.

## Permanent historical-coverage regression

The existing `coverage_qualify.py` reproduces the exact original topology:
nine retired periods plus one active event, oldest local retry record pruned.
The corrected candidate returns all ten events; removal of the old receipt,
Parquet or expected manifest refuses explicitly. Corrupting the anchor refuses.
Death after coverage publication before the prune commit retains the old
expected authority, and retry restores complete coverage.

The original independent failing fixture/script and receipts remain unchanged
in the prior campaign (run `27ee66da-0bb4-4e93-a338-687a281baeff`). That script
is still executable against its exact original source; the new lane reuses the
permanent corrected regression rather than replacing the counterexample.
No 40-day current-volume history-completeness acceptance is invented here.

## Reproduction, environment and custody

Use the retained Python3.12.3 / SQLite3.45.1 / DuckDB1.4.3 / PyArrow22.0.0
environment from the prior architecture campaign. Export `SQLITE_TMPDIR` and
`TMPDIR` before Python starts; use the dispatched owned scratch directory.
The exact command/limits/environment are in private `DISPATCH-225859f1-ea73-4ad5-9af5-747229cc8ba0.json`:

```text
<retained-venv>/bin/python tools/segmented_qualification/horizon_qualify.py <new-owned-occurrence> <new-terminal.json>
```

Producer limits: 2 GiB RSS, two CPUs, 900 seconds. New campaign data admission512 MiB;
root admission256 MiB; shared hard60 GiB free floors on both filesystems unchanged.
No compiler trees, VM, container, new packages, SSH identities or production
contact. The fixture does not allocate the incident's cloud-root capacity.

Primary terminal RESULT SHA256:
`efd1d1db293b7bb5968cb9463a54fba93dc5f3f1420544554164dcaf6d9033e2`.
Producer seal SHA256:
`29e8a947cddb8187215efe400575032031890389ce441903484cf95ca1c5100b`.
An earlier attempt refused the new archive path before executing a product gate;
its terminal evidence is retained under run `46fe81f3-2b6b-4b0e-a399-32a97bdcbb56`.

Formalization consideration: the durable proposition is hot-state boundedness
under the actual accepted timestamp/replay contract. The finite alternate history
and source recurrence falsify an unconditional bound; they do not establish an
impossibility theorem for every segmented design. The prior logical custody model
remains limited to its existing scope. Independent acceptance owns classification
of this defect and terminal decision. Physical host/NFS loss is not tested.

Production mutations: **NONE**. No migration, retention-policy change,
destructive cleanup, reporting/derive activation or engine comparison.

## Focused independent counterexamples

Independent run `d1246d42-b22f-42b6-8032-5ff0f53d55a2` uses four ordinary
events and one future-authored event/day. It accepts and archives the ordinary
events first, then replays them after they cross the actual 40-day floor.
At day 47/day 96, normal replay keys stay 160; future keys grow47→96; full
quarantine rows for ordinary accepted historical events grow28→224. A repeated
ordinary delivery changes seen_count to 2; the actual subject consumer reports 224.
Removing one retired future event key permits a second historical copy.
Thus the quarantine defect is not dependent on never-accepted historical offers
or on future timestamps. This independently matches the user hard-failure rule
for the **unchanged candidate's global-state boundary**.

Independent run `e6f08c73-630f-456b-a4c5-76b8b36dce80` executes the exact
unchanged original coverage generator bytes, preloading the newly sealed modules
before its historical hard-coded import-path instruction. Actual source and
hashes are recorded in EXECUTION-IDENTITY.json; the generator's static 29f3f76
result label is historical. Complete10→missing required history refusal→restored10
passes. Original producer artifacts and failing result remain untouched.

Independent run `4534598a-c43c-4132-96e7-d0c67512da34` establishes another
catalog correctness defect. In the equivalent durable state after archive receipt
publication but before global ARCHIVED commit, the old vessel is SEALED while
a verified receipt/Parquet already exists. TierSession includes both the local
SEALED source and discovered archive, counting **one accepted event twice**
without refusal. Explicit archive retry restores one count. The source really
orders receipt publication before the global commit; the test constructs that
state with owned SQLite substitutions, not an injected process death.

This is distinct from the corrected missing-history regression: expected coverage
now refuses missing history, but receipt discovery can still admit history before
its tier ownership is committed. Catalog discovery and custody authority need an
explicit boundary. Do not erase this counterexample merely because retry recovers.

Full 40-day dense/current-volume query and root/archive payload bounds remain
untested. Independent confirmation of the hard disqualifier is not acceptance
of those unrun properties.
