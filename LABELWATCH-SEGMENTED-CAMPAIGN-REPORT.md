# Labelwatch horizon reopening terminal campaign report

`REJECT_SEGMENTED_SQLITE_PARQUET_DUCKDB`

Existing lane: `lane/labelwatch-segmented-horizon-capacity-20261005`.
Existing owner: [Labelwatch #7](https://github.com/unpingable/atproto-labelwatch/issues/7).
Root occurrence: `28e39263-168f-46e7-8f22-25a7f29fc684`.

Starting/exercised source `6a88178f8d30fed13c582acccd532128c6b306ce`.
The existing worktree/branch fast-forwarded from historical `872693a` to that
qualified shared-custody source. No source implementation change was required:
`Store` and `TierSession` already consume the shared acceptance/committed-owner
contract. Final publication SHA is recorded separately in the immutable terminal
notice/private report; it changes only these three existing documents.
Canonical repo `/data/git/atproto-nutrition/labelwatch`, worktree
`/data/git/.worktrees/labelwatch-segmented-horizon-capacity-20261005`.

## Concrete terminal finding

The current candidate fails **Gate 3**, long-history catalog admission.
Admitting one newly committed owner requires constructing a fresh `VerifiedCatalog`,
which hashes every historical Parquet payload. At 128/1,024/8,192 existing identities
one admission hashes 129/1,025/8,193 files, reading 541,284/4,300,900/34,377,828 bytes
in a deliberately tiny metadata fixture. The cost is O(segment count + total
historical payload bytes), rather than today's payload plus bounded/indexed
metadata. Cached requests do not rehash, but cannot silently omit the new owner;
they correctly refuse incomplete custody coverage. The exact gate excludes this
full-history dependency, independently of absolute benchmark seconds.

[Qualification](LABELWATCH-SEGMENTED-HORIZON-QUALIFICATION.md) contains the full
measurements, source correspondence, provenance and commands. Root producer uses
one newly accepted seed event and 8,193 hardlinked metadata identities, not
synthetic years of event payloads. No 47-day current-volume corpus was allocated.
Gate 1 capacity and Gate 2 full-horizon queries remain **NOT_RUN** after this
concrete independently confirmed failure; no projected result is called qualified.

## Independent acceptance

Reviewer `/root/independent_acceptance` verifies all 16,407 producer seal pointers
and repeats the failing admission sequence with two real custody-committed owners.
The original cached reader returns count 1, then refuses incomplete coverage after
the next owner commits. Fresh admission hashes both old 4,196-byte and new 4,193-byte
files and returns correct count 2. No current incremental canonical-row admission
API bypasses that path. Separate frontdoor summary/catalog and custody membership
indexes do not provide such a consumer contract.

Acceptance `4bf47479-ebb8-4661-85d7-ee53f6c1044e`:
`ACCEPTED_CONCRETE_GATE3_REJECTION`; SHA256
`dcd7302bd8a6bf55a76ab55cf03ebe1e65b56e225b05148d046afc4874628693`.
First preparation run `54c3a588` hit the expected two-local-vessel queue limit;
that setup refusal remains preserved. A fresh successor uses ordinary verified
fixture retirement and passes the targeted counterexample. Both review units
and root producer are terminal; completion is distinct from independent acceptance.

## Preserved claims and artifacts

Shared custody stays qualified at `1fdfd028` / `6a88178`. Existing accepted
rollover, reclamation, crash/replay and exact historical-coverage regressions are
preserved at their owning receipts/source; none are repeated because their
implementation bytes did not change here. No new engine, semantic model,
storage adapter, campaign framework or fixture-preparation abstraction was made.
The direct wrapper invokes the existing catalog qualification helper only.
No fix or optimization was performed inside the prototype in this lane.

Original source/documents from `872693a` are replicated byte-for-byte into the
campaign's historical-source directory with hash reconciliation, and the exact
pre-reopening canonical resume is retained. Prior rejection/custody evidence is
historical, not erased. Private producer fixtures/receipts/logs and independent
setup/final cases have an existing #7 catalog-repair replay dependency; review
October 12 or successor acceptance, without automatic deletion. No retained
evidence or build substrate is removed at closeout. Ordinary isolated source
retirement during reviewer setup is covered by its verified custody receipts.

New producer fixture allocation was 34,074,624 bytes, deduplicating hardlinks;
independent fixtures retain 1,138,688 bytes before final metadata. Seal, history
copies, logs, source and documentation allocation are measured separately in
closeout. Pre-worktree/host-wide gross write baselines are unavailable; concurrent
shared-host changes are not attributed. Both / and /data retain the independent
60 GiB reserve. No NFS bulk allocation, VM, clone, package download or new key.
No inherited SSH agent inspection/mutation or remote revocation claim.

Formalization consideration records the exact constructor recurrence plus counted
hashes and the actual two-owner transition. These falsify the required admission
bound; no new semantic model or impossibility theorem for incremental repair is
claimed. This finding is not an engine defect or proof segmented storage cannot
work. Macro capacity, full-horizon query usability and legacy enrollment remain
explicitly unearned.

## Tracking and next action

Existing #7 / private ATProto Project is corrected to the concrete admission
blocker. Canonical `portfolio-private/ATPROTO-RESUME.md` links the current decision,
run and immutable closeout checkpoint; the custody decision remains qualified.
Exact GitHub requests/readbacks and source publication are in owning evidence.
No duplicate issue is created; incident #6/API #1/Monitor #18 remain separate.

Next: repair only incremental canonical catalog/index admission under the earned
custody/completeness contract, using existing machinery. Prove a new committed
owner enters usable queries without rehashing all prior payloads, including
coverage refusal/recovery. Then complete the same three horizon gates. No engine
comparison, migration, retention-policy change or production action is authorized.

Production systems observed/mutated: **NONE**. Observations are own host capacity,
own isolated durable qualification units, and GitHub tracking. No live database,
service restart, collection, reporting/derive, DNS/TLS or production retention.
