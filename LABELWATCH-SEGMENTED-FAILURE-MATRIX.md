# Labelwatch segmented failure matrix

> Historical qualification/model at `b2b9a416`, preserved in its original scope.
> Current candidate is rejected by the [horizon decision](LABELWATCH-SEGMENTED-DECISION.md):
> mutable historical replay/quarantine lifetime and catalog custody are concrete defects.
> See [new evidence](LABELWATCH-SEGMENTED-HORIZON-QUALIFICATION.md).

The immutable dispatch/result pointers in the campaign report identify the
qualified revisions. Process deaths use exit 73 at explicit transition cuts;
archive unavailable/read-only/full are deterministic local fixtures. No shared
NAS or production unit is deliberately disrupted. No physical power-loss proof.

| Qualification case | Required behavior | Evidence / observed outcome |
|---|---|---|
| Before rollover / prepared transition | Recover same old target or finish prepared successor | Finite rollover cases; cursor and global table digests unchanged. |
| After seal, before next exists | Create/validate next under writer fence | Finite after_seal. |
| Next exists, before global ACTIVE | Validate successor and publish once | Finite after_successor. |
| Global ACTIVE, stale projection | Reconstruct projection from SQLite authority | after_active_before_projection; stale_checkpoint. |
| Death before global acceptance commit | No accepted event/cursor advance | before_global_commit. Original wrong cut placement retained as counterexample. |
| Death after acceptance commit | Journal owns accepted event and cursor | after_global_commit; query-triggered recovery. |
| Vessel commit, before journal drain | Retry exact ID/all columns, no duplicate | after_vessel_commit. |
| Journal drain / duplicate restart | Durable vessel, original cursor, one accepted event | after_journal_drain. |
| Forward / backward clock boundary | Explicit forward transition; backward refuses | Rollover/late cases. Host clock is not authority. |
| Late authored event in sealed period | Current arrival vessel if above floor | Rollover cases. Below floor uses existing quarantine. |
| Archive unavailable / read-only / full | Refuse before expensive conversion; retain source | Deterministic admission fixtures, not a claimed NAS outage. |
| Local root below admitted bound | Refuse before acceptance/export | root_low. Both crow reserves remain 60 GiB. |
| Conversion killed | Partial archive is not custody; retry conversion | conversion_killed. |
| Verification killed | Published bytes remain non-admissible until verification | verification_killed. |
| Parquet renamed, checkpoint missing | Reverify exact source/destination; checkpoint safely | after_parquet_before_checkpoint. |
| Verified checkpoint, retirement not done | Continue retirement idempotently | after_checkpoint_before_retirement. |
| Source retirement fails | Source and verified archive retained; explicit blocked result | retirement_failed. |
| Unlink before RETIRED checkpoint | Reconcile missing source only against verified custody | after_unlink_before_retired. |
| Corrupt Parquet / source content differs | Refuse retirement; source survives | corrupt_parquet; full-cell digest and file hash checks. |
| Duplicate conversion / retirement | Same identity, verified retry; second unlink reclaims zero | Archive cut cases. |
| Floor before archive custody | Refuse | floor_before_archive_custody. |
| Death after floor commit / during key expiry | Extra keys harmless; retry bounded GC; quarantine below floor | after_floor_commit; during_key_gc. |
| Unknown writer column/schema | Refuse before conversion; no silent column loss | unknown_writer_column; schema-window fixtures. |
| NULL, Unicode, rename, new encoding | Full custody; versioned canonical reader or refusal | Schema vectors 23–26. |
| Concurrent writers | One global writer fence and monotonic IDs | Supplement and final guard cases PASS. |
| Reader concurrent with retirement | Reader lease fences unlink | Supplement and final guard cases PASS. |
| Local-only read after recent file retirement | Must not imply complete coverage | Preserved counterexample; cross-tier reader must pass. |
| Archive missing/corrupted after retirement | Historical path unavailable, never empty/clear | Cross-tier supplement requires refusal. |
| Local queue / WAL / page ceiling | Backpressure before new acceptance; durable journal remains recoverable | Per-writer page ceiling, two-vessel queue and pinned-WAL expiry tests PASS; post-acceptance full vessel retains journal and cursor for explicit bounded recovery. |

A control-path PASS is not full workload acceptance. Current-scale data,
query latency/capacity and independent reviewer disposition are separate gates.

## Preserved qualification counterexamples

| Attempt / condition | Actual result | Successor / limits |
|---|---|---|
| finite-001 misplaced pre-commit cut | db.set_cursor had already committed; expected zero was wrong | Corrected actual acceptance boundary; original source/error retained. |
| Local-only read after recent file retirement | Zero local rows despite five accepted archived events | Cross-tier reader returns full archive + active coverage; missing catalog refuses. |
| Arrow worker reads default thread-bound SQLite connection | Actual thread-affinity error | Serialized readonly connection handoff; no simultaneous caller. |
| Creation-only max_page_count | Reopened writer had default unbounded page count | Apply owned cap on every writer; real file-full refusal/recovery tested. |
| Original 12M report query, DuckDB 512 MB / two workers | Memory refusal; original large producer FAILED | Same SQL passes 512 MB / one worker / insertion-order preservation off; equivalent larger-memory and SQL-rewrite controls preserved. |
| SQLITE_TMPDIR set after imports | SQLite reference sort wrote 1.318 GB open-unlinked in /var/tmp | Root estimate exceeded, hard reserve maintained, explicit amendment; successor requires bootstrap environment and direct pathname evidence. |
| Fast NFS custody copy in 512 MiB unit | Kernel resource kill; partial file was never custody | New bounded streaming/fsync copy passes; source and failure receipt preserved. |
| Whole key-expiry writer fence | 639.68-second global writer hold at12M keys | Corrected10k-page fencing expires11,999,880 keys while5,985 new events/cursor/payload pass; p95 probe0.724s. |
| Catalog admitted before a new archived segment | Potential missing required coverage | Missing ARCHIVED/RETIRED entry explicitly refuses; refresh recovers. |
| Invalid successor dispatch argument | Caught before launch; no execution/allocation | New immutable corrected dispatch; never overwrite the invalid one. |

Actual archive-directory permission refusal, write-full/refusal after admission,
two concurrent converters, immutable first receipt, wrong destination schema tag,
full global file, full vessel and pinned key-expiry reader have additional guard
receipts. None deliberately changes NAS permissions, fills shared storage or
breaks a production service. Post-rename corruption is an explicit refusal;
operator recovery must retain the corrupt object and valid receipt before repair.

No physical host/NFS power loss is claimed. Process-death cuts establish fresh
process recovery; storage power-loss behavior still relies on FULL/fsync and the
qualified filesystem contract. Current-scale timings and final independent
acceptance are reported separately from these finite controls.

## Independently discovered coverage failure and successor

Original acceptance 27ee66da reproduces ten accepted events across nine retired
periods plus active. Prune the oldest local retry record, then remove that old
receipt/Parquet from the fixture catalog: historical count silently falls10→9.
The original source 29f3f76 is NOT_ACCEPTED_COUNTEREXAMPLE.

Successor f0161ec and run f2e430a4 introduce anchored expected historical custody.
Five new cases PASS: death after immutable manifest publication before prune
commit, missing pruned receipt, missing pruned Parquet, missing manifest and
corrupted manifest. Retry retains all10 accepted events. Full27 finite/10 guard
regression and15 schema cases also pass. Independent successor acceptance remains
a separate record; a producer fix never overwrites the original counterexample.
