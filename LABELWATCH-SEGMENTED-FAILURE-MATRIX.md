# Labelwatch segmented failure matrix

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
| Concurrent writers | One global writer fence and monotonic IDs | Supplement; evidence pending terminal qualification. |
| Reader concurrent with retirement | Reader lease fences unlink | Supplement; evidence pending terminal qualification. |
| Local-only read after recent file retirement | Must not imply complete coverage | Preserved counterexample; cross-tier reader must pass. |
| Archive missing/corrupted after retirement | Historical path unavailable, never empty/clear | Cross-tier supplement requires refusal. |
| Local queue / WAL / page ceiling | Backpressure before new acceptance; durable journal remains recoverable | Explicit adapter guard; measured limits and remaining caveats in qualification report. |

A control-path PASS is not full workload acceptance. Current-scale data,
query latency/capacity and independent reviewer disposition are separate gates.
