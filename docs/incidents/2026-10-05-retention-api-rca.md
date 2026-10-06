# October 5 Labelwatch retention/API incident — diagnosis and candidate

This is an open incident, not a recovery acceptance. Lane
`lane/labelwatch-retention-api-recovery-20261005`, run
`b8ef5240-d26b-4735-9c95-6ef3cda23dee`; private checkpoint and raw evidence live in
`portfolio-private/campaigns/labelwatch-retention-api-recovery-20261005`.
The architecture spike is separate and cannot change production gates.

## Established production facts

At 13:25:44 UTC, collection was advancing. The database was 44,234,665,984
logical bytes, auto_vacuum NONE, zero free pages, with a 14.6 MB WAL.
Compared with the identified October 3 baseline, the database had grown
1,964,486,656 bytes. This is not evidence of a query-performance regression.
The next seven-day partition, August 15–21 inclusive, still contained
3,043,314 rows. The last successful trim was October 3 at 06:51:54.071521 UTC
(464,221 rows; floor advanced one day to August 15).

Core selected runtime: b5e1a5dac29733b2d83920d1737c35873446a6a9.
Scheduled coordinator/tool source: ff237ae25c1cb7898f650ca76bf4c71ee8ea6aaf.
Forward CLI source 51f923e remains a separate release obligation.

The first scheduled occurrence, `20261005T060809Z-98c7308f`, began at
06:08:09.828 UTC. It started remote export before its first cloud-root reserve
check. That check refused at 06:08:15.423; the coordinator exited 1 at
06:08:16.618703 UTC. Original remote unit ended with Result=signal, status 15,
MainPID=0, despite a shell marker saying `exit=0`. Existing unit-result
acceptance correctly rejected this marker. No completed partition was found;
no pending trim or floor movement was observed. A progress checkpoint remained
in an earlier monitoring phase. The journal/unit evidence was durable; the
progress and shell terminal were misleading. Do not blindly resume or restart
this occurrence with different source/configuration.

Cloud root had 32,797,593,600 bytes free at observation, below its 32 GiB
(34,359,738,368 byte) guard by 1,562,144,768 bytes. Zone had 50,748,575,744
bytes free, above the 47 GiB launch envelope. Other root consumers are
substantial and owned by other campaigns; their size/age alone does not
establish deletion authority. No production cleanup or capacity-policy change
has been performed.

The floor was 51 days old, but policy is bounded catch-up toward a 40-day
horizon, with at most seven days per maintenance occurrence. Do not convert
the target into a previously earned hard 40-day guarantee.

## Rejected observation: archive mount

`/tank/nfs` was initially characterized as read-only from the restricted tool
mount namespace. Direct observation in the actual crow host namespace showed
the configured NFS backing mount was writable. A bounded campaign-owned probe
proved write, flush/fsync/close, byte readback, rename, directory fsync, unlink
and removal of the empty probe directory. No NAS failure was present.
Preserve the earlier reconciliation receipt as historical evidence and link
this correction; do not rewrite its sealed bytes. Future topology claims need
direct host mount and bounded I/O evidence.

## What the 32 GiB requirement actually is

The remote exporter stages on Zone storage, not cloud root. The coordinator's
32 GiB root threshold is a historical free-space reserve guard, not a measured
32 GiB export scratch allocation. Zone admission separately comprises a
32 GiB reserve plus 15 GiB transient export/catalog/rollback envelope. The crow
catalog builder has its own bounded workspace/admission.

Apply preserves rollback on Zone, retires the old root catalog under explicit
identity/open-consumer checks, then installs the new root catalog and performs
bounded row deletion. Live SQLite DELETE makes reusable pages, not immediate
filesystem reclamation under auto_vacuum NONE. Full VACUUM is not a repair
escape hatch. A long source export snapshot can pin ingest WAL. The growing
cold SQLite catalog is itself history-sized. Source inspection does not prove
bounded scratch independent of history across this whole lifecycle.

Classification: the 32 GiB figure is an established conservative reserve
policy, not a mathematical SQLite requirement demonstrated by these receipts.
This lane has neither weakened it nor invented another filesystem topology.
Any successor requires an admitted envelope and exact original-occurrence
reconciliation; a candidate that refuses earlier does not restore capacity.

## Public overview: a distinct reproduced causal path

The public facts API returns 503 `source_unavailable` for Labelwatch overview.
It reads a materialized file, not the live SQLite database. The aggregate file
is mode 0644 but its parent `/var/www/labelwatch` is mode 0700; the facts API
account cannot traverse it. File contents were generated September 11.
Therefore storage/query pressure is not the established cause of this 503.
Changing permissions alone would expose historical output, not prove current
publication. Repair must establish aggregate-only fresh artifact custody and
consumer readability without enabling reporting/derive. Other health surfaces
remained reachable. Outage onset is unknown; October 5 reproduction does not
prove an October 2 onset merely from API deployment age.

## Why Constellation did not save us

It knew its NQ input was stale and a saved SQLite check was indeterminate;
those notices reached the configured Slack provider. It correctly preserved
unknown evidence and did not announce recovery of dependent obligations.
It did not know that the scheduled Labelwatch retention occurrence had failed,
or that the floor had failed to advance. No direct successful-retention-by-
deadline obligation was enrolled. Host filesystem posture used a generic
percentage threshold, not the application's admitted reserve.

The saved check `sqlite.freelist-labeler` targets Driftwatch's
`/opt/driftwatch/deploy/data/labeler.sqlite`, not Labelwatch's `label_events`
database. Its indeterminate interval is not evidence of the retention cause.
After input recovery it returned to a failed SQLite-health finding, not a
healthy Labelwatch condition. NQ issue #18 records missing native SQLite-health
parity, but implementing that symptom sensor alone would not encode a retention
deadline or verified horizon movement.

This is principally deployment/modeling debt and an operator semantics gap.
Collection service enrollment did not enroll crow's user maintenance job.
U2/readiness planning anticipated stronger operational observation; the actual
native service/filesystem bindings were narrower. Production maintenance and a
public aggregate dependency entered use while successful manual receipts and
operator inspection implicitly supplied the missing domain monitoring.
That implicit function did not become a saved obligation. Partial coverage
was not sufficient readiness acceptance for these obligations.

### Coverage at the incident

| Condition | Coverage | Actual evidence / end of chain |
|---|---|---|
| Retention execution / last success | PLANNED_NOT_DEPLOYED | crow unit and last_trim exist; not enrolled as a domain check |
| Oldest live record / backlog | PLANNED_NOT_DEPLOYED | DB floor/partition facts exist; no evaluated horizon deadline |
| Storage headroom | INTEGRATION_PRESENT_BUT_INCOMPLETE | native generic filesystem posture current; does not represent the 32 GiB guard or separate archive/Zone capacities |
| Collector advancing | INTEGRATION_PRESENT_BUT_INCOMPLETE | service-active watcher; actual ingest advancement separately observed; process active alone insufficient |
| Public overview health | PLANNED_NOT_DEPLOYED | materialized publication dependency and endpoint lacked direct freshness/HTTP obligation |
| Failed scheduled maintenance | PLANNED_NOT_DEPLOYED | user-systemd failed occurrence available on crow; cloud service roster omits it |
| Disabled derive/report | INTEGRATION_PRESENT_BUT_INCOMPLETE | intentional disabled state documented; generic service checks must not treat this as new failure |
| Deployment/release identity | INTEGRATION_PRESENT_BUT_INCOMPLETE | release files/receipts available; typed lifecycle/identity correspondence not proved for this job |
| Outstanding activation obligations | PLANNED_NOT_DEPLOYED | GitHub items retain holds; no automated fulfillment of that execution tracker |
| NQ evidence staleness | ACTIVE_AND_WORKING | evaluated input-unavailable, provider acceptance retained |
| Driftwatch saved-check indeterminacy | ACTIVE_AND_WORKING | temporary unknown surfaced; prior failed finding retained |

The direct retention chain ended at
`service failure fact → journal/checkpoint`; no bound typed observation,
encoded deadline, evaluation, operator wording, acknowledgment or escalation
followed. The independent observability chain completed
`NQ stale/saved refusal → persisted evidence → input condition → attention
report → NG intent → accepted Slack delivery`. Human acknowledgment time is
unknown. A site named "labelwatch" is host identity, not proof that every
condition pertains to Labelwatch.

Failure classes: maintenance sensor absent; domain obligation never encoded;
crow/cloud service identity enrollment incomplete; intended integration not
completed; operator condition lacks domain consequence. Cause of NQ staleness
remains unknown. It is not established that staleness suppressed a valid
retention condition, because no such condition was configured.

### Timestamp-normalized counterfactual

| Fact | UTC | Meaning |
|---|---|---|
| Last successful trim | Oct 3 06:51:54.071521 | manual acceptance, not scheduled acceptance |
| First failed scheduled occurrence | Oct 5 06:08:16.618703 | direct failure available in journal |
| Next observed attention pass completes | Oct 5 06:09:04.956 | existing pass had healthy inputs and no retention-domain intent |
| NQ stale first seen | Oct 5 10:17:02 | observability finding |
| NQ stale trigger | Oct 5 10:23:02 | 6:23 AM EDT, not 06:23 UTC |
| Provider accepts stale notice | Oct 5 10:23:05.719779916 | delivery accepted, human acknowledgment unknown |
| Saved-check indeterminate trigger | Oct 5 12:45:04 | Driftwatch SQLite input, not Labelwatch retention |
| Saved-check input resolves | Oct 5 13:12:02 | 9:12 AM EDT; underlying check remained failed |
| Human portfolio discovery | Oct 5, approximately 12:33 | see reconciliation raw observations for exact probe times |

A properly enrolled failed-occurrence observation could reasonably have
surfaced by the observed 06:09:04.956 evaluation, about 48 seconds after failure.
This is a bounded counterfactual using actual cadence, not a guarantee the
existing service-active rule provided. The delivered generic stale notice came
4h14m49.1s after retention failure and did not name that failure. Exact root
reserve crossing and exact first 503 remain unknown. Their bounds must not be
converted into fabricated onset times.

### Acceptance and follow-up

Retention repair is not incident closure without a direct failed/overdue
obligation reaching the intended operator surface. Use deterministic fixtures,
never deliberately fail production retention. Prove missing evidence remains
unknown, a known failed occurrence says "Labelwatch retention failed", recovery
supersedes without erasing the failed occurrence, and separate capacity domains
retain their reasons. A bounded substitute requires an explicit owner, cadence,
issue and retirement condition; no such substitute has been deployed here.
A separate Constellation domain-obligation issue is required rather than hiding
the general modeling gap in this component.

## Candidate and remaining gate

The candidate admits both root and Zone capacity BEFORE remote staging/launch,
preserves measured refusal in a failed checkpoint, and writes non-success
terminal markers for TERM/INT. Focused qualification covers archive-first trim,
original-producer reconciliation, capacity boundaries and signal termination.
It has not been deployed and does not make unavailable capacity available.

NOW: reconcile exact failed occurrence; establish an owned permanent capacity
solution or separately earned working-envelope design; qualify and release the
candidate under the existing gate; accept one actual scheduled cycle and soak.
In parallel, prove a fresh aggregate-only overview artifact and its read custody.
No derive/report resurrection, upgrade, VACUUM, manual data deletion, temporary
volume, or architecture migration is included.

## Architectural evidence, without retrospective certainty

Driftwatch shipped and hardened Parquet capture and bounded DuckDB projection.
Labelwatch subsequently gained complete-column Parquet export and archived
history, but its direct cold reader remained unqualified and production still
uses a SQLite cold catalog. The storage spike must establish whether those
patterns solve this workload, especially late arrivals, transactional cursor
state and cross-period queries. The gap is narrower than "Labelwatch never
implemented Parquet"; existing export evidence is not direct-reader acceptance.
Do not claim an unqualified replacement would certainly have prevented this
incident.

## Formalization consideration

Proposition: no export side effects before both destination admission checks;
failed/signal completion cannot be consumed as successful original production.
Scope: this source candidate and retained finite fixtures; filesystem capacity
can change after admission, so runtime checks remain. Existing durable unit
result is required alongside terminal text. Practical boundary/signal/recovery
cases are sufficient for this bounded control-flow correction; no power-loss
or scheduler-wide proof is claimed. Decision owner: Codex /root. Source and
result identities are recorded in campaign receipts; independent acceptance
remains separate.

## October 6 independent candidate review

Independent review refused `c476532` despite its 50 passing focused tests:
a pre-plan refusal saved a nonempty checkpoint, so retry skipped planning and
failed on a missing floor. The successor binds the original configuration before
admission, distinguishes absent/complete/partial plans, preserves the first
failure, and refuses incomplete plans or producer identities without a plan.
An established floor is never recomputed. The added bounded retry and partial-plan
controls pass alongside the existing coordinator controls (24 cases).

Formalization consideration: the proposition is that a pre-plan refusal can
resume only its bound configuration, while a partial plan or unplanned producer
cannot be silently reconstructed. The scope is this coordinator's persisted
state and finite local fixtures; it is not proof of power-loss safety or remote
producer recovery. Decision owner is the incident integration owner `/root`.
Practical refusal/retry/configuration/partial-state controls suffice for this
small correction; independent successor acceptance is still required.

Original failed occurrence `20261005T060809Z-98c7308f` remains failed. Retrying
its work under a changed source requires a separately identified successor
and immutable reconciliation under the existing occurrence lock. No production
capacity, retention, publication, or monitoring recovery is claimed by this fix.
