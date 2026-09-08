# Production correctness observation: repaired custody, incomplete derive

Assessment: 2026-09-08, through 21:35 UTC. This records a bounded deployment
observation, not a claim that every source or processing stage is healthy.

## Exact release and local qualification

The deployed candidate is `v0.1.0-rc.10`, source
`5ed0a00d132ac3ec9d912b5100baa113b545e954`, tree
`4e104b3b4a481a7804f582922a3fa7ed44850c71`. It followed rc9
`d520981171bb24a196dd5aaf027abf4b91c4a925`; neither tag was moved.
The integrator deployed rc10 at 19:16:33 UTC. All three code-sharing services
used the exact release throughout the retained observations.

The wheel `labelwatch-0.1.0rc10-py3-none-any.whl` has SHA-256
`24ec5e41d411a7c73c22704ec70e6c4c5a7424790609a574da369fc356b2a612`.
Two isolated clean committed-input builds matched under the declared CPython
3.12.3 / pip 24.0 / setuptools 68.1.2 / wheel 0.42 toolchain. This is a bounded
wheel comparison, not a third-party reproducible-build claim. Existing exact
CPython 3.10 dependency wheels were retained without version churn.

Network-disabled hash-locked installation, dependency checks, the 37-event
offline instrument demonstration, and synthetic application recovery passed.
The retained rc9 runtime reopened candidate schema-23 synthetic state with its
events, cursor, rotation, and derive outcome metadata intact. No schema change
was needed. Separately, an offline source-bundle clone reconstructed the exact
rc10 commit/tree with a clean worktree. Source reconstruction and application
recovery are different receipts; neither substitutes for production-data backup
or rollback evidence held by the deployment integrator.

## What production established

The original rc10 window ran from 19:18 to 20:48 UTC, with samples at 0, 15, 30,
60, and 90 minutes. Its one predeclared extension was used because complete
derive evidence was missing. A separately authorized next-natural-cycle window
ran from 20:49 to the fixed 21:35 UTC stop. Both finite observers terminated
successfully. The superseded rc9 window remains separate; no mixed-release
full-window claim is made.

All nine loopback homepage observations returned HTTP 200 within 2.114 seconds,
meeting the declared five-second availability gate. Every optional live weather
strip was explicitly unavailable under its query budget. This does **not** prove
that the optional aggregation became fast enough or that the network was calm.
Public report pages remained accessible; natural report completions occurred at
19:54:13 and 21:30:05 UTC. A rendered static strip is not a fresh live computation.

Ingestion and scan timestamps progressed. Discovery's observed message counter
rose from 92 to 5,894, with zero reported queue drops, parse failures, worker
failures, or reconnects. The same three service processes remained active with
zero restarts. These counters establish the observed interval, not comprehensive
network coverage or absence of upstream omissions.

The two previously durable-but-unobserved terminal-page sources acquired truthful
observation timestamps after the rc9 repair. At the final sample, all 105 durable
cursors had observation timestamps. Another 25 active sources still had no
durable cursor: their latest outcomes were 19 empty acquisitions, three successful
acquisitions, and three errors. Two errors were persistent JSON parsing failures;
the third source had repeated HTTP 503 responses in its last eight attempts.
Earlier classification had four successes and two errors, so the final change is
visible rather than frozen into the initial narrative. Successful acquisition
does not create cursor support that the source never provided. Cursor-v2 and
poll-coverage concerns therefore remained degraded, without manufactured cursors.

## The gate that remains incomplete

The first rc10 derive attempt returned at 20:10:43 UTC with 11 required steps
complete and `update_author_labeler_day` pending. Its durable backlog covered
2026-09-04 through 2026-09-06 after a WAL-pressure defer at 760.2 MiB against
the existing 500 MiB guard. The successful author-day backlog was cleared.
The aggregate success heartbeat correctly did not advance.

After the first report completed, memory occupancy fell and derive was admitted;
this rules out permanent starvation throughout that first window. The next
hourly report overlapped the next derive eligibility. Further pressure deferrals
occurred through 21:28, and the final 21:35 receipt still had no new completed
derive outcome. Scan had completed at 21:34 and the second report at 21:30;
those facts do not prove the following derive attempt completed.

The pressure evidence was not merely an unexplained occupancy ratio: selected
samples showed substantial I/O pressure and, at times, memory reclaim stalls.
No memory limit, pressure threshold, WAL guard, or pending-work record was
overridden. Required-step completion remains **incomplete**, not waived because
other service dimensions improved.

The smallest next action is a bounded read of the next natural derive outcome
and the exact remaining day-range metadata. If the same backlog persists, compare
its durable boundary before selecting a scheduling or bounded-work repair.
Preserve successful earlier steps and the pressure protections. Separately,
optional homepage aggregation cost and failing upstream source endpoints remain
maintenance work; neither should be relabeled healthy.

## Evidence trail

The preceding [custody/homepage finding](2026-09-08-acquisition-custody-and-homepage-budget.md)
and [cadence/outcome finding](2026-09-08-cadence-read-work-and-derive-outcomes.md)
explain the regression tests and implementation decisions. Private campaign
`2026-09-08-observatory-correctness-recovery`, Labelwatch lane, retains immutable
release qualification, selected per-sample JSON, finite observer terminals,
backlog/pressure receipts, and the integrator's deployment and recovery records.
This history intentionally excludes upstream source identities and private operational
paths. Documentation-only history commits do not change the deployed candidate.
