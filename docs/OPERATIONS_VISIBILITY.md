# Repository-declared operations visibility

Labelwatch declares its expected semantic attention surface at
`.ops/concerns.toml` (`project.concerns/v1`). A generic consumer reads that
semantic inventory for stable concern, question, and local-profile identities.
The separate `.ops/observation.toml`
(`project.observation-binding/v1`) binds one project-level producer for
`project.ops.status/v1`:

```bash
labelwatch --db /var/lib/labelwatch/labelwatch.db ops-status --format json
labelwatch --db /var/lib/labelwatch/labelwatch.db ops-status --format text
```

The JSON is producer-local observation, not NQ admission, Pulse qualification,
or a Nightshift attention decision. The declaration is not evidence and
contains no paging severity, retry cadence, or response policy.

## Declared propositions

| Concern | Exact local proposition and basis | Becomes unknown, stale, or degraded when |
|---|---|---|
| `labelwatch.ingest.poll_coverage` | Per-source `ingest_outcomes` show successful bounded polling. `empty` is a successful response with zero events in that query. | No outcome exists (`ABSENT`), only old outcomes exist (`STALE`), or partial/error/timeout outcomes occur (`DEGRADED`). |
| `labelwatch.discovery.stream_coverage` | The current Jetstream discovery session heartbeat says connected and records no parse failure, queue shedding, or fatal worker failure. | No session fact, heartbeat older than 120s, disconnected transport, or any known current-session loss. |
| `labelwatch.ingest.cursor_continuity` | Durable cursors were observed in the last 15m. `observed_at` and `advanced_at` are separate facts. | No cursor, old cursor observation, or a cursor created before cursor-age instrumentation. Lack of advancement never proves upstream silence. |
| `labelwatch.discovery.drain` | The bounded discovery worker queue has no recorded shedding or fatal worker condition. | The stream fact is absent/stale or accepted work was shed. Acquisition can remain live while this concern is degraded. |
| `labelwatch.processing.scan_freshness` | `last_scan_ok_ts` records a scan completed within 15m. | No completion or only an old completion exists. Ingest activity does not renew this clock. |
| `labelwatch.persistence.sqlite_continuity` | The configured DB is readable, `quick_check(1)` is `ok`, and a non-mutating `BEGIN IMMEDIATE` write transaction can be acquired. | The file is absent, unreadable, corrupt, or unable to acquire write intent. This is deliberately narrower than “database healthy.” |
| `labelwatch.persistence.volume_capacity` | The filesystem holding the DB has at least the existing 14 GiB bounded-state work floor. | Capacity cannot be sampled or free bytes fall below that exact floor. |
| `labelwatch.persistence.sqlite_freelist` | The existing NQ compound pathology predicate is false: freelist is not simultaneously at least 20% of pages and 1 GiB. | Geometry is unobservable or both gates are met. This is separate from readability and volume capacity. |
| `labelwatch.output.report_freshness` | `last_report_ok_ts` records an observational report completed within 60m. | No report completion or only an old completion exists. A fresh report does not establish complete coverage. |

The corresponding semantic question IDs are the
`labelwatch.question.*/v1` values in the manifest.
`labelwatch.profile.local_status/v1` names this local representation only; it is
not an NQ inquiry or reliance profile.

## Knowledge boundaries

Absence of observed activity is not evidence of absence unless adequate
observation coverage supports that inference. A successful `empty` poll and
observer silence are therefore different states. The report is observational
output, not authoritative truth, and a live process is not evidence of source
coverage, drain convergence, scan completion, or report freshness.

A missing required observation is visible state, not implicit success. In the
JSON it is `local_state: "ABSENT"` with `observation_present: false`. The
invariant is:

```text
declared concern != observed concern != admitted observation
                 != current qualified observation
```

Local age projection exists so the diagnostic command cannot display old facts
as current. Pulse remains responsible for downstream bounded support,
currentness, and blindness qualification.

## Restart behavior

Polling cursors survive restart as historical continuity facts. Their stored
value does not renew `observed_at`; a pre-instrumentation cursor is `UNKNOWN`.
The discovery daemon generates a new session ID and immediately records
`connected=false` before attempting the stream. It emits a 30s heartbeat.
Consequently restart cannot carry forward the previous connected/good session.

## NQ seam

The locally available `nq-check-pack-labelwatch` is descriptor-only and returns
a generic collection plan; its documentation explicitly says it is not an
executable collector. No production NQ API currently admits this repository
status schema while preserving these concern/question identities. This work
therefore supplies the manifest, deterministic status artifact, shared schema,
and adapter tests, but does not claim NQ integration. An adapter still needs to
map each observation into NQ's real immutable admission path with source,
observation time, schema, and exact semantic identity preserved.

The earlier specimen proof remains:

```bash
python3 scripts/discover_concerns.py .. --run-status
```

It discovers manifests and status commands; it contains no project concern
list.

The reusable implementation now lives in Monitor as `monitor-concerns`.
`discover` is passive; `collect` requires an explicit trusted root and
`--allow-exec`, validates exact declaration/status correspondence, and
retains every absent required observation as
`MISSING_REQUIRED_OBSERVATION`. Labelwatch's script is retained as a narrow
specimen/conformance fixture, not as a second supervisory subsystem.
