# M3 fixture enrollment candidate

Future integration qualification, not production enrollment or deployment.
`scripts/m3_fixture_enrollment.py` prepares a disposable database and per-step
unit candidates. It does not install units, dispatch effects, grant permission,
or decide that a successful systemd job establishes the maintenance condition.

## Guest inputs and custody

Use a fresh campaign-owned local VM and retain the existing reviewed VM lifecycle
and teardown. Required inputs are the exact Labelwatch source revision (including
the preparer), Python 3 with its actual resolved interpreter path and standard
library, the pinned universal `websockets` wheel for actual main/discovery startup,
the actual compatible NQ-ng binary including `labelwatch-relief`, and the existing
AG-ng/Docket composition driver and runtime packages. Record package hashes and
producer/checker revisions; host-built NQ compatibility with a Debian guest must
not be assumed.

The backup directory must be a separately mounted filesystem with room for backup
and restore. A fixture tmpfs can demonstrate separate device identity and actual
restore, but does **not** establish survival across guest/power loss. Production
backup durability and custody remain separate enrollment requirements.

All unit files, helper source/imports, interpreter, step inputs and their enclosing
directories must be root-owned and immutable to application writers while each
step is admitted/executed. AG binds the exact unit name; it does not measure the
unit fragment. The generated unit digest is an enrollment check, not an AG claim.
The bounded fixture can run both application roles as root; it does not qualify
a restricted production service-account arrangement.

## Preparation and execution shape

With `PYTHONPATH` pointing to the enrolled source's `src`, run the preparer:

```text
python3 scripts/m3_fixture_enrollment.py initialize \
  --target /var/lib/m3-fixture/run-001 --backup /mnt/m3-backup/run-001 \
  --revision <exact Labelwatch commit>
python3 scripts/m3_fixture_enrollment.py seal \
  --target /var/lib/m3-fixture/run-001 --action stage \
  --source-root /opt/labelwatch --python /usr/bin/python3.11
```

Parent and backup directories must already exist; the target must not exist.
The preparer never reinitializes a prior fixture. Physical absolute paths use a
deliberately restricted character vocabulary. Before enrolling each candidate,
check its actual step and unit hashes and protect the complete source chain.
Install only the exact digest-named unit in this disposable VM, without enabling
it. Its fixed command has no mutable action arguments; `Restart=no` forbids retry.

Invoke the existing AG/Docket driver with the candidate's exact unit name and
appropriately bound subject/scope. Preserve each driver's authorizations,
admission, Docket attempt and actual systemd evidence. Reopen the independent
helper result at `expected_result`; transport/job completion is not that result.
Seal the next candidate with `--previous <exact retained completed record>`.
Never rerun a STARTED-without-completion step as recovery.

Order: stage, replace, start both actual held application roles, verify-service,
independent observation plus NQ pre-ingest qualification, cleanup, release,
independent observation plus NQ post-release qualification. Each maintenance
effect is a separately admitted digest-named unit. Both writer units must also
use `Restart=no`; the main fixture disables ingest/scan intervals and discovery
uses only a refused loopback endpoint with backstop disabled. This demonstrates
real startup/write resumption, not upstream acquisition.

The existing composition driver's constant `clean_basis` is development fixture
admission, not proof of an NQ maintenance precondition, and is not sufficient for
the final cleanup witness. `qualification/m3-admission/observation_resolver.rs`
provides the application-owned native receipt adapter. It requires the separately
named `labelwatch-cleanup` profile: held cut plus independently reopened backup
and restore facts. The expected observation reference binds the exact native
request/receipt, AG subject/scope and cleanup input digest. Every resolver call
reopens/replays the receipt and checks freshness at AG's clock cut. It does not
grant authority. The exact enrolled driver must use this resolver before cleanup;
missing/stale/indeterminate/substituted evidence refuses instead of supplying
`condition.clean`. Native factual receipts and AG/Docket custody remain separate.
This stronger integration is a candidate pending compilation, independent review
and actual VM exercise. No additional controller/retry engine is introduced.

## Qualification remaining

### Entry diagnosis

Initialization retains `entry-diagnosis.json`: real quiescent SQLite
page_count/page_size/freelist_count/user_version; main, WAL and SHM identity or
observed absence; filesystem device/capacity/free/available bytes; host and time
cut. The sidecar-free immutable SQLite read requires already enrolled writer
quiescence and protected paths. An unavailable probe is NOT_OBSERVABLE, not absent.

The explicit **fixture-only** policy admits at least 64 free pages as freelist
bloat; filesystem pressure is independently classified against a 4096-byte floor.
Pressure alone does not establish that compaction is useful. A roomy fixture
reports pressure NOT_NEEDED, not synthetic pressure. Page bytes are not promised
net disk relief. Stage sealing refuses absent/unknown/not-needed entry evidence
before creating an effect candidate and binds its original file identity.
This retained acquisition cut is not a perpetual freshness assertion: intervening
negative seeds are deliberate qualification substitutions and the helper still
checks its exact source before work. The driver must retain the diagnostic/policy
basis with its separate AG/Docket admission evidence.

This app-owned diagnostic record is not an NQ receipt and grants no authority.
Existing native fixed-predicate 5,000,000-page semantics are not changed to fit
small fixtures. Current production diagnosis, thresholds, service enrollment and
authority remain unqualified pending specifically approved host observation.

### App-owned acquisition interface

`scripts/m3_fixture_capture.py --fixture ABSOLUTE_DIR --staged EXACT_STAGE_COMPLETED_JSON
--output ABSENT_DIR --nq EXACT_BINARY --nq-sha256 SHA256 --phase cleanup|pre|post
--acquisition-budget-seconds N` acquires actual application observations and calls
the descriptor-captured, digest-checked native binary. Post additionally requires
`--pre-ingest-receipt EXACT_PRE_RECEIPT`. Use the enclosing durable campaign driver;
this helper is not a supervisor or an authorization source.

It writes `<phase>-source.json`, `<phase>-request.json`, `<phase>-receipt.json`
and `capture-result.json`. Cleanup uses the v2 held-acquisition/currentness contract;
pre/post retain the v1 relief contract. Exit zero means capture completed, including
REFUTED or NOT_OBSERVABLE receipts: consumers must inspect factual disposition.
An acquisition timeout terminates only its own observation child, reports whether
that child remains, exits 2, and creates no native receipt. Reusing an output
directory refuses without changing retained bytes. Protected fixture/source/input
directories and enrolled interpreter/imports/runtime libraries remain explicit
custody premises; a file alone grants no authority.

Focused execution with the existing pinned v1 binary demonstrated real pre-ingest
ESTABLISHED, actual changed-content REFUTED, preserved repeated-output bytes, and
an actual SQLite-lock acquisition timeout. Cleanup v2 and post capture through
this new helper remain NOT_RUN pending the new native build and integrated cases.

The preparer test exercises real SQLite staging and replacement with a separate
filesystem and verifies step/unit digests, fixed restart policy and no-overwrite
behavior. This is **not** AG/Docket/systemd integration evidence. Final VM evidence
must additionally cover the actual per-step custody path, refusal/interruption,
retained application/NQ evidence, scoped teardown and exact source/package pins.
Production dogfooding is not authorized by these fixture records.
