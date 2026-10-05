# Labelwatch storage architecture executable spike

This directory is an isolated prototype, not an installed Labelwatch backend.
Production paths are not accepted for mutation. The data source is existing,
immutable private Labelwatch Parquet custody, not the live database.

Dependencies: Python 3.12, SQLite from that interpreter, DuckDB 1.4.3,
PyArrow 22.0.0, psycopg-binary 3.2.13, and the PostgreSQL image digest in
`run.py`. The container has no network and no TCP listener; only a Unix
socket inside owner-only campaign custody. Do not use production credentials.

The original execution is supervised through its campaign CHECKPOINT.json
and user-systemd unit. Do not rerun `run.py` into an existing occurrence.
It intentionally refuses existing fixture directories, databases and containers.
New occurrences require new run/namespace identities and their own dispatch
record. Never delete existing evidence to make a command succeed.

A fresh occurrence uses `run.py --occurrence /path/under/campaign/runtime/new-UUID
--container labelwatch-storage-pg-NEWID`. The occurrence directory must not
exist. It contains its own `runtime/` and `evidence/`; retain the old attempt's
terminal record. Record new unit identity and source revision before dispatch.

Original producer command, dispatched durably by user-systemd:

```sh
runtime/venv/bin/python -u /data/git/.worktrees/labelwatch-storage-architecture-20261005/tools/storage_spike/run.py
```

Read the checkpoint for the exact working directory, source revision, unit,
logs, artifacts and inspection commands. Dependencies are installed by the
recorded `runtime/dependencies.sh`, whose hash and terminal receipt are retained.
Independent finite replay of the custody model needs no database:

```sh
python tools/storage_spike/model.py
```

For a fresh isolated finite SQLite replay, create a new child of the admitted
campaign `runtime`, call `qualify.fixture(child, 'sqlite')`, and execute:

```sh
runtime/venv/bin/python tools/storage_spike/lifecycle.py "$owned_fixture" --cut after_receipt
# Expected exit 73; inspect source and receipt, then:
runtime/venv/bin/python tools/storage_spike/lifecycle.py "$owned_fixture"
```

PostgreSQL cases require the owned fixture cluster/socket and fixture namespace;
they never address another database service. The full benchmark's exact inputs,
hashes, sampling algorithm and workload counts are in private DATASET.json.
Private corpus files and result rows must not be added to source/public docs.

Limits: process interruption is tested, not physical host power loss. The
closed-source prototype does not implement Labelwatch cursor transfer,
production writer fencing, or schema-generation reader adapters. It rejects
unsupported SQLite schema generations. Full-volume cold query acceptance is
separate from a stratified query comparison. Benchmarks do not authorize a
migration or retention-policy change.

`qualify_rollover.py` separately exercises a finite one-writer arrival-period
vessel: event/cursor commit, late authored-time arrivals, source-bound successor
creation, monotone clock boundary, stale active pointer and process loss.
It carries only the event/ingest-meta subset, not all mutable Labelwatch state.
Archive-only dedupe and metadata restore remain explicit acceptance gates.

`supplement.py` runs the exact current `trim.py` export/verify/batched-delete
library with NORMAL synchronization and repeats PostgreSQL ingest with every
live event-index shape. It requires the primary occurrence's terminal PASS
and exact stopped fixture cluster identity; its own source/run/paths must be
dispatched before use. It preserves the primary occurrence's original receipts.


`sample_ingest.py /exact/new-owned-occurrence` supplies the final matched
180k insert comparison. `supplement.py --resume-pretrim ORIGINAL_OCCURRENCE`
was used only after the failed supplement's source, unchanged floor, complete
export and no-pending-trim facts were reconciled. Exact recorded commands are
in immutable DISPATCH JSON and CHECKPOINT; do not infer resume authorization
from directory existence. Source identities differ by occurrence and are listed
in CAMPAIGN-REPORT.md. Read the decision gates before interpreting timings.
