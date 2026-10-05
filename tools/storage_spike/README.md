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
prototype does not implement Labelwatch cursor transfer, concurrent writer
fencing, production rollover, or schema-generation reader adapters. It rejects
unsupported SQLite schema generations. Full-volume cold query acceptance is
separate from a stratified query comparison. Benchmarks do not authorize a
migration or retention-policy change.
