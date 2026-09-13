#!/usr/bin/env python3
"""Prototype exact frontdoor aggregates for cold Labelwatch event partitions.

The output is one immutable SQLite summary per verified event partition. It is
not a production retention path and never deletes or modifies source events.
Multiple summaries can be combined deterministically with the live working set
by summing counts, taking min/max observation times, unioning state tuples, and
ranking URIs only after all contributing partitions are merged.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labelwatch.frontdoor import attachment_locus
from tools.label_events_cold_archive import verify_manifest


FORMAT = "labelwatch.frontdoor-cold-summary"
FORMAT_VERSION = 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _min(current: str | None, value: str) -> str:
    return value if current is None or value < current else current


def _max(current: str | None, value: str) -> str:
    return value if current is None or value > current else current


def build_summary(manifest_path: Path, destination: Path) -> dict:
    """Build an immutable per-partition summary through an atomic destination."""
    manifest = verify_manifest(manifest_path)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("summary destination must not exist")
    tmp = destination.with_name(f".{destination.name}.{os.getpid()}.incomplete")
    if tmp.exists() or tmp.is_symlink():
        raise FileExistsError("incomplete summary destination already exists")

    label_values: dict[tuple[str, str, str], list] = {}
    states: set[tuple[str, str, str, int | None]] = set()
    loci: defaultdict[tuple[str, str, str], int] = defaultdict(int)
    uri_values: dict[tuple[str, str, str, str], list] = {}
    parquet = manifest_path.parent / manifest["partition"]["file"]
    included = 0
    skipped_without_target = 0
    for batch in pq.ParquetFile(parquet).iter_batches(batch_size=50_000):
        for row in pa.Table.from_batches([batch]).to_pylist():
            target = row["target_did"]
            if target is None:
                skipped_without_target += 1
                continue
            labeler = row["labeler_did"]
            val = row["val"]
            ts = row["ts"]
            key = (target, labeler, val)
            aggregate = label_values.setdefault(key, [0, None, None])
            aggregate[0] += 1
            aggregate[1] = _min(aggregate[1], ts)
            aggregate[2] = _max(aggregate[2], ts)
            states.add((target, labeler, val, row["neg"]))
            loci[(target, labeler, attachment_locus(row["uri"], target))] += 1
            if not row["uri"].startswith("did:"):
                uri_key = (target, labeler, row["uri"], val)
                uri_aggregate = uri_values.setdefault(uri_key, [0, None, None])
                uri_aggregate[0] += 1
                uri_aggregate[1] = _min(uri_aggregate[1], ts)
                uri_aggregate[2] = _max(uri_aggregate[2], ts)
            included += 1

    destination.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(tmp)
    try:
        conn.executescript("""
            PRAGMA journal_mode=DELETE;
            PRAGMA synchronous=FULL;
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE label_values (
                target_did TEXT NOT NULL, labeler_did TEXT NOT NULL,
                val TEXT NOT NULL, event_count INTEGER NOT NULL,
                first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
                PRIMARY KEY (target_did, labeler_did, val)
            ) WITHOUT ROWID;
            CREATE TABLE states (
                target_did TEXT NOT NULL, labeler_did TEXT NOT NULL,
                val TEXT NOT NULL, neg INTEGER,
                UNIQUE (target_did, labeler_did, val, neg)
            );
            CREATE INDEX states_subject ON states(target_did, labeler_did);
            CREATE TABLE loci (
                target_did TEXT NOT NULL, labeler_did TEXT NOT NULL,
                locus TEXT NOT NULL, event_count INTEGER NOT NULL,
                PRIMARY KEY (target_did, labeler_did, locus)
            ) WITHOUT ROWID;
            CREATE TABLE uri_values (
                target_did TEXT NOT NULL, labeler_did TEXT NOT NULL,
                uri TEXT NOT NULL, val TEXT NOT NULL,
                event_count INTEGER NOT NULL,
                first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
                PRIMARY KEY (target_did, labeler_did, uri, val)
            ) WITHOUT ROWID;
        """)
        metadata = {
            "format": FORMAT,
            "format_version": str(FORMAT_VERSION),
            "day": manifest["day"],
            "manifest_sha256": _sha256(manifest_path),
            "partition_sha256": manifest["partition"]["sha256"],
            "source_database_sha256": manifest["source"]["database_sha256"],
        }
        conn.executemany("INSERT INTO metadata(key,value) VALUES(?,?)", metadata.items())
        conn.executemany(
            "INSERT INTO label_values VALUES(?,?,?,?,?,?)",
            ((*key, *value) for key, value in sorted(label_values.items())),
        )
        conn.executemany("INSERT INTO states VALUES(?,?,?,?)", sorted(states, key=repr))
        conn.executemany(
            "INSERT INTO loci VALUES(?,?,?,?)",
            ((*key, value) for key, value in sorted(loci.items())),
        )
        conn.executemany(
            "INSERT INTO uri_values VALUES(?,?,?,?,?,?,?)",
            ((*key, *value) for key, value in sorted(uri_values.items())),
        )
        conn.commit()
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("summary quick_check failed")
    except Exception:
        conn.close()
        tmp.unlink(missing_ok=True)
        raise
    else:
        conn.close()
    with tmp.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(tmp, destination)
    dir_fd = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return {
        "format": FORMAT,
        "format_version": FORMAT_VERSION,
        "day": manifest["day"],
        "included_events": included,
        "events_without_target_did": skipped_without_target,
        "summary_bytes": destination.stat().st_size,
        "summary_sha256": _sha256(destination),
        "manifest_sha256": _sha256(manifest_path),
    }


def query_subject(summaries: list[Path], target_did: str, top_n: int = 50) -> dict:
    """Return the four frontdoor aggregate shapes across cold summaries."""
    values: dict[tuple[str, str], list] = {}
    states: defaultdict[str, set[tuple[str, int | None]]] = defaultdict(set)
    loci: defaultdict[tuple[str, str], int] = defaultdict(int)
    uris: dict[tuple[str, str, str], list] = {}
    for path in summaries:
        conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            metadata = dict(conn.execute("SELECT key,value FROM metadata"))
            if metadata.get("format") != FORMAT or metadata.get("format_version") != str(FORMAT_VERSION):
                raise RuntimeError("unsupported cold summary")
            for row in conn.execute("SELECT * FROM label_values WHERE target_did=?", (target_did,)):
                key = (row["labeler_did"], row["val"])
                value = values.setdefault(key, [0, None, None])
                value[0] += row["event_count"]
                value[1] = _min(value[1], row["first_seen"])
                value[2] = _max(value[2], row["last_seen"])
            for row in conn.execute("SELECT labeler_did,val,neg FROM states WHERE target_did=?", (target_did,)):
                states[row["labeler_did"]].add((row["val"], row["neg"]))
            for row in conn.execute("SELECT labeler_did,locus,event_count FROM loci WHERE target_did=?", (target_did,)):
                loci[(row["labeler_did"], row["locus"])] += row["event_count"]
            for row in conn.execute("SELECT * FROM uri_values WHERE target_did=?", (target_did,)):
                key = (row["labeler_did"], row["uri"], row["val"])
                value = uris.setdefault(key, [0, None, None])
                value[0] += row["event_count"]
                value[1] = _min(value[1], row["first_seen"])
                value[2] = _max(value[2], row["last_seen"])
        finally:
            conn.close()

    uri_totals: defaultdict[tuple[str, str], int] = defaultdict(int)
    for (labeler, uri, _val), value in uris.items():
        uri_totals[(labeler, uri)] += value[0]
    selected: set[tuple[str, str]] = set()
    by_labeler: defaultdict[str, list[tuple[str, int]]] = defaultdict(list)
    for (labeler, uri), count in uri_totals.items():
        by_labeler[labeler].append((uri, count))
    for labeler, candidates in by_labeler.items():
        selected.update((labeler, uri) for uri, _ in sorted(candidates, key=lambda item: (-item[1], item[0]))[:top_n])

    return {
        "label_values": [
            {"labeler_did": key[0], "val": key[1], "event_count": value[0],
             "first_seen": value[1], "last_seen": value[2]}
            for key, value in sorted(values.items())
        ],
        "distinct_states": [
            {"labeler_did": labeler, "distinct_states": len(value)}
            for labeler, value in sorted(states.items())
        ],
        "loci": [
            {"labeler_did": key[0], "locus": key[1], "event_count": value}
            for key, value in sorted(loci.items())
        ],
        "uri_values": [
            {"labeler_did": key[0], "uri": key[1], "val": key[2],
             "val_count": value[0], "first_seen": value[1],
             "last_seen": value[2], "uri_total": uri_totals[(key[0], key[1])]}
            for key, value in sorted(uris.items()) if (key[0], key[1]) in selected
        ],
    }
