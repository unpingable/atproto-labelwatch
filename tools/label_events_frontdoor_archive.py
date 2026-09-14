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
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labelwatch.frontdoor import attachment_locus
from labelwatch.frontdoor_archive import (
    FORMAT as CATALOG_FORMAT,
    FORMAT_VERSION as CATALOG_FORMAT_VERSION,
    load_catalog,
)
from tools.label_events_cold_archive import verify_manifest


FORMAT = "labelwatch.frontdoor-cold-summary"
FORMAT_VERSION = 2


@dataclass(frozen=True)
class SummaryRef:
    path: Path
    sha256: str


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
    labeler_totals: dict[str, list] = {}
    states: set[tuple[str, str, str, int | None]] = set()
    loci: defaultdict[tuple[str, str, str], int] = defaultdict(int)
    uri_values: dict[tuple[str, str, str, str], list] = {}
    parquet = manifest_path.parent / manifest["partition"]["file"]
    included = 0
    skipped_without_target = 0
    for batch in pq.ParquetFile(parquet).iter_batches(batch_size=50_000):
        for row in pa.Table.from_batches([batch]).to_pylist():
            labeler = row["labeler_did"]
            ts = row["ts"]
            labeler_total = labeler_totals.setdefault(labeler, [0, None, None])
            labeler_total[0] += 1
            labeler_total[1] = _min(labeler_total[1], ts)
            labeler_total[2] = _max(labeler_total[2], ts)
            target = row["target_did"]
            if target is None:
                skipped_without_target += 1
                continue
            val = row["val"]
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
            CREATE TABLE labeler_totals (
                labeler_did TEXT PRIMARY KEY, event_count INTEGER NOT NULL,
                first_seen TEXT NOT NULL, last_seen TEXT NOT NULL
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
        conn.executemany(
            "INSERT INTO labeler_totals VALUES(?,?,?,?)",
            ((key, *value) for key, value in sorted(labeler_totals.items())),
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


def verify_summary(reference: SummaryRef) -> dict[str, str]:
    path = reference.path
    if not path.is_file() or path.is_symlink():
        raise RuntimeError("cold summary is absent or symlinked")
    if _sha256(path) != reference.sha256:
        raise RuntimeError("cold summary checksum mismatch")
    conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    try:
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("cold summary quick_check failed")
        metadata = dict(conn.execute("SELECT key,value FROM metadata"))
        expected_keys = {
            "format", "format_version", "day", "manifest_sha256",
            "partition_sha256", "source_database_sha256",
        }
        if set(metadata) != expected_keys:
            raise RuntimeError("cold summary metadata keys do not match v1")
        if metadata["format"] != FORMAT or metadata["format_version"] != str(FORMAT_VERSION):
            raise RuntimeError("unsupported cold summary")
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='table'"
        )}
        if tables != {"metadata", "label_values", "labeler_totals", "states", "loci", "uri_values"}:
            raise RuntimeError("cold summary tables do not match v2")
        return metadata
    finally:
        conn.close()


def build_catalog(references: list[SummaryRef], destination: Path) -> Path:
    """Merge a complete contiguous set of summaries into one local catalog."""
    if not references:
        raise ValueError("at least one summary is required")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("catalog destination must not exist")
    manifest_path = destination.with_suffix(destination.suffix + ".manifest.json")
    if manifest_path.exists() or manifest_path.is_symlink():
        raise FileExistsError("catalog manifest destination must not exist")

    verified = []
    for reference in references:
        metadata = verify_summary(reference)
        verified.append((metadata["day"], reference, metadata))
    verified.sort(key=lambda item: item[0])
    days = [item[0] for item in verified]
    if len(set(days)) != len(days):
        raise RuntimeError("catalog contains duplicate summary days")
    expected = []
    cursor = date.fromisoformat(days[0])
    end = date.fromisoformat(days[-1]) + timedelta(days=1)
    while cursor < end:
        expected.append(cursor.isoformat())
        cursor += timedelta(days=1)
    if days != expected:
        raise RuntimeError("catalog summaries do not form a complete closed-day interval")

    tmp = destination.with_name(f".{destination.name}.{os.getpid()}.incomplete")
    if tmp.exists() or tmp.is_symlink():
        raise FileExistsError("incomplete catalog destination already exists")
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
            CREATE TABLE labeler_totals (
                labeler_did TEXT PRIMARY KEY, event_count INTEGER NOT NULL,
                first_seen TEXT NOT NULL, last_seen TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE states (
                target_did TEXT NOT NULL, labeler_did TEXT NOT NULL,
                val TEXT NOT NULL, neg INTEGER
            );
            CREATE UNIQUE INDEX states_identity ON states(
                target_did,labeler_did,val,COALESCE(neg,-9223372036854775808)
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
        conn.executemany("INSERT INTO metadata(key,value) VALUES(?,?)", {
            "format": CATALOG_FORMAT,
            "format_version": str(CATALOG_FORMAT_VERSION),
            "start_day": days[0],
            "end_day_exclusive": end.isoformat(),
            "days_json": json.dumps(days, separators=(",", ":")),
        }.items())
        for _, reference, _ in verified:
            conn.execute("ATTACH DATABASE ? AS source_summary", (
                f"file:{reference.path}?mode=ro&immutable=1",
            ))
            try:
                conn.execute("""
                    INSERT INTO label_values
                    SELECT * FROM source_summary.label_values WHERE true
                    ON CONFLICT(target_did,labeler_did,val) DO UPDATE SET
                      event_count=label_values.event_count+excluded.event_count,
                      first_seen=MIN(label_values.first_seen,excluded.first_seen),
                      last_seen=MAX(label_values.last_seen,excluded.last_seen)
                """)
                conn.execute("""
                    INSERT INTO labeler_totals
                    SELECT * FROM source_summary.labeler_totals WHERE true
                    ON CONFLICT(labeler_did) DO UPDATE SET
                      event_count=labeler_totals.event_count+excluded.event_count,
                      first_seen=MIN(labeler_totals.first_seen,excluded.first_seen),
                      last_seen=MAX(labeler_totals.last_seen,excluded.last_seen)
                """)
                conn.execute("INSERT OR IGNORE INTO states SELECT * FROM source_summary.states")
                conn.execute("""
                    INSERT INTO loci
                    SELECT * FROM source_summary.loci WHERE true
                    ON CONFLICT(target_did,labeler_did,locus) DO UPDATE SET
                      event_count=loci.event_count+excluded.event_count
                """)
                conn.execute("""
                    INSERT INTO uri_values
                    SELECT * FROM source_summary.uri_values WHERE true
                    ON CONFLICT(target_did,labeler_did,uri,val) DO UPDATE SET
                      event_count=uri_values.event_count+excluded.event_count,
                      first_seen=MIN(uri_values.first_seen,excluded.first_seen),
                      last_seen=MAX(uri_values.last_seen,excluded.last_seen)
                """)
                conn.commit()
            finally:
                conn.execute("DETACH DATABASE source_summary")
        conn.commit()
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("catalog quick_check failed")
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

    sources = [{
        "day": day,
        "summary_sha256": reference.sha256,
        "manifest_sha256": metadata["manifest_sha256"],
        "partition_sha256": metadata["partition_sha256"],
        "source_database_sha256": metadata["source_database_sha256"],
    } for day, reference, metadata in verified]
    manifest = {
        "format": CATALOG_FORMAT,
        "format_version": CATALOG_FORMAT_VERSION,
        "coverage": {
            "start_day": days[0],
            "end_day_exclusive": end.isoformat(),
            "days": days,
        },
        "catalog": {
            "file": destination.name,
            "sha256": _sha256(destination),
            "logical_bytes": destination.stat().st_size,
            "allocated_bytes": destination.stat().st_blocks * 512,
        },
        "sources": sources,
    }
    from tools.label_events_cold_archive import _atomic_json
    _atomic_json(manifest_path, manifest)
    load_catalog(manifest_path)
    return manifest_path


def query_subject(
    summaries: list[SummaryRef],
    target_did: str,
    top_n: int = 50,
    live_conn: sqlite3.Connection | None = None,
    live_exclude_interval: tuple[str, str] | None = None,
) -> dict:
    """Return aggregate shapes across cold summaries and disjoint live rows.

    During pre-cutover qualification archived rows may still exist in the live
    database.  Callers must pass the catalog-owned closed interval so those
    physical duplicates cannot be counted twice.
    """
    values: dict[tuple[str, str], list] = {}
    states: defaultdict[str, set[tuple[str, int | None]]] = defaultdict(set)
    loci: defaultdict[tuple[str, str], int] = defaultdict(int)
    uris: dict[tuple[str, str, str], list] = {}
    for reference in summaries:
        verify_summary(reference)
        conn = sqlite3.connect(f"file:{reference.path}?mode=ro&immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        try:
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

    if live_conn is not None:
        if live_exclude_interval is None:
            raise RuntimeError("live/cold merge requires an explicit ownership interval")
        start_day, end_day_exclusive = live_exclude_interval
        live_owned = "(ts IS NULL OR ts < ? OR ts >= ?)"
        live_params = (target_did, start_day, end_day_exclusive)
        for labeler, val, count, first_seen, last_seen in live_conn.execute(
            "SELECT labeler_did,val,COUNT(*),MIN(ts),MAX(ts) FROM label_events "
            f"WHERE target_did=? AND {live_owned} GROUP BY labeler_did,val",
            live_params,
        ):
            key = (labeler, val)
            value = values.setdefault(key, [0, None, None])
            value[0] += count
            value[1] = _min(value[1], first_seen)
            value[2] = _max(value[2], last_seen)
        for labeler, val, neg in live_conn.execute(
            "SELECT DISTINCT labeler_did,val,neg FROM label_events "
            f"WHERE target_did=? AND {live_owned}", live_params,
        ):
            states[labeler].add((val, neg))
        for labeler, locus, count in live_conn.execute(
            "SELECT labeler_did, CASE "
            "WHEN uri LIKE 'did:%' THEN 'account' "
            "WHEN uri LIKE 'at://%/app.bsky.feed.post/%' THEN 'post' "
            "WHEN uri LIKE 'at://%/app.bsky.actor.profile/%' THEN 'profile' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.list/%' THEN 'list' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.listitem/%' THEN 'list_item' "
            "WHEN uri LIKE 'at://%/app.bsky.feed.generator/%' THEN 'feed_generator' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.starterpack/%' THEN 'starterpack' "
            "WHEN uri LIKE 'at://%' THEN 'record' ELSE 'unknown' END AS locus, "
            f"COUNT(*) FROM label_events WHERE target_did=? AND {live_owned} "
            "GROUP BY labeler_did,locus", live_params,
        ):
            loci[(labeler, locus)] += count
        for labeler, uri, val, count, first_seen, last_seen in live_conn.execute(
            "SELECT labeler_did,uri,val,COUNT(*),MIN(ts),MAX(ts) FROM label_events "
            f"WHERE target_did=? AND uri NOT LIKE 'did:%' AND {live_owned} "
            "GROUP BY labeler_did,uri,val", live_params,
        ):
            key = (labeler, uri, val)
            value = uris.setdefault(key, [0, None, None])
            value[0] += count
            value[1] = _min(value[1], first_seen)
            value[2] = _max(value[2], last_seen)

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
