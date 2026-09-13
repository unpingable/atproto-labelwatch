#!/usr/bin/env python3
"""Off-host prototype for content-bound Labelwatch event partitions.

This tool never deletes source rows and is not a production retention path.
It exports one closed UTC day, verifies the finalized Parquet and manifest,
and can reconstruct the archived event rows into a fresh Labelwatch database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from labelwatch import db as labelwatch_db


FORMAT = "labelwatch.label-events-partition"
FORMAT_VERSION = 1
FIELDS = (
    "id", "labeler_did", "src", "uri", "cid", "val", "neg", "exp",
    "sig", "ts", "event_hash", "target_did",
)
ARROW_SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("labeler_did", pa.string(), nullable=False),
    pa.field("src", pa.string()),
    pa.field("uri", pa.string(), nullable=False),
    pa.field("cid", pa.string()),
    pa.field("val", pa.string(), nullable=False),
    pa.field("neg", pa.int64()),
    pa.field("exp", pa.string()),
    pa.field("sig", pa.string()),
    pa.field("ts", pa.string(), nullable=False),
    pa.field("event_hash", pa.string(), nullable=False),
    pa.field("target_did", pa.string()),
])
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_day(value: str) -> date:
    if not DAY_RE.fullmatch(value):
        raise ValueError("day must be YYYY-MM-DD")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("day is not canonical")
    return parsed


def _bounds(day: str) -> tuple[str, str]:
    parsed = _parse_day(day)
    return day, (parsed + timedelta(days=1)).isoformat()


def _canonical_row(row: dict) -> bytes:
    return (json.dumps([row[name] for name in FIELDS], ensure_ascii=False,
                       allow_nan=False, separators=(",", ":")) + "\n").encode()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".incomplete",
                               dir=path.parent)
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        tmp.unlink(missing_ok=True)


def _source_schema(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("PRAGMA table_info(label_events)").fetchall()
    return [dict(zip(("cid", "name", "type", "notnull", "default", "pk"), row))
            for row in rows]


def export_day(source_db: Path, output_dir: Path, day: str,
               source_sha256: str, batch_size: int = 50_000) -> Path:
    start, end = _bounds(day)
    if not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
        raise ValueError("source_sha256 must be a lowercase SHA-256")
    output_dir.mkdir(parents=True, exist_ok=True)
    final = output_dir / f"date={day}.parquet"
    manifest_path = output_dir / f"date={day}.manifest.json"
    if final.exists() or manifest_path.exists():
        raise FileExistsError("partition or manifest already exists")
    tmp = output_dir / f".{final.name}.{os.getpid()}.incomplete"

    conn = sqlite3.connect(f"file:{source_db}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        schema = _source_schema(conn)
        if tuple(row["name"] for row in schema) != FIELDS:
            raise RuntimeError("label_events schema does not match archive v1")
        schema_version_row = conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
        schema_version = int(schema_version_row[0]) if schema_version_row else None
        cursor = conn.execute(
            "SELECT " + ",".join(FIELDS) + " FROM label_events "
            "WHERE ts >= ? AND ts < ? ORDER BY id", (start, end)
        )
        writer = pq.ParquetWriter(tmp, ARROW_SCHEMA, compression="zstd")
        count = 0
        first_id = last_id = None
        first_ts = last_ts = None
        content = hashlib.sha256()
        try:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                values = [dict(row) for row in rows]
                for row in values:
                    content.update(_canonical_row(row))
                writer.write_table(pa.Table.from_pylist(values, schema=ARROW_SCHEMA))
                count += len(values)
                first_id = values[0]["id"] if first_id is None else first_id
                first_ts = values[0]["ts"] if first_ts is None else first_ts
                last_id = values[-1]["id"]
                last_ts = values[-1]["ts"]
        finally:
            writer.close()
        if count == 0:
            tmp.unlink(missing_ok=True)
            raise RuntimeError("refused: selected day contains no events")
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, final)
        dir_fd = os.open(output_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        manifest = {
            "format": FORMAT,
            "format_version": FORMAT_VERSION,
            "day": day,
            "source": {
                "database_sha256": source_sha256,
                "schema_version": schema_version,
                "label_events_schema": schema,
            },
            "partition": {
                "file": final.name,
                "sha256": _sha256(final),
                "rows": count,
                "first_id": first_id,
                "last_id": last_id,
                "first_ts": first_ts,
                "last_ts": last_ts,
                "ordered_row_sha256": content.hexdigest(),
            },
            "semantics": {
                "source_rows_deleted": False,
                "event_columns_complete": True,
                "related_state_included": False,
                "reader_equivalence": "unqualified_until_dependencies_and_cold_reader_are_bound",
            },
        }
        _atomic_json(manifest_path, manifest)
        verify_manifest(manifest_path)
        return manifest_path
    finally:
        conn.close()
        tmp.unlink(missing_ok=True)


def verify_manifest(manifest_path: Path) -> dict:
    manifest = json.loads(manifest_path.read_text())
    if set(manifest) != {"format", "format_version", "day", "source", "partition", "semantics"}:
        raise RuntimeError("manifest root keys do not match archive v1")
    if manifest["format"] != FORMAT or manifest["format_version"] != FORMAT_VERSION:
        raise RuntimeError("unsupported archive format")
    start, end = _bounds(manifest["day"])
    part = manifest["partition"]
    parquet = manifest_path.parent / part["file"]
    if not parquet.is_file() or parquet.is_symlink():
        raise RuntimeError("partition is absent or symlinked")
    if _sha256(parquet) != part["sha256"]:
        raise RuntimeError("partition checksum mismatch")
    source_schema = manifest["source"]["label_events_schema"]
    if tuple(row["name"] for row in source_schema) != FIELDS:
        raise RuntimeError("manifest source schema does not match archive v1")
    parquet_file = pq.ParquetFile(parquet)
    if parquet_file.schema_arrow != ARROW_SCHEMA:
        raise RuntimeError("Parquet schema does not match archive v1")
    digest = hashlib.sha256()
    count = 0
    first_id = last_id = None
    first_ts = last_ts = None
    previous_id = None
    for batch in parquet_file.iter_batches(batch_size=50_000):
        for row in pa.Table.from_batches([batch]).to_pylist():
            if not (start <= row["ts"] < end):
                raise RuntimeError("row is outside declared day")
            if previous_id is not None and row["id"] <= previous_id:
                raise RuntimeError("event IDs are not strictly ordered")
            digest.update(_canonical_row(row))
            previous_id = row["id"]
            count += 1
            first_id = row["id"] if first_id is None else first_id
            first_ts = row["ts"] if first_ts is None else first_ts
            last_id = row["id"]
            last_ts = row["ts"]
    observed = {
        "rows": count, "first_id": first_id, "last_id": last_id,
        "first_ts": first_ts, "last_ts": last_ts,
        "ordered_row_sha256": digest.hexdigest(),
    }
    for key, value in observed.items():
        if part[key] != value:
            raise RuntimeError(f"partition {key} mismatch")
    return manifest


def reconstruct(manifest_path: Path, destination: Path) -> dict:
    manifest = verify_manifest(manifest_path)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("destination must not exist")
    tmp = destination.with_name(f".{destination.name}.{os.getpid()}.incomplete")
    if tmp.exists():
        raise FileExistsError("incomplete destination already exists")
    conn = labelwatch_db.connect(str(tmp))
    try:
        labelwatch_db.init_db(conn)
        parquet = manifest_path.parent / manifest["partition"]["file"]
        placeholders = ",".join("?" for _ in FIELDS)
        sql = f"INSERT INTO label_events({','.join(FIELDS)}) VALUES({placeholders})"
        for batch in pq.ParquetFile(parquet).iter_batches(batch_size=10_000):
            rows = pa.Table.from_batches([batch]).to_pylist()
            conn.executemany(sql, ([row[name] for name in FIELDS] for row in rows))
            conn.commit()
        labelwatch_db.set_meta(conn, "cold_archive_format", f"{FORMAT}@{FORMAT_VERSION}")
        labelwatch_db.set_meta(conn, "cold_archive_manifest_sha256", _sha256(manifest_path))
        conn.commit()
        check = conn.execute("PRAGMA quick_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"reconstructed database quick_check: {check}")
        rows = conn.execute("SELECT " + ",".join(FIELDS) + " FROM label_events ORDER BY id")
        digest = hashlib.sha256()
        count = 0
        for row in rows:
            digest.update(_canonical_row(dict(row)))
            count += 1
        if count != manifest["partition"]["rows"] or digest.hexdigest() != manifest["partition"]["ordered_row_sha256"]:
            raise RuntimeError("reconstructed event identity mismatch")
    except Exception:
        conn.close()
        tmp.unlink(missing_ok=True)
        raise
    else:
        conn.close()
    os.replace(tmp, destination)
    return {
        "database": str(destination),
        "rows": manifest["partition"]["rows"],
        "ordered_row_sha256": manifest["partition"]["ordered_row_sha256"],
        "quick_check": "ok",
        "scope": "event_history_only",
        "reader_equivalence": "not_claimed",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export-day")
    export.add_argument("--source-db", type=Path, required=True)
    export.add_argument("--output-dir", type=Path, required=True)
    export.add_argument("--day", required=True)
    export.add_argument("--source-sha256", required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("manifest", type=Path)
    restore = sub.add_parser("reconstruct")
    restore.add_argument("manifest", type=Path)
    restore.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.command == "export-day":
        result = {"manifest": str(export_day(args.source_db, args.output_dir, args.day, args.source_sha256))}
    elif args.command == "verify":
        value = verify_manifest(args.manifest)
        result = {"manifest": str(args.manifest), "rows": value["partition"]["rows"], "verified": True}
    else:
        result = reconstruct(args.manifest, args.destination)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
