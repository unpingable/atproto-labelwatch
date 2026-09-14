#!/usr/bin/env python3
"""Reconstruct a complete Labelwatch cold interval into an empty database."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import time

import pyarrow as pa
import pyarrow.parquet as pq

from labelwatch import db
from tools.label_events_cold_archive import FIELDS, verify_manifest
from tools.label_events_working_set import _days, _sha256


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partitions", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day-exclusive", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()

    if args.destination.exists() or args.destination.is_symlink():
        raise SystemExit("refused: destination already exists")
    incomplete = args.destination.with_name(
        f".{args.destination.name}.{os.getpid()}.incomplete"
    )
    if incomplete.exists() or incomplete.is_symlink():
        raise SystemExit("refused: incomplete destination already exists")

    started = time.monotonic()
    days = _days(args.start_day, args.end_day_exclusive)
    conn = db.connect(str(incomplete))
    rows = 0
    try:
        db.init_db(conn)
        placeholders = ",".join("?" for _ in FIELDS)
        sql = f"INSERT INTO label_events({','.join(FIELDS)}) VALUES({placeholders})"
        for day in days:
            manifest_path = (
                args.partitions / day / f"date={day}.manifest.json"
            )
            manifest = verify_manifest(manifest_path)
            if manifest["source"]["database_sha256"] != args.source_sha256:
                raise RuntimeError("partition source identity mismatch")
            parquet = manifest_path.parent / manifest["partition"]["file"]
            inserted = 0
            for batch in pq.ParquetFile(parquet).iter_batches(batch_size=25_000):
                values = pa.Table.from_batches([batch]).to_pylist()
                conn.executemany(
                    sql, ([row[name] for name in FIELDS] for row in values)
                )
                inserted += len(values)
            if inserted != int(manifest["partition"]["rows"]):
                raise RuntimeError("partition insert count mismatch")
            conn.commit()
            rows += inserted
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("reconstructed database quick_check failed")
        observed = int(conn.execute("SELECT COUNT(*) FROM label_events").fetchone()[0])
        if observed != rows:
            raise RuntimeError("reconstructed row count mismatch")
    except Exception:
        conn.close()
        incomplete.unlink(missing_ok=True)
        raise
    else:
        conn.close()

    with incomplete.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(incomplete, args.destination)
    dir_fd = os.open(args.destination.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    result = {
        "schema": "labelwatch.complete-working-set-reconstruction.v1",
        "source_database_sha256": args.source_sha256,
        "start_day": args.start_day,
        "end_day_exclusive": args.end_day_exclusive,
        "days": len(days),
        "rows": rows,
        "database_sha256": _sha256(args.destination),
        "logical_bytes": args.destination.stat().st_size,
        "allocated_bytes": args.destination.stat().st_blocks * 512,
        "quick_check": "ok",
        "elapsed_seconds": time.monotonic() - started,
        "production_changed": False,
    }
    args.result.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
