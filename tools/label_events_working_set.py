#!/usr/bin/env python3
"""Build a contiguous cold archive and local catalog from one frozen source.

This is an off-host qualification tool. It never mutates the source and has no
production retention command.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from tools.label_events_cold_archive import export_day, verify_manifest
from tools.label_events_frontdoor_archive import (
    SummaryRef,
    build_catalog,
    build_summary,
    verify_summary,
)


def _days(start: str, end_exclusive: str) -> list[str]:
    current = date.fromisoformat(start)
    end = date.fromisoformat(end_exclusive)
    if current >= end:
        raise ValueError("start must precede end-exclusive")
    result = []
    while current < end:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day-exclusive", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--summaries-only", action="store_true")
    args = parser.parse_args()

    if args.output.is_symlink():
        raise SystemExit("refused: output is symlinked")
    if args.output.exists() and not args.resume:
        raise SystemExit("refused: output already exists")
    if args.output.exists() and not args.output.is_dir():
        raise SystemExit("refused: output is not a directory")
    args.output.mkdir(parents=True, mode=0o700, exist_ok=args.resume)
    partitions = args.output / "partitions"
    summaries = args.output / "summaries"
    partitions.mkdir(exist_ok=args.resume)
    summaries.mkdir(exist_ok=args.resume)
    refs = []
    rows = included = skipped = 0
    started = time.time()
    days = _days(args.start_day, args.end_day_exclusive)
    for day in days:
        day_started = time.time()
        day_dir = partitions / day
        manifest_path = day_dir / f"date={day}.manifest.json"
        if manifest_path.exists():
            if not args.resume:
                raise RuntimeError("partition unexpectedly exists")
            manifest = verify_manifest(manifest_path)
            if manifest["day"] != day or manifest["source"]["database_sha256"] != args.source_sha256:
                raise RuntimeError("resumed partition is bound to a different source or day")
        else:
            if day_dir.exists() and any(day_dir.iterdir()):
                raise RuntimeError("refused: incomplete partition directory is not empty")
            manifest_path = export_day(
                args.source_db, day_dir, day, args.source_sha256
            )
        manifest = json.loads(manifest_path.read_text())
        summary_path = summaries / f"date={day}.sqlite"
        if summary_path.exists():
            if not args.resume:
                raise RuntimeError("summary unexpectedly exists")
            reference = SummaryRef(summary_path, _sha256(summary_path))
            metadata = verify_summary(reference)
            if metadata["day"] != day or metadata["manifest_sha256"] != _sha256(manifest_path):
                raise RuntimeError("resumed summary is bound to a different partition")
            summary_conn = sqlite3.connect(
                f"file:{summary_path}?mode=ro&immutable=1", uri=True
            )
            try:
                included_events = int(summary_conn.execute(
                    "SELECT COALESCE(SUM(event_count),0) FROM label_values"
                ).fetchone()[0])
            finally:
                summary_conn.close()
            summary = {
                "included_events": included_events,
                "events_without_target_did": (
                    int(manifest["partition"]["rows"]) - included_events
                ),
                "summary_bytes": summary_path.stat().st_size,
                "summary_sha256": reference.sha256,
            }
        else:
            summary = build_summary(manifest_path, summary_path)
        rows += int(manifest["partition"]["rows"])
        included += int(summary["included_events"])
        skipped += int(summary["events_without_target_did"])
        refs.append(SummaryRef(summary_path, summary["summary_sha256"]))
        print(json.dumps({
            "day": day,
            "rows": manifest["partition"]["rows"],
            "summary_bytes": summary["summary_bytes"],
            "elapsed_seconds": time.time() - day_started,
        }, sort_keys=True), flush=True)

    summary_result = {
        "schema": "labelwatch.complete-working-set-summaries.v1",
        "source_database_sha256": args.source_sha256,
        "start_day": args.start_day,
        "end_day_exclusive": args.end_day_exclusive,
        "days": len(days),
        "archived_rows": rows,
        "catalog_included_events": included,
        "events_without_target_did": skipped,
        "elapsed_seconds": time.time() - started,
        "source_rows_deleted": False,
        "production_changed": False,
    }
    (args.output / "summaries-qualification.json").write_text(
        json.dumps(summary_result, sort_keys=True, indent=2) + "\n"
    )
    if args.summaries_only:
        print(json.dumps(summary_result, sort_keys=True), flush=True)
        return

    source_conn = sqlite3.connect(
        f"file:{args.source_db}?mode=ro&immutable=1", uri=True
    )
    try:
        source_event_id_upper_bound = int(source_conn.execute(
            "SELECT COALESCE(MAX(id),0) FROM label_events"
        ).fetchone()[0])
    finally:
        source_conn.close()
    catalog_manifest = build_catalog(
        refs,
        args.output / "frontdoor-cold-catalog.sqlite",
        source_event_id_upper_bound=source_event_id_upper_bound,
    )
    catalog = json.loads(catalog_manifest.read_text())
    result = {
        "schema": "labelwatch.complete-working-set-archive.v1",
        "source_database_sha256": args.source_sha256,
        "start_day": args.start_day,
        "end_day_exclusive": args.end_day_exclusive,
        "days": len(days),
        "archived_rows": rows,
        "catalog_included_events": included,
        "events_without_target_did": skipped,
        "catalog": catalog["catalog"],
        "elapsed_seconds": time.time() - started,
        "source_rows_deleted": False,
        "production_changed": False,
    }
    (args.output / "qualification.json").write_text(
        json.dumps(result, sort_keys=True, indent=2) + "\n"
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
