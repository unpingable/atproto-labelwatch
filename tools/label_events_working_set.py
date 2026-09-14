#!/usr/bin/env python3
"""Build a contiguous cold archive and local catalog from one frozen source.

This is an off-host qualification tool. It never mutates the source and has no
production retention command.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
from pathlib import Path
import time

from tools.label_events_cold_archive import export_day
from tools.label_events_frontdoor_archive import SummaryRef, build_catalog, build_summary


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day-exclusive", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.output.exists() or args.output.is_symlink():
        raise SystemExit("refused: output already exists")
    args.output.mkdir(parents=True, mode=0o700)
    partitions = args.output / "partitions"
    summaries = args.output / "summaries"
    partitions.mkdir()
    summaries.mkdir()
    refs = []
    rows = included = skipped = 0
    started = time.time()
    days = _days(args.start_day, args.end_day_exclusive)
    for day in days:
        day_started = time.time()
        manifest_path = export_day(
            args.source_db, partitions / day, day, args.source_sha256
        )
        manifest = json.loads(manifest_path.read_text())
        summary_path = summaries / f"date={day}.sqlite"
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

    catalog_manifest = build_catalog(refs, args.output / "frontdoor-cold-catalog.sqlite")
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
