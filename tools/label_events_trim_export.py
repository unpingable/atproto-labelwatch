#!/usr/bin/env python3
"""Export the rows a retention trim to --floor would delete.

Opens one read-only snapshot of the live database, records W' = MAX(id), and
writes every row with ts < floor AND id <= W' to a standalone SQLite partition
plus a manifest binding the id range, row count, per-day counts, a canonical
row digest and the partition's SHA-256. Read-only against the source; it
never schedules anything. Feed the manifest to `labelwatch retention-trim`.

Long read transactions keep the WAL from checkpointing past the snapshot;
run it when ingest can tolerate WAL growth for the export's duration.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from labelwatch.trim import TrimRefused, export_partition, verify_partition  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", required=True, help="Live labelwatch SQLite database")
    parser.add_argument("--floor", required=True, help="New floor (UTC midnight)")
    parser.add_argument("--out-dir", required=True, help="Directory for partition + manifest")
    parser.add_argument("--name", help="File stem (default label-events-lt-<day>)")
    parser.add_argument("--batch-rows", type=int, default=50_000)
    parser.add_argument("--verify", action="store_true",
                        help="Re-verify the written partition against its manifest")
    args = parser.parse_args(argv)
    try:
        manifest_path = export_partition(args.db, args.floor, args.out_dir,
                                         batch_rows=args.batch_rows, name=args.name)
        manifest = verify_partition(manifest_path) if args.verify else json.loads(
            manifest_path.read_text())
    except TrimRefused as exc:
        print(json.dumps({"refused": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps({"manifest": str(manifest_path), "rows": manifest["rows"]["count"],
                      "watermark": manifest["selection"]["watermark"],
                      "id_min": manifest["rows"]["id_min"],
                      "id_max": manifest["rows"]["id_max"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
