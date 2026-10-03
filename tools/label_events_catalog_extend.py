#!/usr/bin/env python3
"""Extend a cold catalog with one weekly trim partition (off-host).

Inputs: the installed catalog's manifest (its ``sources`` list binds every
existing per-day summary by SHA-256), the directory holding those per-day
summaries (``date=YYYY-MM-DD.sqlite``), and a trim partition manifest written
by tools/label_events_trim_export.py. The tool

1. verifies the partition against its manifest;
2. requires the partition to start exactly where the catalog ends
   (previous_floor == end_day_exclusive) and its watermark not to regress;
3. builds one summary per new day from the partition (SQLite, no pyarrow),
   refusing if any partition row falls outside the new days;
4. re-merges every per-day summary into a new catalog (full re-merge, so
   top-N URI pruning stays exact) with watermark W' and verifies it.

The output keeps the v3 catalog format the server reads; per-day summaries
are the segments it is rebuilt from. Nothing is installed or deleted: install
the catalog, then run ``labelwatch retention-trim`` with the same partition.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from labelwatch.frontdoor_archive import FORMAT as CATALOG_FORMAT  # noqa: E402
from labelwatch.frontdoor_archive import FORMAT_VERSION as CATALOG_FORMAT_VERSION  # noqa: E402
from labelwatch.trim import verify_partition  # noqa: E402
from tools.label_events_frontdoor_archive import (  # noqa: E402
    FORMAT as SUMMARY_FORMAT,
    FORMAT_VERSION as SUMMARY_FORMAT_VERSION,
    SummaryRef,
    build_catalog,
    write_summary,
)


class ExtendRefused(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def summary_name(day: str) -> str:
    return f"date={day}.sqlite"


def _days(start: str, end_exclusive: str) -> list[str]:
    cur, end = date.fromisoformat(start), date.fromisoformat(end_exclusive)
    out = []
    while cur < end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def summarize_partition(partition_manifest: Path, out_dir: Path) -> list[SummaryRef]:
    """Write one v2 summary per day the partition covers; return their refs."""
    manifest = verify_partition(partition_manifest)
    previous = manifest.get("previous_floor")
    if not previous:
        raise ExtendRefused("partition has no previous floor to extend from")
    days = _days(previous[:10], manifest["floor"][:10])
    partition = partition_manifest.parent / manifest["partition"]["file"]
    metadata_base = {
        "format": SUMMARY_FORMAT,
        "format_version": str(SUMMARY_FORMAT_VERSION),
        "manifest_sha256": _sha256(partition_manifest),
        "partition_sha256": manifest["partition"]["sha256"],
        # Trim partitions are cut from the live database, which is not hashed;
        # the canonical row digest stands in for the source identity.
        "source_database_sha256": manifest["rows"]["sha256"],
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{partition}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    refs = []
    assigned = 0
    try:
        for day in days:
            nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
            rows = [dict(r) for r in conn.execute(
                "SELECT labeler_did, uri, val, neg, ts, target_did FROM label_events "
                "WHERE ts >= ? AND ts < ? ORDER BY id", (day, nxt))]
            assigned += len(rows)
            destination = out_dir / summary_name(day)
            receipt = write_summary(iter(rows), destination,
                                    {**metadata_base, "day": day})
            refs.append(SummaryRef(destination, receipt["summary_sha256"]))
    finally:
        conn.close()
    if assigned != manifest["rows"]["count"]:
        for ref in refs:
            ref.path.unlink(missing_ok=True)
        raise ExtendRefused(
            f"{manifest['rows']['count'] - assigned} partition rows fall outside "
            f"the days {days[0]}..{days[-1]}")
    return refs


def extend_catalog(base_manifest: Path, summaries_dir: Path,
                   partition_manifest: Path, out_dir: Path) -> dict:
    base = json.loads(Path(base_manifest).read_text())
    if base.get("format") != CATALOG_FORMAT or base.get("format_version") != CATALOG_FORMAT_VERSION:
        raise ExtendRefused("base catalog manifest is not the supported format")
    partition = json.loads(Path(partition_manifest).read_text())
    end = base["coverage"]["end_day_exclusive"]
    if partition.get("previous_floor") != f"{end}T00:00:00Z":
        raise ExtendRefused(
            f"partition starts at {partition.get('previous_floor')}, catalog ends at {end}")
    watermark = int(partition["selection"]["watermark"])
    if watermark < base["ownership"]["source_event_id_upper_bound"]:
        raise ExtendRefused("partition watermark regresses below the catalog watermark")

    out_dir = Path(out_dir)
    new_refs = summarize_partition(Path(partition_manifest), out_dir / "summaries")
    base_refs = [SummaryRef(Path(summaries_dir) / summary_name(src["day"]),
                            src["summary_sha256"]) for src in base["sources"]]
    destination = out_dir / "frontdoor-cold-catalog.sqlite"
    manifest_path = build_catalog(
        base_refs + new_refs, destination,
        source_event_id_upper_bound=watermark,
        uri_top_n=base["catalog"]["uri_top_n"],
    )
    built = json.loads(manifest_path.read_text())
    return {
        "manifest": str(manifest_path),
        "start_day": built["coverage"]["start_day"],
        "end_day_exclusive": built["coverage"]["end_day_exclusive"],
        "watermark": watermark,
        "sha256": built["catalog"]["sha256"],
        "new_days": [ref.path.name for ref in new_refs],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-manifest", required=True, type=Path)
    parser.add_argument("--summaries-dir", required=True, type=Path)
    parser.add_argument("--partition-manifest", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        receipt = extend_catalog(args.base_manifest, args.summaries_dir,
                                 args.partition_manifest, args.out_dir)
    except ExtendRefused as exc:
        print(json.dumps({"refused": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(receipt, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
