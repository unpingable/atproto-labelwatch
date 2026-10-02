"""Build small v3 cold catalogs for tests without pyarrow.

Mirrors the schema and manifest written by tools/label_events_frontdoor_archive
(build_summary + build_catalog) so labelwatch.frontdoor_archive.load_catalog
verifies the result exactly as it would a production catalog.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from labelwatch.frontdoor import attachment_locus
from labelwatch.frontdoor_archive import FORMAT, FORMAT_VERSION

SCHEMA = """
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
"""


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _days(start_day: str, end_day_exclusive: str) -> list[str]:
    out, cur, end = [], date.fromisoformat(start_day), date.fromisoformat(end_day_exclusive)
    while cur < end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def build_test_catalog(directory: Path, rows, *, start_day: str,
                       end_day_exclusive: str, watermark: int,
                       uri_top_n: int = 50, name: str = "catalog.sqlite") -> Path:
    """rows: iterables of (labeler_did, uri, val, neg, ts, target_did)."""
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / name
    days = _days(start_day, end_day_exclusive)
    values, totals, loci, uris = {}, {}, defaultdict(int), {}
    states = set()

    def bump(store, key, ts):
        agg = store.setdefault(key, [0, ts, ts])
        agg[0] += 1
        agg[1] = min(agg[1], ts)
        agg[2] = max(agg[2], ts)

    for labeler, uri, val, neg, ts, target in rows:
        bump(totals, labeler, ts)
        if target is None:
            continue
        bump(values, (target, labeler, val), ts)
        states.add((target, labeler, val, neg))
        loci[(target, labeler, attachment_locus(uri, target))] += 1
        if not uri.startswith("did:"):
            bump(uris, (target, labeler, uri, val), ts)
    uri_totals = defaultdict(int)
    for (target, labeler, uri, _), agg in uris.items():
        uri_totals[(target, labeler, uri)] += agg[0]
    ranked = defaultdict(list)
    for (target, labeler, uri), total in uri_totals.items():
        ranked[(target, labeler)].append((-total, uri))
    keep = {(t, l, u) for (t, l), cands in ranked.items()
            for _, u in sorted(cands)[:uri_top_n]}

    conn = sqlite3.connect(database)
    conn.executescript(SCHEMA)
    conn.executemany("INSERT INTO metadata VALUES(?,?)", {
        "format": FORMAT,
        "format_version": str(FORMAT_VERSION),
        "start_day": start_day,
        "end_day_exclusive": end_day_exclusive,
        "days_json": json.dumps(days, separators=(",", ":")),
        "source_event_id_upper_bound": str(watermark),
        "uri_top_n": str(uri_top_n),
    }.items())
    conn.executemany("INSERT INTO label_values VALUES(?,?,?,?,?,?)",
                     [(*k, *v) for k, v in values.items()])
    conn.executemany("INSERT INTO labeler_totals VALUES(?,?,?,?)",
                     [(k, *v) for k, v in totals.items()])
    conn.executemany("INSERT INTO states VALUES(?,?,?,?)", sorted(states, key=repr))
    conn.executemany("INSERT INTO loci VALUES(?,?,?,?)",
                     [(*k, v) for k, v in loci.items()])
    conn.executemany("INSERT INTO uri_values VALUES(?,?,?,?,?,?,?)",
                     [(*k, *v) for k, v in uris.items() if k[:3] in keep])
    conn.commit()
    conn.close()

    manifest = {
        "format": FORMAT,
        "format_version": FORMAT_VERSION,
        "coverage": {"start_day": start_day, "end_day_exclusive": end_day_exclusive,
                     "days": days},
        "ownership": {"source_event_id_upper_bound": watermark,
                      "live_exclusion": "id_lte_watermark_and_ts_in_interval"},
        "catalog": {"file": database.name, "sha256": _sha256(database),
                    "logical_bytes": database.stat().st_size,
                    "allocated_bytes": database.stat().st_blocks * 512,
                    "uri_top_n": uri_top_n},
        "sources": [{"day": d, "summary_sha256": "0" * 64,
                     "manifest_sha256": "0" * 64, "partition_sha256": "0" * 64,
                     "source_database_sha256": "0" * 64} for d in days],
    }
    manifest_path = directory / (database.name + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path
