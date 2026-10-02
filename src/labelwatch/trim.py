"""Weekly delta export and trim of the live working set.

Two steps, run by an operator (nothing here is scheduled):

1. ``export_partition`` opens one read snapshot of the live database, records
   the watermark W' = MAX(id), and copies every row with ``ts < F_new AND
   id <= W'`` into a standalone SQLite partition file plus a JSON manifest
   that binds the id range, row count, per-day counts, a canonical row digest
   and the file's SHA-256.

2. ``run_trim`` (CLI ``labelwatch retention-trim``) refuses unless the
   manifest verifies, its row count and id range equal the live rows for the
   same predicate, and (when a cold catalog is installed) the catalog ends
   exactly at F_new with watermark W'. It then records the floor, deletes the
   rows in id-range batches with a WAL checkpoint between batches (never
   VACUUM), moves any late rows below F_new (id > W') into quarantine, and
   asserts no row below F_new remains.

Order is install catalog -> move floor -> delete, so a reader never sees a
hole and nothing is counted twice. Absence is never inferred from retention
scope: until a complete catalog is installed the API keeps reporting cold
history as unavailable.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from . import db, retention
from .utils import format_ts, parse_ts

PARTITION_FORMAT = "labelwatch.label-events-trim-partition"
PARTITION_FORMAT_VERSION = 1
SELECTION = "ts_lt_floor_and_id_lte_watermark"
TRIM_PENDING_KEY = "retention:trim_pending"
TRIM_LAST_KEY = "retention:last_trim"

COLUMNS = ("id", "labeler_did", "src", "uri", "cid", "val", "neg", "exp",
           "sig", "ts", "event_hash", "target_did")
_COLS = ",".join(COLUMNS)

PARTITION_SCHEMA = """
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE label_events (
    id INTEGER PRIMARY KEY,
    labeler_did TEXT NOT NULL,
    src TEXT,
    uri TEXT NOT NULL,
    cid TEXT,
    val TEXT NOT NULL,
    neg INTEGER,
    exp TEXT,
    sig TEXT,
    ts TEXT NOT NULL,
    event_hash TEXT NOT NULL,
    target_did TEXT
);
"""


class TrimRefused(RuntimeError):
    """A precondition for exporting or trimming does not hold."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_line(row) -> bytes:
    return (json.dumps(list(row), separators=(",", ":"), ensure_ascii=True)
            + "\n").encode("ascii")


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def validate_floor_instant(floor: str) -> str:
    """A floor must be an ISO-8601 UTC timestamp at exactly midnight."""
    retention._validate_floor(floor)
    dt = parse_ts(floor)
    if (dt.hour, dt.minute, dt.second, dt.microsecond) != (0, 0, 0, 0):
        raise TrimRefused(f"floor must be a UTC midnight: {floor!r}")
    return dt.strftime("%Y-%m-%dT00:00:00Z")


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def _meta_value(conn, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return (row[0] or None) if row else None


def export_partition(db_path: str, floor: str, out_dir: str, *,
                     batch_rows: int = 50_000,
                     name: Optional[str] = None) -> Path:
    """Export rows ``ts < floor AND id <= MAX(id)`` from one read snapshot.

    Returns the manifest path. The partition and manifest are written to
    temporary names, fsynced and renamed; existing files are never replaced.
    """
    floor = validate_floor_instant(floor)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = name or f"label-events-lt-{floor[:10]}"
    partition = out / f"{stem}.sqlite"
    manifest_path = out / f"{stem}.manifest.json"
    for path in (partition, manifest_path):
        if path.exists() or path.is_symlink():
            raise FileExistsError(f"{path} already exists")
    tmp = out / f".{partition.name}.{os.getpid()}.incomplete"
    if tmp.exists():
        raise FileExistsError(f"{tmp} already exists")

    src = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    try:
        src.execute("PRAGMA busy_timeout=5000")
        # One read transaction = one snapshot for W', the range and the rows.
        src.execute("BEGIN")
        watermark = src.execute("SELECT MAX(id) FROM label_events").fetchone()[0]
        if watermark is None:
            raise TrimRefused("label_events is empty")
        previous_floor = _meta_value(src, retention.RETENTION_FLOOR_KEY)
        schema_version = _meta_value(src, "schema_version")
        id_min, id_max = src.execute(
            "SELECT MIN(id), MAX(id) FROM label_events WHERE ts < ? AND id <= ?",
            (floor, watermark),
        ).fetchone()

        digest = hashlib.sha256()
        per_day: dict[str, int] = {}
        count = 0
        ts_min = ts_max = None
        below_previous = 0
        dst = sqlite3.connect(tmp)
        try:
            dst.executescript("PRAGMA journal_mode=DELETE;" + PARTITION_SCHEMA)
            cursor_id = (id_min - 1) if id_min is not None else None
            while cursor_id is not None:
                rows = src.execute(
                    f"SELECT {_COLS} FROM label_events "
                    "WHERE id > ? AND id <= ? AND ts < ? ORDER BY id LIMIT ?",
                    (cursor_id, id_max, floor, batch_rows),
                ).fetchall()
                if not rows:
                    break
                dst.executemany(
                    f"INSERT INTO label_events({_COLS}) VALUES({','.join('?' * len(COLUMNS))})",
                    rows,
                )
                for row in rows:
                    digest.update(_row_line(row))
                    ts = row[9]
                    day = ts[:10]
                    per_day[day] = per_day.get(day, 0) + 1
                    ts_min = ts if ts_min is None or ts < ts_min else ts_min
                    ts_max = ts if ts_max is None or ts > ts_max else ts_max
                    if previous_floor and ts < previous_floor:
                        below_previous += 1
                count += len(rows)
                cursor_id = rows[-1][0]
            meta = {
                "format": PARTITION_FORMAT,
                "format_version": str(PARTITION_FORMAT_VERSION),
                "floor": floor,
                "watermark": str(watermark),
                "row_count": str(count),
                "rows_sha256": digest.hexdigest(),
            }
            dst.executemany("INSERT INTO metadata VALUES(?,?)", meta.items())
            dst.commit()
            if dst.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("partition quick_check failed")
        finally:
            dst.close()
        src.execute("COMMIT")
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        src.close()

    with tmp.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(tmp, partition)
    _fsync_dir(out)

    manifest = {
        "format": PARTITION_FORMAT,
        "format_version": PARTITION_FORMAT_VERSION,
        "created_at": format_ts(datetime.now(timezone.utc)),
        "floor": floor,
        "previous_floor": previous_floor,
        "selection": {"predicate": SELECTION, "watermark": watermark},
        "rows": {
            "count": count,
            "id_min": id_min,
            "id_max": id_max,
            "ts_min": ts_min,
            "ts_max": ts_max,
            "below_previous_floor": below_previous,
            "per_day": dict(sorted(per_day.items())),
            "sha256": digest.hexdigest(),
            "digest": "sha256 over JSON arrays of (" + ",".join(COLUMNS) + ") in id order, one per line",
        },
        "partition": {
            "file": partition.name,
            "sha256": _sha256_file(partition),
            "bytes": partition.stat().st_size,
        },
        "source": {
            "database": str(Path(db_path).resolve()),
            "schema_version": schema_version,
        },
    }
    tmp_manifest = out / f".{manifest_path.name}.{os.getpid()}.incomplete"
    tmp_manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    with tmp_manifest.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(tmp_manifest, manifest_path)
    _fsync_dir(out)
    return manifest_path


def verify_partition(manifest_path: str | Path) -> dict:
    """Verify a partition against its manifest; return the manifest."""
    manifest_path = Path(manifest_path)
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise TrimRefused("partition manifest is absent or symlinked")
    manifest = json.loads(manifest_path.read_text())
    if (manifest.get("format") != PARTITION_FORMAT
            or manifest.get("format_version") != PARTITION_FORMAT_VERSION):
        raise TrimRefused("unsupported partition manifest format")
    if manifest["selection"]["predicate"] != SELECTION:
        raise TrimRefused("unsupported partition selection predicate")
    rel = Path(manifest["partition"]["file"])
    if rel.is_absolute() or len(rel.parts) != 1:
        raise TrimRefused("partition file must be a sibling of its manifest")
    partition = manifest_path.parent / rel
    if not partition.is_file() or partition.is_symlink():
        raise TrimRefused("partition file is absent or symlinked")
    if partition.stat().st_size != manifest["partition"]["bytes"]:
        raise TrimRefused("partition size mismatch")
    if _sha256_file(partition) != manifest["partition"]["sha256"]:
        raise TrimRefused("partition checksum mismatch")
    conn = sqlite3.connect(f"file:{partition}?mode=ro&immutable=1", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM metadata"))
        rows = manifest["rows"]
        expected = {
            "format": PARTITION_FORMAT,
            "format_version": str(PARTITION_FORMAT_VERSION),
            "floor": manifest["floor"],
            "watermark": str(manifest["selection"]["watermark"]),
            "row_count": str(rows["count"]),
            "rows_sha256": rows["sha256"],
        }
        if meta != expected:
            raise TrimRefused("partition metadata does not match manifest")
        digest = hashlib.sha256()
        count = 0
        for row in conn.execute(f"SELECT {_COLS} FROM label_events ORDER BY id"):
            digest.update(_row_line(row))
            count += 1
        if count != rows["count"] or digest.hexdigest() != rows["sha256"]:
            raise TrimRefused("partition rows do not match manifest digest")
        id_min, id_max = conn.execute("SELECT MIN(id), MAX(id) FROM label_events").fetchone()
        if (id_min, id_max) != (rows["id_min"], rows["id_max"]):
            raise TrimRefused("partition id range does not match manifest")
    finally:
        conn.close()
    return manifest


# ---------------------------------------------------------------------------
# Trim
# ---------------------------------------------------------------------------

def _catalog_check(conn, floor: str, watermark: int, custody) -> dict:
    """The installed catalog must cover exactly up to floor, at watermark."""
    if custody is None or custody.manifest_path is None:
        return {"installed": False}
    if custody.load_status != "loaded":
        raise TrimRefused(
            f"cold catalog is configured but not usable: {custody.load_status} "
            f"({custody.reason})")
    cat = custody.catalog
    if cat.end_day_exclusive + "T00:00:00Z" != floor:
        raise TrimRefused(
            f"installed catalog ends at {cat.end_day_exclusive}, not at the new floor {floor}")
    if cat.source_event_id_upper_bound != watermark:
        raise TrimRefused(
            f"installed catalog watermark {cat.source_event_id_upper_bound} "
            f"!= partition watermark {watermark}")
    start = retention.history_start(conn)
    if start is None or parse_ts(cat.start_day + "T00:00:00Z") > parse_ts(start):
        raise TrimRefused("installed catalog does not reach retention:history_start")
    return {"installed": True, "start_day": cat.start_day,
            "end_day_exclusive": cat.end_day_exclusive,
            "sha256": cat.database_sha256}


def run_trim(conn, floor: str, manifest_path: str | Path, *,
             catalog_custody=None,
             archive_ref: Optional[str] = None,
             now: Optional[datetime] = None,
             min_age_days: int = 31,
             batch_ids: int = 50_000,
             checkpoint: str = "PASSIVE",
             dry_run: bool = False,
             on_batch: Optional[Callable[[int], None]] = None) -> dict:
    """Move the live floor to ``floor`` and delete the exported rows.

    Refuses (TrimRefused) before changing anything unless every precondition
    holds. Resumable: an interrupted trim leaves retention:trim_pending and a
    rerun with the same manifest continues the deletion.
    """
    floor = validate_floor_instant(floor)
    if os.environ.get(retention._FLOOR_ENV, "").strip():
        raise TrimRefused(f"{retention._FLOOR_ENV} overrides the meta floor; unset it first")
    if checkpoint.upper() not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
        raise TrimRefused(f"invalid checkpoint mode {checkpoint!r}")
    now = now or datetime.now(timezone.utc)
    manifest_path = Path(manifest_path)
    manifest = verify_partition(manifest_path)
    manifest_sha = _sha256_file(manifest_path)
    if manifest["floor"] != floor:
        raise TrimRefused(f"manifest floor {manifest['floor']} != requested floor {floor}")
    watermark = int(manifest["selection"]["watermark"])
    rows = manifest["rows"]

    pending_raw = db.get_meta(conn, TRIM_PENDING_KEY)
    pending = json.loads(pending_raw) if pending_raw else None
    if pending and pending.get("manifest_sha256") != manifest_sha:
        raise TrimRefused(f"another trim is pending: {pending}")
    resuming = pending is not None

    current = retention.live_floor(conn)
    if not resuming:
        if current is not None and parse_ts(floor) <= parse_ts(current):
            raise TrimRefused(f"new floor {floor} does not advance the current floor {current}")
        if parse_ts(floor) > now - timedelta(days=min_age_days):
            raise TrimRefused(
                f"new floor {floor} is less than {min_age_days} days before now; "
                "the live set must keep the full raw analysis window")
        if rows["below_previous_floor"]:
            raise TrimRefused(
                f"partition holds {rows['below_previous_floor']} rows below the previous "
                "floor; they belong to an already-archived interval")
        if manifest.get("previous_floor") != current:
            raise TrimRefused(
                f"partition was exported under floor {manifest.get('previous_floor')}, "
                f"live floor is {current}")
        live_count, live_min, live_max = conn.execute(
            "SELECT COUNT(*), MIN(id), MAX(id) FROM label_events WHERE ts < ? AND id <= ?",
            (floor, watermark),
        ).fetchone()
        if (live_count, live_min, live_max) != (rows["count"], rows["id_min"], rows["id_max"]):
            raise TrimRefused(
                f"live rows below the floor (count={live_count}, ids {live_min}..{live_max}) "
                f"do not match the manifest (count={rows['count']}, "
                f"ids {rows['id_min']}..{rows['id_max']})")
    elif current != floor:
        raise TrimRefused(f"pending trim expects floor {floor}, live floor is {current}")

    catalog = _catalog_check(conn, floor, watermark, catalog_custody)
    archive = archive_ref or f"{manifest_path.resolve()}#sha256={manifest_sha}"
    receipt = {
        "floor": floor,
        "previous_floor": manifest.get("previous_floor"),
        "watermark": watermark,
        "manifest": str(manifest_path),
        "manifest_sha256": manifest_sha,
        "archive_ref": archive,
        "catalog": catalog,
        "rows_expected": rows["count"],
        "resumed": resuming,
        "dry_run": dry_run,
    }
    if dry_run:
        return receipt

    # Move the floor first (and mark the trim pending) in one commit.
    if not resuming:
        db.set_meta(conn, TRIM_PENDING_KEY, json.dumps({
            "manifest_sha256": manifest_sha, "floor": floor, "watermark": watermark,
            "started_at": format_ts(now)}, sort_keys=True))
        retention.set_retention_floor(conn, floor, archive)
        conn.commit()

    deleted = 0
    batches = 0
    lo = rows["id_min"]
    hi = rows["id_max"]
    if lo is not None:
        start = lo
        while start <= hi:
            end = min(start + batch_ids - 1, hi)
            cur = conn.execute(
                "DELETE FROM label_events WHERE id >= ? AND id <= ? AND ts < ?",
                (start, end, floor),
            )
            deleted += cur.rowcount
            conn.commit()
            conn.execute(f"PRAGMA wal_checkpoint({checkpoint.upper()})").fetchone()
            batches += 1
            if on_batch is not None:
                on_batch(batches)
            start = end + 1

    # Late rows below the new floor (ids above W') are counted by neither the
    # partition nor the catalog: quarantine them like ingest would now.
    late = conn.execute(
        f"SELECT {','.join(COLUMNS[1:])} FROM label_events WHERE ts < ? AND id > ?",
        (floor, watermark),
    ).fetchall()
    if late:
        db.quarantine_label_events(
            conn, [tuple(r) for r in late], reason="below_retention_floor",
            live_floor=floor, now_ts=format_ts(datetime.now(timezone.utc)))
        conn.execute("DELETE FROM label_events WHERE ts < ? AND id > ?", (floor, watermark))
        conn.commit()

    remaining = conn.execute(
        "SELECT COUNT(*) FROM label_events WHERE ts < ?", (floor,)).fetchone()[0]
    if remaining:
        raise RuntimeError(f"{remaining} rows below the floor remain after trim")
    receipt.update({"deleted": deleted, "batches": batches,
                    "late_rows_quarantined": len(late),
                    "completed_at": format_ts(datetime.now(timezone.utc))})
    db.set_meta(conn, TRIM_LAST_KEY, json.dumps(receipt, sort_keys=True))
    conn.execute("DELETE FROM meta WHERE key = ?", (TRIM_PENDING_KEY,))
    conn.commit()
    return receipt
