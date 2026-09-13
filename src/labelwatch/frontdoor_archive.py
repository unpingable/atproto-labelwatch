"""Read-only frontdoor projection over a verified local cold-summary catalog.

The catalog is built off-host from content-bound event partitions. Production
loads and verifies it once at startup; requests never consult archive storage.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3


FORMAT = "labelwatch.frontdoor-cold-catalog"
FORMAT_VERSION = 1
CATALOG_TABLES = {"metadata", "label_values", "states", "loci", "uri_values"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _filesystem_type(path: Path) -> str | None:
    """Return the longest matching mount type without invoking a subprocess."""
    resolved = path.resolve()
    best: tuple[int, str] | None = None
    try:
        lines = Path("/proc/self/mountinfo").read_text().splitlines()
    except OSError:
        return None
    for line in lines:
        before, separator, after = line.partition(" - ")
        if not separator:
            continue
        fields = before.split()
        tail = after.split()
        if len(fields) < 5 or not tail:
            continue
        mountpoint = Path(fields[4].replace("\\040", " ")).resolve()
        try:
            resolved.relative_to(mountpoint)
        except ValueError:
            continue
        candidate = (len(str(mountpoint)), tail[0])
        if best is None or candidate[0] > best[0]:
            best = candidate
    return best[1] if best else None


@dataclass(frozen=True)
class ColdCatalog:
    manifest_path: Path
    database_path: Path
    database_sha256: str
    days: tuple[str, ...]
    start_day: str
    end_day_exclusive: str
    allocated_bytes: int


def load_catalog(manifest_path: str | Path, *, filesystem_type: str | None = None) -> ColdCatalog:
    manifest_path = Path(manifest_path)
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise RuntimeError("cold catalog manifest is absent or symlinked")
    manifest = json.loads(manifest_path.read_text())
    if set(manifest) != {"format", "format_version", "coverage", "catalog", "sources"}:
        raise RuntimeError("cold catalog manifest root keys do not match v1")
    if manifest["format"] != FORMAT or manifest["format_version"] != FORMAT_VERSION:
        raise RuntimeError("unsupported cold catalog format")

    coverage = manifest["coverage"]
    if set(coverage) != {"start_day", "end_day_exclusive", "days"}:
        raise RuntimeError("cold catalog coverage keys do not match v1")
    days = tuple(coverage["days"])
    if not days or days != tuple(sorted(set(days))):
        raise RuntimeError("cold catalog days must be nonempty, unique, and ordered")
    start = date.fromisoformat(coverage["start_day"])
    end = date.fromisoformat(coverage["end_day_exclusive"])
    expected = []
    cursor = start
    while cursor < end:
        expected.append(cursor.isoformat())
        cursor += timedelta(days=1)
    if days != tuple(expected):
        raise RuntimeError("cold catalog coverage is not a complete closed-day interval")

    catalog = manifest["catalog"]
    if set(catalog) != {"file", "sha256", "logical_bytes", "allocated_bytes"}:
        raise RuntimeError("cold catalog file keys do not match v1")
    relative = Path(catalog["file"])
    if relative.is_absolute() or len(relative.parts) != 1 or relative.name in {"", ".", ".."}:
        raise RuntimeError("cold catalog file must be a local sibling")
    database = manifest_path.parent / relative
    if not database.is_file() or database.is_symlink():
        raise RuntimeError("cold catalog database is absent or symlinked")
    observed_type = filesystem_type if filesystem_type is not None else _filesystem_type(database)
    if observed_type in {"nfs", "nfs4", "cifs", "smb3", "fuse.sshfs"}:
        raise RuntimeError("cold catalog must reside on the local serving filesystem")
    if database.stat().st_size != catalog["logical_bytes"]:
        raise RuntimeError("cold catalog logical size mismatch")
    if database.stat().st_blocks * 512 != catalog["allocated_bytes"]:
        raise RuntimeError("cold catalog allocated size mismatch")
    if _sha256(database) != catalog["sha256"]:
        raise RuntimeError("cold catalog checksum mismatch")

    sources = manifest["sources"]
    if not isinstance(sources, list) or [item.get("day") for item in sources] != list(days):
        raise RuntimeError("cold catalog sources do not exactly cover declared days")
    required_source_keys = {
        "day", "summary_sha256", "manifest_sha256", "partition_sha256",
        "source_database_sha256",
    }
    if any(set(item) != required_source_keys for item in sources):
        raise RuntimeError("cold catalog source keys do not match v1")

    conn = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
    try:
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("cold catalog quick_check failed")
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='table'"
        )}
        if tables != CATALOG_TABLES:
            raise RuntimeError("cold catalog tables do not match v1")
        metadata = dict(conn.execute("SELECT key,value FROM metadata"))
        if metadata != {
            "format": FORMAT,
            "format_version": str(FORMAT_VERSION),
            "start_day": coverage["start_day"],
            "end_day_exclusive": coverage["end_day_exclusive"],
            "days_json": json.dumps(list(days), separators=(",", ":")),
        }:
            raise RuntimeError("cold catalog database metadata mismatch")
    finally:
        conn.close()
    return ColdCatalog(
        manifest_path=manifest_path,
        database_path=database,
        database_sha256=catalog["sha256"],
        days=days,
        start_day=coverage["start_day"],
        end_day_exclusive=coverage["end_day_exclusive"],
        allocated_bytes=catalog["allocated_bytes"],
    )


def _minimum(current: str | None, value: str) -> str:
    return value if current is None or value < current else current


def _maximum(current: str | None, value: str) -> str:
    return value if current is None or value > current else current


def query_subject(catalog: ColdCatalog, target_did: str, *, live_conn=None,
                  top_n: int = 50) -> dict:
    """Merge one verified local catalog with the current online working set."""
    values: dict[tuple[str, str], list] = {}
    states: defaultdict[str, set[tuple[str, int | None]]] = defaultdict(set)
    loci: defaultdict[tuple[str, str], int] = defaultdict(int)
    uris: dict[tuple[str, str, str], list] = {}

    cold = sqlite3.connect(f"file:{catalog.database_path}?mode=ro&immutable=1", uri=True)
    cold.row_factory = sqlite3.Row
    try:
        for row in cold.execute("SELECT * FROM label_values WHERE target_did=?", (target_did,)):
            values[(row["labeler_did"], row["val"])] = [
                row["event_count"], row["first_seen"], row["last_seen"]]
        for row in cold.execute("SELECT labeler_did,val,neg FROM states WHERE target_did=?", (target_did,)):
            states[row["labeler_did"]].add((row["val"], row["neg"]))
        for row in cold.execute("SELECT labeler_did,locus,event_count FROM loci WHERE target_did=?", (target_did,)):
            loci[(row["labeler_did"], row["locus"])] += row["event_count"]
        for row in cold.execute("SELECT * FROM uri_values WHERE target_did=?", (target_did,)):
            uris[(row["labeler_did"], row["uri"], row["val"])] = [
                row["event_count"], row["first_seen"], row["last_seen"]]
    finally:
        cold.close()

    if live_conn is not None:
        for labeler, val, count, first_seen, last_seen in live_conn.execute(
            "SELECT labeler_did,val,COUNT(*),MIN(ts),MAX(ts) FROM label_events "
            "WHERE target_did=? GROUP BY labeler_did,val", (target_did,)
        ):
            value = values.setdefault((labeler, val), [0, None, None])
            value[0] += count
            value[1] = _minimum(value[1], first_seen)
            value[2] = _maximum(value[2], last_seen)
        for labeler, val, neg in live_conn.execute(
            "SELECT DISTINCT labeler_did,val,neg FROM label_events WHERE target_did=?",
            (target_did,),
        ):
            states[labeler].add((val, neg))
        locus_sql = (
            "SELECT labeler_did, CASE WHEN uri LIKE 'did:%' THEN 'account' "
            "WHEN uri LIKE 'at://%/app.bsky.feed.post/%' THEN 'post' "
            "WHEN uri LIKE 'at://%/app.bsky.actor.profile/%' THEN 'profile' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.list/%' THEN 'list' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.listitem/%' THEN 'list_item' "
            "WHEN uri LIKE 'at://%/app.bsky.feed.generator/%' THEN 'feed_generator' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.starterpack/%' THEN 'starterpack' "
            "WHEN uri LIKE 'at://%' THEN 'record' ELSE 'unknown' END, COUNT(*) "
            "FROM label_events WHERE target_did=? GROUP BY labeler_did,2"
        )
        for labeler, locus, count in live_conn.execute(locus_sql, (target_did,)):
            loci[(labeler, locus)] += count
        for labeler, uri, val, count, first_seen, last_seen in live_conn.execute(
            "SELECT labeler_did,uri,val,COUNT(*),MIN(ts),MAX(ts) FROM label_events "
            "WHERE target_did=? AND uri NOT LIKE 'did:%' GROUP BY labeler_did,uri,val",
            (target_did,),
        ):
            value = uris.setdefault((labeler, uri, val), [0, None, None])
            value[0] += count
            value[1] = _minimum(value[1], first_seen)
            value[2] = _maximum(value[2], last_seen)

    totals: defaultdict[tuple[str, str], int] = defaultdict(int)
    by_labeler: defaultdict[str, list[tuple[str, int]]] = defaultdict(list)
    for (labeler, uri, _), value in uris.items():
        totals[(labeler, uri)] += value[0]
    for (labeler, uri), count in totals.items():
        by_labeler[labeler].append((uri, count))
    selected = {
        (labeler, uri)
        for labeler, candidates in by_labeler.items()
        for uri, _ in sorted(candidates, key=lambda item: (-item[1], item[0]))[:top_n]
    }
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
             "last_seen": value[2], "uri_total": totals[(key[0], key[1])]}
            for key, value in sorted(uris.items()) if (key[0], key[1]) in selected
        ],
    }
