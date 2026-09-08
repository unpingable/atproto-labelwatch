"""Repository-declared, producer-local visibility for Labelwatch.

This module reports observations and their local age.  It does not admit them
into NQ, qualify them for Pulse, or attach escalation policy.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_MANIFEST_PATH = REPO_ROOT / ".ops" / "concerns.toml"
MANIFEST_PATH = (REPO_MANIFEST_PATH if REPO_MANIFEST_PATH.is_file()
                 else Path(__file__).resolve().parent / "_ops" / "concerns.toml")
STATUS_SCHEMA = "project.ops.status/v1"

POLL_WINDOW_S = 30 * 60
DISCOVERY_MAX_AGE_S = 120
CURSOR_MAX_AGE_S = 15 * 60
SCAN_MAX_AGE_S = 15 * 60
REPORT_MAX_AGE_S = 60 * 60
VOLUME_FREE_FLOOR = 14 * 1024**3
FREELIST_RATIO = 0.20
FREELIST_BYTES = 1024**3
SQLITE_PROBE_KIND = "bounded_schema_and_write_intent/v1"
SQLITE_CRITICAL_OBJECTS = (
    ("table", "meta"),
    ("table", "label_events"),
    ("table", "labelers"),
    ("table", "ingest_outcomes"),
    ("index", "idx_label_events_state"),
)
SQLITE_BOUNDED_READ_TABLES = ("meta", "label_events", "labelers", "ingest_outcomes")


def _freelist_pathological(page_size: int, page_count: int, freelist: int) -> bool:
    ratio = freelist / page_count if page_count else 0.0
    return ratio >= FREELIST_RATIO and freelist * page_size >= FREELIST_BYTES


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _age(now: datetime, value: Any) -> float | None:
    parsed = _parse_ts(value)
    return max(0.0, (now - parsed).total_seconds()) if parsed else None


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    with path.open("rb") as handle:
        manifest = tomllib.load(handle)
    if manifest.get("schema") != "project.concerns/v1":
        raise ValueError("unsupported concern manifest schema")
    ids = [item.get("id") for item in manifest.get("concerns", [])]
    if not ids or any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError("concern IDs must be present and unique")
    return manifest


def _observation(
    state: str,
    observed_at: str | None,
    valid_for_seconds: int | None,
    reason: str,
    facts: dict[str, Any] | None = None,
    *,
    domain_state: str | None = None,
) -> dict[str, Any]:
    return {
        "observation_present": state != "ABSENT",
        "local_state": state,
        "domain_state": domain_state,
        "observed_at": observed_at,
        "valid_for_seconds": valid_for_seconds,
        "reason": reason,
        "facts": facts or {},
    }


def _meta(conn: sqlite3.Connection) -> dict[str, str]:
    try:
        return {str(row[0]): str(row[1]) for row in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:
        return {}


def _poll_coverage(conn: sqlite3.Connection, now: datetime) -> dict[str, Any]:
    cutoff = datetime.fromtimestamp(now.timestamp() - POLL_WINDOW_S, timezone.utc).isoformat()
    try:
        rows = conn.execute(
            "SELECT outcome, COUNT(*) FROM ingest_outcomes WHERE ts >= ? GROUP BY outcome",
            (cutoff,),
        ).fetchall()
        latest = conn.execute("SELECT MAX(ts) FROM ingest_outcomes").fetchone()[0]
    except sqlite3.Error as exc:
        return _observation("UNKNOWN", None, POLL_WINDOW_S, "ingest outcomes are unreadable", {"error": str(exc)})
    counts = {str(row[0]): int(row[1]) for row in rows}
    total = sum(counts.values())
    facts = {"window_seconds": POLL_WINDOW_S, "outcomes": counts, "attempts": total}
    if total:
        impaired = sum(value for key, value in counts.items() if key not in {"success", "empty"})
        if impaired:
            return _observation(
                "DEGRADED", latest, POLL_WINDOW_S,
                "recent polls include partial, failed, or timed-out per-source outcomes; successful empty responses remain distinct",
                facts,
            )
        return _observation("PRESENT", latest, POLL_WINDOW_S, "recent per-source polls completed successfully; empty means observed zero in the bounded query, not observer silence", facts)
    if latest:
        return _observation("STALE", latest, POLL_WINDOW_S, "only historical poll outcomes exist", facts)
    return _observation("ABSENT", None, POLL_WINDOW_S, "no poll outcome has been produced", facts)


def _discovery_coverage(meta: dict[str, str], now: datetime) -> dict[str, Any]:
    observed_at = meta.get("ops:discovery:observed_at")
    facts = {
        "session_id": meta.get("ops:discovery:session_id"),
        "connected": meta.get("ops:discovery:connected") == "true",
        "messages": int(meta.get("ops:discovery:messages", "0")),
        "parse_failures": int(meta.get("ops:discovery:parse_failures", "0")),
        "queue_dropped": int(meta.get("ops:discovery:queue_dropped", "0")),
        "worker_failures": int(meta.get("ops:discovery:worker_failures", "0")),
        "reconnects": int(meta.get("ops:discovery:reconnects", "0")),
    }
    if not observed_at:
        return _observation("ABSENT", None, DISCOVERY_MAX_AGE_S, "the discovery producer has emitted no session observation", facts)
    if (_age(now, observed_at) or 0) > DISCOVERY_MAX_AGE_S:
        return _observation("STALE", observed_at, DISCOVERY_MAX_AGE_S, "the last discovery session observation is stale", facts)
    if not facts["connected"]:
        return _observation("DEGRADED", observed_at, DISCOVERY_MAX_AGE_S, "the current discovery session reports disconnected", facts)
    if facts["parse_failures"] or facts["queue_dropped"] or facts["worker_failures"]:
        return _observation("DEGRADED", observed_at, DISCOVERY_MAX_AGE_S, "the current discovery session reports known loss or worker failure", facts)
    return _observation("PRESENT", observed_at, DISCOVERY_MAX_AGE_S, "the current discovery session reports connected without known loss", facts)


def _cursor_continuity(
    meta: dict[str, str],
    active_sources: set[str] | None,
    now: datetime,
) -> dict[str, Any]:
    observed = {key.removeprefix("ops:cursor:observed_at:"): value for key, value in meta.items() if key.startswith("ops:cursor:observed_at:")}
    advanced = {key.removeprefix("ops:cursor:advanced_at:"): value for key, value in meta.items() if key.startswith("ops:cursor:advanced_at:")}
    durable = sorted(key.removeprefix("ingest_cursor:") for key in meta if key.startswith("ingest_cursor:"))
    facts: dict[str, Any] = {
        "durable_sources": durable,
        "observed_at_by_source": observed,
        "advanced_at_by_source": advanced,
        "scope": "active_accessible_sources_with_endpoints/v2",
    }
    if active_sources is None:
        return _observation(
            "UNKNOWN", None, CURSOR_MAX_AGE_S,
            "the active acquisition source set is unavailable; retained cursors are not treated as current by default",
            facts,
        )
    active = sorted(active_sources)
    active_durable = sorted(active_sources.intersection(durable))
    inactive_retained = sorted(set(durable).difference(active_sources))
    active_without_cursor = sorted(active_sources.difference(durable))
    facts.update({
        "active_sources": active,
        "active_durable_sources": active_durable,
        "inactive_retained_sources": inactive_retained,
        "active_without_cursor": active_without_cursor,
    })
    if not active:
        return _observation(
            "UNKNOWN", None, CURSOR_MAX_AGE_S,
            "no active acquisition source is declared; inactive retained cursors cannot establish current acquisition",
            facts,
        )
    if active_without_cursor:
        return _observation(
            "DEGRADED", now.isoformat(), CURSOR_MAX_AGE_S,
            "one or more active acquisition sources lack a durable cursor; inactive retained cursors do not satisfy this concern",
            facts,
        )
    active_observed = {
        source: observed[source] for source in active_durable if source in observed
    }
    if not active_observed:
        return _observation(
            "UNKNOWN", None, CURSOR_MAX_AGE_S,
            "active durable cursors predate cursor-observation instrumentation",
            facts,
        )
    latest = max(active_observed.values(), key=lambda value: _parse_ts(value) or datetime.min.replace(tzinfo=timezone.utc))
    stale = sorted(
        source for source in active_durable
        if _age(now, observed.get(source)) is None
        or (_age(now, observed.get(source)) or 0) > CURSOR_MAX_AGE_S
    )
    facts["stale_active_sources"] = stale
    if stale:
        return _observation(
            "STALE", latest, CURSOR_MAX_AGE_S,
            "one or more active durable cursors have not been observed recently; inactive retained cursors are reported separately",
            facts,
        )
    return _observation(
        "PRESENT", latest, CURSOR_MAX_AGE_S,
        "active durable cursors were observed recently; inactive retention and advancement are reported separately",
        facts,
    )


def _discovery_drain(meta: dict[str, str], now: datetime) -> dict[str, Any]:
    coverage = _discovery_coverage(meta, now)
    facts = dict(coverage["facts"])
    facts["queue_depth"] = int(meta.get("ops:discovery:queue_depth", "0"))
    facts["queue_capacity"] = int(meta.get("ops:discovery:queue_capacity", "0"))
    if coverage["local_state"] in {"ABSENT", "STALE"}:
        return _observation(coverage["local_state"], coverage["observed_at"], DISCOVERY_MAX_AGE_S, "no current drain observation is available", facts)
    if facts["worker_failures"] or facts["queue_dropped"]:
        return _observation("DEGRADED", coverage["observed_at"], DISCOVERY_MAX_AGE_S, "accepted discovery work was lost or the worker failed", facts)
    if facts["queue_capacity"] and facts["queue_depth"] >= facts["queue_capacity"]:
        return _observation("DEGRADED", coverage["observed_at"], DISCOVERY_MAX_AGE_S, "the bounded discovery queue is saturated", facts)
    return _observation("PRESENT", coverage["observed_at"], DISCOVERY_MAX_AGE_S, "the bounded discovery drain reports no known shedding", facts)


def _heartbeat(meta: dict[str, str], key: str, max_age: int, now: datetime, label: str) -> dict[str, Any]:
    observed_at = meta.get(key)
    if not observed_at:
        return _observation("ABSENT", None, max_age, f"no {label} completion has been recorded")
    age = _age(now, observed_at)
    facts = {"age_seconds": round(age, 3) if age is not None else None}
    if age is None:
        return _observation("UNKNOWN", observed_at, max_age, f"the {label} timestamp is invalid", facts)
    if age > max_age:
        return _observation("STALE", observed_at, max_age, f"the last {label} completion is stale", facts)
    return _observation("PRESENT", observed_at, max_age, f"a recent {label} completion is recorded", facts)


def _sqlite_observations(db_path: Path, now: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    if not db_path.exists():
        absent = _observation("ABSENT", None, None, "the configured SQLite file does not exist", {"path": str(db_path)})
        return absent, absent
    facts: dict[str, Any] = {
        "path": str(db_path),
        "probe": SQLITE_PROBE_KIND,
        "integrity_check_performed": False,
    }
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=rw", uri=True, timeout=1)
        schema_cookie = int(conn.execute("PRAGMA schema_version").fetchone()[0])
        names = tuple(name for _, name in SQLITE_CRITICAL_OBJECTS)
        placeholders = ",".join("?" for _ in names)
        found = {
            (str(row[0]), str(row[1]))
            for row in conn.execute(
                f"SELECT type, name FROM sqlite_schema WHERE name IN ({placeholders})",
                names,
            )
        }
        missing = [f"{kind}:{name}" for kind, name in SQLITE_CRITICAL_OBJECTS if (kind, name) not in found]
        readable: list[str] = []
        for table in SQLITE_BOUNDED_READ_TABLES:
            if ("table", table) in found:
                # Constant, repository-owned identifiers only. LIMIT 1 touches
                # at most the schema/root path plus one row; it is not a scan.
                conn.execute(f'SELECT 1 FROM "{table}" LIMIT 1').fetchone()
                readable.append(table)
        schema_row = (
            conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
            if ("table", "meta") in found
            else None
        )
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        conn.execute("BEGIN IMMEDIATE")
        conn.rollback()
        facts.update({
            "schema_cookie": schema_cookie,
            "application_schema_version": str(schema_row[0]) if schema_row else None,
            "critical_objects": [f"{kind}:{name}" for kind, name in SQLITE_CRITICAL_OBJECTS],
            "missing_critical_objects": missing,
            "bounded_read_tables": readable,
            "write_transaction_acquired": True,
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist,
        })
        complete = not missing and schema_row is not None
        continuity = _observation(
            "PRESENT" if complete else "DEGRADED",
            now.isoformat(),
            None,
            (
                "the bounded schema/object reads succeeded and SQLite can acquire a write transaction; "
                "no integrity check was performed"
                if complete
                else "the bounded SQLite probe found missing critical schema objects or schema metadata; "
                "no integrity check was performed"
            ),
            facts,
        )
        free_bytes = freelist * page_size
        ratio = freelist / page_count if page_count else 0.0
        ffacts = {"page_size": page_size, "page_count": page_count, "freelist_count": freelist, "freelist_bytes": free_bytes, "freelist_ratio": round(ratio, 6), "ratio_threshold": FREELIST_RATIO, "bytes_threshold": FREELIST_BYTES}
        pathological = _freelist_pathological(page_size, page_count, freelist)
        freelist_obs = _observation("DEGRADED" if pathological else "PRESENT", now.isoformat(), None, "both freelist pathology gates are met" if pathological else "the compound freelist pathology predicate is false", ffacts)
        return continuity, freelist_obs
    except sqlite3.Error as exc:
        if conn is not None and conn.in_transaction:
            conn.rollback()
        facts.update({"write_transaction_acquired": False, "error": str(exc)})
        unknown = _observation(
            "DEGRADED", now.isoformat(), None,
            "the bounded SQLite schema/read/write-intent probe failed; no integrity check was performed",
            facts,
        )
        return unknown, _observation("UNKNOWN", now.isoformat(), None, "freelist geometry is unavailable because the SQLite probe failed", facts)
    finally:
        if conn is not None:
            conn.close()


def _volume(db_path: Path, now: datetime) -> dict[str, Any]:
    target = db_path.parent if db_path.parent.exists() else Path.cwd()
    try:
        usage = shutil.disk_usage(target)
    except OSError as exc:
        return _observation("UNKNOWN", now.isoformat(), None, "filesystem capacity could not be observed", {"path": str(target), "error": str(exc)})
    facts = {"path": str(target), "free_bytes": usage.free, "total_bytes": usage.total, "floor_bytes": VOLUME_FREE_FLOOR}
    return _observation("PRESENT" if usage.free >= VOLUME_FREE_FLOOR else "DEGRADED", now.isoformat(), None, "free capacity is at or above the existing floor" if usage.free >= VOLUME_FREE_FLOOR else "free capacity is below the existing floor", facts)


def build_status(db_path: str | os.PathLike[str], *, now: datetime | None = None, manifest_path: Path = MANIFEST_PATH) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    manifest = load_manifest(manifest_path)
    path = Path(db_path).resolve()
    produced: dict[str, dict[str, Any]] = {}
    meta: dict[str, str] = {}
    active_sources: set[str] | None = None
    if path.exists():
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            meta = _meta(conn)
            produced["labelwatch.ingest.poll_coverage"] = _poll_coverage(conn, now)
            try:
                active_sources = {
                    str(row[0]) for row in conn.execute(
                        "SELECT labeler_did FROM labelers "
                        "WHERE endpoint_status='accessible' "
                        "AND service_endpoint IS NOT NULL AND service_endpoint <> ''"
                    )
                }
            except sqlite3.Error:
                active_sources = None
            conn.close()
        except sqlite3.Error as exc:
            produced["labelwatch.ingest.poll_coverage"] = _observation("UNKNOWN", now.isoformat(), POLL_WINDOW_S, "the local outcome store is unobservable", {"error": str(exc)})
    else:
        produced["labelwatch.ingest.poll_coverage"] = _observation("ABSENT", None, POLL_WINDOW_S, "the configured SQLite store does not exist", {"path": str(path)})

    produced["labelwatch.discovery.stream_coverage"] = _discovery_coverage(meta, now)
    produced["labelwatch.ingest.cursor_continuity"] = _cursor_continuity(meta, active_sources, now)
    produced["labelwatch.discovery.drain"] = _discovery_drain(meta, now)
    produced["labelwatch.processing.scan_freshness"] = _heartbeat(meta, "last_scan_ok_ts", SCAN_MAX_AGE_S, now, "scan")
    produced["labelwatch.output.report_freshness"] = _heartbeat(meta, "last_report_ok_ts", REPORT_MAX_AGE_S, now, "report")
    continuity, freelist = _sqlite_observations(path, now)
    produced["labelwatch.persistence.sqlite_continuity"] = continuity
    produced["labelwatch.persistence.sqlite_freelist"] = freelist
    produced["labelwatch.persistence.volume_capacity"] = _volume(path, now)

    concerns = []
    for declared in manifest["concerns"]:
        observation = produced.get(declared["id"])
        if observation is None:
            observation = {"observation_present": False, "local_state": "ABSENT", "domain_state": None, "observed_at": None, "valid_for_seconds": None, "reason": "required concern has no local observation producer", "facts": {}}
        concerns.append({**declared, "observation": observation})
    return {
        "schema": STATUS_SCHEMA,
        "project": manifest["project"],
        "generated_at": now.isoformat(),
        "manifest": {"schema": manifest["schema"], "path": ".ops/concerns.toml"},
        "producer": {"id": "labelwatch.ops-status"},
        "authority": {"kind": "producer_local_observation", "does_not_establish": ["nq_admission", "pulse_qualification", "nightshift_attention"]},
        "concerns": concerns,
    }


def render_text(status: dict[str, Any]) -> str:
    lines = [f"{status['project']} local visibility ({status['generated_at']})"]
    for item in status["concerns"]:
        obs = item["observation"]
        marker = {"PRESENT": "+", "DEGRADED": "!", "UNKNOWN": "?", "STALE": "~", "ABSENT": "-", "NOT_APPLICABLE": "="}.get(obs["local_state"], "?")
        lines.append(f"[{marker}] {item['id']}: {obs['local_state']} — {obs['reason']}")
    lines.append("Local observations only: not NQ-admitted, Pulse-qualified, or Nightshift-escalated.")
    return "\n".join(lines)


def dumps(status: dict[str, Any]) -> str:
    return json.dumps(status, indent=2, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    """Dependency-light status entry point for the repository binding."""
    parser = argparse.ArgumentParser(prog="python -m labelwatch.ops_status")
    parser.add_argument("--db", default="labelwatch.db")
    parser.add_argument("--now", default=None)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    args = parser.parse_args(argv)
    observed_now = _parse_ts(args.now) if args.now else datetime.now(timezone.utc)
    if observed_now is None:
        parser.error("--now must be an RFC3339 date-time")
    status = build_status(args.db, now=observed_now)
    print(dumps(status) if args.format == "json" else render_text(status))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
