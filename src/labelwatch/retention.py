"""Retention floor for the live working set.

The live SQLite database may hold only events with ``ts`` at or after a
retention floor; earlier history is archived off host and not served.
Any answer whose claimed coverage reaches before the floor must say that
cold history is unavailable. Absence is never inferred from retention scope.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

from . import db
from .utils import parse_ts

RETENTION_FLOOR_KEY = "retention:live_floor"
RETENTION_ARCHIVE_KEY = "retention:archive_ref"
# Earliest instant the observatory claims history for. A cold catalog is
# complete only when it starts at or before this point; an absent key means
# the start of history is unknown and coverage is reported as a gap.
RETENTION_HISTORY_START_KEY = "retention:history_start"

COLD_CATALOG_ENV = "LABELWATCH_FRONTDOOR_COLD_CATALOG"

COVERAGE_COMPLETE = "complete"
COVERAGE_GAP = "gap"
COVERAGE_STALE = "stale"
COVERAGE_ABSENT = "absent"
COVERAGE_TAMPERED = "tampered"

_FLOOR_ENV = "LABELWATCH_RETENTION_FLOOR"
_ARCHIVE_ENV = "LABELWATCH_RETENTION_ARCHIVE_REF"


def _validate_floor(value: str) -> str:
    """Return value unchanged if it is an ISO-8601 UTC timestamp; raise otherwise."""
    try:
        dt = parse_ts(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid retention floor {value!r}") from exc
    if dt.utcoffset() != timedelta(0):
        raise ValueError(f"retention floor must be UTC: {value!r}")
    return value


def live_floor(conn) -> Optional[str]:
    """Live-set retention floor, or None when the database holds full history.

    An invalid configured value raises (fail closed).
    """
    value = os.environ.get(_FLOOR_ENV, "").strip()
    if not value:
        value = (db.get_meta(conn, RETENTION_FLOOR_KEY) or "").strip()
    if not value:
        return None
    return _validate_floor(value)


def archive_ref(conn) -> Optional[str]:
    value = os.environ.get(_ARCHIVE_ENV, "").strip()
    if not value:
        value = (db.get_meta(conn, RETENTION_ARCHIVE_KEY) or "").strip()
    return value or None


def history_start(conn) -> Optional[str]:
    value = (db.get_meta(conn, RETENTION_HISTORY_START_KEY) or "").strip()
    return value or None


# ---------------------------------------------------------------------------
# Cold catalog custody and coverage
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CatalogCustody:
    """Outcome of loading (and fully verifying) a cold catalog at startup.

    load_status is one of "loaded", "absent", "not_local", "tampered".
    ``stat`` fingerprints the verified database file so a later replacement
    or rewrite is detected without rehashing on every request.
    """
    manifest_path: Optional[str]
    load_status: str
    reason: Optional[str] = None
    catalog: Any = None
    stat: Optional[tuple] = None


def _stat_fingerprint(path) -> tuple:
    st = os.stat(path)
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def load_catalog_custody(manifest_path: Optional[str] = None, *,
                         filesystem_type: Optional[str] = None) -> CatalogCustody:
    """Load and verify the configured cold catalog without raising.

    manifest_path defaults to $LABELWATCH_FRONTDOOR_COLD_CATALOG. Verification
    (manifest shape, closed-day interval, logical size, SHA-256, quick_check,
    embedded metadata, local filesystem) is done by frontdoor_archive.
    """
    from .frontdoor_archive import CatalogNotLocal, load_catalog

    if manifest_path is None:
        manifest_path = os.environ.get(COLD_CATALOG_ENV, "").strip() or None
    if not manifest_path:
        return CatalogCustody(None, "absent", "not_configured")
    path = Path(manifest_path)
    if not path.exists() and not path.is_symlink():
        return CatalogCustody(str(path), "absent", "manifest_missing")
    try:
        catalog = load_catalog(path, filesystem_type=filesystem_type)
    except CatalogNotLocal as exc:
        return CatalogCustody(str(path), "not_local", str(exc))
    except Exception as exc:  # any verification failure is a custody breach
        return CatalogCustody(str(path), "tampered", f"{type(exc).__name__}: {exc}")
    return CatalogCustody(str(path), "loaded", None, catalog,
                          _stat_fingerprint(catalog.database_path))


def _as_custody(catalog) -> Optional[CatalogCustody]:
    if catalog is None or isinstance(catalog, CatalogCustody):
        return catalog
    # A bare verified ColdCatalog: fingerprint it now.
    return CatalogCustody(str(catalog.manifest_path), "loaded", None, catalog,
                          _stat_fingerprint(catalog.database_path))


def _catalog_info(catalog) -> Optional[dict]:
    if catalog is None:
        return None
    return {
        "start_day": catalog.start_day,
        "end_day_exclusive": catalog.end_day_exclusive,
        "days": len(catalog.days),
        "source_event_id_upper_bound": catalog.source_event_id_upper_bound,
        "sha256": catalog.database_sha256,
        "uri_top_n": catalog.uri_top_n,
        "allocated_bytes": catalog.allocated_bytes,
    }


def catalog_coverage(conn, catalog) -> dict:
    """Decide whether a cold catalog completes history below the live floor.

    catalog is a CatalogCustody, a verified ColdCatalog, or None. Status:

    - complete: catalog end (UTC midnight) == live floor, catalog start <=
      retention:history_start, and the verified file is unchanged;
    - gap: history_start is unknown/invalid or precedes the catalog start;
    - stale: catalog end != live floor (or there is no floor to meet);
    - absent: no usable local catalog (unconfigured, missing, network fs);
    - tampered: verification failed or the file changed after verification.

    Only complete authorizes serving catalog aggregates. An invalid live floor
    raises (fail closed), as live_floor does.
    """
    floor = live_floor(conn)
    start = history_start(conn)
    custody = _as_custody(catalog)
    result = {
        "status": None,
        "reason": None,
        "live_floor": floor,
        "history_start": start,
        "catalog": None,
        "manifest": custody.manifest_path if custody else None,
    }

    def done(status, reason=None):
        result["status"] = status
        result["reason"] = reason
        return result

    if custody is None:
        return done(COVERAGE_ABSENT, "not_configured")
    if custody.load_status == "absent":
        return done(COVERAGE_ABSENT, custody.reason)
    if custody.load_status == "not_local":
        return done(COVERAGE_ABSENT, "catalog_not_on_local_filesystem")
    if custody.load_status != "loaded" or custody.catalog is None:
        return done(COVERAGE_TAMPERED, custody.reason)
    cat = custody.catalog
    result["catalog"] = _catalog_info(cat)
    try:
        current = _stat_fingerprint(cat.database_path)
    except OSError:
        return done(COVERAGE_TAMPERED, "catalog_file_missing_after_verification")
    if current != custody.stat:
        return done(COVERAGE_TAMPERED, "catalog_file_changed_after_verification")

    if floor is None:
        return done(COVERAGE_STALE, "no_live_floor")
    catalog_end = parse_ts(cat.end_day_exclusive + "T00:00:00Z")
    floor_dt = parse_ts(floor)
    if catalog_end < floor_dt:
        return done(COVERAGE_STALE, "catalog_ends_before_live_floor")
    if catalog_end > floor_dt:
        return done(COVERAGE_STALE, "catalog_ends_after_live_floor")

    if start is None:
        return done(COVERAGE_GAP, "history_start_unknown")
    try:
        start_dt = parse_ts(_validate_floor(start))
    except ValueError:
        return done(COVERAGE_GAP, "history_start_invalid")
    if parse_ts(cat.start_day + "T00:00:00Z") > start_dt:
        return done(COVERAGE_GAP, "catalog_starts_after_history_start")
    return done(COVERAGE_COMPLETE)


def usable_catalog(conn, catalog):
    """Return (ColdCatalog or None, coverage). The catalog only when complete."""
    coverage = catalog_coverage(conn, catalog)
    if coverage["status"] != COVERAGE_COMPLETE:
        return None, coverage
    return _as_custody(catalog).catalog, coverage


def is_unavailable(block: Optional[dict]) -> bool:
    """True when a cold_history block means part of the claim is not served."""
    return block is not None and block.get("status") == "unavailable"


def cold_history_block(conn, requested_start: Optional[str],
                       coverage: Optional[dict] = None) -> Optional[dict]:
    """Describe cold history for a query starting at requested_start.

    requested_start=None means the query claims all history. Returns None when
    the requested coverage lies entirely inside the live working set. When the
    caller serves catalog aggregates and passes a coverage that is complete,
    returns an informational {"status": "catalog", ...} block; otherwise the
    block says cold history is unavailable.
    """
    floor = live_floor(conn)
    if floor is None:
        return None
    if requested_start is not None and parse_ts(requested_start) >= parse_ts(floor):
        return None
    if coverage is not None and coverage.get("status") == COVERAGE_COMPLETE:
        return {
            "status": "catalog",
            "live_floor": floor,
            "archive": archive_ref(conn),
            "requested_start": requested_start,
            "scope": "all_history" if requested_start is None else "partial",
            "history_start": coverage.get("history_start"),
            "catalog": coverage.get("catalog"),
        }
    return {
        "status": "unavailable",
        "live_floor": floor,
        "archive": archive_ref(conn),
        "requested_start": requested_start,
        "scope": "all_history" if requested_start is None else "partial",
    }


def health_block(conn, catalog) -> dict:
    """The single /health retention block: floor, catalog range, consistency.

    consistent is True when the served history has no hole: either there is
    no floor and no catalog (full history live), or coverage is complete.
    """
    coverage = catalog_coverage(conn, catalog)
    consistent = coverage["status"] == COVERAGE_COMPLETE or (
        coverage["live_floor"] is None and coverage["status"] == COVERAGE_ABSENT)
    return {
        "live_floor": coverage["live_floor"],
        "archive": archive_ref(conn),
        "history_start": coverage["history_start"],
        "catalog": coverage["catalog"],
        "coverage": coverage["status"],
        "reason": coverage["reason"],
        "consistent": consistent,
    }


def set_retention_floor(conn, floor: str, archive_ref: Optional[str]) -> None:
    """Record the live floor (and archive reference) in meta."""
    db.set_meta(conn, RETENTION_FLOOR_KEY, _validate_floor(floor))
    if archive_ref:
        db.set_meta(conn, RETENTION_ARCHIVE_KEY, archive_ref)
