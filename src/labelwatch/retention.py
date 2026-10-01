"""Retention floor for the live working set.

The live SQLite database may hold only events with ``ts`` at or after a
retention floor; earlier history is archived off host and not served.
Any answer whose claimed coverage reaches before the floor must say that
cold history is unavailable. Absence is never inferred from retention scope.
"""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Optional

from . import db
from .utils import parse_ts

RETENTION_FLOOR_KEY = "retention:live_floor"
RETENTION_ARCHIVE_KEY = "retention:archive_ref"

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


def cold_history_block(conn, requested_start: Optional[str]) -> Optional[dict]:
    """Describe unavailable cold history for a query starting at requested_start.

    requested_start=None means the query claims all history. Returns None when
    the requested coverage lies entirely inside the live working set.
    """
    floor = live_floor(conn)
    if floor is None:
        return None
    if requested_start is not None and parse_ts(requested_start) >= parse_ts(floor):
        return None
    return {
        "status": "unavailable",
        "live_floor": floor,
        "archive": archive_ref(conn),
        "requested_start": requested_start,
        "scope": "all_history" if requested_start is None else "partial",
    }


def set_retention_floor(conn, floor: str, archive_ref: Optional[str]) -> None:
    """Record the live floor (and archive reference) in meta."""
    db.set_meta(conn, RETENTION_FLOOR_KEY, _validate_floor(floor))
    if archive_ref:
        db.set_meta(conn, RETENTION_ARCHIVE_KEY, archive_ref)
