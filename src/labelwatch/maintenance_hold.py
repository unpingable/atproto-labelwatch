"""Persistent, database-specific startup write hold for enrolled services.

All old writers must first be stopped under the separately enrolled operational
custody. This guard is not a lock on arbitrary SQLite clients or a source of
authority. Missing/malformed release evidence never releases an existing hold.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import time


class MaintenanceHeld(RuntimeError):
    pass


def paths(database: str) -> tuple[Path, Path]:
    path = Path(database).absolute()
    directory = path.parent / (path.name + '.maintenance')
    return directory / 'hold.json', directory / 'release.json'


def _decode(path: Path) -> dict:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise MaintenanceHeld('duplicate hold/release field')
            result[key] = value
        return result
    if path.is_symlink() or path.parent.resolve(strict=True) != path.parent:
        raise MaintenanceHeld('hold custody path is not physical')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise MaintenanceHeld('hold/release is not a regular record')
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise MaintenanceHeld('hold/release record exceeds bound')
    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError) as exc:
        raise MaintenanceHeld('invalid hold/release record') from exc
    if not isinstance(value, dict):
        raise MaintenanceHeld('hold/release record must be object')
    return value


def active_hold(database: str) -> dict | None:
    hold_path, release_path = paths(database)
    # Directory presence with an incomplete hold is a fail-closed interrupted
    # installation, not permission to start a writer.
    if not hold_path.parent.exists() and not hold_path.parent.is_symlink():
        return None
    if not hold_path.is_file():
        raise MaintenanceHeld('maintenance directory exists without complete hold')
    hold = _decode(hold_path)
    if (set(hold) != {'schema', 'operation', 'database', 'manifest_sha256'}
            or hold['schema'] != 'labelwatch.maintenance-hold/v1'
            or hold['database'] != str(Path(database).resolve())
            or not isinstance(hold['operation'], str) or not hold['operation']
            or not isinstance(hold['manifest_sha256'], str)
            or len(hold['manifest_sha256']) != 64
            or any(c not in '0123456789abcdef' for c in hold['manifest_sha256'])):
        raise MaintenanceHeld('hold binding differs from enrolled database')
    if not release_path.exists() and not release_path.is_symlink():
        return hold
    release = _decode(release_path)
    expected_keys = {'schema', 'operation', 'hold_sha256', 'release_receipt'}
    hold_digest = hashlib.sha256(json.dumps(hold, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if (set(release) != expected_keys or release['schema'] != 'labelwatch.maintenance-release/v1'
            or release['operation'] != hold['operation'] or release['hold_sha256'] != hold_digest
            or not isinstance(release['release_receipt'], str) or not release['release_receipt']):
        raise MaintenanceHeld('release does not bind exact hold and external receipt')
    # This is local configured operational custody, NOT receipt authentication.
    # The controller must verify the external authority before publishing release.
    return None


def require_writes_released(database: str) -> None:
    if active_hold(database) is not None:
        raise MaintenanceHeld('ingress/write hold active; writable connection refused')


def wait_before_writer_start(database: str) -> None:
    """Start the real service behind its hold, before writable DB/bootstrap.

    Read-only functional acceptance is a separate controller check; mere presence
    of this waiting process is not SERVICE_ACCEPTED_PRE_INGEST.
    """
    while active_hold(database) is not None:
        time.sleep(0.25)
