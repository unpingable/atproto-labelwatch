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
import tempfile
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
    if (set(hold) != {'schema', 'operation', 'database', 'manifest_sha256', 'application_revision'}
            or hold['schema'] != 'labelwatch.maintenance-hold/v1'
            or hold['database'] != str(Path(database).resolve())
            or not isinstance(hold['operation'], str) or not hold['operation']
            or not isinstance(hold['manifest_sha256'], str)
            or len(hold['manifest_sha256']) != 64
            or any(c not in '0123456789abcdef' for c in hold['manifest_sha256'])):
        raise MaintenanceHeld('hold binding differs from enrolled database')
    if (not isinstance(hold['application_revision'], str) or len(hold['application_revision']) != 40
            or any(c not in '0123456789abcdef' for c in hold['application_revision'])):
        raise MaintenanceHeld('exact application revision required by hold')
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


def record_write_resumption(conn, database: str) -> None:
    """Commit the first post-release generation before admitting ordinary writes.

    This is itself a real post-cut write. It deliberately makes subsequent
    rollback to the original ineligible, even if ingestion has not yet advanced.
    Existing databases without a maintenance enrollment are untouched.
    """
    hold_path, release_path = paths(database)
    if not hold_path.parent.exists():
        return
    require_writes_released(database)
    hold = _decode(hold_path)
    release = _decode(release_path)
    conn.execute('INSERT OR IGNORE INTO meta(key,value) VALUES (?,?)',
                 ('maintenance_release:' + hold['operation'], release['release_receipt']))
    conn.commit()


def process_start_ticks(pid: int) -> int:
    raw = Path(f'/proc/{pid}/stat').read_text()
    fields = raw[raw.rfind(')') + 2:].split()
    if fields[0] in {'Z', 'X'}:
        raise MaintenanceHeld('service process is not live')
    return int(fields[19])


def _publish_ready(ready: Path, raw: bytes) -> None:
    """Publish complete readiness bytes without replacing an existing record.

    The enrolled parent is protected from unrelated pathname changes. A reader
    may see the final name only after the file contents have been synced. Return
    also requires directory sync. Interrupted private files are not readiness.
    """
    if ready.parent.resolve(strict=True) != ready.parent:
        raise MaintenanceHeld('enrolled readiness directory must be physical')
    descriptor, name = tempfile.mkstemp(prefix='.ready-pending-', dir=ready.parent)
    pending = Path(name)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        # Same-directory hard-link creation is atomic and fails if the final
        # pathname already exists. No overwrite/rename fallback is allowed.
        os.link(pending, ready, follow_symlinks=False)
        pending.unlink()
        directory = os.open(ready.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        # Only this invocation's newly allocated private file is removed.
        # Process loss may leave it behind; readers ignore its non-final name.
        if pending.exists():
            pending.unlink()


def wait_before_writer_start(database: str, role: str) -> None:
    """Start the real service behind its hold, before writable DB/bootstrap.

    Read-only functional acceptance is a separate controller check; mere presence
    of this waiting process is not SERVICE_ACCEPTED_PRE_INGEST.
    """
    from .maintenance_artifacts import readonly
    from .maintenance_manifest import offline_application_verify
    if role not in {'main', 'discovery'}:
        raise MaintenanceHeld('unknown enrolled writer role')
    hold = active_hold(database)
    if hold is None:
        return
    with readonly(Path(database).resolve()) as conn:
        conn.execute('BEGIN')
        verified = offline_application_verify(conn, application_revision=hold['application_revision'])
    conn.close()
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    if hashlib.sha256(canonical(verified)).hexdigest() != hold['manifest_sha256']:
        raise MaintenanceHeld('held service read-only application verification differs')
    ready_directory = paths(database)[0].parent / 'ready'
    if ready_directory.resolve(strict=True) != ready_directory:
        raise MaintenanceHeld('enrolled readiness directory must be physical')
    ready = ready_directory / f'{role}-{os.getpid()}.ready.json'
    record = {'schema': 'labelwatch.held-writer-ready/v1', 'operation': hold['operation'],
              'role': role, 'pid': os.getpid(), 'start_ticks': process_start_ticks(os.getpid()),
              'hold_sha256': hashlib.sha256(canonical(hold)).hexdigest(),
              'verification_sha256': hold['manifest_sha256']}
    _publish_ready(ready, canonical(record) + b'\n')
    while active_hold(database) is not None:
        time.sleep(0.25)
