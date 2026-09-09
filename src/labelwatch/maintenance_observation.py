"""Fresh read-only application facts, separate from maintenance effect receipts.

This observer does not read a step completion/enactment claim to decide whether
the database is readable or the filesystem has space. NQ owns qualification;
the output is source-labeled testimony, not an authorization or terminal verdict.
"""
from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3

from .maintenance_artifacts import identity, readonly
from .maintenance_manifest import offline_application_verify
from .maintenance_hold import active_hold, process_start_ticks
from .maintenance_step import digest


def observe_cleanup(*, backup: Path, restore: Path, **arguments) -> dict:
    """Fresh backup and actual restored-application reads, not helper claims.

    Separate filesystem identity does not establish power-loss durability or
    off-host custody. Both copies are fully re-read under their enrolled paths.
    """
    started = datetime.now(timezone.utc).isoformat()
    held = observe(**arguments)
    unknowns = []
    def copy(slot, path):
        try:
            before = identity(path)
            with readonly(path) as conn:
                conn.execute('BEGIN')
                verified = offline_application_verify(conn, application_revision=arguments['revision'])
            conn.close()
            after = identity(path)
            if before != after:
                raise ValueError('copy changed during observation')
            return {'state': 'OBSERVED', 'value': {'path': str(path), 'identity': after,
                'verification_sha256': digest(verified), 'integrity': verified['integrity'],
                'application_schema': verified['logical_manifest']['application_schema']}}
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
            unknowns.append({'slot': slot, 'reason': type(exc).__name__})
            return {'state': 'NOT_OBSERVABLE', 'value': None}
    return {'schema': 'labelwatch.sqlite-cleanup-observation/v1',
        'source_owner': 'Labelwatch read-only observer', 'started_at': started,
        'held_source': held, 'backup': copy('backup', backup), 'restore': copy('restore', restore),
        'unknowns': unknowns, 'completed_at': datetime.now(timezone.utc).isoformat(),
        'limitations': ['separate filesystem identity, not independent power-loss or off-host custody',
                       'bounded sequential reads under enrolled stable custody, not atomic global snapshot']}


def observe(*, operation: str, source: Path, original: Path, revision: str,
            expected_verification_sha256: str, writer_identities: dict) -> dict:
    started = datetime.now(timezone.utc).isoformat()
    unknowns = []
    def observation(slot, acquire):
        try:
            return {'state': 'OBSERVED', 'value': acquire()}
        except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
            unknowns.append({'slot': slot, 'reason': type(exc).__name__})
            return {'state': 'NOT_OBSERVABLE', 'value': None}
    def database():
        before = identity(source)
        with readonly(source) as conn:
            conn.execute('BEGIN')
            verified = offline_application_verify(conn, application_revision=revision)
            # Progress remains a separately named complete meta projection, not
            # an invented universal generation number. Its digest can change for
            # reasons other than useful ingestion, so no progress claim follows.
            meta = [list(row) for row in conn.execute('SELECT key,value FROM meta ORDER BY key')]
        conn.close()
        after = identity(source)
        if before != after:
            raise ValueError('database changed during bounded observation')
        return {'identity': after, 'verification_sha256': digest(verified),
                'matches_declared_cut': digest(verified) == expected_verification_sha256,
                'application_schema': verified['logical_manifest']['application_schema'],
                'integrity': verified['integrity'], 'progress_meta_sha256': digest(meta)}
    def original_presence():
        try:
            original.lstat()
        except FileNotFoundError:
            return {'present': False, 'scope': 'enrolled original pathname, not whole filesystem'}
        return {'present': True, 'identity': identity(original)}
    def filesystem():
        value = os.statvfs(source.parent)
        return {'device': source.parent.stat().st_dev,
                'free_bytes': value.f_bavail * value.f_frsize}
    def writers():
        if set(writer_identities) != {'main', 'discovery'}:
            raise ValueError('exact enrolled writer roles required')
        result = {}
        for role, expected in writer_identities.items():
            if (set(expected) != {'pid', 'start_ticks'} or type(expected['pid']) is not int
                    or expected['pid'] <= 0 or type(expected['start_ticks']) is not int):
                raise ValueError('invalid writer identity')
            try:
                ticks = process_start_ticks(expected['pid'])
            except FileNotFoundError:
                result[role] = {'active': False, **expected}
                continue
            result[role] = {'active': ticks == expected['start_ticks'], **expected}
        return result
    return {'schema': 'labelwatch.sqlite-relief-observation/v1', 'source_owner': 'Labelwatch read-only observer',
            'operation': operation, 'source': str(source), 'original': str(original),
            'application_revision': revision, 'started_at': started,
            'database': observation('database', database),
            'filesystem': observation('filesystem', filesystem),
            'original_presence': observation('original_presence', original_presence),
            'write_hold': observation('write_hold', lambda: active_hold(str(source)) is not None),
            'writers': observation('writers', writers), 'unknowns': unknowns,
            'completed_at': datetime.now(timezone.utc).isoformat(),
            'limitations': ['local trusted-directory/enrolled-writer custody; not an atomic global snapshot',
                            'database content at read transaction; writers may advance afterward',
                            'no authorization, effect occurrence, or aggregate health claim']}
