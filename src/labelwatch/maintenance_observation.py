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
import stat
import time

from .maintenance_artifacts import identity, readonly
from .maintenance_manifest import offline_application_verify
from .maintenance_hold import active_hold, process_start_ticks
from .maintenance_step import digest


def observe_cleanup(*, backup: Path, restore: Path, acquisition_budget_seconds: int = 30, **arguments) -> dict:
    """Fresh backup and actual restored-application reads, not helper claims.

    Separate filesystem identity does not establish power-loss durability or
    off-host custody. Both copies are fully re-read under their enrolled paths.
    """
    started = datetime.now(timezone.utc).isoformat()
    monotonic_start = time.monotonic()
    if type(acquisition_budget_seconds) is not int or not 1 <= acquisition_budget_seconds <= 7200:
        raise ValueError('explicit finite acquisition budget required')
    unknowns = []
    def custody(slot):
        try:
            hold = active_hold(str(arguments['source']))
            if hold is None:
                raise ValueError('enrolled hold absent')
            files = {}
            for name, path in {'source': arguments['source'], 'original': arguments['original'],
                               'backup': backup, 'restore': restore}.items():
                if path.resolve(strict=True) != path:
                    raise ValueError('exact physical custody path required')
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ValueError('single-link regular custody file required')
                files[name] = {'path': str(path), 'device': info.st_dev, 'inode': info.st_ino,
                    'bytes': info.st_size, 'uid': info.st_uid, 'gid': info.st_gid,
                    'mode': stat.S_IMODE(info.st_mode), 'mtime_ns': str(info.st_mtime_ns), 'ctime_ns': str(info.st_ctime_ns)}
            writers = {role: {'pid': expected['pid'], 'start_ticks': process_start_ticks(expected['pid']),
                             'active': process_start_ticks(expected['pid']) == expected['start_ticks']}
                       for role, expected in arguments['writer_identities'].items()}
            return {'state': 'OBSERVED', 'value': {'hold_sha256': digest(hold), 'files': files, 'writers': writers}}
        except (OSError, ValueError, RuntimeError) as exc:
            unknowns.append({'slot': slot, 'reason': type(exc).__name__})
            return {'state': 'NOT_OBSERVABLE', 'value': None}
    opening = custody('opening_custody')
    held = observe(**arguments)
    def copy(slot, path):
        try:
            if time.monotonic() - monotonic_start >= acquisition_budget_seconds:
                raise TimeoutError('acquisition budget exhausted; no further full copy read')
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
    backup_observation = copy('backup', backup)
    restore_observation = copy('restore', restore)
    completed = datetime.now(timezone.utc).isoformat()
    duration_ms = int((time.monotonic() - monotonic_start) * 1000)
    final_started = datetime.now(timezone.utc).isoformat()
    final = custody('final_custody')
    return {'schema': 'labelwatch.sqlite-cleanup-observation/v2',
        'source_owner': 'Labelwatch read-only observer', 'started_at': started,
        'held_source': held, 'backup': backup_observation, 'restore': restore_observation,
        'opening_custody': opening, 'final_custody': final,
        'acquisition_budget_seconds': acquisition_budget_seconds, 'acquisition_duration_ms': duration_ms,
        'acquisition_exclusions': ['direct writers excluded by external all-writer quiescence enrollment',
                                 'power-loss/off-host backup durability not assessed'],
        'unknowns': unknowns, 'completed_at': completed,
        'currentness_started_at': final_started, 'currentness_completed_at': datetime.now(timezone.utc).isoformat(),
        'limitations': ['separate filesystem identity, not independent power-loss or off-host custody',
                       'bounded sequential reads under enrolled stable custody, not atomic global snapshot',
                       'metadata equality binds retained acquisition only under protected files and all-writer quiescence',
                       'external capture timeout must enforce declared acquisition budget; over-budget observations refuse']}


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
