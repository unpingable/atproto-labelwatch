"""Finite held-fixture entry diagnosis, not permission or a live DB probe.

Requires independently enrolled all-writer quiescence and protected physical
paths. SQLite immutable reads are only permitted after observed sidecar absence.
Never use this fixture acquisition against a live production database.
"""
from datetime import datetime, timezone
import os
from pathlib import Path
import socket
import sqlite3

from .maintenance_artifacts import identity
from .maintenance_manifest import VerificationRefused


def diagnose(source: Path, *, minimum_freelist_pages: int, pressure_floor_bytes: int) -> dict:
    if (type(minimum_freelist_pages) is not int or minimum_freelist_pages <= 0
            or type(pressure_floor_bytes) is not int or pressure_floor_bytes < 0):
        raise ValueError('explicit positive freelist threshold and nonnegative pressure floor required')
    result = {'schema': 'labelwatch.m3-entry-diagnosis/v1', 'host': socket.gethostname(),
        'source': str(source), 'started_at': datetime.now(timezone.utc).isoformat(),
        'policy': {'owner': 'EXPLICIT_FIXTURE_CONFIGURATION',
            'minimum_freelist_pages': minimum_freelist_pages,
            'pressure_floor_bytes': pressure_floor_bytes,
            'admission_rule': 'freelist_bloat; pressure is separately reported, not sufficient alone'},
        'premises': ['all writers quiescent', 'protected physical source and parent custody'],
        'facts': {}, 'unknowns': [], 'authority': 'NONE', 'production': 'NOT_QUALIFIED'}
    facts = result['facts']
    for role, path in [('main', source), ('wal', Path(str(source) + '-wal')), ('shm', Path(str(source) + '-shm'))]:
        try:
            value = identity(path)
            facts[role] = {'state': 'OBSERVED_PRESENT', 'owner': 'OS_FILE_ACQUISITION', 'path': str(path), 'identity': value}
        except FileNotFoundError:
            facts[role] = {'state': 'OBSERVED_ABSENT', 'owner': 'OS_FILE_ACQUISITION', 'path': str(path)}
        except (OSError, ValueError, RuntimeError) as error:
            facts[role] = {'state': 'NOT_OBSERVABLE', 'owner': 'OS_FILE_ACQUISITION', 'path': str(path)}
            result['unknowns'].append({'slot': role, 'reason': type(error).__name__})
    try:
        if not source.is_absolute() or source.parent.resolve(strict=True) != source.parent:
            raise VerificationRefused('physical absolute source required')
        fs = os.statvfs(source.parent)
        facts['filesystem'] = {'state': 'OBSERVED', 'owner': 'OS_STATVFS',
            'device': source.parent.stat().st_dev, 'fragment_bytes': fs.f_frsize,
            'available_bytes': fs.f_bavail * fs.f_frsize, 'free_bytes': fs.f_bfree * fs.f_frsize,
            'capacity_bytes': fs.f_blocks * fs.f_frsize}
        if facts['main']['state'] != 'OBSERVED_PRESENT' or any(facts[key]['state'] != 'OBSERVED_ABSENT' for key in ('wal', 'shm')):
            raise VerificationRefused('quiescent sidecar-free main required for immutable diagnostic read')
        connection = sqlite3.connect(source.as_uri() + '?mode=ro&immutable=1', uri=True, timeout=1)
        try:
            values = {name: connection.execute('PRAGMA ' + name).fetchone()[0]
                for name in ('page_count', 'page_size', 'freelist_count', 'user_version')}
        finally:
            connection.close()
        if identity(source) != facts['main']['identity'] or any(Path(str(source) + suffix).exists() for suffix in ('-wal', '-shm')):
            raise VerificationRefused('source changed across diagnostic acquisition')
        if not (0 <= values['freelist_count'] <= values['page_count'] and values['page_size'] > 0):
            raise VerificationRefused('inconsistent SQLite page statistics')
        facts['sqlite'] = {'state': 'OBSERVED', 'owner': 'SQLITE_READONLY_PRAGMA', **values,
            'freelist_bytes': values['freelist_count'] * values['page_size'],
            'claim_limit': 'allocated free pages; not promised net filesystem relief'}
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        result['unknowns'].append({'slot': 'diagnostic_cut', 'reason': type(error).__name__})
    result['freelist_bloat'] = ('NOT_OBSERVABLE' if 'sqlite' not in facts else
        'ESTABLISHED' if facts['sqlite']['freelist_count'] >= minimum_freelist_pages else 'NOT_NEEDED')
    result['filesystem_pressure'] = ('NOT_OBSERVABLE' if 'filesystem' not in facts else
        'ESTABLISHED' if facts['filesystem']['available_bytes'] < pressure_floor_bytes else 'NOT_NEEDED')
    result['entry_disposition'] = ('NOT_OBSERVABLE' if result['unknowns'] else
        'NEED_ESTABLISHED' if result['freelist_bloat'] == 'ESTABLISHED' else 'NOT_NEEDED')
    result['completed_at'] = datetime.now(timezone.utc).isoformat()
    return result


def require_entry(record: dict, expected_source_identity: dict) -> None:
    if (record.get('schema') != 'labelwatch.m3-entry-diagnosis/v1'
            or record.get('entry_disposition') != 'NEED_ESTABLISHED'
            or record.get('unknowns') != []
            or record.get('facts', {}).get('main', {}).get('identity') != expected_source_identity):
        raise VerificationRefused('entry need or exact diagnostic source not established')
