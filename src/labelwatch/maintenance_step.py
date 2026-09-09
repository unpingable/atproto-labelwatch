"""Exact, externally authorized Labelwatch maintenance step executor.

The digest-bound input and output records describe local effects; they grant no
authority. A separately enrolled immutable systemd unit is authorized by AG-ng
and executed through Docket. This module never dispatches another step/retries.
"""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path

from .maintenance_artifacts import (
    identity, _sync, copy_restore_verify, compact_verified,
    verify_closed, space_prerequisites,
)
from .maintenance_hold import active_hold, process_start_ticks, paths as hold_paths
from .maintenance_manifest import VerificationRefused


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def read_record(path: Path) -> tuple[dict, bytes]:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise VerificationRefused('duplicate input field')
            result[key] = value
        return result
    info = identity(path)
    if info['bytes'] > 2 * 1024 * 1024:
        raise VerificationRefused('step record exceeds bound')
    raw = path.read_bytes()
    value = json.loads(raw, object_pairs_hook=unique,
                       parse_constant=lambda _: (_ for _ in ()).throw(VerificationRefused('nonfinite JSON')))
    if not isinstance(value, dict) or raw != canonical(value) + b'\n':
        raise VerificationRefused('step record is not canonical closed JSON')
    if identity(path) != info:
        raise VerificationRefused('record changed while reading')
    return value, raw


def retain(path: Path, value: dict) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(canonical(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    _sync(path)


def rename_no_replace(source: Path, target: Path) -> None:
    # Atomic no-overwrite rename, not a pre-check followed by clobbering rename.
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as exc:
        raise VerificationRefused('Linux renameat2 required for bounded swap') from exc
    if renameat2(-100, os.fsencode(source), -100, os.fsencode(target), 1) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(target))
    directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _physical(path: str) -> Path:
    value = Path(path)
    if not value.is_absolute() or value.parent.resolve(strict=True) != value.parent:
        raise VerificationRefused('exact physical artifact parent required')
    return value


def reconcile(step: dict) -> dict:
    """Read-only recovery disposition, never permission to replay an effect."""
    observed = {}
    for name in ('source', 'original', 'staging', 'backup', 'restore'):
        path = _physical(step[name])
        if not path.exists() and not path.is_symlink():
            observed[name] = {'presence': 'ABSENT'}
            continue
        try:
            observed[name] = {'presence': 'PRESENT', 'identity': identity(path)}
        except (OSError, VerificationRefused):
            observed[name] = {'presence': 'NOT_OBSERVABLE'}
    try:
        held = active_hold(step['source']) is not None
    except (OSError, ValueError, RuntimeError):
        held = None
    original_exact = observed['original'].get('identity') == step['source_identity']
    source_exact = observed['source'].get('identity') == step['source_identity']
    if held is not True:
        disposition = 'INDETERMINATE_WRITE_RESUMPTION_KEEP_STOPPED'
    elif source_exact:
        disposition = 'SOURCE_RETAINED_REOPEN_STAGING_CUSTODY'
    elif original_exact and observed['source']['presence'] == 'ABSENT':
        disposition = 'SWAP_INCOMPLETE_KEEP_STOPPED'
    elif original_exact and observed['source']['presence'] == 'PRESENT':
        disposition = 'REPLACEMENT_REQUIRES_VERIFICATION_KEEP_HELD'
    else:
        disposition = 'IDENTITY_UNRESOLVED_KEEP_STOPPED'
    return {'schema': 'labelwatch.relief-recovery-observation/v1',
            'operation': step['operation'], 'disposition': disposition,
            'write_hold': held, 'artifacts': observed,
            'next': 'independent evidence qualification and exact new authorized step; no automatic replay'}


def execute(step_path: Path, expected_sha256: str) -> dict:
    step, raw = read_record(step_path)
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise VerificationRefused('step differs from externally enrolled digest')
    fields = {'schema', 'operation', 'action', 'source', 'backup', 'restore', 'staging',
              'original', 'journal', 'revision', 'expected', 'source_identity',
              'operating_margin', 'predecessor', 'predecessor_sha256', 'ready_records'}
    if set(step) != fields or step['schema'] != 'labelwatch.sqlite-relief-step/v1':
        raise VerificationRefused('closed step schema differs')
    binding = digest({key: value for key, value in step.items()
                      if key not in {'action', 'predecessor', 'predecessor_sha256', 'ready_records'}})
    if step['action'] not in {'stage', 'replace', 'verify-installed', 'verify-service', 'cleanup', 'release'}:
        raise VerificationRefused('action is not implemented/admitted')
    paths = {name: _physical(step[name]) for name in
             ('source', 'backup', 'restore', 'staging', 'original', 'journal')}
    if len(set(paths.values())) != len(paths):
        raise VerificationRefused('step artifact paths overlap')
    if paths['source'].parent != paths['staging'].parent or paths['source'].parent != paths['original'].parent:
        raise VerificationRefused('swap artifacts must share exact target directory')
    if paths['restore'].parent != paths['backup'].parent:
        raise VerificationRefused('restore must use the separately budgeted backup directory')
    hold = active_hold(str(paths['source']))
    if hold is None or hold['operation'] != step['operation'] or hold['manifest_sha256'] != digest(step['expected']):
        raise VerificationRefused('exact operation quiescence/startup hold absent')
    journal = paths['journal']
    if not journal.is_dir() or journal.resolve() != journal:
        raise VerificationRefused('enrolled journal directory required')
    lock = os.open(journal / 'lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise VerificationRefused('another enrolled step holds operation custody') from exc
        started = journal / (expected_sha256 + '.started.json')
        terminal = journal / (expected_sha256 + '.completed.json')
        if terminal.exists():
            result, _ = read_record(terminal)
            if result.get('step_sha256') != expected_sha256:
                raise VerificationRefused('terminal identity differs')
            return result
        if started.exists():
            raise VerificationRefused('OUTCOME_UNKNOWN: reopen artifacts; no automatic retry')
        predecessor = None
        if step['action'] != 'stage':
            predecessor, predecessor_raw = read_record(_physical(step['predecessor']))
            if (hashlib.sha256(predecessor_raw).hexdigest() != step['predecessor_sha256']
                    or predecessor.get('operation') != step['operation']
                    or predecessor.get('binding_sha256') != binding):
                raise VerificationRefused('exact predecessor evidence differs')
        elif step['predecessor'] is not None or step['predecessor_sha256'] is not None:
            raise VerificationRefused('stage must have no procedure predecessor')
        retain(started, {'schema': 'labelwatch.relief-started/v1',
                         'operation': step['operation'], 'step_sha256': expected_sha256,
                         'action': step['action'], 'disposition': 'OUTCOME_UNKNOWN_UNTIL_RECONCILED'})
        source = paths['source']
        if step['action'] == 'stage':
            if identity(source) != step['source_identity']:
                raise VerificationRefused('source identity changed before quiescence cut')
            for suffix in ('-wal', '-journal', '-shm'):
                sidecar = Path(str(source) + suffix)
                if sidecar.exists() or sidecar.is_symlink():
                    raise VerificationRefused('source sidecar remains after declared quiescence')
            space = space_prerequisites(source, paths['backup'].parent,
                                        operating_margin=step['operating_margin'])
            backup = copy_restore_verify(source, paths['backup'], paths['restore'],
                revision=step['revision'], expected=step['expected'])
            if not backup['separate_filesystem']:
                raise VerificationRefused('backup custody must remain separate filesystem')
            compact = compact_verified(source, paths['staging'], revision=step['revision'], expected=step['expected'])
            detail = {'space': space, 'backup': backup, 'compact': compact}
            disposition = 'STAGED_NOT_INSTALLED'
        elif step['action'] == 'replace':
            if predecessor.get('disposition') != 'STAGED_NOT_INSTALLED':
                raise VerificationRefused('replacement requires exact staging completion')
            if identity(source) != step['source_identity']:
                raise VerificationRefused('source changed before swap')
            staged = verify_closed(paths['staging'], revision=step['revision'], expected=step['expected'])
            if staged != predecessor['detail']['compact']['staging']:
                raise VerificationRefused('staging identity differs from verified predecessor')
            rename_no_replace(source, paths['original'])
            rename_no_replace(paths['staging'], source)
            detail = {'replacement': verify_closed(source, revision=step['revision'], expected=step['expected']),
                      'original': identity(paths['original'])}
            disposition = 'REPLACEMENT_ACCEPTED'
        elif step['action'] == 'verify-installed':
            if predecessor.get('disposition') != 'REPLACEMENT_ACCEPTED':
                raise VerificationRefused('installed verification requires exact replacement')
            original = identity(paths['original'])
            if original != step['source_identity']:
                raise VerificationRefused('original identity changed after swap')
            detail = {'replacement': verify_closed(source, revision=step['revision'], expected=step['expected']),
                      'original': original}
            disposition = 'REPLACEMENT_ACCEPTED'
        else:
            required = {'verify-service': 'REPLACEMENT_ACCEPTED',
                        'cleanup': 'SERVICE_ACCEPTED_PRE_INGEST',
                        'release': 'CLEANUP_COMPLETED_NOT_RELIEF'}[step['action']]
            if predecessor.get('disposition') != required:
                raise VerificationRefused('step lacks exact required predecessor disposition')
            replacement = verify_closed(source, revision=step['revision'], expected=step['expected'])
            ready_records = step['ready_records']
            if not isinstance(ready_records, dict) or set(ready_records) != {'main', 'discovery'}:
                raise VerificationRefused('both enrolled held writer records required')
            verified_ready = {}
            for role, path in ready_records.items():
                record, _ = read_record(_physical(path))
                if (set(record) != {'schema', 'operation', 'role', 'pid', 'start_ticks',
                                    'hold_sha256', 'verification_sha256'}
                        or record['schema'] != 'labelwatch.held-writer-ready/v1'
                        or record['operation'] != step['operation'] or record['role'] != role
                        or type(record['pid']) is not int or record['pid'] <= 0
                        or record['hold_sha256'] != digest(hold)
                        or record['verification_sha256'] != digest(step['expected'])
                        or process_start_ticks(record['pid']) != record['start_ticks']):
                    raise VerificationRefused('held service identity/readiness differs')
                verified_ready[role] = record
            if step['action'] == 'verify-service':
                if identity(paths['original']) != step['source_identity']:
                    raise VerificationRefused('original no longer exact before service acceptance')
                detail = {'replacement': replacement, 'held_writers': verified_ready}
                disposition = 'SERVICE_ACCEPTED_PRE_INGEST'
            elif step['action'] == 'cleanup':
                # This exact cleanup action must itself be authorized externally.
                # Reopen backup AND restore now; service acceptance alone is not
                # sufficient custody for deleting the original.
                backup = verify_closed(paths['backup'], revision=step['revision'], expected=step['expected'])
                restored = verify_closed(paths['restore'], revision=step['revision'], expected=step['expected'])
                if backup['identity']['device'] == identity(source)['device']:
                    raise VerificationRefused('verified backup no longer on separate filesystem')
                if identity(paths['original']) != step['source_identity']:
                    raise VerificationRefused('cleanup target is not exact retained original')
                retain(journal / (expected_sha256 + '.cleanup-authorized.json'),
                       {'step_sha256': expected_sha256, 'stage': 'CLEANUP_AUTHORIZED',
                        'authority': 'EXTERNAL_ENROLLED_UNIT_INVOCATION', 'original': step['source_identity'],
                        'backup': backup, 'restored': restored})
                paths['original'].unlink()
                directory = os.open(source.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                detail = {'original': 'ABSENT', 'backup': backup, 'restored': restored}
                disposition = 'CLEANUP_COMPLETED_NOT_RELIEF'
            else:
                if paths['original'].exists() or paths['original'].is_symlink():
                    raise VerificationRefused('original still present before relief verification')
                filesystem = os.statvfs(source.parent)
                free = filesystem.f_bavail * filesystem.f_frsize
                if free < step['operating_margin']:
                    raise VerificationRefused('resource margin still insufficient; ingestion remains held')
                release = {'schema': 'labelwatch.maintenance-release/v1', 'operation': step['operation'],
                           'hold_sha256': digest(hold), 'release_receipt': expected_sha256}
                retain(hold_paths(str(source))[1], release)
                detail = {'replacement_before_release': replacement,
                          'fresh_pre_release_free_bytes': free, 'margin': step['operating_margin'],
                          'rollback': 'INDETERMINATE_UNTIL_POST_RELEASE_GENERATION_OBSERVED'}
                disposition = 'INGRESS_RELEASED_POSTCONDITION_PENDING'
        result = {'schema': 'labelwatch.relief-step-result/v1', 'operation': step['operation'],
                  'binding_sha256': binding,
                  'step_sha256': expected_sha256, 'action': step['action'],
                  'disposition': disposition, 'detail': detail,
                  'authority': 'NOT_GRANTED_BY_THIS_RECORD', 'resource_relief': 'NOT_ESTABLISHED'}
        retain(terminal, result)
        return result
    finally:
        os.close(lock)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--step', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    args = parser.parse_args()
    try:
        print(canonical(execute(args.step, args.expected_sha256)).decode())
    except Exception as exc:
        print(canonical({'disposition': 'REFUSED_OR_OUTCOME_UNKNOWN',
                         'reason': str(exc), 'next': 'reopen exact journal; do not retry automatically'}).decode())
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
