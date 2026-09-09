#!/usr/bin/env python3
"""Bounded app-owned fixture observation and native qualification; no effects.

Exit zero means capture completed, not that the factual condition was established.
The named native receipt owns its disposition. Incomplete capture never fabricates
one. Run under the enclosing campaign's durable execution/custody mechanism.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import stat
import subprocess

from labelwatch.maintenance_hold import paths
from labelwatch.maintenance_observation import observe, observe_cleanup
from labelwatch.maintenance_step import canonical, digest, read_record, retain


def _acquire(arguments, backup, restore, budget, cleanup, destination):
    try:
        value = observe_cleanup(backup=backup, restore=restore, acquisition_budget_seconds=budget,
                                **arguments) if cleanup else observe(**arguments)
        retain(destination, value)
    except Exception as error:
        retain(destination.with_suffix('.error.json'), {'status': 'NOT_OBSERVABLE', 'reason': type(error).__name__})
        raise SystemExit(1)


def _enrollment(fixture, staged):
    base, _ = read_record(fixture / 'fixture-base.json')
    result, _ = read_record(staged)
    step_sha = result.get('step_sha256', '')
    if (staged.parent != Path(base['journal']) or staged.name != step_sha + '.completed.json'
            or result.get('action') != 'stage' or result.get('disposition') != 'STAGED_NOT_INSTALLED'
            or result.get('operation') != base['operation']
            or result.get('binding_sha256') != digest({key: value for key, value in base.items()
                if key not in {'action', 'predecessor', 'predecessor_sha256', 'ready_records'}})):
        raise ValueError('exact fixture stage custody differs')
    sealed, raw = read_record(fixture / 'enrollment-candidates' / step_sha / 'step.json')
    if hashlib.sha256(raw).hexdigest() != step_sha or sealed != base:
        raise ValueError('stage result is not bound to exact enrolled input')
    ready = paths(base['source'])[0].parent / 'ready'
    identities = {}
    hold = {'schema': 'labelwatch.maintenance-hold/v1', 'operation': base['operation'],
        'database': base['source'], 'manifest_sha256': digest(base['expected']), 'application_revision': base['revision']}
    for role in ('main', 'discovery'):
        candidates = list(ready.glob(role + '-*.ready.json'))
        if len(candidates) != 1:
            raise ValueError('exact single readiness per enrolled role required')
        record, _ = read_record(candidates[0])
        if (record.get('role') != role or record.get('operation') != base['operation']
                or record.get('hold_sha256') != digest(hold)
                or record.get('verification_sha256') != digest(base['expected'])):
            raise ValueError('readiness does not bind enrolled held cut')
        identities[role] = {key: record[key] for key in ('pid', 'start_ticks')}
    return base, result, identities, digest(hold)


def _native(binary, expected, operation, source, request):
    # Linux descriptor capture prevents pathname replacement selecting another
    # binary after hashing. Runtime loader/libs remain the enrolled host premise.
    captured = os.open(binary, os.O_PATH | os.O_NOFOLLOW)
    descriptor = None
    try:
        info = os.fstat(captured)
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 256 * 1024 * 1024:
            raise ValueError('bounded regular native executable required')
        descriptor = os.open('/proc/self/fd/' + str(captured), os.O_RDONLY)
        hasher = hashlib.sha256()
        while block := os.read(descriptor, 1024 * 1024):
            hasher.update(block)
        after = os.fstat(descriptor)
        if hasher.hexdigest() != expected or (info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError('native executable identity changed or pin differs')
        result = subprocess.run(['/proc/self/fd/' + str(descriptor), operation, '--source', str(source),
            '--request', str(request)], pass_fds=(descriptor,), capture_output=True, timeout=15)
        if result.returncode != 0 or len(result.stdout) > 4 * 1024 * 1024:
            raise ValueError('native qualification refused intake or failed')
        value = json.loads(result.stdout)
        if value.get('disposition') not in {'ESTABLISHED', 'REFUTED', 'NOT_OBSERVABLE'}:
            raise ValueError('native result lacks closed disposition')
        return value
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(captured)


def capture(args):
    args.output.mkdir(mode=0o700)  # no overwrite, adopt, retry or resume
    args.output_created = True
    base, staged, identities, hold_sha = _enrollment(args.fixture, args.staged)
    arguments = dict(operation=base['operation'], source=Path(base['source']), original=Path(base['original']),
        revision=base['revision'], expected_verification_sha256=digest(base['expected']), writer_identities=identities)
    source_path = args.output / (args.phase + '-source.json')
    process = multiprocessing.get_context('spawn').Process(target=_acquire, args=(arguments,
        Path(base['backup']), Path(base['restore']), args.acquisition_budget_seconds, args.phase == 'cleanup', source_path))
    process.start()
    retain(args.output / 'acquisition-started.json', {'schema': 'labelwatch.m3-capture-started/v1',
        'phase': args.phase, 'pid': process.pid, 'budget_seconds': args.acquisition_budget_seconds,
        'operation': base['operation'], 'authority': 'NONE'})
    process.join(args.acquisition_budget_seconds)
    if process.is_alive():
        process.terminate()
        process.join(5)
        retain(args.output / 'capture-result.json', {'status': 'NOT_OBSERVABLE',
            'reason': 'acquisition_timeout', 'child_pid': process.pid, 'child_still_present': process.is_alive(),
            'native_receipt': None, 'authority': 'NONE'})
        return 2
    if process.exitcode != 0:
        raise ValueError('independent acquisition incomplete')
    observed, _ = read_record(source_path)
    replacement = staged['detail']['compact']['staging']['identity']
    request = {'schema': 'nq.labelwatch-relief-request/v1', 'operation': base['operation'],
        'source': base['source'], 'original': base['original'], 'application_revision': base['revision'],
        'expected_cut_sha256': digest(base['expected']), 'original_identity': base['source_identity'],
        'replacement_device': replacement['device'], 'replacement_inode': replacement['inode'],
        'writer_identities': identities, 'phase': 'post_release' if args.phase == 'post' else 'pre_ingest',
        'required_free_bytes': base['operating_margin'] if args.required_free_bytes is None else args.required_free_bytes,
        'maximum_age_seconds': 30, 'evaluated_at': datetime.now(timezone.utc).isoformat(),
        'pre_ingest_qualification': None}
    if args.phase == 'post':
        if args.pre_ingest_receipt is None:
            raise ValueError('post phase requires exact pre-ingest qualification')
        request['pre_ingest_qualification'], _ = read_record(args.pre_ingest_receipt)
    elif args.pre_ingest_receipt is not None:
        raise ValueError('predecessor only belongs to post phase')
    if args.phase == 'cleanup':
        request.update(schema='nq.labelwatch-held-acquisition-request/v1',
            evaluated_at=observed['completed_at'], maximum_age_seconds=args.acquisition_budget_seconds)
        request = {'schema': 'nq.labelwatch-cleanup-request/v2', 'held_request': request,
            'backup': base['backup'], 'restore': base['restore'],
            'backup_identity': staged['detail']['backup']['backup']['identity'],
            'restore_identity': staged['detail']['backup']['restored']['identity'],
            'expected_hold_sha256': hold_sha, 'acquisition_budget_seconds': args.acquisition_budget_seconds,
            'maximum_currentness_age_seconds': 30, 'evaluated_at': datetime.now(timezone.utc).isoformat()}
    request_path = args.output / (args.phase + '-request.json')
    retain(request_path, request)
    receipt = _native(args.nq, args.nq_sha256, 'labelwatch-cleanup' if args.phase == 'cleanup' else 'labelwatch-relief', source_path, request_path)
    receipt_path = args.output / (args.phase + '-receipt.json')
    retain(receipt_path, receipt)
    retain(args.output / 'capture-result.json', {'status': 'CAPTURE_COMPLETED', 'factual_disposition': receipt['disposition'],
        'receipt': str(receipt_path), 'receipt_sha256': hashlib.sha256(receipt_path.read_bytes()).hexdigest(), 'authority': 'NONE'})
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ('fixture', 'staged', 'output', 'nq'):
        parser.add_argument('--' + field, type=Path, required=True)
    parser.add_argument('--nq-sha256', required=True)
    parser.add_argument('--phase', choices=('cleanup', 'pre', 'post'), required=True)
    parser.add_argument('--acquisition-budget-seconds', type=int, required=True)
    parser.add_argument('--pre-ingest-receipt', type=Path)
    parser.add_argument('--required-free-bytes', type=int)
    args = parser.parse_args()
    args.output_created = False
    if not 1 <= args.acquisition_budget_seconds <= 7200:
        parser.error('finite acquisition budget 1..7200 required')
    try:
        return capture(args)
    except Exception as error:
        if args.output_created and not (args.output / 'capture-result.json').exists():
            retain(args.output / 'capture-result.json', {'status': 'NOT_OBSERVABLE', 'reason': type(error).__name__,
                'native_receipt': None, 'authority': 'NONE'})
        print(canonical({'status': 'CAPTURE_INCOMPLETE', 'reason': type(error).__name__, 'authority': 'NONE'}).decode())
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
