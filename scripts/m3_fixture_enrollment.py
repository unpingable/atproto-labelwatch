#!/usr/bin/env python3
"""Prepare disposable M3 fixtures and sealed unit candidates; never start units.

An operator/controller must independently enroll immutable files before AG/Docket
invocation. These local records do not grant authority. Not a production installer.
"""
import argparse
import hashlib
from pathlib import Path
import re

from labelwatch import db
from labelwatch.maintenance_artifacts import identity, _sync
from labelwatch.maintenance_hold import paths
from labelwatch.maintenance_manifest import offline_application_verify, VerificationRefused
from labelwatch.maintenance_step import canonical, digest, read_record, retain
from labelwatch.maintenance_diagnosis import diagnose, require_entry

INTERRUPTION_CUTS = ('before_started', 'after_started', 'before_terminal', 'after_terminal',
    'after_original_rename', 'after_replacement_rename', 'after_backup_sync',
    'after_restore_sync', 'after_staging_sync', 'before_cleanup_unlink',
    'after_cleanup_unlink', 'after_cleanup_authorized', 'after_release_record')


def path_argument(value):
    path = Path(value)
    # Deliberately bounded systemd argument vocabulary; no interpolation/quoting
    # language is accepted by this fixture-only preparer.
    if not path.is_absolute() or not re.fullmatch(r'/[A-Za-z0-9_./-]+', value) or path.resolve() != path:
        raise VerificationRefused('exact physical absolute fixture path required')
    return path


def initialize(target, backup, revision):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise VerificationRefused('exact enrolled Labelwatch revision required')
    if not backup.is_dir() or backup.stat().st_dev == target.parent.stat().st_dev:
        raise VerificationRefused('separate pre-enrolled backup filesystem required')
    target.mkdir(mode=0o700)  # never adopt/reinitialize an existing fixture
    source = target / 'source.sqlite'
    connection = db.connect(str(source))
    try:
        db.init_db(connection)
        connection.execute('CREATE TABLE maintenance_types (key TEXT PRIMARY KEY, value)')
        connection.executemany('INSERT INTO maintenance_types VALUES (?, ?)',
            [('null', None), ('int', 7), ('real', 7.25), ('text', 'a\0b'), ('blob', b'a\0b')])
        connection.execute('CREATE TABLE m3_fixture_freelist (value BLOB)')
        connection.executemany('INSERT INTO m3_fixture_freelist VALUES (?)', [(b'x' * 4096,)] * 128)
        connection.commit()
        connection.execute('DELETE FROM m3_fixture_freelist')
        connection.commit()
        expected = offline_application_verify(connection, application_revision=revision)
    finally:
        connection.close()
    hold, _ = paths(str(source))
    hold.parent.mkdir(mode=0o700)
    (hold.parent / 'ready').mkdir(mode=0o700)
    operation = 'm3-fixture-' + hashlib.sha256(str(target).encode()).hexdigest()[:20]
    retain(hold, {'schema': 'labelwatch.maintenance-hold/v1', 'operation': operation,
        'database': str(source), 'manifest_sha256': digest(expected), 'application_revision': revision})
    (target / 'journal').mkdir(mode=0o700)
    (target / 'enrollment-candidates').mkdir(mode=0o700)
    step = {'schema': 'labelwatch.sqlite-relief-step/v1', 'operation': operation,
        'action': 'stage', 'source': str(source), 'backup': str(backup / 'backup.sqlite'),
        'restore': str(backup / 'restored.sqlite'), 'staging': str(target / 'staging.sqlite'),
        'original': str(target / 'original.sqlite'), 'journal': str(target / 'journal'),
        'revision': revision, 'expected': expected, 'source_identity': identity(source),
        'operating_margin': 4096, 'predecessor': None, 'predecessor_sha256': None, 'ready_records': {}}
    retain(target / 'fixture-base.json', step)
    entry = diagnose(source, minimum_freelist_pages=64, pressure_floor_bytes=4096)
    retain(target / 'entry-diagnosis.json', entry)
    require_entry(entry, step['source_identity'])
    return {'fixture': str(target), 'operation': operation, 'status': 'PREPARED_NOT_AUTHORIZED',
            'production': 'NOT_RUN', 'backup_durability': 'FILESYSTEM_DEPENDENT_NOT_INFERRED'}


def seal(target, action, previous, source_root, python, interruption_cut=None, qualification_restore_substitution=False):
    if interruption_cut is not None and interruption_cut not in INTERRUPTION_CUTS:
        raise VerificationRefused('closed qualification interruption cut required')
    if qualification_restore_substitution and (action != 'stage' or interruption_cut is not None):
        raise VerificationRefused('restore substitution is a separate stage-only fixture')
    base, _ = read_record(target / 'fixture-base.json')
    if action == 'stage':
        entry, _ = read_record(target / 'entry-diagnosis.json')
        require_entry(entry, base['source_identity'])
    base['action'] = action
    if action != 'stage':
        if previous is None:
            raise VerificationRefused('exact retained predecessor required')
        _, raw = read_record(previous)
        base.update(predecessor=str(previous), predecessor_sha256=hashlib.sha256(raw).hexdigest())
    elif previous is not None:
        raise VerificationRefused('stage has no predecessor')
    if action in {'verify-service', 'cleanup', 'release'}:
        ready = paths(base['source'])[0].parent / 'ready'
        for role in ('main', 'discovery'):
            records = list(ready.glob(role + '-*.ready.json'))
            if len(records) != 1:
                raise VerificationRefused('exactly one enrolled readiness per role required')
            base['ready_records'][role] = str(records[0])
    raw = canonical(base) + b'\n'
    step_sha = hashlib.sha256(raw).hexdigest()
    directory = target / 'enrollment-candidates' / step_sha
    directory.mkdir(mode=0o700)
    step_path = directory / 'step.json'
    retain(step_path, base)
    suffix = '' if interruption_cut is None else '-q-' + interruption_cut
    if qualification_restore_substitution:
        suffix = '-q-restore-substitution'
    unit = 'labelwatch-relief-' + step_sha + suffix + '.service'
    command = str(python) + ' -m labelwatch.maintenance_step'
    if interruption_cut is not None:
        command = str(python) + ' ' + str(source_root / 'qualification/m3-admission/interrupted_step.py')
    if qualification_restore_substitution:
        command = str(python) + ' ' + str(source_root / 'qualification/m3-admission/restore_substitution_step.py')
    command += ' --step ' + str(step_path) + ' --expected-sha256 ' + step_sha
    if interruption_cut is not None:
        command += ' --cut ' + interruption_cut
    unit_text = ('[Unit]\nDescription=M3 exact enrolled fixture step\n'
        '[Service]\nType=oneshot\nUser=root\nGroup=root\nRestart=no\n'
        'TimeoutStartSec=25\nEnvironment=PYTHONPATH=' + str(source_root / 'src') + '\n'
        'ExecStart=' + command + '\n[Install]\nWantedBy=multi-user.target\n')
    with (directory / unit).open('x') as output:
        output.write(unit_text)
    _sync(directory / unit)
    result = {'schema': 'labelwatch.m3-enrollment-candidate/v1', 'action': action,
        'step_sha256': step_sha, 'step': str(step_path), 'unit': unit,
        'unit_sha256': hashlib.sha256(unit_text.encode()).hexdigest(),
        'labelwatch_source': str(source_root), 'python': str(python),
        'source_revision': base['revision'], 'status': 'NOT_ENROLLED_NOT_AUTHORIZED',
        'qualification_interruption': interruption_cut,
        'qualification_restore_substitution': qualification_restore_substitution,
        'required_custody': 'root-owned immutable unit, interpreter/imports, input and containing directories',
        'measurement': 'AG binds unit name, not fragment bytes; enrollment is an explicit premise',
        'expected_result': str(Path(base['journal']) / (step_sha + '.completed.json'))}
    retain(directory / 'candidate.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    initialize_parser = commands.add_parser('initialize')
    initialize_parser.add_argument('--target', type=path_argument, required=True)
    initialize_parser.add_argument('--backup', type=path_argument, required=True)
    initialize_parser.add_argument('--revision', required=True)
    seal_parser = commands.add_parser('seal')
    seal_parser.add_argument('--target', type=path_argument, required=True)
    seal_parser.add_argument('--action', choices=['stage', 'replace', 'verify-installed', 'verify-service',
        'cleanup', 'release', 'rollback-pre-ingest', 'reconcile-cleanup'], required=True)
    seal_parser.add_argument('--previous', type=path_argument)
    seal_parser.add_argument('--source-root', type=path_argument, required=True)
    seal_parser.add_argument('--python', type=path_argument, required=True)
    seal_parser.add_argument('--interruption-cut', choices=INTERRUPTION_CUTS)
    seal_parser.add_argument('--qualification-restore-substitution', action='store_true')
    arguments = vars(parser.parse_args())
    command = arguments.pop('command')
    print(canonical(initialize(**arguments) if command == 'initialize' else seal(**arguments)).decode(), end='')


if __name__ == '__main__':
    main()
