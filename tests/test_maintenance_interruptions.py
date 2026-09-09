"""Finite interruption boundaries; no retry is mistaken for recovery authority."""
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import subprocess
import sys
import time
import fcntl

import pytest

from labelwatch import maintenance_step as step_module
from labelwatch import maintenance_artifacts as artifacts
from labelwatch.maintenance_manifest import VerificationRefused
from test_maintenance_step import enrolled, invoke


def test_concurrent_step_custody_refuses_before_started_record(tmp_path):
    step = enrolled(tmp_path, tmp_path)
    with open(Path(step['journal']) / 'lock', 'wb') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(VerificationRefused, match='another enrolled step'):
            invoke(tmp_path, step, 'concurrent')
    assert not list(Path(step['journal']).glob('*.started.json'))
    assert artifacts.identity(Path(step['source'])) == step['source_identity']


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_insufficient_actual_target_space_refuses_before_backup_or_staging(tmp_path, monkeypatch):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-space-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        actual = artifacts.os.statvfs
        def full_target(path):
            value = actual(path)
            if Path(path) == tmp_path:
                return os.statvfs_result(tuple(value[:4]) + (0,) + tuple(value[5:]))
            return value
        monkeypatch.setattr(artifacts.os, 'statvfs', full_target)
        with pytest.raises(VerificationRefused, match='space'):
            invoke(tmp_path, step, 'full-target')
        assert not Path(step['backup']).exists() and not Path(step['staging']).exists()
        assert artifacts.identity(Path(step['source'])) == step['source_identity']


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_reconciliation_refuses_same_bytes_under_replaced_original_pathname(tmp_path):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-path-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        for action in ('stage', 'replace'):
            step['action'] = action
            _, step_hash, _ = invoke(tmp_path, step, action)
            previous = Path(step['journal']) / (step_hash + '.completed.json')
            step.update(predecessor=str(previous), predecessor_sha256=hashlib.sha256(previous.read_bytes()).hexdigest())
        original = Path(step['original'])
        retained = tmp_path / 'original-retained-not-enrolled.sqlite'
        original.rename(retained)
        original.write_bytes(retained.read_bytes())
        assert artifacts.identity(original)['sha256'] == step['source_identity']['sha256']
        assert artifacts.identity(original)['inode'] != step['source_identity']['inode']
        assert step_module.reconcile(step)['disposition'] == 'IDENTITY_UNRESOLVED_KEEP_STOPPED'
        step['action'] = 'rollback-pre-ingest'
        with pytest.raises(VerificationRefused, match='exact retained original'):
            invoke(tmp_path, step, 'wrong-path-rollback')
        assert artifacts.identity(retained) == step['source_identity']


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
@pytest.mark.parametrize('boundary', ['cleanup_completion', 'release_completion', 'resource_margin'])
def test_cleanup_release_interruption_has_explicit_recovery(tmp_path, monkeypatch, boundary):
    from labelwatch.maintenance_hold import paths, active_hold
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-final-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        def transition(action):
            step['action'] = action
            result, step_hash, _ = invoke(tmp_path, step, action)
            previous = Path(step['journal']) / (step_hash + '.completed.json')
            step.update(predecessor=str(previous), predecessor_sha256=hashlib.sha256(previous.read_bytes()).hexdigest())
            return result
        transition('stage')
        transition('replace')
        ready = paths(step['source'])[0].parent / 'ready'
        ready.mkdir()
        # Real independent processes prove readiness. They deliberately do not
        # resume writes after release, so unknown resumption remains testable.
        code = ('import sys,time; from labelwatch.maintenance_hold import wait_before_writer_start; '
                'wait_before_writer_start(sys.argv[1],sys.argv[2]); time.sleep(20)')
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        children = [subprocess.Popen([sys.executable, '-c', code, step['source'], role], env=environment)
                    for role in ('main', 'discovery')]
        try:
            deadline = time.monotonic() + 10
            while len(list(ready.glob('*.ready.json'))) != 2:
                assert time.monotonic() < deadline and all(child.poll() is None for child in children)
                time.sleep(.02)
            step['ready_records'] = {role: str(next(ready.glob(role + '-*.ready.json')))
                                     for role in ('main', 'discovery')}
            transition('verify-service')
            if boundary != 'cleanup_completion':
                transition('cleanup')
            action = 'cleanup' if boundary == 'cleanup_completion' else 'release'
            step['action'] = action
            if boundary == 'resource_margin':
                actual_statvfs = step_module.os.statvfs
                def no_margin(path):
                    actual = actual_statvfs(path)
                    return os.statvfs_result(tuple(actual[:4]) + (0,) + tuple(actual[5:]))
                monkeypatch.setattr(step_module.os, 'statvfs', no_margin)
            else:
                original_retain = step_module.retain
                def interrupted(path, value):
                    if path.name.endswith('.completed.json'):
                        raise VerificationRefused('fixture controller disappeared before terminal')
                    original_retain(path, value)
                monkeypatch.setattr(step_module, 'retain', interrupted)
            with pytest.raises(VerificationRefused):
                invoke(tmp_path, step, action)
            assert not Path(step['original']).exists()
            assert Path(step['backup']).exists() and Path(step['restore']).exists()
            if boundary == 'release_completion':
                assert active_hold(step['source']) is None
                assert step_module.reconcile(step)['disposition'] == 'INDETERMINATE_WRITE_RESUMPTION_KEEP_STOPPED'
                # A release record alone cannot license pre-ingest rollback.
                step['action'] = 'rollback-pre-ingest'
                with pytest.raises(VerificationRefused, match='hold absent'):
                    invoke(tmp_path, step, 'rollback-after-release')
            else:
                assert active_hold(step['source']) is not None
                path = tmp_path / (action + '.json')
                with pytest.raises(VerificationRefused, match='OUTCOME_UNKNOWN'):
                    step_module.execute(path, hashlib.sha256(path.read_bytes()).hexdigest())
                if boundary == 'cleanup_completion':
                    monkeypatch.setattr(step_module, 'retain', original_retain)
                    started = Path(step['journal']) / (hashlib.sha256(path.read_bytes()).hexdigest() + '.started.json')
                    step.update(predecessor=str(started), predecessor_sha256=hashlib.sha256(started.read_bytes()).hexdigest())
                    assert transition('reconcile-cleanup')['disposition'] == 'CLEANUP_COMPLETED_NOT_RELIEF'
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
                child.wait(timeout=5)


def test_changed_source_before_cut_retains_original_and_never_stages(tmp_path):
    step = enrolled(tmp_path, tmp_path)
    with sqlite3.connect(step['source']) as conn:
        conn.execute("UPDATE maintenance_types SET value='changed' WHERE key='text'")
    conn.close()
    with pytest.raises(VerificationRefused, match='source identity changed'):
        invoke(tmp_path, step, 'source-changed')
    assert not Path(step['backup']).exists()
    assert not Path(step['staging']).exists()


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
@pytest.mark.parametrize('boundary', ['restore_application', 'compact', 'stage_completion'])
def test_staging_interruption_retains_source_and_never_installs(tmp_path, monkeypatch, boundary):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-interrupt-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        if boundary == 'restore_application':
            original = artifacts.verify_closed
            def verify(path, **arguments):
                if path == Path(step['restore']):
                    raise VerificationRefused('fixture restore application verification failed')
                return original(path, **arguments)
            monkeypatch.setattr(artifacts, 'verify_closed', verify)
        elif boundary == 'compact':
            def compact(*args, **kwargs):
                raise VerificationRefused('fixture compact staging failed')
            monkeypatch.setattr(step_module, 'compact_verified', compact)
        else:
            original = step_module.retain
            def retain(path, value):
                if path.name.endswith('.completed.json'):
                    raise VerificationRefused('fixture controller disappeared before completion')
                return original(path, value)
            monkeypatch.setattr(step_module, 'retain', retain)
        with pytest.raises(VerificationRefused, match='fixture'):
            invoke(tmp_path, step, boundary)
        assert artifacts.identity(Path(step['source'])) == step['source_identity']
        assert not Path(step['original']).exists()
        assert Path(step['backup']).exists()
        assert step_module.reconcile(step)['disposition'] == 'SOURCE_RETAINED_REOPEN_STAGING_CUSTODY'
        path = tmp_path / (boundary + '.json')
        with pytest.raises(VerificationRefused, match='OUTCOME_UNKNOWN'):
            step_module.execute(path, hashlib.sha256(path.read_bytes()).hexdigest())


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
@pytest.mark.parametrize('boundary', ['missing_service', 'post_start_contents'])
def test_service_acceptance_failure_keeps_original_and_ingress_hold(tmp_path, boundary):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-service-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        for action in ('stage', 'replace'):
            step['action'] = action
            _, step_hash, _ = invoke(tmp_path, step, action)
            previous = Path(step['journal']) / (step_hash + '.completed.json')
            step.update(predecessor=str(previous), predecessor_sha256=hashlib.sha256(previous.read_bytes()).hexdigest())
        step['action'] = 'verify-service'
        if boundary == 'post_start_contents':
            with sqlite3.connect(step['source']) as conn:
                conn.execute("UPDATE maintenance_types SET value='changed' WHERE key='text'")
            conn.close()
        with pytest.raises(VerificationRefused):
            invoke(tmp_path, step, boundary)
        assert artifacts.identity(Path(step['original'])) == step['source_identity']
        assert Path(step['backup']).exists()
        from labelwatch.maintenance_hold import active_hold
        assert active_hold(step['source']) is not None


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_one_process_cannot_be_rebound_as_both_held_writers(tmp_path):
    from labelwatch.maintenance_hold import active_hold, process_start_ticks
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-roles-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        for action in ('stage', 'replace'):
            step['action'] = action
            _, step_hash, _ = invoke(tmp_path, step, action)
            previous = Path(step['journal']) / (step_hash + '.completed.json')
            step.update(predecessor=str(previous), predecessor_sha256=hashlib.sha256(previous.read_bytes()).hexdigest())
        hold = active_hold(step['source'])
        for role in ('main', 'discovery'):
            path = tmp_path / (role + '-rebound.json')
            step_module.retain(path, {'schema': 'labelwatch.held-writer-ready/v1',
                'operation': step['operation'], 'role': role, 'pid': os.getpid(),
                'start_ticks': process_start_ticks(os.getpid()), 'hold_sha256': step_module.digest(hold),
                'verification_sha256': step_module.digest(step['expected'])})
            step['ready_records'][role] = str(path)
        step['action'] = 'verify-service'
        with pytest.raises(VerificationRefused, match='distinct enrolled writer'):
            invoke(tmp_path, step, 'rebound-service')
        assert Path(step['original']).exists()
