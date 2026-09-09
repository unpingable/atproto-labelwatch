import hashlib
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import time

import pytest

from labelwatch.maintenance_artifacts import identity
from labelwatch.maintenance_hold import paths
from labelwatch.maintenance_manifest import VerificationRefused
from labelwatch.maintenance_step import execute, retain, digest, reconcile
from test_maintenance_artifacts import source_cut
from test_maintenance_manifest import REV


def enrolled(tmp_path, backup_root):
    source, expected = source_cut(tmp_path)
    hold, _ = paths(str(source))
    hold.parent.mkdir()
    retain(hold, {'schema': 'labelwatch.maintenance-hold/v1', 'operation': 'fixture-relief',
                  'database': str(source), 'manifest_sha256': digest(expected), 'application_revision': REV})
    journal = tmp_path / 'journal'
    journal.mkdir()
    filesystem = os.statvfs(tmp_path)
    baseline = filesystem.f_bavail * filesystem.f_frsize
    # Retained enrollment overhead still exercises target allocation, while the
    # qualification fixture above prevents unrelated shared-volume changes from
    # changing the enrolled availability observation.
    (tmp_path / 'baseline-enrollment-overhead').write_bytes(b'0' * 4096)
    minimum_net_gain = 4096
    return {'schema': 'labelwatch.sqlite-relief-step/v2', 'operation': 'fixture-relief',
        'action': 'stage', 'source': str(source), 'backup': str(backup_root / 'backup.sqlite'),
        'restore': str(backup_root / 'restored.sqlite'), 'staging': str(tmp_path / 'staging.sqlite'),
        'original': str(tmp_path / 'original.sqlite'), 'journal': str(journal), 'revision': REV,
        'expected': expected, 'source_identity': identity(source),
        'temporary_operating_margin': 4096,
        'pre_operation_device': tmp_path.stat().st_dev,
        'pre_operation_available': baseline,
        'pre_operation_observed_at_unix_ns': time.time_ns(),
        'pre_operation_maximum_age_seconds': 3600,
        'minimum_net_gain': minimum_net_gain,
        'required_final_available': baseline + minimum_net_gain,
        'predecessor': None, 'predecessor_sha256': None, 'ready_records': {}}


def invoke(tmp_path, step, name):
    path = tmp_path / (name + '.json')
    retain(path, step)
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    return execute(path, expected), expected, path


def test_fixture_availability_substitution_tracks_only_fixture_owned_allocation(tmp_path):
    before = os.statvfs(tmp_path)
    (tmp_path / 'fixture-owned-allocation').write_bytes(b'x' * 8192)
    after = os.statvfs(tmp_path)
    assert after.f_bavail < before.f_bavail
    assert before.f_frsize == after.f_frsize
    (tmp_path / 'fixture-owned-allocation').unlink()
    assert os.statvfs(tmp_path).f_bavail == before.f_bavail


def test_staging_prerequisite_failure_retains_source_and_refuses_retry(tmp_path):
    step = enrolled(tmp_path, tmp_path)
    path = tmp_path / 'stage.json'
    retain(path, step)
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(VerificationRefused, match='separately identified'):
        execute(path, expected)
    assert identity(Path(step['source'])) == step['source_identity']
    with pytest.raises(VerificationRefused, match='OUTCOME_UNKNOWN'):
        execute(path, expected)


def test_wrong_enrolled_digest_never_starts(tmp_path):
    step = enrolled(tmp_path, tmp_path)
    path = tmp_path / 'stage.json'
    retain(path, step)
    with pytest.raises(VerificationRefused, match='enrolled digest'):
        execute(path, '0' * 64)
    assert not list(Path(step['journal']).glob('*.started.json'))


@pytest.mark.parametrize('condition', ['stale', 'device', 'relation'])
def test_invalid_pre_operation_baseline_refuses_before_started(tmp_path, condition):
    step = enrolled(tmp_path, tmp_path)
    if condition == 'stale':
        step['pre_operation_observed_at_unix_ns'] -= 7200 * 1_000_000_000
        message = 'stale or from the future'
    elif condition == 'device':
        step['pre_operation_device'] += 1
        message = 'filesystem identity differs'
    else:
        step['required_final_available'] += 1
        message = 'baseline plus required net gain'
    with pytest.raises(VerificationRefused, match=message):
        invoke(tmp_path, step, 'invalid-baseline-' + condition)
    assert not list(Path(step['journal']).glob('*.started.json'))


def test_unrelated_relief_before_first_execution_refuses(tmp_path, monkeypatch):
    from labelwatch import maintenance_step as implementation
    step = enrolled(tmp_path, tmp_path)
    actual = implementation.os.statvfs
    def increased(path):
        value = actual(path)
        blocks = step['pre_operation_available'] // value.f_frsize + 1
        return os.statvfs_result(tuple(value[:4]) + (blocks,) + tuple(value[5:]))
    monkeypatch.setattr(implementation.os, 'statvfs', increased)
    with pytest.raises(VerificationRefused, match='no longer bounds current state'):
        invoke(tmp_path, step, 'unrelated-relief-before-stage')
    assert not list(Path(step['journal']).glob('*.started.json'))


def test_unrelated_relief_between_validation_and_staging_refuses(tmp_path, monkeypatch):
    from labelwatch import maintenance_step as implementation
    step = enrolled(tmp_path, tmp_path)
    monkeypatch.setattr(implementation, 'space_prerequisites', lambda *args, **kwargs: {
        'target_free': step['pre_operation_available'] + 4096})
    with pytest.raises(VerificationRefused, match='no longer bounds staging cut'):
        invoke(tmp_path, step, 'unrelated-relief-during-stage')
    assert not Path(step['backup']).exists() and not Path(step['staging']).exists()


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_real_separate_filesystem_stage_swap_and_duplicate(tmp_path):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        staged, staged_hash, stage_path = invoke(tmp_path, step, 'stage')
        assert staged['disposition'] == 'STAGED_NOT_INSTALLED'
        assert execute(stage_path, staged_hash) == staged
        predecessor = Path(step['journal']) / (staged_hash + '.completed.json')
        step.update(action='replace', predecessor=str(predecessor),
                    predecessor_sha256=hashlib.sha256(predecessor.read_bytes()).hexdigest())
        replacement, replacement_hash, replacement_path = invoke(tmp_path, step, 'replace')
        assert replacement['disposition'] == 'REPLACEMENT_ACCEPTED'
        assert identity(Path(step['original'])) == step['source_identity']
        assert execute(replacement_path, replacement_hash) == replacement
        assert replacement['resource_relief'] == 'NOT_ESTABLISHED'
        assert Path(step['backup']).is_file()


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_completed_stage_replays_after_baseline_ages_and_available_space_changes(tmp_path, monkeypatch):
    from labelwatch import maintenance_step as implementation
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-replay-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        staged, staged_hash, stage_path = invoke(tmp_path, step, 'stage')
        monkeypatch.setattr(implementation.time, 'time_ns',
            lambda: step['pre_operation_observed_at_unix_ns'] + 7200 * 1_000_000_000)
        actual_statvfs = implementation.os.statvfs
        def increased_available(path):
            value = actual_statvfs(path)
            return os.statvfs_result(tuple(value[:4]) + (value.f_bavail + 1024,) + tuple(value[5:]))
        monkeypatch.setattr(implementation.os, 'statvfs', increased_available)
        assert execute(stage_path, staged_hash) == staged


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_distinct_service_cleanup_release_and_post_cut_writes(tmp_path):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        def transition(action):
            step['action'] = action
            result, step_hash, _ = invoke(tmp_path, step, action)
            predecessor = Path(step['journal']) / (step_hash + '.completed.json')
            step.update(predecessor=str(predecessor),
                        predecessor_sha256=hashlib.sha256(predecessor.read_bytes()).hexdigest())
            return result
        transition('stage')
        transition('replace')
        ready_directory = paths(step['source'])[0].parent / 'ready'
        ready_directory.mkdir()
        code = '''import sys
from labelwatch.maintenance_hold import wait_before_writer_start
from labelwatch import db
wait_before_writer_start(sys.argv[1], sys.argv[2])
conn = db.connect(sys.argv[1])
db.set_cursor(conn, sys.argv[2], 'post-cut-write')
conn.commit()
conn.close()
'''
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        processes = [subprocess.Popen([sys.executable, '-c', code, step['source'], role],
                                      env=environment) for role in ('main', 'discovery')]
        try:
            deadline = time.monotonic() + 10
            while len(list(ready_directory.glob('*.ready.json'))) != 2:
                assert time.monotonic() < deadline
                assert all(process.poll() is None for process in processes)
                time.sleep(0.05)
            step['ready_records'] = {role: str(next(ready_directory.glob(role + '-*.ready.json')))
                                     for role in ('main', 'discovery')}
            assert transition('verify-service')['disposition'] == 'SERVICE_ACCEPTED_PRE_INGEST'
            assert Path(step['original']).exists()
            assert transition('cleanup')['disposition'] == 'CLEANUP_COMPLETED_NOT_RELIEF'
            assert not Path(step['original']).exists()
            assert all(process.poll() is None for process in processes)
            actual_statvfs = os.statvfs
            def exact_final_floor(path):
                current = actual_statvfs(path)
                blocks = step['required_final_available'] // current.f_frsize
                return os.statvfs_result(tuple(current[:4]) + (blocks,) + tuple(current[5:]))
            step['action'] = 'release'
            from labelwatch import maintenance_step as implementation
            original_statvfs = implementation.os.statvfs
            implementation.os.statvfs = exact_final_floor
            try:
                released, released_hash, released_path = invoke(tmp_path, step, 'release')
            finally:
                implementation.os.statvfs = original_statvfs
            assert released['disposition'] == 'INGRESS_RELEASED_POSTCONDITION_PENDING'
            assert released['detail']['fresh_pre_release_free_bytes'] == step['required_final_available']
            assert execute(released_path, released_hash) == released
            for process in processes:
                assert process.wait(timeout=5) == 0
            assert reconcile(step)['disposition'] == 'FORWARD_RECOVERY_ONLY'
            # Actual post-cut progress is now in the replacement, not original.
            from labelwatch.maintenance_artifacts import readonly
            from labelwatch import db
            with readonly(Path(step['source'])) as conn:
                assert db.get_cursor(conn, 'main') == 'post-cut-write'
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=5)


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
def test_interruption_between_renames_preserves_both_and_refuses_repeat(tmp_path, monkeypatch):
    from labelwatch import maintenance_step as implementation
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        _, staged_hash, _ = invoke(tmp_path, step, 'stage')
        predecessor = Path(step['journal']) / (staged_hash + '.completed.json')
        step.update(action='replace', predecessor=str(predecessor),
                    predecessor_sha256=hashlib.sha256(predecessor.read_bytes()).hexdigest())
        rename = implementation.rename_no_replace
        calls = []
        def interrupted(source, destination):
            calls.append(source)
            if len(calls) == 2:
                raise RuntimeError('fixture controller disappeared between renames')
            rename(source, destination)
        monkeypatch.setattr(implementation, 'rename_no_replace', interrupted)
        with pytest.raises(RuntimeError, match='controller disappeared'):
            invoke(tmp_path, step, 'replace')
        observed = reconcile(step)
        assert observed['disposition'] == 'SWAP_INCOMPLETE_KEEP_STOPPED'
        assert observed['artifacts']['original']['identity'] == step['source_identity']
        assert observed['artifacts']['staging']['presence'] == 'PRESENT'
        path = tmp_path / 'replace.json'
        with pytest.raises(VerificationRefused, match='OUTCOME_UNKNOWN'):
            execute(path, hashlib.sha256(path.read_bytes()).hexdigest())
        # A distinct, separately authorized bounded recovery step can restore the
        # exact original while the hold remains. This is not a retry of replace.
        started = Path(step['journal']) / (hashlib.sha256(path.read_bytes()).hexdigest() + '.started.json')
        step.update(action='rollback-pre-ingest', predecessor=str(started),
                    predecessor_sha256=hashlib.sha256(started.read_bytes()).hexdigest())
        recovered, _, _ = invoke(tmp_path, step, 'rollback')
        assert recovered['disposition'] == 'ORIGINAL_RESTORED_KEEP_HELD'
        assert identity(Path(step['source'])) == step['source_identity']
        assert Path(step['staging']).exists()
