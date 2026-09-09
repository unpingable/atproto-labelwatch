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
    return {'schema': 'labelwatch.sqlite-relief-step/v1', 'operation': 'fixture-relief',
        'action': 'stage', 'source': str(source), 'backup': str(backup_root / 'backup.sqlite'),
        'restore': str(backup_root / 'restored.sqlite'), 'staging': str(tmp_path / 'staging.sqlite'),
        'original': str(tmp_path / 'original.sqlite'), 'journal': str(journal), 'revision': REV,
        'expected': expected, 'source_identity': identity(source), 'operating_margin': 4096,
        'predecessor': None, 'predecessor_sha256': None, 'ready_records': {}}


def invoke(tmp_path, step, name):
    path = tmp_path / (name + '.json')
    retain(path, step)
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    return execute(path, expected), expected, path


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
    assert list(Path(step['journal']).iterdir()) == []


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
            assert transition('release')['disposition'] == 'INGRESS_RELEASED_POSTCONDITION_PENDING'
            for process in processes:
                assert process.wait(timeout=5) == 0
            assert reconcile(step)['disposition'] == 'INDETERMINATE_WRITE_RESUMPTION_KEEP_STOPPED'
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
