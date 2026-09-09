"""Finite interruption boundaries; no retry is mistaken for recovery authority."""
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile

import pytest

from labelwatch import maintenance_step as step_module
from labelwatch import maintenance_artifacts as artifacts
from labelwatch.maintenance_manifest import VerificationRefused
from test_maintenance_step import enrolled, invoke


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
