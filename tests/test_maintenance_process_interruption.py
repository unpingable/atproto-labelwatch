import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pytest

from labelwatch.maintenance_step import retain, execute, reconcile
from labelwatch.maintenance_artifacts import identity
from labelwatch.maintenance_manifest import VerificationRefused
from test_maintenance_step import enrolled


@pytest.mark.skipif(not os.environ.get('M3_BACKUP_ROOT'), reason='explicit separate fixture filesystem required')
@pytest.mark.parametrize('cut', ['before_started', 'after_started', 'after_backup_sync',
    'after_restore_sync', 'after_staging_sync', 'before_terminal', 'after_terminal'])
def test_real_abrupt_stage_exit_preserves_cut_and_has_no_automatic_retry(tmp_path, cut):
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-exit-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        path = tmp_path / 'step.json'
        retain(path, step)
        step_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run([sys.executable, str(root / 'qualification/m3-admission/interrupted_step.py'),
            '--step', str(path), '--expected-sha256', step_sha, '--cut', cut],
            env=dict(os.environ, PYTHONPATH=str(root / 'src')), capture_output=True, text=True, timeout=10)
        assert result.returncode == 77, result.stderr
        assert 'INTENTIONAL_PROCESS_EXIT_NO_CLEANUP' in result.stdout
        assert identity(Path(step['source'])) == step['source_identity']
        assert reconcile(step)['disposition'] == 'SOURCE_RETAINED_REOPEN_STAGING_CUSTODY'
        started = Path(step['journal']) / (step_sha + '.started.json')
        terminal = Path(step['journal']) / (step_sha + '.completed.json')
        if cut == 'before_started':
            assert not started.exists() and not Path(step['backup']).exists()
        elif cut == 'after_terminal':
            assert terminal.exists()
            assert execute(path, step_sha)['disposition'] == 'STAGED_NOT_INSTALLED'
        else:
            assert started.exists() and not terminal.exists()
            with pytest.raises(VerificationRefused, match='OUTCOME_UNKNOWN'):
                execute(path, step_sha)
