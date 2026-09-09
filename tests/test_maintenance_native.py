"""Actual independent observer → pinned native NQ, not synthetic verdict fixtures."""
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import pytest

from labelwatch.maintenance_hold import paths
from labelwatch.maintenance_observation import observe
from labelwatch.maintenance_step import digest, reconcile
from test_maintenance_step import enrolled, invoke
from test_maintenance_manifest import REV


@pytest.mark.skipif(not os.environ.get('M3_NQ_BIN') or not os.environ.get('M3_BACKUP_ROOT'),
                    reason='actual pinned native NQ and separate fixture filesystem required')
def test_actual_observation_native_pre_and_post_qualification(tmp_path):
    binary = Path(os.environ['M3_NQ_BIN'])
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == os.environ['M3_NQ_SHA256']
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-native-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        step = enrolled(tmp_path, Path(temporary))
        def transition(action):
            step['action'] = action
            result, step_hash, _ = invoke(tmp_path, step, action)
            previous = Path(step['journal']) / (step_hash + '.completed.json')
            step.update(predecessor=str(previous), predecessor_sha256=hashlib.sha256(previous.read_bytes()).hexdigest())
            return result
        transition('stage')
        replacement = transition('replace')['detail']['replacement']['identity']
        ready_directory = paths(step['source'])[0].parent / 'ready'
        ready_directory.mkdir()
        code = '''import sys,time
from labelwatch.maintenance_hold import wait_before_writer_start
from labelwatch import db
wait_before_writer_start(sys.argv[1], sys.argv[2])
conn = db.connect(sys.argv[1])
db.set_cursor(conn, sys.argv[2], 'post-cut-write')
conn.commit()
conn.close()
while True: time.sleep(0.1)
'''
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        processes = [subprocess.Popen([sys.executable, '-c', code, step['source'], role], env=environment)
                     for role in ('main', 'discovery')]
        try:
            deadline = time.monotonic() + 10
            while len(list(ready_directory.glob('*.ready.json'))) != 2:
                assert time.monotonic() < deadline and all(p.poll() is None for p in processes)
                time.sleep(0.05)
            step['ready_records'] = {role: str(next(ready_directory.glob(role + '-*.ready.json')))
                                     for role in ('main', 'discovery')}
            identities = {role: {k: json.loads(Path(path).read_text())[k] for k in ('pid', 'start_ticks')}
                          for role, path in step['ready_records'].items()}
            transition('verify-service')
            request = {'schema': 'nq.labelwatch-relief-request/v1', 'operation': step['operation'],
                'source': step['source'], 'original': step['original'], 'application_revision': REV,
                'expected_cut_sha256': digest(step['expected']), 'original_identity': step['source_identity'],
                'replacement_device': replacement['device'], 'replacement_inode': replacement['inode'],
                'writer_identities': identities, 'phase': 'pre_ingest', 'required_free_bytes': 4096,
                'maximum_age_seconds': 30, 'pre_ingest_qualification': None}
            def qualify(name, policy):
                observed = observe(operation=step['operation'], source=Path(step['source']),
                    original=Path(step['original']), revision=REV,
                    expected_verification_sha256=digest(step['expected']), writer_identities=identities)
                policy['evaluated_at'] = datetime.now(timezone.utc).isoformat()
                source = tmp_path / (name + '-source.json')
                req = tmp_path / (name + '-request.json')
                source.write_text(json.dumps(observed))
                req.write_text(json.dumps(policy))
                result = subprocess.run([str(binary), 'labelwatch-relief', '--source', str(source),
                    '--request', str(req)], capture_output=True, text=True, timeout=10)
                assert result.returncode == 0, result.stderr
                return json.loads(result.stdout)
            pre = qualify('pre', request)
            assert pre['disposition'] == 'ESTABLISHED'
            transition('cleanup')
            transition('release')
            deadline = time.monotonic() + 5
            while reconcile(step)['disposition'] != 'FORWARD_RECOVERY_ONLY':
                assert time.monotonic() < deadline
                time.sleep(0.05)
            request.update(phase='post_release', pre_ingest_qualification=pre)
            post = qualify('post', request)
            assert post['disposition'] == 'ESTABLISHED'
            assert post['claim'] == 'resource_relief_postcondition'
            # Actual fresh filesystem evidence refuses a larger unsupported
            # margin despite successful cleanup/enactment records.
            insufficient = copy.deepcopy(request)
            insufficient['required_free_bytes'] = (1 << 50)
            negative = qualify('insufficient', insufficient)
            assert negative['disposition'] == 'REFUTED'
            assert 'resource margin absent' in negative['refuted']
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=5)
