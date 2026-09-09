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

from labelwatch.maintenance_hold import paths, active_hold
from labelwatch.maintenance_observation import observe, observe_cleanup
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
        staged = transition('stage')
        replacement = transition('replace')['detail']['replacement']['identity']
        ready_directory = paths(step['source'])[0].parent / 'ready'
        ready_directory.mkdir()
        python_path = str(Path(__file__).resolve().parents[1] / 'src')
        if os.environ.get('M3_WEBSOCKETS_WHEEL'):
            wheel = Path(os.environ['M3_WEBSOCKETS_WHEEL'])
            assert hashlib.sha256(wheel.read_bytes()).hexdigest() == os.environ['M3_WEBSOCKETS_SHA256']
            python_path += os.pathsep + str(wheel)
        environment = dict(os.environ, PYTHONPATH=python_path,
                           JETSTREAM_URL='ws://127.0.0.1:9')
        # Real application entrypoints. Acquisition is explicitly disabled or
        # pointed at a refused loopback fixture; no external service is queried.
        commands = [['run', '--ingest-interval', '0', '--scan-interval', '0'],
                    ['discover-stream', '--backstop-interval', '0']]
        logs = [(tmp_path / (role + '.log')).open('wb') for role in ('main', 'discovery')]
        processes = [subprocess.Popen([sys.executable, '-m', 'labelwatch.cli', '--db', step['source'], *command],
                     env=environment, stdout=log, stderr=subprocess.STDOUT) for command, log in zip(commands, logs)]
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
                'writer_identities': identities, 'phase': 'pre_ingest',
                'required_free_bytes': step['required_final_available'],
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
            cleanup_source = observe_cleanup(backup=Path(step['backup']), restore=Path(step['restore']),
                operation=step['operation'], source=Path(step['source']), original=Path(step['original']),
                revision=REV, expected_verification_sha256=digest(step['expected']), writer_identities=identities)
            cleanup_held = copy.deepcopy(request)
            cleanup_held['schema'] = 'nq.labelwatch-held-acquisition-request/v1'
            cleanup_held['evaluated_at'] = cleanup_source['completed_at']
            cleanup_request = {'schema': 'nq.labelwatch-cleanup-request/v2', 'held_request': cleanup_held,
                'backup': step['backup'], 'restore': step['restore'],
                'backup_identity': staged['detail']['backup']['backup']['identity'],
                'restore_identity': staged['detail']['backup']['restored']['identity'],
                'expected_hold_sha256': digest(active_hold(step['source'])),
                'acquisition_budget_seconds': 30, 'maximum_currentness_age_seconds': 30,
                'evaluated_at': datetime.now(timezone.utc).isoformat()}
            (tmp_path / 'cleanup-source.json').write_text(json.dumps(cleanup_source))
            (tmp_path / 'cleanup-request.json').write_text(json.dumps(cleanup_request))
            result = subprocess.run([str(binary), 'labelwatch-cleanup', '--source', str(tmp_path / 'cleanup-source.json'),
                '--request', str(tmp_path / 'cleanup-request.json')], capture_output=True, text=True, timeout=10)
            assert result.returncode == 0, result.stderr
            cleanup_receipt = json.loads(result.stdout)
            assert cleanup_receipt['disposition'] == 'ESTABLISHED'
            (tmp_path / 'cleanup-receipt.json').write_text(result.stdout)
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
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                    raise
            for log in logs:
                log.close()
