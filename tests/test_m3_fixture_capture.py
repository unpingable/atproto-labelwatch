import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sqlite3
import sys
import tempfile
import time

import pytest

from labelwatch.maintenance_step import execute
from labelwatch.maintenance_hold import paths


@pytest.mark.skipif(not os.environ.get('M3_NQ_BIN') or not os.environ.get('M3_BACKUP_ROOT'),
                    reason='actual pinned native NQ and separate fixture filesystem required')
def test_actual_capture_cli_preserves_native_disposition_and_repeated_output(tmp_path):
    root = Path(__file__).resolve().parents[1]
    generator = runpy.run_path(str(root / 'scripts/m3_fixture_enrollment.py'))
    with tempfile.TemporaryDirectory(prefix='labelwatch-m3-capture-', dir=os.environ['M3_BACKUP_ROOT']) as temporary:
        fixture = tmp_path / 'fixture'
        generator['initialize'](fixture, Path(temporary), 'a' * 40)
        stage = generator['seal'](fixture, 'stage', None, root, Path(sys.executable))
        execute(Path(stage['step']), stage['step_sha256'])
        replacement = generator['seal'](fixture, 'replace', Path(stage['expected_result']), root, Path(sys.executable))
        execute(Path(replacement['step']), replacement['step_sha256'])
        database = fixture / 'source.sqlite'
        ready = paths(str(database))[0].parent / 'ready'
        code = 'import sys; from labelwatch.maintenance_hold import wait_before_writer_start; wait_before_writer_start(sys.argv[1],sys.argv[2])'
        environment = dict(os.environ, PYTHONPATH=str(root / 'src'))
        writers = [subprocess.Popen([sys.executable, '-c', code, str(database), role], env=environment)
                   for role in ('main', 'discovery')]
        try:
            deadline = time.monotonic() + 10
            while len(list(ready.glob('*.ready.json'))) != 2:
                assert time.monotonic() < deadline and all(p.poll() is None for p in writers)
                time.sleep(.02)
            output = tmp_path / 'capture'
            command = [sys.executable, str(root / 'scripts/m3_fixture_capture.py'),
                '--fixture', str(fixture), '--staged', stage['expected_result'], '--output', str(output),
                '--nq', os.environ['M3_NQ_BIN'], '--nq-sha256', os.environ['M3_NQ_SHA256'],
                '--phase', 'pre', '--acquisition-budget-seconds', '10']
            completed = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=20)
            assert completed.returncode == 0, completed.stderr + completed.stdout
            result = json.loads((output / 'capture-result.json').read_text())
            assert result['status'] == 'CAPTURE_COMPLETED'
            assert result['factual_disposition'] == 'ESTABLISHED'
            assert result['authority'] == 'NONE'
            before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in output.iterdir()}
            repeated = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=10)
            assert repeated.returncode == 2
            assert before == {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in output.iterdir()}
            with sqlite3.connect(database) as connection:
                connection.execute("UPDATE maintenance_types SET value='changed-after-cut' WHERE key='text'")
            connection.close()
            negative_command = list(command)
            negative_output = tmp_path / 'refuted'
            negative_command[negative_command.index('--output') + 1] = str(negative_output)
            negative = subprocess.run(negative_command, env=environment, capture_output=True, text=True, timeout=20)
            assert negative.returncode == 0  # transport/capture completion only
            disposition = json.loads((negative_output / 'capture-result.json').read_text())
            assert disposition['factual_disposition'] == 'REFUTED'
            assert json.loads((negative_output / 'pre-receipt.json').read_text())['disposition'] == 'REFUTED'
            # A real SQLite read lock outlasts the one-second capture budget.
            # Only this disposable source is locked; no fake NQ receipt follows.
            lock = sqlite3.connect(database)
            try:
                lock.execute('PRAGMA journal_mode=DELETE')
                lock.execute('BEGIN EXCLUSIVE')
                timed_command = list(command)
                timed_output = tmp_path / 'timed-out'
                timed_command[timed_command.index('--output') + 1] = str(timed_output)
                timed_command[timed_command.index('--acquisition-budget-seconds') + 1] = '1'
                timed = subprocess.run(timed_command, env=environment, capture_output=True, text=True, timeout=10)
                assert timed.returncode == 2, timed.stderr
                outcome = json.loads((timed_output / 'capture-result.json').read_text())
                assert outcome['reason'] == 'acquisition_timeout'
                assert outcome['native_receipt'] is None
                assert outcome['child_still_present'] is False
                assert not (timed_output / 'pre-receipt.json').exists()
            finally:
                lock.rollback()
                lock.close()
        finally:
            for writer in writers:
                if writer.poll() is None:
                    writer.terminate()
                writer.wait(timeout=5)
