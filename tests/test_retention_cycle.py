"""Dispatch reconciliation and archive publication for the weekly coordinator."""
import io
import json
from types import SimpleNamespace

import pytest

from tools import label_events_retention_cycle as module


@pytest.mark.parametrize('terminal,log_exists,expected', [('exit=0', True, 'consume'), ('exit=1', True, 'refuse'), (None, True, 'refuse'), (None, False, 'launch')])
def test_collected_unit_is_not_permission_to_duplicate_a_producer(tmp_path, monkeypatch, terminal, log_exists, expected):
    cycle = module.Cycle({'ssh_key': '/existing/key', 'host': 'root@existing', 'host_python': '/deployed/python'}, tmp_path, 3)
    monkeypatch.setattr(module, 'run', lambda *a, **k: SimpleNamespace(stdout='LoadState=not-found\nActiveState=inactive\n'))
    calls = []
    def remote(*args):
        calls.append(args)
        if args[0] == '/deployed/python':
            return json.dumps({'terminal': terminal, 'log_exists': log_exists})
        return ''
    monkeypatch.setattr(cycle, 'remote', remote)
    if expected == 'refuse':
        with pytest.raises(RuntimeError, match='original producer evidence exists'):
            cycle.dispatch('exact-original.service', '/original/export.sh', '/original/TERMINAL', '/original/export.log', 60, '50%', '512M')
    else:
        cycle.dispatch('exact-original.service', '/original/export.sh', '/original/TERMINAL', '/original/export.log', 60, '50%', '512M')
    assert sum(args[0] == 'systemd-run' for args in calls) == (expected == 'launch')


def test_archive_copy_recovers_incomplete_name_and_refuses_mutating_published_identity(tmp_path):
    source = tmp_path / 'source'
    destination = tmp_path / 'archive'
    source.write_bytes(b'complete retained bytes')
    (tmp_path / 'archive.incomplete').write_bytes(b'partial')
    module.publish(source, destination)
    assert destination.read_bytes() == source.read_bytes()
    assert not (tmp_path / 'archive.incomplete').exists()
    source.write_bytes(b'different source occurrence')
    with pytest.raises(RuntimeError, match='published archive identity differs'):
        module.publish(source, destination)
    assert destination.read_bytes() == b'complete retained bytes'


@pytest.mark.parametrize('rows,accepted', [
    ('autofs systemd-1 rw\nnfs4 existing:/archive rw,hard,vers=4.2\n', True),
    ('autofs systemd-1 rw\n', False),
    ('ext4 /dev/local rw\n', False),
    ('autofs systemd-1 rw\nnfs4 existing:/archive ro,hard\n', False),
    ('nfs4 unexpected:/archive rw,hard\n', False),
])
def test_archive_checks_backing_mount_and_refuses_local_or_readonly_substitution(tmp_path, monkeypatch, rows, accepted):
    from labelwatch.trim import TrimRefused
    cycle = module.Cycle({'archive_root': str(tmp_path), 'archive_source': 'existing:/archive',
                          'ssh_key': '/existing/key', 'host': 'root@existing'}, tmp_path / 'occurrence', 3)
    monkeypatch.setattr(module, 'run', lambda *a, **k: SimpleNamespace(stdout=rows))
    monkeypatch.setattr(module.shutil, 'disk_usage', lambda path: SimpleNamespace(free=100 * 1024**3))
    if accepted:
        cycle.mounted_archive()
    else:
        with pytest.raises(TrimRefused, match='NFS'):
            cycle.mounted_archive()


@pytest.mark.parametrize('result,accepted', [('signal', False), ('exit-code', False), ('success', True)])
def test_zero_shell_terminal_does_not_accept_failed_original_unit(tmp_path, monkeypatch, result, accepted):
    cycle = module.Cycle({'ssh_key': '/existing/key', 'host': 'root@existing'}, tmp_path, 3)
    status = f'LoadState=loaded\nActiveState=inactive\nMainPID=0\nResult={result}\n'
    monkeypatch.setattr(cycle, 'remote', lambda *args: status if args[0] == 'systemctl' else 'exit=0\n')
    if accepted:
        cycle.poll('original.service', '/original/TERMINAL', 1)
    else:
        with pytest.raises(RuntimeError, match='original producer failed'):
            cycle.poll('original.service', '/original/TERMINAL', 1)


@pytest.mark.parametrize('available,accepted', [(5 * 1024**3 - 1, False), (5 * 1024**3, True)])
def test_host_apply_memory_gate_refuses_before_launch(tmp_path, monkeypatch, available, accepted):
    from labelwatch.trim import TrimRefused
    cycle = module.Cycle({'ssh_key': '/existing/key', 'host': 'root@existing', 'host_python': '/deployed/python'}, tmp_path, 3)
    calls = []
    def remote(*args):
        calls.append(args)
        return str(available)
    monkeypatch.setattr(cycle, 'remote', remote)
    if accepted:
        cycle.admit_apply_memory()
        assert cycle.state['apply_memory_max_bytes'] == 4 * 1024**3
    else:
        with pytest.raises(TrimRefused, match='available-memory gate failed'):
            cycle.admit_apply_memory()
        assert not (tmp_path / 'checkpoint.json').exists()
    assert len(calls) == 1 and calls[0][0] == '/deployed/python'


def test_low_root_reserve_refuses_before_remote_export_work(tmp_path, monkeypatch):
    """The real incident's capacity cut must refuse before mkdir or dispatch."""
    from labelwatch.trim import TrimRefused
    config = {'ssh_key': '/existing/key', 'host': 'root@existing',
              'host_python': '/deployed/python', 'source': '/qualified/source',
              'tools_revision': 'fixture-revision', 'host_work_root': '/remote/work',
              'zone_work_root': '/zone/work', 'archive_root': str(tmp_path / 'archive')}
    (tmp_path / 'archive').mkdir()
    cycle = module.Cycle(config, tmp_path / 'occurrence', 3)
    monkeypatch.setattr(cycle, 'mounted_archive', lambda: None)
    monkeypatch.setattr(module, 'run', lambda *a, **k: SimpleNamespace(stdout='fixture-revision'))
    def remote(*args):
        if args[0] == '/deployed/python':
            return json.dumps({'retention:live_floor': '2026-08-15T00:00:00Z'})
        if args[0] == 'curl':
            return json.dumps({'retention': {'coverage': 'complete', 'catalog': {'sha256': 'exact-old'}}})
        if args[0] == 'df':
            return 'Avail\n32797593600\n' if args[-1] == '/' else 'Avail\n50748575744\n'
        pytest.fail('Remote export work reached before capacity refusal: ' + str(args))
    monkeypatch.setattr(cycle, 'remote', remote)
    with pytest.raises(TrimRefused, match='root32GiB'):
        cycle.execute()
    assert not cycle.state.get('export_launched')
    assert cycle.state['phase'] == 'failed'
    assert cycle.state['failure_type'] == 'TrimRefused'
    assert cycle.state['root_available_bytes'] == 32797593600
    assert cycle.state['zone_available_bytes'] == 50748575744
    assert json.loads((cycle.path / 'checkpoint.json').read_text())['phase'] == 'failed'


def test_export_capacity_boundary_is_measured_and_recorded(tmp_path, monkeypatch):
    cycle = module.Cycle({'ssh_key': '/existing/key', 'host': 'root@existing',
                         'zone_work_root': '/zone/work'}, tmp_path, 3)
    monkeypatch.setattr(cycle, 'remote', lambda *args: 'Avail\n' + str((32 if args[-1] == '/' else 47) * 1024**3) + '\n')
    cycle.admit_export_capacity()
    assert cycle.state['root_available_bytes'] == 32 * 1024**3
    assert cycle.state['zone_available_bytes'] == 47 * 1024**3


def test_terminated_shell_cannot_seal_a_success_marker(tmp_path):
    """A finite local process fixture models the observed whole-unit signal."""
    import os
    import signal
    import subprocess
    import sys
    import time
    ready = tmp_path / 'ready'
    terminal = tmp_path / "TERMINAL ' with spaces"
    script = module.producer_script([sys.executable, '-c',
        'import pathlib,time;pathlib.Path(' + repr(str(ready)) + ').touch();time.sleep(5)'],
        tmp_path / 'log', terminal)
    process = subprocess.Popen(['/bin/bash', '-c', script], start_new_session=True)
    try:
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert ready.exists()
        os.killpg(process.pid, signal.SIGTERM)
        assert process.wait(timeout=3) != 0
        assert terminal.read_text().strip() == 'exit=143'
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3)


@pytest.mark.parametrize('root,zone,accepted', [
    (32 * 1024**3 - 1, 47 * 1024**3, False),
    (32 * 1024**3, 47 * 1024**3 - 1, False),
    (32 * 1024**3, 47 * 1024**3, True),
])
def test_neither_filesystem_can_cover_the_others_reserve(tmp_path, monkeypatch, root, zone, accepted):
    from labelwatch.trim import TrimRefused
    cycle = module.Cycle({'ssh_key': '/existing/key', 'host': 'root@existing',
                         'zone_work_root': '/zone/work'}, tmp_path, 3)
    monkeypatch.setattr(cycle, 'remote', lambda *args: 'Avail\n' + str(root if args[-1] == '/' else zone) + '\n')
    if accepted:
        cycle.admit_export_capacity()
    else:
        with pytest.raises(TrimRefused):
            cycle.admit_export_capacity()
        assert not (tmp_path / 'checkpoint.json').exists()
