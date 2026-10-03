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
