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
