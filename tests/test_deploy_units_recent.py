"""Recent-observatory unit examples carry the required gates and bounds."""
from __future__ import annotations

from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parents[1] / 'deploy'
SERVICES = ['labelwatch-recent-collect.service', 'labelwatch-recent-maintain.service',
            'labelwatch-recent-product.service']
HARDENING = {'NoNewPrivileges': 'yes', 'ProtectSystem': 'strict', 'KillMode': 'control-group',
             'User': 'labelwatch', 'EnvironmentFile': '/etc/labelwatch-recent.env',
             'ReadWritePaths': '/REPLACE_WITH_ADMITTED_RECENT_ROOT'}
REQUIRED = ['MemoryMax', 'CPUQuota', 'TimeoutStartSec', 'TimeoutStopSec',
            'LogRateLimitIntervalSec', 'LogRateLimitBurst']


def parse(name):
    """systemd units allow repeated keys; keep every value."""
    sections: dict[str, dict[str, list[str]]] = {}
    current = None
    for raw in (DEPLOY / name).read_text().replace('\\\n', ' ').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('['):
            current = sections.setdefault(line.strip('[]'), {})
            continue
        key, _, value = line.partition('=')
        current.setdefault(key, []).append(value)
    return sections


@pytest.mark.parametrize('name', SERVICES)
def test_service_gates_and_hardening(name):
    unit = parse(name)
    assert unit['Unit']['ConditionPathExists'] == ['!/etc/labelwatch-operator-safe-hold',
                                                   '/etc/labelwatch-recent-qualified-activation']
    service = unit['Service']
    for key, value in HARDENING.items():
        assert service[key] == [value], key
    for key in REQUIRED:
        assert service.get(key), key
    assert '/opt/labelwatch/releases/' in service['ExecStart'][0]
    assert '--store ${LABELWATCH_RECENT_STORE}' in service['ExecStart'][0]


def test_collect_and_maintain_serialise_on_unit_lock():
    collect = parse('labelwatch-recent-collect.service')['Service']
    maintain = parse('labelwatch-recent-maintain.service')['Service']
    assert collect['Type'] == maintain['Type'] == ['oneshot']
    assert collect['ExecStart'][0].startswith('/usr/bin/flock -n -E 75 ${LABELWATCH_RECENT_ADMITTED_ROOT}/units.lock ')
    assert collect['SuccessExitStatus'] == ['75'] and collect['Restart'] == ['no']
    assert maintain['ExecStart'][0].startswith('/usr/bin/flock -w 120 ${LABELWATCH_RECENT_ADMITTED_ROOT}/units.lock ')
    assert 'recent_maintain.py' in maintain['ExecStart'][0]
    assert 'recent_collect.py' in collect['ExecStart'][0] and '--network' in collect['ExecStart'][0]
    assert "'duckdb':'1.4.3','pyarrow':'22.0.0'" in collect['ExecStartPre'][0]


def test_timers():
    collect = parse('labelwatch-recent-collect.timer')['Timer']
    assert collect['OnCalendar'] == ['*-*-* *:00/2:00 UTC']
    assert collect['Persistent'] == ['false'] and collect['RandomizedDelaySec'] == ['15']
    assert collect['Unit'] == ['labelwatch-recent-collect.service']
    maintain = parse('labelwatch-recent-maintain.timer')['Timer']
    assert maintain['OnCalendar'] == ['*-*-* 00:05:00 UTC'] and maintain['Persistent'] == ['true']
    assert 'RandomizedDelaySec' not in maintain
    assert maintain['Unit'] == ['labelwatch-recent-maintain.service']


def test_env_example_names_every_storage_setting():
    keys = {line.split('=', 1)[0] for line in (DEPLOY / 'labelwatch-recent.env.example').read_text().splitlines()
            if line and not line.startswith('#')}
    assert {'LABELWATCH_RECENT_ADMITTED_ROOT', 'LABELWATCH_RECENT_STORE', 'LABELWATCH_RECENT_RESERVES',
            'LABELWATCH_RECENT_MIN_FREE_INODES', 'LABELWATCH_RECENT_WINDOW_DAYS',
            'LABELWATCH_RECENT_ROSTER'} <= keys
    assert 'LABELWATCH_RECENT_RESERVES_OVERRIDE_ACK' not in keys


def test_env_example_reserves_parse():
    import json
    line = next(l for l in (DEPLOY / 'labelwatch-recent.env.example').read_text().splitlines()
                if l.startswith('LABELWATCH_RECENT_RESERVES='))
    value = json.loads(line.split('=', 1)[1])
    assert value and all(set(e) == {'mount', 'bytes'} for e in value)
    assert not any(e['mount'] == '/data' for e in value)
