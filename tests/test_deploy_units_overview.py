"""Overview publisher and product unit examples."""
from test_deploy_units_recent import parse


def test_overview_unit_publishes_held_artifact_instead_of_being_gated():
    unit = parse('labelwatch-recent-overview.service')
    assert 'ConditionPathExists' not in unit['Unit']
    service = unit['Service']
    command = service['ExecStart'][0]
    assert 'recent_publish.py --store ${LABELWATCH_RECENT_STORE} --pubdir ${LABELWATCH_RECENT_PUBDIR}' in command
    assert '--hold-marker /etc/labelwatch-operator-safe-hold' in command
    assert '--activation-marker /etc/labelwatch-recent-qualified-activation' in command
    assert service['UMask'] == ['0022'] and service['User'] == ['labelwatch']
    assert service['NoNewPrivileges'] == ['yes'] and service['ProtectSystem'] == ['strict']
    timer = parse('labelwatch-recent-overview.timer')['Timer']
    assert timer['OnCalendar'] == ['*-*-* *:10/30:00 UTC'] and timer['Persistent'] == ['false']


def test_product_unit_runs_loopback_site():
    service = parse('labelwatch-recent-product.service')['Service']
    assert '--port 8431' in service['ExecStart'][0] and '--stale-after-seconds 900' in service['ExecStart'][0]
    assert '--resolve-handles' not in service['ExecStart'][0]
    assert service['IPAddressDeny'] == ['any'] and service['IPAddressAllow'] == ['localhost']
