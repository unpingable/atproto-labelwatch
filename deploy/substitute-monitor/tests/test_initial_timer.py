"""A restarted collection timer is pending until a bounded first trigger."""
from datetime import timedelta

import pytest

from test_watch import NOW, ago, facts, kinds, step, sysd, w
from test_watch import nc6  # noqa: F401


def starting(now=NOW, *, age=60, due_in=60):
    f = facts(now)
    f['units']['labelwatch-recent-collect.timer'].update(
        LastTriggerUSec='n/a', ActiveEnterTimestamp=sysd(ago(now, seconds=age)),
        NextElapseUSecRealtime=sysd(now + timedelta(seconds=due_in)))
    return f


@pytest.mark.parametrize('age,due_in', [(0, 120), (60, 60), (120, 0), (300, -180), (600, -480)])
def test_initial_timer_waits_without_claiming_success_or_emitting_notice(age, due_in):
    result, intents = step({}, starting(age=age, due_in=due_in))
    assert result['L2']['verdict'] == 'skip'
    assert result['L2']['facts']['mode'] == 'awaiting-first-trigger'
    assert not [i for i in intents if i['obligation'] == 'L2']


def test_first_trigger_deadline_does_not_slide_with_next_due():
    state = {}
    result, _ = step(state, starting(age=60, due_in=60))
    assert result['L2']['verdict'] == 'skip'
    now = NOW + timedelta(seconds=541)
    # The active entry stays fixed, but systemd advertises another future due.
    f = starting(now, age=601, due_in=120)
    result, intents = step(state, f, now)
    assert result['L2']['verdict'] == 'collection-not-scheduled'
    assert ('L2', 'collection-not-scheduled', 'trigger', 'notice') in kinds(intents)


@pytest.mark.parametrize('field,value', [
    ('ActiveEnterTimestamp', ''), ('ActiveEnterTimestamp', 'garbage'),
    ('ActiveEnterTimestamp', sysd(NOW+timedelta(seconds=1))),
    ('NextElapseUSecRealtime', ''), ('NextElapseUSecRealtime', 'garbage'),
    ('NextElapseUSecRealtime', sysd(NOW-timedelta(seconds=61))),
    ('NextElapseUSecRealtime', sysd(NOW+timedelta(seconds=541))),
    ('LastTriggerUSec', 'garbage'), ('LastTriggerUSec', sysd(NOW+timedelta(seconds=1))),
])
def test_inconsistent_initial_evidence_is_unknown_and_does_not_resolve(field, value):
    state = {}
    overdue = facts()
    overdue['units']['labelwatch-recent-collect.timer']['LastTriggerUSec'] = sysd(ago(NOW, seconds=601))
    step(state, overdue)
    f = starting()
    f['units']['labelwatch-recent-collect.timer'][field] = value
    result, intents = step(state, f)
    assert result['L2']['verdict'] == 'unknown'
    assert not [i for i in intents if i['obligation'] == 'L2']
    assert any(c['obligation'] == 'L2' for c in state['conditions'].values())


def test_inactive_timer_without_trigger_notifies_immediately():
    f = starting()
    f['units']['labelwatch-recent-collect.timer']['ActiveState'] = 'inactive'
    result, intents = step({}, f)
    assert result['L2']['verdict'] == 'collection-not-scheduled'
    assert ('L2', 'collection-not-scheduled', 'trigger', 'notice') in kinds(intents)


def test_restart_preserves_existing_incident_until_actual_fresh_trigger():
    state, overdue = {}, facts()
    overdue['units']['labelwatch-recent-collect.timer']['LastTriggerUSec'] = sysd(ago(NOW, seconds=601))
    step(state, overdue)
    result, intents = step(state, starting())
    assert result['L2']['verdict'] == 'skip'
    assert not [i for i in intents if i['obligation'] == 'L2']
    assert any(c['obligation'] == 'L2' for c in state['conditions'].values())
    assert all(c['observation'] == 'skip' for c in state['conditions'].values() if c['obligation'] == 'L2')
    result, intents = step(state, facts())
    assert result['L2']['verdict'] == 'ok'
    assert ('L2', 'collection-not-scheduled', 'resolve', 'notice') in kinds(intents)


def test_old_active_entry_notifies_even_when_next_due_is_missing():
    f = starting(age=601)
    f['units']['labelwatch-recent-collect.timer']['NextElapseUSecRealtime'] = ''
    result, intents = step({}, f)
    assert result['L2']['verdict'] == 'collection-not-scheduled'
    assert ('L2', 'collection-not-scheduled', 'trigger', 'notice') in kinds(intents)


def test_two_failed_invocations_still_notify_during_initial_wait():
    from test_watch import svc
    state, f = {}, starting()
    f['units']['labelwatch-recent-collect.service'] = svc(ago(NOW, seconds=30), 'exit-code', '1', 'c-fail-1')
    step(state, f)
    later = NOW + timedelta(seconds=60)
    f = starting(later)
    f['units']['labelwatch-recent-collect.service'] = svc(later-timedelta(seconds=30), 'exit-code', '1', 'c-fail-2')
    result, intents = step(state, f, later)
    assert result['L2']['verdict'] == 'collection-failing'
    assert ('L2', 'collection-failing', 'trigger', 'notice') in kinds(intents)


def test_gather_requests_timer_active_entry_from_systemctl(nc6, monkeypatch):
    from test_watch import write_fixtures
    tmp_path, fixture, profile = nc6
    write_fixtures(fixture, NOW, through_min=1, overview_min=1)
    calls = []
    run_cmd = w.run_cmd

    def observe(argv, **kw):
        calls.append(argv)
        return run_cmd(argv, **kw)

    monkeypatch.setattr(w, 'run_cmd', observe)
    monkeypatch.setenv('FIXTURE_TS', sysd(ago(NOW, seconds=60)))
    cfg = dict(w.DEFAULTS, **w.load_env(profile))
    w.gather(cfg, NOW)
    timer_calls = [argv for argv in calls if len(argv)>3 and argv[1]=='show' and argv[2].endswith('.timer')]
    assert timer_calls
    assert all('ActiveEnterTimestamp' in argv[-1].split(',') for argv in timer_calls)
