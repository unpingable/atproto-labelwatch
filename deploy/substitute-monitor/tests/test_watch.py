"""Fixture-driven tests for labelwatch-recent-watch (stdlib + pytest only).

Each obligation L1-L10 is driven through its state transitions with typed fact fixtures, and the NC1-NC6
negative-control semantics are asserted end to end through run() with retained intent files and a fake transport.
"""
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent.parent
SCRIPT = HERE / 'labelwatch-recent-watch.py'
_spec = importlib.util.spec_from_file_location('lw_watch', SCRIPT)
w = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(w)

UTC = timezone.utc
GIB = 1 << 30
NOW = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)
CTL = 'labelwatch-recent-control'
CFG = dict(w.DEFAULTS)


def sysd(d):
    """A `systemctl show` timestamp as printed under TZ=UTC."""
    return d.strftime('%a %Y-%m-%d %H:%M:%S UTC')


def iso(d):
    return d.strftime('%Y-%m-%dT%H:%M:%SZ')


def ago(now, **kw):
    return now - timedelta(**kw)


def svc(exit_at, result='success', status='0', inv='i1', active='inactive', cond='yes', cond_at=None):
    return {'LoadState': 'loaded', 'ActiveState': active, 'Result': result, 'ExecMainStatus': status,
            'ExecMainExitTimestamp': sysd(exit_at) if exit_at else '', 'InvocationID': inv, 'ConditionResult': cond,
            'ConditionTimestamp': sysd(cond_at) if cond_at else '', 'NRestarts': '0'}


def facts(now=NOW):
    """A healthy, activated, unheld fact set as gather() would return it."""
    today_maint = datetime.combine(now.date(), datetime.min.time(), UTC) + timedelta(minutes=6)
    if now < today_maint + timedelta(minutes=29):
        today_maint -= timedelta(days=1)
    return {
        'sources': {},
        'markers': {'hold': False, 'activation': True, 'control_hold': False, 'control_activation': True},
        'units': {
            'labelwatch-recent-collect.service': svc(ago(now, seconds=60), inv='c-1'),
            'labelwatch-recent-collect.timer': {'LoadState': 'loaded', 'ActiveState': 'active', 'LastTriggerUSec': sysd(ago(now, seconds=70))},
            'labelwatch-recent-maintain.service': svc(today_maint, inv='m-1'),
            'labelwatch-recent-maintain.timer': {'LoadState': 'loaded', 'ActiveState': 'active', 'UnitFileState': 'enabled',
                                                 'NextElapseUSecRealtime': sysd(today_maint + timedelta(days=1))},
            'labelwatch-recent-overview.service': svc(ago(now, minutes=5), inv='o-1'),
            'labelwatch-recent-product.service': {'LoadState': 'loaded', 'ActiveState': 'active', 'NRestarts': '0'},
            CTL + '.service': svc(ago(now, minutes=4), inv='x-1'),
            CTL + '.timer': {'LoadState': 'loaded', 'ActiveState': 'active'},
        },
        'status': {'schema': 'labelwatch.recent-status.v1', 'generated_at': iso(ago(now, seconds=60)),
                   'coverage': {'status': 'warming_up', 'observed_through': iso(ago(now, seconds=120))},
                   'last_maintenance': {'generation': 5, 'recovery_pending': False, 'at': iso(today_maint)},
                   'round': {'status': 'ok', 'reason': None, 'store_hold': None}},
        'overview': {'schema_version': 'labelwatch.recent-overview.v1', 'generated_at': iso(ago(now, minutes=5)), 'hold': False,
                     'coverage': {'status': 'warming_up'}},
        'health': {'http': 200, 'body': {'status': 'ok', 'freshness': {'state': 'fresh'}, 'status_age_seconds': 30}},
        'facts': {'http': 200, 'body': {'instruments': [{'instrument': 'labelwatch', 'state': 'PRESENT', 'freshness_state': 'fresh',
                                                         'generated_at': iso(ago(now, minutes=5))}]}},
        'reserves': [{'mount': '/', 'reserve_bytes': 20 * GIB, 'free_bytes': 29 * GIB, 'free_inodes': 5000000, 'inode_floor': 1000000}],
    }


def step(st, f, now=NOW, cfg=CFG):
    results, intents = w.evaluate(f, st, now, cfg)
    return results, intents


def kinds(intents):
    return [(i['obligation'], i['condition'], i['action'], i['response_class']) for i in intents]


def test_healthy_baseline_is_ok_everywhere_and_silent():
    results, intents = step({}, facts())
    assert {k: v['verdict'] for k, v in results.items()} == {k: 'unknown' if k == 'L4' else 'ok' for k in results}
    assert set(results) == {'L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'L8', 'L9', 'C1'}
    assert intents == []


def test_timestamp_parsing_systemctl_and_iso():
    assert w.ts('Thu 2026-10-09 00:05:03 UTC') == datetime(2026, 10, 9, 0, 5, 3, tzinfo=UTC)
    assert w.ts('2026-10-09T00:05:03Z') == datetime(2026, 10, 9, 0, 5, 3, tzinfo=UTC)
    assert w.ts('2026-10-09T00:05:03.123456+00:00').microsecond == 123456
    assert w.ts('n/a') is None and w.ts('') is None and w.ts(None) is None and w.ts('2026-10-09T00:05:03') is None


# ---- L1 collection-progress -------------------------------------------------------------------------------------------

def test_l1_stalled_notice_at_15_min_page_at_60_then_resolve_on_fresh_progress():
    st, f = {}, facts()
    f['status']['coverage']['observed_through'] = iso(ago(NOW, seconds=901))
    r, i = step(st, f)
    assert r['L1']['verdict'] == 'collection-stalled' and kinds(i) == [('L1', 'collection-stalled', 'trigger', 'notice')]
    assert 'accepted no labeler page for 15 min' in i[0]['text']
    later = NOW + timedelta(minutes=45)
    f2 = facts(later)
    f2['status']['coverage']['observed_through'] = iso(ago(NOW, seconds=901))
    r, i = step(st, f2, later)
    assert kinds(i) == [('L1', 'collection-stalled', 'trigger', 'page')]
    assert i[0]['dedup_key'] == 'labelwatch-recent:L1:collection-stalled:labelwatch-recent-collect'
    r, i = step(st, f2, later + timedelta(minutes=5))
    assert i == []  # no repeat while the class does not rise
    r, i = step(st, facts(later + timedelta(minutes=10)), later + timedelta(minutes=10))
    assert kinds(i) == [('L1', 'collection-stalled', 'resolve', 'page')] and st['conditions'] == {}


def test_l1_stale_status_is_unknown_not_stalled_and_never_resolves():
    st, f = {}, facts()
    f['status']['coverage']['observed_through'] = iso(ago(NOW, minutes=20))
    step(st, f)
    g = facts(NOW + timedelta(minutes=5))
    g['status']['generated_at'] = iso(NOW - timedelta(seconds=10))  # STATUS is 610 s old at evaluation
    r, i = step(st, g, NOW + timedelta(minutes=10))
    assert r['L1']['verdict'] == 'unknown' and i == []
    assert st['conditions']['L1:collection-stalled:labelwatch-recent-collect']['observation'] == 'unknown'
    g['status'] = {'schema': 'something-else'}
    assert step(st, g, NOW + timedelta(minutes=10))[0]['L1']['verdict'] == 'unknown'


def test_l1_held_for_maintenance_inside_the_rotation_window():
    now = datetime(2026, 10, 9, 0, 20, tzinfo=UTC)
    f = facts(now)
    f['status']['coverage']['observed_through'] = iso(datetime(2026, 10, 9, 0, 0, tzinfo=UTC))
    f['status']['round']['store_hold'] = 'rotate daily observation owner before ingestion'
    r, i = step({}, f, now)
    assert r['L1']['verdict'] == 'held' and r['L1']['facts']['mode'] == 'held-for-maintenance' and i == []
    f['status']['round']['store_hold'] = None
    assert step({}, f, now)[0]['L1']['verdict'] == 'collection-stalled'


# ---- L2 collection-unit-result ----------------------------------------------------------------------------------------

def test_l2_two_consecutive_failed_invocations_notice_exit_75_ok():
    st, f = {}, facts()
    f['units']['labelwatch-recent-collect.service'] = svc(ago(NOW, seconds=30), 'exit-code', '1', 'c-2')
    assert step(st, f)[0]['L2']['verdict'] == 'ok'  # one failure is not yet failing
    g = facts(NOW + timedelta(minutes=5))
    g['units']['labelwatch-recent-collect.service'] = svc(NOW + timedelta(minutes=4), 'exit-code', '1', 'c-3')
    g['status']['round'] = {'status': 'refused', 'reason': 'roster refused'}
    r, i = step(st, g, NOW + timedelta(minutes=5))
    assert kinds(i) == [('L2', 'collection-failing', 'trigger', 'notice')] and 'roster refused' in i[0]['text']
    h = facts(NOW + timedelta(minutes=10))
    h['units']['labelwatch-recent-collect.service'] = svc(NOW + timedelta(minutes=9), 'success', '75', 'c-4')  # lock held: success
    r, i = step(st, h, NOW + timedelta(minutes=10))
    assert r['L2']['verdict'] == 'ok' and kinds(i) == [('L2', 'collection-failing', 'resolve', 'notice')]


def test_l2_condition_skip_held_timer_silent_not_scheduled_unreadable_unknown():
    f = facts()
    f['units']['labelwatch-recent-collect.service']['ConditionResult'] = 'no'
    assert step({}, f)[0]['L2']['verdict'] == 'held'
    f = facts()
    f['units']['labelwatch-recent-collect.timer']['LastTriggerUSec'] = sysd(ago(NOW, minutes=11))
    assert step({}, f)[0]['L2']['verdict'] == 'collection-not-scheduled'
    f['units']['labelwatch-recent-collect.timer']['LastTriggerUSec'] = 'n/a'
    assert step({}, f)[0]['L2']['verdict'] == 'collection-not-scheduled'
    f['units']['labelwatch-recent-collect.service'] = None
    assert step({}, f)[0]['L2']['verdict'] == 'unknown'


# ---- L3 maintenance-by-deadline ---------------------------------------------------------------------------------------

def maint(f, **kw):
    f['units']['labelwatch-recent-maintain.service'] = svc(**kw)
    return f


def test_l3_failed_pages_with_journal_reason_and_differs_from_overdue():
    f = maint(facts(), exit_at=datetime(2026, 10, 9, 0, 7, tzinfo=UTC), result='exit-code', status='1', inv='m-2')
    f['journal'] = 'starting\nRuntimeError: daily archive allocation not admitted\n'
    r, i = step({}, f)
    assert r['L3']['verdict'] == 'maintenance-failed'
    assert ('L3', 'maintenance-failed', 'trigger', 'page') in kinds(i)
    assert 'daily archive allocation not admitted' in r['L3']['text'] and r['L3']['ids']['invocation_id'] == 'm-2'
    g = maint(facts(), exit_at=datetime(2026, 10, 8, 0, 6, tzinfo=UTC), inv='m-0')
    g['units']['labelwatch-recent-maintain.timer']['UnitFileState'] = 'disabled'
    r, i = step({}, g)
    assert r['L3']['verdict'] == 'maintenance-overdue' and kinds(i) == [('L3', 'maintenance-overdue', 'trigger', 'page')]
    assert 'timer disabled' in i[0]['text']
    timeout = maint(facts(), exit_at=datetime(2026, 10, 9, 2, 5, tzinfo=UTC), result='timeout', status='0')
    assert step({}, timeout)[0]['L3']['verdict'] == 'maintenance-failed'


def test_l3_running_late_notice_after_0035_skip_before_and_held_occurrence():
    late = datetime(2026, 10, 9, 0, 40, tzinfo=UTC)
    f = maint(facts(late), exit_at=None, active='activating', inv='m-3')
    r, i = step({}, f, late)
    assert kinds(i) == [('L3', 'maintenance-running-late', 'trigger', 'notice')]
    early = datetime(2026, 10, 9, 0, 20, tzinfo=UTC)
    assert step({}, maint(facts(early), exit_at=None, active='activating'), early)[0]['L3']['verdict'] == 'skip'
    held = maint(facts(), exit_at=datetime(2026, 10, 8, 0, 6, tzinfo=UTC), cond='no', cond_at=datetime(2026, 10, 9, 0, 5, tzinfo=UTC))
    r, i = step({}, held)
    assert r['L3']['verdict'] == 'held' and i == []


def test_l3_before_0035_judges_yesterday_and_unknown_keeps_incident_open():
    early = datetime(2026, 10, 9, 0, 20, tzinfo=UTC)
    f = maint(facts(early), exit_at=datetime(2026, 10, 8, 0, 6, tzinfo=UTC))
    assert step({}, f, early)[0]['L3']['verdict'] == 'ok'
    st = {}
    step(st, maint(facts(), exit_at=datetime(2026, 10, 8, 0, 6, tzinfo=UTC)))  # overdue, paged
    g = facts()
    g['units']['labelwatch-recent-maintain.service'] = None
    r, i = step(st, g)
    assert r['L3']['verdict'] == 'unknown' and i == []
    assert st['conditions']['L3:maintenance-overdue:labelwatch-recent-maintain']['notified'] == 'page'


# ---- L4 window-advanced -----------------------------------------------------------------------------------------------

def day_facts(day, generation, at_day=None, pending=False):
    now = datetime(2026, 10, day, 1, 0, tzinfo=UTC)
    f = facts(now)
    f['status']['last_maintenance'] = {'generation': generation, 'recovery_pending': pending,
                                       'at': iso(datetime(2026, 10, at_day or day, 0, 5, 3, tzinfo=UTC))}
    return now, f


def test_l4_advanced_ok_not_advanced_notice_then_page_on_day_two_then_recover():
    st = {}
    now, f = day_facts(7, 3)
    assert step(st, f, now)[0]['L4']['verdict'] == 'unknown'
    now, f = day_facts(8, 3, at_day=7)
    r, i = step(st, f, now)
    assert kinds(i) == [('L4', 'window-not-advanced', 'trigger', 'notice')]
    now, f = day_facts(9, 3, at_day=7)
    r, i = step(st, f, now)
    assert kinds(i) == [('L4', 'window-not-advanced', 'trigger', 'page')]
    now, f = day_facts(10, 4)
    r, i = step(st, f, now)
    assert r['L4']['verdict'] == 'ok' and kinds(i) == [('L4', 'window-not-advanced', 'resolve', 'page')]


def test_l4_expiry_stalled_and_settle_skip():
    st = {}
    now, f = day_facts(7, 3, pending=True)
    assert step(st, f, now)[0]['L4']['verdict'] == 'unknown'
    now, f = day_facts(8, 4, pending=True)
    r, i = step(st, f, now)
    assert kinds(i) == [('L4', 'expiry-stalled', 'trigger', 'page')]
    now = datetime(2026, 10, 9, 0, 37, tzinfo=UTC)
    f = maint(facts(now), exit_at=datetime(2026, 10, 9, 0, 36, tzinfo=UTC))
    assert step({}, f, now)[0]['L4']['verdict'] == 'skip'  # less than 2 min after exit


# ---- L5 reserve-admission ---------------------------------------------------------------------------------------------

def test_l5_reserve_refusals_page_headroom_notice_unknown():
    f = facts()
    f['status']['round'] = {'status': 'refused', 'reason': 'shared_reserve_unavailable: filesystem without configured reserve'}
    r, i = step({}, f)
    assert ('L5', 'reserve-refused', 'trigger', 'page') in kinds(i) and '20 GiB on /, free 29.0 GiB' in r['L5']['text']
    g = maint(facts(), exit_at=datetime(2026, 10, 9, 0, 9, tzinfo=UTC), result='exit-code', status='1')
    g['journal'] = 'RuntimeError: global compaction allocation not admitted'
    assert step({}, g)[0]['L5']['facts']['refusals'][0]['refusal_source'] == 'maintain'
    h = facts()
    h['reserves'][0]['free_bytes'] = 21 * GIB
    r, i = step({}, h)
    assert kinds(i) == [('L5', 'reserve-headroom-low', 'trigger', 'notice')]
    h['reserves'] = None
    assert step({}, h)[0]['L5']['verdict'] == 'unknown'
    j = maint(facts(), exit_at=datetime(2026, 10, 9, 0, 9, tzinfo=UTC), result='exit-code', status='1')
    j['journal'] = None  # journal unreadable while maintenance failed
    assert step({}, j)[0]['L5']['verdict'] == 'unknown'
    k = facts()
    k['units']['labelwatch-recent-maintain.service'] = None  # cannot rule out a maintenance refusal
    assert step({}, k)[0]['L5']['verdict'] == 'unknown'


# ---- L6 overview-published --------------------------------------------------------------------------------------------

def test_l6_stale_after_45_min_unavailable_is_not_stale_refused_twice():
    f = facts()
    f['overview']['generated_at'] = iso(ago(NOW, seconds=2600))
    assert step({}, f)[0]['L6']['verdict'] == 'ok'
    f['overview']['generated_at'] = iso(ago(NOW, seconds=2701))
    r, i = step({}, f)
    assert kinds(i) == [('L6', 'overview-stale', 'trigger', 'notice')]
    f['overview'] = None
    assert step({}, f)[0]['L6']['verdict'] == 'overview-unavailable'
    f['overview'] = {'schema_version': 23, 'generated_at': iso(NOW)}
    assert step({}, f)[0]['L6']['verdict'] == 'overview-unavailable'
    st, g = {}, facts()
    g['units']['labelwatch-recent-overview.service'] = svc(ago(NOW, minutes=31), 'exit-code', '2', 'o-2')
    step(st, g)
    g['units']['labelwatch-recent-overview.service'] = svc(ago(NOW, minutes=1), 'exit-code', '2', 'o-3')
    assert step(st, g)[0]['L6']['verdict'] == 'overview-refused'


def test_l6_hold_artifact_contradicts_only_when_published_after_the_hold_lifted():
    st, f = {}, facts()
    f['overview']['hold'] = True
    assert step(st, f)[0]['L6']['verdict'] == 'overview-hold-contradiction'  # hold never seen
    st = {'hold_seen': {'prod': iso(ago(NOW, minutes=6))}}
    assert step(st, f)[0]['L6']['verdict'] == 'ok'  # published while the hold was still in place


# ---- L7 public-facts-freshness ----------------------------------------------------------------------------------------

def lw(f, **entry):
    f['facts']['body']['instruments'][0].update(entry)
    return f


def test_l7_states():
    assert step({}, lw(facts(), freshness_state='warming_up'))[0]['L7']['verdict'] == 'ok'
    st = {}
    r, i = step(st, lw(facts(), freshness_state='stale'))
    assert kinds(i) == [('L7', 'public-stale', 'trigger', 'notice')]
    later = NOW + timedelta(hours=2)
    r, i = step(st, lw(facts(later), freshness_state='stale'), later)
    assert kinds(i) == [('L7', 'public-stale', 'trigger', 'page')]
    assert step({}, lw(facts(), state='UNAVAILABLE'))[0]['L7']['verdict'] == 'public-unavailable'
    assert step({}, lw(facts(), freshness_state='unavailable'))[0]['L7']['verdict'] == 'public-unavailable'
    assert step({}, lw(facts(), freshness_state='held'))[0]['L7']['verdict'] == 'hold-contradiction'
    f = facts()
    f['facts'] = None  # timeout or connection refused
    assert step({}, f)[0]['L7']['verdict'] == 'unknown'


# ---- L8 hold-and-activation (hold first) ------------------------------------------------------------------------------

def test_l8_hold_gates_everything_never_pages_and_keeps_incidents_open():
    st, f = {}, facts()
    f['status']['coverage']['observed_through'] = iso(ago(NOW, hours=2))
    step(st, f)  # collection-stalled paged
    held = facts(NOW + timedelta(minutes=5))
    held['markers']['hold'] = True
    held['units']['labelwatch-recent-product.service']['ActiveState'] = 'inactive'
    held['status']['coverage']['observed_through'] = iso(ago(NOW, hours=3))
    held['units']['labelwatch-recent-maintain.service'] = svc(datetime(2026, 10, 7, 0, 6, tzinfo=UTC), 'exit-code', '1')
    r, i = step(st, held, NOW + timedelta(minutes=5))
    assert i == [] and all(r[k]['verdict'] == 'held' for k in ('L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'L9'))
    assert r['L8']['verdict'] == 'ok' and r['L8']['facts']['mode'] == 'held'
    assert st['conditions']['L1:collection-stalled:labelwatch-recent-collect']['observation'] == 'held'
    held['units']['labelwatch-recent-product.service']['ActiveState'] = 'active'
    r, i = step(st, held, NOW + timedelta(minutes=10))
    assert kinds(i) == [('L8', 'hold-incomplete', 'trigger', 'notice')]  # the only intent a hold allows, and never a page


def test_l8_not_activated_and_marker_stat_failure_is_unknown():
    f = facts()
    f['markers']['activation'] = False
    r, i = step({}, f)
    assert r['L1']['verdict'] == 'not_activated' and r['L8']['facts']['mode'] == 'not_activated' and i == []
    f['markers']['activation'] = None
    r, i = step({}, f)
    assert all(r[k]['verdict'] == 'unknown' for k in ('L1', 'L3', 'L8', 'L9')) and i == []


# ---- L9 product-reachability ------------------------------------------------------------------------------------------

def test_l9_product_down_pages_after_5_min_health_stale_notice_timeout_unknown():
    st, f = {}, facts()
    f['health'] = {'refused': True}
    r, i = step(st, f)
    assert r['L9']['verdict'] == 'product-down' and i == []
    r, i = step(st, f, NOW + timedelta(minutes=5))
    assert kinds(i) == [('L9', 'product-down', 'trigger', 'page')]
    g = facts()
    g['health'] = {'http': 503, 'body': {'status': 'stale'}}
    assert kinds(step({}, g)[1]) == [('L9', 'health-stale', 'trigger', 'notice')]
    g['health'] = None
    assert step({}, g)[0]['L9']['verdict'] == 'unknown'


# ---- run(): persistence, delivery, L10 --------------------------------------------------------------------------------

class Transport:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def __call__(self, url, body, timeout=10):
        self.calls.append((url, body))
        if self.fail:
            raise OSError('unreachable')
        return 202 if 'pagerduty' in url else 200


@pytest.fixture
def env(tmp_path, monkeypatch):
    transport = Transport()
    monkeypatch.setattr(w, 'post', transport)
    cfg = dict(w.DEFAULTS, WATCH_STATE_DIR=str(tmp_path / 'state'), WATCH_SLACK_WEBHOOK_URL='https://slack.invalid/hook',
               WATCH_PAGERDUTY_ROUTING_KEY='prod-key', WATCH_CONTROL_PAGERDUTY_ROUTING_KEY='test-key',
               WATCH_PAGERDUTY_URL='https://pagerduty.invalid/v2/enqueue')
    return cfg, tmp_path / 'state', transport


def run(cfg, f, now):
    return w.run(cfg, now, gather_fn=lambda c, n: f)


def intents_on_disk(sd):
    return sorted(p.name for p in (sd / 'intents').glob('*.json'))


def test_run_persists_observation_state_report_and_delivery_off_by_default(tmp_path):
    cfg = dict(w.DEFAULTS, WATCH_STATE_DIR=str(tmp_path))
    f = facts()
    f['sources'] = {'status': {'source': '/x/STATUS.json', 'sha256': 'ab' * 32, 'read_at': iso(NOW), 'ok': True, 'error': None}}
    f['overview']['generated_at'] = iso(ago(NOW, hours=1))
    run(cfg, f, NOW)
    obs = [json.loads(x) for x in (tmp_path / 'observations.jsonl').read_text().splitlines()]
    assert obs[0]['sources']['status']['sha256'] == 'ab' * 32 and obs[0]['obligations_digest'] == w.DIGEST
    state = json.loads((tmp_path / 'state.json').read_text())
    row = state['conditions']['L6:overview-stale:labelwatch-recent-overview']
    assert row['observation'] == 'present' and row['response_class'] == 'notice' and row['first_seen'] == iso(NOW)
    assert 'persisted_seconds' in row
    files = intents_on_disk(tmp_path)
    assert len(files) == 1 and json.loads((tmp_path / 'intents' / files[0]).read_text())['delivery'] == []
    assert json.loads((tmp_path / 'report.json').read_text())['verdicts']['L6'] == 'overview-stale'


def test_run_rotates_observations_at_10_mb(tmp_path):
    (tmp_path / 'observations.jsonl').write_bytes(b'x' * (w.ROTATE_BYTES + 1))
    run(dict(w.DEFAULTS, WATCH_STATE_DIR=str(tmp_path)), facts(), NOW)
    assert (tmp_path / 'observations.jsonl.1').stat().st_size == w.ROTATE_BYTES + 1
    assert (tmp_path / 'observations.jsonl').stat().st_size < 100000


def test_l10_evaluator_stale_notice_and_daily_digest_once(env):
    cfg, sd, transport = env
    run(cfg, facts(), NOW)
    run(cfg, facts(NOW + timedelta(minutes=20)), NOW + timedelta(minutes=20))
    assert any('-L10-obligation-evaluator-stale-notice' in n for n in intents_on_disk(sd))
    morning = datetime(2026, 10, 10, 0, 41, tzinfo=UTC)
    run(cfg, facts(morning), morning)
    run(cfg, facts(morning + timedelta(minutes=5)), morning + timedelta(minutes=5))
    assert sum('daily-digest' in n for n in intents_on_disk(sd)) == 1
    assert all('pagerduty' not in url for url, _ in transport.calls)  # L10 notices never page


def test_report_failure_posts_one_line_only_on_failure(env):
    cfg, sd, transport = env
    assert w.report_failure(cfg, {'SERVICE_RESULT': 'success'}) == 0 and transport.calls == []
    (sd / 'intents').mkdir(parents=True)
    assert w.report_failure(cfg, {'SERVICE_RESULT': 'exit-code', 'EXIT_CODE': 'exited', 'EXIT_STATUS': '1'}) == 0
    assert len(transport.calls) == 1 and 'slack' in transport.calls[0][0] and 'watcher run failed' in transport.calls[0][1]['text']
    assert any(n.endswith('-run-failed.json') for n in intents_on_disk(sd))


def test_failed_delivery_is_retained_and_retried_only_on_failed_routes(env, monkeypatch):
    cfg, sd, _ = env
    failing = Transport(fail=True)
    monkeypatch.setattr(w, 'post', failing)
    f = facts()
    f['units'][CTL + '.service'] = svc(ago(NOW, minutes=1), 'exit-code', '1', 'x-2')
    run(cfg, f, NOW)
    state = json.loads((sd / 'state.json').read_text())
    assert len(state['pending']) == 1
    ok = Transport()
    monkeypatch.setattr(w, 'post', ok)
    run(cfg, f, NOW + timedelta(minutes=5))
    assert sorted(b.get('event_action', 'slack') for _, b in ok.calls) == ['slack', 'trigger']
    doc = json.loads((sd / 'intents' / next(iter(state['pending']))).read_text())
    assert [d['accepted'] for d in doc['delivery']] == [False, False, True, True]


def test_secrets_never_reach_retained_evidence(env):
    cfg, sd, _ = env
    f = facts()
    f['units'][CTL + '.service'] = svc(ago(NOW, minutes=1), 'exit-code', '1', 'x-2')
    run(cfg, f, NOW)
    blob = ''.join(p.read_text() for p in sd.rglob('*') if p.is_file())
    assert 'test-key' not in blob and 'prod-key' not in blob and 'slack.invalid' not in blob


# ---- NC1-NC5: the control occurrence end to end ------------------------------------------------------------------------

def control(f, **kw):
    f['units'][CTL + '.service'] = svc(**kw)
    return f


def test_nc1_failed_occurrence_pages_on_the_test_route_with_derived_dedup_key(env):
    cfg, sd, transport = env
    run(cfg, control(facts(), exit_at=ago(NOW, minutes=1), result='exit-code', status='1', inv='inv-nc1'), NOW)
    [name] = intents_on_disk(sd)
    intent = json.loads((sd / 'intents' / name).read_text())
    assert intent['condition'] == 'scheduled-occurrence-failed' and intent['target'] == CTL
    assert intent['dedup_key'] == 'labelwatch-recent:C1:scheduled-occurrence-failed:' + CTL
    assert intent['ids'] == {'unit': CTL + '.service', 'invocation_id': 'inv-nc1', 'obligation': 'C1'}
    pd = [b for u, b in transport.calls if 'pagerduty' in u]
    assert len(pd) == 1 and pd[0]['routing_key'] == 'test-key' and pd[0]['event_action'] == 'trigger'
    assert pd[0]['dedup_key'] == intent['dedup_key'] and pd[0]['payload']['severity'] == 'critical'
    assert [d['route'] for d in intent['delivery'] if d['accepted']] == ['slack', 'pagerduty']  # notice + page


def test_nc2_overdue_is_distinct_from_failed_and_from_unknown():
    st = {}
    step(st, control(facts(), exit_at=ago(NOW, minutes=4)))  # one success, then the timer is stopped
    later = NOW + timedelta(minutes=12)
    f = control(facts(later), exit_at=ago(NOW, minutes=4))
    f['units'][CTL + '.timer']['ActiveState'] = 'inactive'
    r, i = step(st, f, later)
    assert r['C1']['verdict'] == 'scheduled-occurrence-overdue'
    assert kinds(i) == [('C1', 'scheduled-occurrence-overdue', 'trigger', 'page')]
    assert i[0]['dedup_key'] != 'labelwatch-recent:C1:scheduled-occurrence-failed:' + CTL


def test_nc3_control_hold_produces_no_intent_and_no_provider_call(env):
    cfg, sd, transport = env
    for k in range(4):  # the timer keeps firing; condition-skipped occurrences
        now = NOW + timedelta(minutes=5 * k)
        f = control(facts(now), exit_at=ago(NOW, hours=1), cond='no', cond_at=ago(now, minutes=2))
        f['markers']['control_hold'] = True
        r = run(cfg, f, now)
        assert r['C1']['verdict'] == 'held'
    assert intents_on_disk(sd) == [] and transport.calls == []


def test_nc3_condition_skip_without_marker_is_still_held():
    f = control(facts(), exit_at=ago(NOW, hours=1), cond='no', cond_at=ago(NOW, minutes=2))
    r, i = step({}, f)
    assert r['C1']['verdict'] == 'held' and i == []


def test_nc4_missing_evidence_keeps_nc1_open_and_raises_one_evidence_notice(env):
    cfg, sd, transport = env
    run(cfg, control(facts(), exit_at=ago(NOW, minutes=1), result='exit-code', status='1'), NOW)
    src = 'systemd:' + CTL + '.service'
    for minutes in (5, 10, 15, 20, 25):
        f = facts(NOW + timedelta(minutes=minutes))
        f['units'][CTL + '.service'] = None
        f['sources'] = {src: {'source': 'systemctl show', 'ok': False, 'error': 'LoadState=not-found', 'sha256': None}}
        r = run(cfg, f, NOW + timedelta(minutes=minutes))
        assert r['C1']['verdict'] == 'unknown'
    state = json.loads((sd / 'state.json').read_text())
    assert state['conditions']['C1:scheduled-occurrence-failed:' + CTL]['notified'] == 'page'
    assert state['conditions']['C1:scheduled-occurrence-failed:' + CTL]['observation'] == 'unknown'
    names = intents_on_disk(sd)
    assert sum('evidence-unavailable-trigger' in n for n in names) == 1
    assert not any('resolve' in n for n in names)
    assert [b['event_action'] for u, b in transport.calls if 'pagerduty' in u] == ['trigger']


def test_nc5_recovery_resolves_only_after_a_fresh_success_and_keeps_the_trigger_record(env):
    cfg, sd, transport = env
    run(cfg, control(facts(), exit_at=ago(NOW, minutes=1), result='exit-code', status='1'), NOW)
    stale_success = NOW + timedelta(minutes=20)  # the last failure ages out with no new occurrence: overdue, not resolved
    r = run(cfg, control(facts(stale_success), exit_at=ago(NOW, minutes=1), result='exit-code', status='1'), stale_success)
    assert r['C1']['verdict'] == 'scheduled-occurrence-overdue'
    assert not any('resolve' in n for n in intents_on_disk(sd))
    fresh = NOW + timedelta(minutes=25)
    r = run(cfg, control(facts(fresh), exit_at=fresh - timedelta(minutes=1), inv='x-ok'), fresh)
    assert r['C1']['verdict'] == 'ok'
    names = intents_on_disk(sd)
    assert sum('C1-scheduled-occurrence-failed-trigger' in n for n in names) == 1  # NC1's trigger is retained
    assert sum('-resolve.json' in n for n in names) == 2  # failed and overdue both resolve
    resolves = [b for u, b in transport.calls if 'pagerduty' in u and b['event_action'] == 'resolve']
    assert sorted(b['dedup_key'] for b in resolves) == sorted('labelwatch-recent:C1:%s:%s' % (c, CTL) for c in
                                                               ('scheduled-occurrence-failed', 'scheduled-occurrence-overdue'))
    assert json.loads((sd / 'state.json').read_text())['conditions'] == {}


# ---- NC6: fixture STATUS.json / overview.json through the real gather() and CLI ----------------------------------------

FAKE_SYSTEMCTL = """#!/bin/sh
case "$2" in
  *.timer) printf 'LoadState=loaded\\nActiveState=active\\nUnitFileState=enabled\\nLastTriggerUSec=%s\\nNextElapseUSecRealtime=\\n' "$FIXTURE_TS" ;;
  *) printf 'LoadState=loaded\\nActiveState=inactive\\nResult=success\\nExecMainStatus=0\\nExecMainExitTimestamp=%s\\nInvocationID=fx\\nConditionResult=yes\\n' "$FIXTURE_TS" ;;
esac
"""


@pytest.fixture
def nc6(tmp_path):
    fixture = tmp_path / 'control-fixture'
    fixture.mkdir()
    (tmp_path / 'control-activation').touch()
    systemctl = tmp_path / 'systemctl'
    systemctl.write_text(FAKE_SYSTEMCTL)
    systemctl.chmod(0o755)
    profile = tmp_path / 'nc6.env'
    profile.write_text('\n'.join([
        'WATCH_STORE=%s' % fixture, 'WATCH_PUBDIR=%s' % fixture, 'WATCH_HOLD_MARKER=%s' % (tmp_path / 'control-hold'),
        'WATCH_ACTIVATION_MARKER=%s' % (tmp_path / 'control-activation'), 'WATCH_CONTROL_UNIT=',
        'WATCH_SYSTEMCTL=%s' % systemctl, 'WATCH_RECENT_ENV=%s' % (tmp_path / 'absent.env'),
        'WATCH_HEALTH_URL=http://127.0.0.1:9/health', 'WATCH_FACTS_BASE=http://127.0.0.1:9',
        'WATCH_RESERVES=[{"mount": "/", "bytes": 1}]', 'WATCH_STATE_DIR=%s' % (tmp_path / 'state')]) + '\n')
    return tmp_path, fixture, profile


def write_fixtures(fixture, now, through_min, overview_min, hold=False):
    (fixture / 'STATUS.json').write_text(json.dumps({
        'schema': 'labelwatch.recent-status.v1', 'generated_at': iso(ago(now, seconds=30)),
        'coverage': {'status': 'warming_up', 'observed_through': iso(ago(now, minutes=through_min))},
        'last_maintenance': {'generation': 0, 'recovery_pending': False, 'at': None}, 'round': {'status': 'ok'}}))
    (fixture / 'overview.json').write_text(json.dumps({
        'schema_version': 'labelwatch.recent-overview.v1', 'generated_at': iso(ago(now, minutes=overview_min)), 'hold': hold,
        'coverage': {'status': 'held' if hold else 'warming_up', 'observed_through': None}}))


def cli(profile, now, tmp_path):
    env = dict(os.environ, FIXTURE_TS=w.datetime.strftime(now - timedelta(minutes=1), '%a %Y-%m-%d %H:%M:%S UTC'))
    env = {k: v for k, v in env.items() if not k.startswith('WATCH_')}
    out = subprocess.run([sys.executable, '-I', str(SCRIPT), '--config', str(profile), '--now', iso(now)], env=env,
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr  # failed obligations never fail the process
    return json.loads(out.stdout)


def test_nc6_frozen_progress_stalls_at_15_min_and_old_overview_is_stale_at_45(nc6):
    tmp_path, fixture, profile = nc6
    now = datetime.now(UTC).replace(microsecond=0)
    write_fixtures(fixture, now, through_min=14, overview_min=44)
    v = cli(profile, now, tmp_path)
    assert v['L1'] == 'ok' and v['L6'] == 'ok'
    write_fixtures(fixture, now, through_min=16, overview_min=46)
    v = cli(profile, now, tmp_path)
    assert v['L1'] == 'collection-stalled' and v['L6'] == 'overview-stale'
    assert v['L7'] == 'unknown' and v['L9'] == 'product-down'  # refused facts API is unknown; refused product is down
    sd = tmp_path / 'state'
    obs = [json.loads(x) for x in (sd / 'observations.jsonl').read_text().splitlines()]
    assert obs[-1]['sources']['status']['sha256'] == hashlib.sha256((fixture / 'STATUS.json').read_bytes()).hexdigest()
    assert obs[-1]['sources']['status']['source'] == str(fixture / 'STATUS.json')
    names = sorted(p.name for p in (sd / 'intents').glob('*.json'))
    assert any('L1-collection-stalled-trigger' in n for n in names) and any('L6-overview-stale-trigger' in n for n in names)


def test_nc6_hold_artifact_without_control_hold_marker_is_a_contradiction(nc6):
    tmp_path, fixture, profile = nc6
    now = datetime.now(UTC).replace(microsecond=0)
    write_fixtures(fixture, now, through_min=1, overview_min=1, hold=True)
    assert cli(profile, now, tmp_path)['L6'] == 'overview-hold-contradiction'
    (tmp_path / 'control-hold').touch()  # with the control hold marker present everything is held, silently
    before = sorted(p.name for p in (tmp_path / 'state' / 'intents').glob('*.json'))
    v = cli(profile, now + timedelta(seconds=1), tmp_path)
    assert v['L6'] == 'held' and v['L1'] == 'held'
    assert sorted(p.name for p in (tmp_path / 'state' / 'intents').glob('*.json')) == before


def test_nc6_unreadable_status_is_unknown_and_raises_evidence_notice_after_15_min(nc6):
    tmp_path, fixture, profile = nc6
    now = datetime.now(UTC).replace(microsecond=0)
    write_fixtures(fixture, now, through_min=1, overview_min=1)
    (fixture / 'STATUS.json').write_text('{not json')
    assert cli(profile, now, tmp_path)['L1'] == 'unknown'
    cli(profile, now + timedelta(minutes=15), tmp_path)
    docs = [json.loads(p.read_text()) for p in (tmp_path / 'state' / 'intents').glob('*.json')]
    evidence = [d['target'] for d in docs if d['condition'] == 'evidence-unavailable' and d['action'] == 'trigger']
    assert sorted(evidence) == ['facts:instruments', 'status']  # refused facts API is unknown evidence too, one notice each


def test_l4_absent_baseline_stays_unknown_until_later_generation():
    st = {}
    now, f = day_facts(7, 3)
    assert step(st, f, now)[0]['L4']['verdict'] == 'unknown'
    assert step(st, f, now + timedelta(minutes=5))[0]['L4']['verdict'] == 'unknown'
    now, f = day_facts(8, 4)
    assert step(st, f, now)[0]['L4']['verdict'] == 'ok'

def test_gather_journal_reason_is_bound_to_current_invocation(monkeypatch, tmp_path):
    current = 'a' * 32
    calls = []
    def command(argv, **kw):
        calls.append(argv)
        if argv[0] == 'journal-fixture':
            assert '_SYSTEMD_INVOCATION_ID=' + current in argv
            # The previous invocation's reserve refusal is excluded by the match.
            return b'current occurrence failed for another reason\n'
        return ('LoadState=loaded\nResult=exit-code\nInvocationID=%s\n' % current).encode()
    monkeypatch.setattr(w, 'run_cmd', command)
    monkeypatch.setattr(w, 'http_get', lambda *a, **kw: (_ for _ in ()).throw(TimeoutError()))
    cfg = dict(w.DEFAULTS, WATCH_JOURNALCTL='journal-fixture', WATCH_RECENT_ENV=str(tmp_path / 'absent'),
               WATCH_STORE=str(tmp_path), WATCH_PUBDIR=str(tmp_path), WATCH_RESERVES='[]')
    f = w.gather(cfg, NOW)
    assert f['journal'] == 'current occurrence failed for another reason\n'
    assert not w.MAINT_RESERVE_RE.search(f['journal'])
    assert current in f['sources']['journal:labelwatch-recent-maintain']['source']

def test_gather_missing_invocation_does_not_attribute_journal(monkeypatch, tmp_path):
    calls = []
    def command(argv, **kw):
        calls.append(argv)
        return b'LoadState=loaded\nResult=exit-code\nInvocationID=\n'
    monkeypatch.setattr(w, 'run_cmd', command)
    monkeypatch.setattr(w, 'http_get', lambda *a, **kw: (_ for _ in ()).throw(TimeoutError()))
    cfg = dict(w.DEFAULTS, WATCH_JOURNALCTL='journal-fixture', WATCH_RECENT_ENV=str(tmp_path / 'absent'),
               WATCH_STORE=str(tmp_path), WATCH_PUBDIR=str(tmp_path), WATCH_RESERVES='[]')
    f = w.gather(cfg, NOW)
    assert f['journal'] is None
    assert not any(a[0] == 'journal-fixture' for a in calls)


@pytest.mark.parametrize('bad_generation', [None, '3', True, -1, {}, 3.5])
def test_l4_invalid_generation_cannot_establish_next_day_baseline(bad_generation):
    st = {}
    now, f = day_facts(7, 3)
    if bad_generation is None:
        f['status']['last_maintenance'] = {}
    else:
        f['status']['last_maintenance']['generation'] = bad_generation
    assert step(st, f, now)[0]['L4']['verdict'] == 'unknown'
    assert st['l4_days']['2026-10-07']['generation'] is None
    now, f = day_facts(8, 4)
    assert step(st, f, now)[0]['L4']['verdict'] == 'unknown'
    assert st['l4_days']['2026-10-08']['advanced'] is None
    now, f = day_facts(9, 5)
    assert step(st, f, now)[0]['L4']['verdict'] == 'ok'

def test_l4_invalid_current_generation_does_not_advance_valid_baseline():
    st = {}
    now, f = day_facts(7, 3)
    step(st, f, now)
    now, f = day_facts(8, 4)
    f['status']['last_maintenance']['generation'] = None
    assert step(st, f, now)[0]['L4']['verdict'] == 'unknown'
    assert st['l4_days']['2026-10-08']['advanced'] is None


def test_gather_corroboration_matches_actual_facts_api_route_table(monkeypatch, tmp_path):
    # Cross-repository integration qualification supplies the actual selected source.
    # Standalone fixture runs still verify the canonical request below.
    import ast
    routes_source = os.environ.get('FACTS_API_ROUTES_SOURCE')
    routes = {'/atproto/v1/instruments': 'instruments',
              '/atproto/v1/labelwatch/overview': 'labelwatch_overview'}
    if routes_source:
        namespace = {}
        tree = ast.parse(Path(routes_source).read_text())
        for node in tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
                if node.targets[0].id in ('NAMESPACE', 'API_PREFIX', 'FACT_ROUTES'):
                    exec(compile(ast.Module(body=[node], type_ignores=[]), routes_source, 'exec'), {}, namespace)
        routes = namespace['FACT_ROUTES']
    seen = []
    def get(url, **kw):
        from urllib.parse import urlsplit
        path = urlsplit(url).path
        seen.append(path)
        if path == '/health':
            return 200, b'{}'
        assert path in routes, 'monitor requests path absent from actual Facts API table'
        return 503 if routes[path] == 'labelwatch_overview' else 200, b'{"error":{"code":"UNAVAILABLE"}}'
    monkeypatch.setattr(w, 'http_get', get)
    monkeypatch.setattr(w, 'run_cmd', lambda *a, **kw: b'LoadState=loaded\nResult=success\n')
    cfg = dict(w.DEFAULTS, WATCH_RECENT_ENV=str(tmp_path / 'absent'), WATCH_STORE=str(tmp_path),
               WATCH_PUBDIR=str(tmp_path), WATCH_RESERVES='[]')
    f = w.gather(cfg, NOW)
    assert '/atproto/v1/labelwatch/overview' in seen
    assert f['facts']['overview_http'] == 503
    assert f['facts']['overview_code'] == 'UNAVAILABLE'
