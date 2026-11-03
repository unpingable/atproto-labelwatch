"""Coverage loss must reach the existing watcher without live delivery."""
import copy

import pytest

from test_watch import NOW, ago, facts, iso, kinds, step


def fresh_coverage(now=NOW):
    f = facts(now)
    f['status']['coverage']['gap_overflow'] = False
    f['status']['discovery'] = {'status': 'accepted', 'attempted_at': iso(ago(now, seconds=60)),
                               'current': True, 'terminal': True}
    return f


@pytest.mark.parametrize('missing', ['status', 'gap_flag', 'stale', 'invalid'])
def test_gap_overflow_notifies_and_unknown_does_not_resolve(missing):
    st, f = {}, fresh_coverage()
    f['status']['coverage']['gap_overflow'] = True
    result, intents = step(st, f)
    assert result['L12']['verdict'] == 'coverage-gap-overflow'
    assert ('L12', 'coverage-gap-overflow', 'trigger', 'notice') in kinds(intents)
    unknown = copy.deepcopy(f)
    if missing == 'status':
        unknown['status'] = None
    elif missing == 'gap_flag':
        del unknown['status']['coverage']['gap_overflow']
    elif missing == 'stale':
        unknown['status']['generated_at'] = iso(ago(NOW, seconds=601))
    else:
        unknown['status']['coverage']['gap_overflow'] = 'false'
    result, intents = step(st, unknown)
    assert result['L12']['verdict'] == 'unknown'
    assert not [i for i in intents if i['obligation'] == 'L12']
    assert any(c['obligation'] == 'L12' for c in st['conditions'].values())
    result, intents = step(st, fresh_coverage())
    assert ('L12', 'coverage-gap-overflow', 'resolve', 'notice') in kinds(intents)


def test_discovery_failure_notifies_after_persistence_and_recovers():
    from datetime import timedelta
    st, f = {}, fresh_coverage()
    f['status']['discovery'].update(status='refused', current=False, reason='TimeoutError')
    result, intents = step(st, f)
    assert result['L13']['verdict'] == 'discovery-unavailable'
    assert not [i for i in intents if i['obligation'] == 'L13']
    later = NOW + timedelta(seconds=900)
    f['status']['generated_at'] = iso(later)
    f['status']['discovery']['attempted_at'] = iso(later)
    result, intents = step(st, f, later)
    assert ('L13', 'discovery-unavailable', 'trigger', 'notice') in kinds(intents)
    result, intents = step(st, fresh_coverage(later), later)
    assert ('L13', 'discovery-unavailable', 'resolve', 'notice') in kinds(intents)


def test_missing_or_stale_discovery_is_unknown():
    f = fresh_coverage()
    del f['status']['discovery']
    assert step({}, f)[0]['L13']['verdict'] == 'unknown'
    f = fresh_coverage()
    f['status']['discovery']['attempted_at'] = iso(ago(NOW, seconds=601))
    assert step({}, f)[0]['L13']['verdict'] == 'unknown'
