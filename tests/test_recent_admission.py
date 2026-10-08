"""Admitted operator root and configurable per-filesystem reserves."""
import hashlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage
from recent_storage import RecentStore

NOW = '2026-10-08T00:00:00Z'
SMALL = json.dumps([{'mount': '/', 'bytes': 1}])


@pytest.fixture
def env(tmp_path, monkeypatch):
    campaign = tmp_path / 'campaigns' / 'one' / 'runtime'; campaign.mkdir(parents=True)
    admitted = tmp_path / 'admitted'; admitted.mkdir()
    monkeypatch.setattr(storage, 'ROOT', campaign)
    for key in (storage.ADMITTED_ENV, storage.RESERVES_ENV, storage.RESERVES_FILE_ENV,
                storage.INODES_ENV, storage.OVERRIDE_ENV):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(storage.ADMITTED_ENV, str(admitted))
    monkeypatch.setenv(storage.RESERVES_ENV, SMALL)
    monkeypatch.setenv(storage.INODES_ENV, '1')
    return SimpleNamespace(campaign=campaign, admitted=admitted, tmp=tmp_path, mp=monkeypatch)


def recorded(store):
    with storage.connect(store.state, readonly=True) as c:
        return c.execute('SELECT value FROM meta WHERE key=?', (storage.RESERVES_KEY,)).fetchone()[0]


def test_admitted_root_child_is_created_and_records_policy(env):
    store = RecentStore.create(env.admitted / 'store', NOW)
    assert store.root == env.admitted / 'store'
    policy = json.loads(recorded(store))
    assert policy == {'schema': 'labelwatch.recent-reserves.v1', 'min_free_inodes': 1,
                      'reserves': [{'mount': '/', 'bytes': 1}]}
    store.check_local()
    assert RecentStore(env.admitted / 'store').frontier()


def test_admitted_root_itself_is_not_a_store(env):
    with pytest.raises(ValueError, match='campaign-owned'):
        storage.owned(env.admitted)


def test_symlinked_admitted_root_refused(env):
    link = env.tmp / 'link'; link.symlink_to(env.admitted)
    env.mp.setenv(storage.ADMITTED_ENV, str(link))
    with pytest.raises(ValueError, match='symlink'):
        RecentStore.create(link / 'store', NOW)
    assert not (env.admitted / 'store').exists()


def test_symlinked_store_component_refused(env):
    real = env.tmp / 'elsewhere'; real.mkdir()
    (env.admitted / 'hop').symlink_to(real)
    with pytest.raises(ValueError, match='symlink'):
        storage.owned(env.admitted / 'hop' / 'store')


@pytest.mark.parametrize('value', ['relative/root', '/'])
def test_relative_or_filesystem_root_refused(env, value):
    env.mp.setenv(storage.ADMITTED_ENV, value)
    with pytest.raises(ValueError, match='admitted root'):
        storage.owned('/x/y')


def test_unset_admitted_root_refuses_non_campaign(env):
    env.mp.delenv(storage.ADMITTED_ENV)
    with pytest.raises(ValueError, match='campaign-owned'):
        RecentStore.create(env.admitted / 'store', NOW)
    assert not (env.admitted / 'store').exists()


def test_admitted_root_containing_campaign_refused(env):
    env.mp.setenv(storage.ADMITTED_ENV, str(env.tmp / 'campaigns'))
    with pytest.raises(ValueError, match='overlaps a campaign root'):
        storage.owned(env.tmp / 'campaigns' / 'other' / 'store')


def test_admitted_root_inside_campaign_refused(env):
    inner = env.campaign / 'inner'; inner.mkdir()
    env.mp.setenv(storage.ADMITTED_ENV, str(inner))
    with pytest.raises(ValueError, match='overlaps a campaign root'):
        storage.admitted_root()


def test_unset_reserves_refused_before_effect(env):
    env.mp.delenv(storage.RESERVES_ENV)
    with pytest.raises(RuntimeError, match='reserve_policy_unconfigured'):
        RecentStore.create(env.admitted / 'store', NOW)
    assert not (env.admitted / 'store').exists()


@pytest.mark.parametrize('raw', ['', '[]', '{', '[{"mount":"/"}]', '[{"mount":"/","bytes":0}]',
                                 '[{"mount":"rel","bytes":1}]', '[{"mount":"/","bytes":true}]',
                                 '[{"mount":"/","bytes":1},{"mount":"/","bytes":2}]'])
def test_malformed_or_empty_reserves_refused(env, raw):
    env.mp.setenv(storage.RESERVES_ENV, raw)
    with pytest.raises(RuntimeError, match='reserve_policy_(malformed|unconfigured)'):
        storage.reserve_policy(env.admitted / 'store')


def test_missing_mount_refused(env):
    env.mp.setenv(storage.RESERVES_ENV, json.dumps([{'mount': str(env.tmp / 'absent'), 'bytes': 1}]))
    with pytest.raises(RuntimeError, match='reserve_policy_mount_missing'):
        RecentStore.create(env.admitted / 'store', NOW)


def test_reserves_file(env):
    path = env.tmp / 'reserves.json'; path.write_text(SMALL)
    env.mp.delenv(storage.RESERVES_ENV); env.mp.setenv(storage.RESERVES_FILE_ENV, str(path))
    assert storage.reserve_policy(env.admitted / 'store')['reserves'] == [{'mount': '/', 'bytes': 1}]


def test_reserve_and_inode_floor_refuse(env):
    store = RecentStore.create(env.admitted / 'store', NOW)
    huge = os.statvfs('/').f_bavail * os.statvfs('/').f_frsize * 4
    env.mp.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': huge}]))
    with pytest.raises(RuntimeError, match='shared_reserve_unavailable'):
        store.check_local()
    env.mp.setenv(storage.RESERVES_ENV, SMALL); env.mp.setenv(storage.INODES_ENV, str(10**15))
    with pytest.raises(RuntimeError, match='shared_reserve_unavailable'):
        store.check_local()


def test_store_filesystem_must_be_covered(env):
    other = Path('/dev/shm')
    if not other.is_dir() or os.stat(other).st_dev == os.stat(env.admitted).st_dev:
        pytest.skip('no second filesystem')
    store = RecentStore.create(env.admitted / 'store', NOW)
    env.mp.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': 1}, {'mount': str(other), 'bytes': 1}]))
    store.check_local()
    env.mp.setenv(storage.RESERVES_ENV, json.dumps([{'mount': str(other), 'bytes': 1}]))
    env.mp.setenv(storage.OVERRIDE_ENV, hashlib.sha256(recorded(store).encode()).hexdigest())
    with pytest.raises(RuntimeError, match='without configured reserve'):
        store.check_local()


def test_weaker_policy_refused_at_open_unless_acknowledged(env):
    store = RecentStore.create(env.admitted / 'store', NOW)
    record = recorded(store)
    env.mp.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': 2}]))
    RecentStore(store.root)  # stronger is accepted
    env.mp.setenv(storage.INODES_ENV, '0')
    with pytest.raises(RuntimeError, match='weaker_than_recorded'):
        RecentStore(store.root)
    env.mp.setenv(storage.INODES_ENV, '1')
    other = str(env.tmp)  # a policy that drops the recorded '/' entry is weaker
    env.mp.setenv(storage.RESERVES_ENV, json.dumps([{'mount': other, 'bytes': 10}]))
    with pytest.raises(RuntimeError, match='weaker_than_recorded'):
        RecentStore(store.root)
    env.mp.setenv(storage.OVERRIDE_ENV, 'not-the-hash')
    with pytest.raises(RuntimeError, match='weaker_than_recorded'):
        RecentStore(store.root)
    env.mp.setenv(storage.OVERRIDE_ENV, hashlib.sha256(record.encode()).hexdigest())
    RecentStore(store.root)
    assert recorded(store) == record  # acknowledgement never rewrites the record


def test_admitted_store_without_record_refused(env):
    store = RecentStore.create(env.admitted / 'store', NOW)
    with storage.connect(store.state) as c:
        c.execute('DELETE FROM meta WHERE key=?', (storage.RESERVES_KEY,)); c.commit()
    with pytest.raises(RuntimeError, match='record_missing'):
        RecentStore(store.root)


def test_campaign_default_preserves_crow_pair(env):
    env.mp.delenv(storage.RESERVES_ENV); env.mp.delenv(storage.INODES_ENV)
    if not Path('/data').is_dir():
        with pytest.raises(RuntimeError, match='mount_missing'):
            storage.reserve_policy(env.campaign / 'store')
        return
    policy = storage.reserve_policy(env.campaign / 'store')
    assert policy['reserves'] == [{'mount': '/', 'bytes': 64424509440}, {'mount': '/data', 'bytes': 64424509440}]
    assert policy['min_free_inodes'] == 1000000
    store = RecentStore.create(env.campaign / 'store', NOW)
    real = os.statvfs
    def low(path):
        s = real(path)
        if path == '/data':
            return SimpleNamespace(f_bavail=64424509439, f_frsize=1, f_favail=s.f_favail)
        return s
    env.mp.setattr(os, 'statvfs', low)
    with pytest.raises(RuntimeError, match='shared_reserve_unavailable'):
        store.check_local()
