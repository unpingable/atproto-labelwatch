"""Ordinary writers refuse absent state without obstructing explicit init."""
import datetime
from contextlib import closing
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage
import recent_init
import recent_maintain
from recent_storage import RecentStore
from labelwatch.recent_collector import Collector

NOW = datetime.datetime(2026, 11, 3, 12, tzinfo=datetime.timezone.utc)
STAMP = '2026-11-03T12:00:00Z'


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, 'ROOT', tmp_path / 'campaign/runtime')
    admitted = tmp_path / 'admitted'
    admitted.mkdir()
    monkeypatch.setenv(storage.ADMITTED_ENV, str(admitted))
    mounts = [p for p in ('/', '/data') if Path(p).is_dir()]
    monkeypatch.setenv(storage.RESERVES_ENV, json.dumps([{'mount': p, 'bytes': 1} for p in mounts]))
    monkeypatch.setenv(storage.INODES_ENV, '1')
    for key in (storage.RESERVES_FILE_ENV, storage.OVERRIDE_ENV):
        monkeypatch.delenv(key, raising=False)
    root = admitted / 'epoch'
    root.mkdir()
    return root


class NoRequests:
    def __init__(self):
        self.calls = 0

    def request(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError('missing store must refuse before any request')


@pytest.mark.parametrize('operation', ['collector', 'discovery', 'gap', 'maintenance'])
def test_missing_state_writer_refuses_cleanly_and_same_root_init_works(root, operation):
    store = RecentStore(root)
    transport = NoRequests()
    with pytest.raises((OSError, sqlite3.DatabaseError, RuntimeError)):
        if operation == 'collector':
            Collector(store, transport, now=lambda: NOW).run_round(discover=True)
        elif operation == 'discovery':
            store.record_discovery_outcome({'status': 'refused', 'reason': 'TimeoutError'}, STAMP)
        elif operation == 'maintenance':
            recent_maintain.main(['--store', str(root), '--now', STAMP])
        else:
            store.record_gap('2026-11-03T11:59:00Z', STAMP, 'unknown')
    assert list(root.iterdir()) == []
    assert transport.calls == 0
    receipt = recent_init.init(root, STAMP, wall=NOW)
    assert receipt['started_at'] == '2026-11-03T12:00:00.000000Z'
    with closing(storage.connect(root / 'state.sqlite', readonly=True)) as c:
        assert RecentStore(root).acquisition(c)['generation_id'] == receipt['generation_id']


@pytest.mark.parametrize('name', ['reader.lock', 'writer.lock', 'daily.lock',
                                  'archive.lock', 'archive-2026-11-03.lock'])
def test_lock_refuses_missing_state_without_creating_file(root, name):
    with pytest.raises(FileNotFoundError):
        with storage.lock(root, name=name):
            pytest.fail('missing store was locked')
    assert list(root.iterdir()) == []


def test_writer_connection_cannot_create_missing_state(root):
    with pytest.raises((OSError, sqlite3.DatabaseError)):
        storage.connect(root / 'state.sqlite')
    assert list(root.iterdir()) == []


def test_state_removed_after_writer_lock_is_not_recreated(root):
    recent_init.init(root, STAMP, wall=NOW)
    with storage.lock(root):
        inode = (root / 'writer.lock').stat().st_ino
        (root / 'state.sqlite').unlink()
        with pytest.raises((OSError, sqlite3.DatabaseError)):
            storage.connect(root / 'state.sqlite')
        assert not (root / 'state.sqlite').exists()
        assert (root / 'writer.lock').stat().st_ino == inode


def test_existing_writer_lock_still_fences_a_second_writer(root):
    recent_init.init(root, STAMP, wall=NOW)
    with storage.lock(root):
        inode = (root / 'writer.lock').stat().st_ino
        with pytest.raises(TimeoutError, match='lease deadline exceeded'):
            with storage.lock(root, timeout=.02):
                pytest.fail('concurrent writer was admitted')
    with storage.lock(root, timeout=.02):
        assert (root / 'writer.lock').stat().st_ino == inode
