"""Empty-store initialisation with an unknown-coverage roster seed."""
import datetime
import hashlib
import os
import subprocess
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage
import recent_init
from recent_storage import RecentStore

WALL = datetime.datetime(2026, 10, 8, 0, 5, tzinfo=datetime.timezone.utc)
NOW = '2026-10-08T00:05:00Z'
SEED = Path(__file__).parents[1] / 'deploy/roster-epoch2-seed.json'


@pytest.fixture(autouse=True)
def wall(monkeypatch):
    """Pin the wall clock the init command compares --now against."""
    class Fixed(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return WALL
    monkeypatch.setattr(recent_init.dt, 'datetime', Fixed)


@pytest.fixture
def admitted(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, 'ROOT', tmp_path / 'campaign' / 'runtime')
    root = tmp_path / 'admitted'; root.mkdir()
    for key in (storage.RESERVES_FILE_ENV, storage.OVERRIDE_ENV): monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(storage.ADMITTED_ENV, str(root))
    monkeypatch.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': 1}]))
    monkeypatch.setenv(storage.INODES_ENV, '1')
    return root


def roster(tmp_path, dids):
    path = tmp_path / 'roster.json'
    path.write_text(json.dumps({'schema': 'labelwatch.roster-seed.v1', 'count': len(dids), 'dids': dids}))
    return path


def test_creates_and_seeds_committed_roster(admitted, tmp_path, capsys):
    receipt_path = tmp_path / 'receipt.json'
    assert recent_init.main(['--root', str(admitted / 'store'), '--now', NOW, '--roster', str(SEED),
                             '--receipt', str(receipt_path)]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed == json.loads(receipt_path.read_text())
    seed = json.loads(SEED.read_text())
    assert printed['roster']['count'] == seed['count'] == len(seed['dids'])
    assert printed['roster']['sha256'] == hashlib.sha256(SEED.read_bytes()).hexdigest()
    store = RecentStore(admitted / 'store')
    sources = store.collector_sources(2049)
    assert [s['did'] for s in sources] == sorted(seed['dids'])
    assert all(s['endpoint'] is None and store.collector_cursor(s['did']) is None for s in sources)


def test_receipt_fields_and_acquisition_metadata(admitted, tmp_path):
    receipt = recent_init.init(admitted / 'store', NOW, roster=roster(tmp_path, ['did:plc:aaaaaaaaaaaaaaaaaaaaaaaa']))
    assert set(receipt) >= {'generation_id', 'started_at', 'basis', 'days', 'roster', 'reserves', 'source', 'versions'}
    assert receipt['days'] == 30 and receipt['basis'] == 'local_acceptance_clock'
    assert receipt['started_at'] == '2026-10-08T00:05:00.000000Z'
    assert receipt['reserves']['reserves'] == [{'mount': '/', 'bytes': 1}]
    assert receipt['roster']['count'] == 1 and receipt['roster']['coverage'] == 'unknown_until_bootstrap'
    assert receipt['source']['sha'] and set(receipt['versions']) >= {'python', 'sqlite', 'pyarrow', 'duckdb'}
    with storage.connect(admitted / 'store' / 'state.sqlite', readonly=True) as c:
        assert RecentStore.acquisition(c)['generation_id'] == receipt['generation_id']


def test_empty_placeholder_admitted_without_roster(admitted):
    (admitted / 'store').mkdir()
    assert recent_init.init(admitted / 'store', NOW)['roster']['count'] == 0


def test_refuses_non_empty_root(admitted):
    (admitted / 'store').mkdir(); (admitted / 'store' / 'state.sqlite').write_text('keep')
    with pytest.raises(RuntimeError, match='not empty'):
        recent_init.init(admitted / 'store', NOW)
    assert (admitted / 'store' / 'state.sqlite').read_text() == 'keep'


@pytest.mark.parametrize('dids', [['not-a-did'], ['did:plc:aaaaaaaaaaaaaaaaaaaaaaaa'] * 2])
def test_bad_roster_refused_before_effect(admitted, tmp_path, dids):
    with pytest.raises(ValueError, match='roster seed'):
        recent_init.init(admitted / 'store', NOW, roster=roster(tmp_path, dids))
    assert not (admitted / 'store').exists()


def test_refuses_outside_admitted_root(admitted, tmp_path):
    with pytest.raises(ValueError, match='campaign-owned'):
        recent_init.init(tmp_path / 'elsewhere', NOW)


def test_refuses_existing_receipt(admitted, tmp_path):
    receipt = tmp_path / 'receipt.json'; receipt.write_text('{}')
    with pytest.raises(SystemExit):
        recent_init.main(['--root', str(admitted / 'store'), '--now', NOW, '--receipt', str(receipt)])
    assert not (admitted / 'store').exists()


def test_committed_seed_is_valid():
    dids, _ = recent_init.load_roster(SEED)
    assert dids == sorted(dids) and len(dids) == 580


@pytest.mark.parametrize('offset', [-301, 301, -40 * 86400])
def test_refuses_clock_far_from_wall(admitted, offset):
    now = (WALL + datetime.timedelta(seconds=offset)).isoformat()
    with pytest.raises(ValueError, match='exceeds 300 s'):
        recent_init.init(admitted / 'store', now)
    assert not (admitted / 'store').exists()


def test_explicit_skew_allowance_is_bounded_and_recorded(admitted):
    now = (WALL - datetime.timedelta(seconds=900)).isoformat()
    with pytest.raises(ValueError, match='exceeds 600 s'):
        recent_init.init(admitted / 'store', now, allow_skew_seconds=600)
    assert not (admitted / 'store').exists()
    receipt = recent_init.init(admitted / 'store', now, allow_skew_seconds=900)
    assert receipt['acceptance_clock'] == {'requested': '2026-10-07T23:50:00.000000Z',
                                           'wall': '2026-10-08T00:05:00.000000Z', 'skew_seconds': -900.0,
                                           'allowed_skew_seconds': 900, 'explicit_allowance': True}
    assert receipt['started_at'] == '2026-10-07T23:50:00.000000Z'


def test_cli_skew_flag(admitted, capsys):
    now = (WALL + datetime.timedelta(seconds=400)).isoformat()
    with pytest.raises(ValueError):
        recent_init.main(['--root', str(admitted / 'store'), '--now', now])
    assert recent_init.main(['--root', str(admitted / 'store'), '--now', now, '--allow-clock-skew-seconds', '400']) == 0
    assert json.loads(capsys.readouterr().out)['acceptance_clock']['skew_seconds'] == 400.0


def test_refuses_when_reserve_does_not_hold_now(admitted, monkeypatch):
    s = os.statvfs('/')
    monkeypatch.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': s.f_bavail * s.f_frsize * 4}]))
    with pytest.raises(RuntimeError, match='shared_reserve_unavailable'):
        recent_init.init(admitted / 'store', NOW)
    assert not (admitted / 'store').exists()


def test_dirty_flag_counts_untracked(monkeypatch):
    calls = []
    def run(cmd, **kw):
        calls.append(cmd)
        out = 'abc\n' if 'rev-parse' in cmd else '?? new-file\n'
        return subprocess.CompletedProcess(cmd, 0, out, '')
    monkeypatch.setattr(recent_init.subprocess, 'run', run)
    assert recent_init.source_sha() == {'sha': 'abc', 'dirty': True, 'basis': 'git'}
    assert not any('--untracked-files=no' in c for c in calls)
