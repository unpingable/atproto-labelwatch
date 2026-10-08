"""Empty-store initialisation with an unknown-coverage roster seed."""
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage
import recent_init
from recent_storage import RecentStore

NOW = '2026-10-08T00:05:00Z'
SEED = Path(__file__).parents[1] / 'deploy/roster-epoch2-seed.json'


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
