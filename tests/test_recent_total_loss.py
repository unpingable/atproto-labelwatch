"""Total store loss and re-init: the new epoch acknowledges the loss honestly.

Synthetic and local: stores under tmp_path, no network, no collection.
"""
import datetime
import json
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage  # noqa: E402
import recent_init  # noqa: E402
import recent_publish  # noqa: E402
from recent_storage import RecentStore, LEGACY_PREDECESSOR_CUTOFF  # noqa: E402
from recent_provider import RecentProvider  # noqa: E402
from labelwatch import recent_overview  # noqa: E402
from labelwatch.recent_observations import RecentObservations, GAP_REASONS  # noqa: E402
from test_recent_site import module, get  # noqa: E402

UTC = datetime.timezone.utc
T0 = datetime.datetime(2026, 10, 8, 0, 5, tzinfo=UTC)
T1 = datetime.datetime(2026, 10, 13, 1, 0, tzinfo=UTC)
NO_GAPS = 'No specific gaps are recorded'


def iso(t):
    return t.isoformat().replace('+00:00', 'Z')


@pytest.fixture
def admitted(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, 'ROOT', tmp_path / 'campaign' / 'runtime')
    root = tmp_path / 'admitted'; root.mkdir()
    for key in (storage.RESERVES_FILE_ENV, storage.OVERRIDE_ENV): monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(storage.ADMITTED_ENV, str(root))
    monkeypatch.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': 1}]))
    monkeypatch.setenv(storage.INODES_ENV, '1')
    return root


def init(root, when, **kw):
    return recent_init.init(root, iso(when), wall=when, **kw)


def lose(root):
    shutil.rmtree(root)


def pages(root, now):
    store = RecentStore(root)
    recent_publish.write_status(store, {'collection': {'round': {'status': 'completed'}}}, now=now)
    mod = module()
    product = RecentObservations(RecentProvider(store), now=lambda: now)
    h = mod.handler(product, status=mod.SiteStatus(product))
    return {path: get(h, path)[2].decode() for path in ('/', '/about', '/previous-epoch')}, product


def test_reinit_without_predecessor_says_previous_epoch_unknown(admitted):
    root = admitted / 'epoch'
    first = init(root, T0)
    lose(root)
    second = init(root, T1)
    assert second['predecessor']['recorded'] is False and second['generation_id'] != first['generation_id']
    out, _ = pages(root, T1 + datetime.timedelta(hours=1))
    for body in out.values():
        assert '2026-10-06' not in body
    assert 'The previous epoch, if any, is unknown' in out['/about']
    assert 'The previous epoch is unknown' in out['/previous-epoch']
    assert 'Continuity break' not in out['/']


def test_continuity_break_shows_observed_then_lost_and_no_gap_free_claim(admitted, tmp_path, capsys):
    root = admitted / 'epoch'
    init(root, T0)
    lose(root)
    assert recent_init.main(['--root', str(root), '--now', iso(datetime.datetime.now(UTC)), '--continuity-break']) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt['predecessor'] == {'schema': 'labelwatch.predecessor.v1', 'recorded': True, 'generation_id': None,
                                      'started_at': None, 'observed_through': None, 'ended_on': None,
                                      'end_reason': 'store_lost', 'basis': 'continuity_break'}
    started = datetime.datetime.fromisoformat(receipt['started_at'].replace('Z', '+00:00'))
    now = started + datetime.timedelta(hours=1)
    out, product = pages(root, now)
    landing = out['/']
    assert 'Continuity break' in landing and 'its extent is unknown' in landing
    assert 'Recorded gaps <strong>1</strong>' in landing
    assert GAP_REASONS['observed_then_lost'].replace("'", '&#x27;') in landing
    assert NO_GAPS not in landing
    assert 'was observed through an unknown time and then lost' in out['/about']
    assert 'The previous epoch was lost' in out['/previous-epoch'] and '2026-10-06' not in out['/previous-epoch']
    # The overview and account manifests carry the same interval, never as quiet.
    env = recent_overview.overview(recent_publish.read_store(RecentStore(root), now), now)
    assert [g['reason'] for g in env['coverage']['gaps']] == ['observed_then_lost']
    lo = iso(now - datetime.timedelta(days=29))
    answer = product.create('did:plc:subject', lo, RecentStore(root).frontier()['end'])
    gaps = product.page(answer['cursor'])['manifest']['coverage']['gaps']
    assert 'observed_then_lost' in {g['reason'] for g in gaps}


def test_predecessor_receipt_records_known_bounds(admitted, tmp_path):
    root = admitted / 'epoch'
    receipt_path = tmp_path / 'epoch1-receipt.json'
    first = init(root, T0)
    receipt_path.write_text(json.dumps(first))
    lose(root)
    second = init(root, T1, predecessor=recent_init.predecessor_from_receipt(receipt_path))
    pred = second['predecessor']
    assert (pred['generation_id'], pred['started_at'], pred['observed_through'], pred['basis']) == (
        first['generation_id'], first['started_at'], None, 'predecessor_receipt')
    out, _ = pages(root, T1 + datetime.timedelta(hours=1))
    assert 'Continuity break' in out['/'] and NO_GAPS not in out['/']
    assert 'Oct 08, 2026 at 00:05 UTC' in out['/previous-epoch'] and first['generation_id'] in out['/previous-epoch']
    assert 'A previous epoch, which began on Oct 08, 2026 at 00:05 UTC, was observed through an unknown time' in out['/about']
    # A status or overview from the lost store also bounds the end of what it observed.
    status = tmp_path / 'STATUS.json'
    status.write_text(json.dumps({'schema': 'labelwatch.recent-status.v1',
                                  'acquisition': {'generation_id': first['generation_id'], 'started_at': first['started_at']},
                                  'coverage': {'observed_through': '2026-10-10T12:00:00Z'}}))
    lose(root)
    init(root, T1, predecessor=recent_init.predecessor_from_receipt(status))
    gaps = RecentStore(root)
    stats = recent_publish.read_store(gaps, T1 + datetime.timedelta(hours=1))
    assert [g for g in stats['gaps'] if g['reason'] == 'observed_then_lost'] == [
        {'start': '2026-10-08T00:05:00.000000Z', 'end': '2026-10-10T12:00:00.000000Z', 'reason': 'observed_then_lost'}]
    held = tmp_path / 'held.json'
    held.write_text(json.dumps(recent_overview.held(T1)))
    with pytest.raises(ValueError):
        recent_init.predecessor_from_receipt(held)


def test_store_without_predecessor_meta_keeps_the_2026_10_06_epoch(admitted):
    # The production store predates predecessor records: no meta key, started before the cutoff.
    legacy = RecentStore.create(admitted / 'legacy', iso(T0))
    assert legacy.predecessor()['basis'] == 'legacy_default'
    out, _ = pages(legacy.root, T0 + datetime.timedelta(hours=1))
    assert 'ended on 2026-10-06' in out['/previous-epoch'] and 'withdrawn' in out['/previous-epoch']
    assert 'Observations from the previous epoch, which ended 2026-10-06, are not part of this service.' in out['/about']
    assert NO_GAPS in out['/'] and 'Continuity break' not in out['/']
    # The default is not extended to stores begun after this change.
    later = datetime.datetime.fromisoformat(LEGACY_PREDECESSOR_CUTOFF.replace('Z', '+00:00')) + datetime.timedelta(days=1)
    assert RecentStore.create(admitted / 'later', iso(later)).predecessor()['recorded'] is False


def test_reader_against_empty_store_dir_leaves_nothing_and_init_proceeds(admitted):
    root = admitted / 'epoch'
    init(root, T0)
    lose(root)
    root.mkdir()                     # an emptied store directory after the loss
    mod = module()
    product = RecentObservations(RecentProvider(RecentStore(root)), now=lambda: T1)
    h = mod.handler(product, status=mod.SiteStatus(product))
    for path in ('/', '/about', '/previous-epoch', '/health'):
        get(h, path)
    with pytest.raises(FileNotFoundError):
        recent_publish.read_store(RecentStore(root), T1)
    assert list(root.iterdir()) == []
    assert storage.admitted_root() == admitted
    assert init(root, T1)['predecessor']['recorded'] is False
