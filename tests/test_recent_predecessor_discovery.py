"""Init discovers its predecessor from the published overview; uncertainty is the default.

Synthetic and local: stores and publish directories under tmp_path, no network.
"""
import datetime
import json

import pytest

from test_recent_total_loss import admitted, init, iso, lose, pages, NO_GAPS, T0  # noqa: F401  (sets sys.path)
import recent_init  # noqa: E402
import recent_publish  # noqa: E402
from recent_storage import RecentStore  # noqa: E402
from labelwatch import recent_overview  # noqa: E402

UTC = datetime.timezone.utc
CONTINUITY_UNKNOWN = 'Continuity before this epoch is unknown'


def now():
    return datetime.datetime.now(UTC)


def run_init(root, capsys, *flags):
    assert recent_init.main(['--root', str(root), '--now', iso(now()), *flags]) == 0
    return json.loads(capsys.readouterr().out)


@pytest.fixture
def pubdir(tmp_path, monkeypatch):
    pub = tmp_path / 'pub'
    monkeypatch.setenv(recent_init.PUBDIR_ENV, str(pub))
    publish = recent_overview.publish
    monkeypatch.setattr(recent_overview, 'publish', lambda d, e: publish(d, e, ancestors=False))
    return pub


def publish_overview(root, pub, when, capsys):
    assert recent_publish.main(['--store', str(root), '--pubdir', str(pub), '--now', iso(when)]) == 0
    capsys.readouterr()
    return json.loads((pub / 'overview.json').read_text())


def assert_continuity_unknown(root, receipt, outcome):
    assert receipt['predecessor']['recorded'] is False and receipt['predecessor']['basis'] == 'undiscovered'
    assert receipt['predecessor_discovery']['outcome'] == outcome
    started = datetime.datetime.fromisoformat(receipt['started_at'].replace('Z', '+00:00'))
    out, _ = pages(root, started + datetime.timedelta(hours=1))
    assert CONTINUITY_UNKNOWN in out['/'] and NO_GAPS not in out['/']
    assert CONTINUITY_UNKNOWN in out['/about'] and CONTINUITY_UNKNOWN in out['/previous-epoch']
    assert 'Continuity break' not in out['/']


def test_first_install_flag_records_no_predecessor_and_no_uncertainty(admitted, pubdir, capsys):
    root = admitted / 'epoch'
    receipt = run_init(root, capsys, '--first-install')
    assert receipt['predecessor']['basis'] == 'first_install' and receipt['predecessor']['recorded'] is False
    assert receipt['predecessor_discovery'] is None
    started = datetime.datetime.fromisoformat(receipt['started_at'].replace('Z', '+00:00'))
    out, _ = pages(root, started + datetime.timedelta(hours=1))
    assert NO_GAPS in out['/'] and CONTINUITY_UNKNOWN not in ''.join(out.values())
    assert 'Continuity break' not in out['/'] and 'there is no previous epoch' in out['/about']
    assert 'There is no previous epoch' in out['/previous-epoch']


def test_discovers_predecessor_from_published_overview(admitted, pubdir, capsys):
    root = admitted / 'epoch'
    first = init(root, T0)
    published = publish_overview(root, pubdir, T0 + datetime.timedelta(hours=2), capsys)
    # Collection would set observed_through; stand in for one accepted page.
    published['coverage']['observed_through'] = iso(T0 + datetime.timedelta(hours=1))
    (pubdir / 'overview.json').write_text(json.dumps(published))
    lose(root)
    receipt = run_init(root, capsys)
    pred = receipt['predecessor']
    assert (pred['recorded'], pred['generation_id'], pred['started_at'], pred['end_reason'], pred['basis']) == (
        True, first['generation_id'], first['started_at'], 'store_lost', 'published_overview')
    assert pred['observed_through'] == '2026-10-08T01:05:00.000000Z'
    assert receipt['predecessor_discovery'] == {'path': str(pubdir / 'overview.json'), 'outcome': 'discovered'}
    started = receipt['started_at']
    later = datetime.datetime.fromisoformat(started.replace('Z', '+00:00')) + datetime.timedelta(hours=1)
    # Observed then lost through the new epoch's start: the predecessor may have kept observing after it last published.
    gaps = recent_publish.read_store(RecentStore(root), later)['gaps']
    assert [g for g in gaps if g['reason'] == 'observed_then_lost'] == [
        {'start': first['started_at'], 'end': started, 'reason': 'observed_then_lost'}]
    out, _ = pages(root, later)
    assert 'Continuity break' in out['/'] and NO_GAPS not in out['/'] and CONTINUITY_UNKNOWN not in out['/']
    assert first['generation_id'] in out['/previous-epoch'] and 'Oct 08, 2026 at 01:05 UTC' in out['/previous-epoch']


def test_held_overview_records_unknown_continuity(admitted, pubdir, capsys):
    root = admitted / 'epoch'
    pubdir.mkdir()
    (pubdir / 'overview.json').write_text(json.dumps(recent_overview.held(T0)))
    assert_continuity_unknown(root, run_init(root, capsys), 'held')


def test_missing_overview_records_unknown_continuity(admitted, pubdir, capsys):
    root = admitted / 'epoch'
    assert_continuity_unknown(root, run_init(root, capsys), 'absent')


def test_no_publish_location_records_unknown_continuity(admitted, monkeypatch, capsys):
    monkeypatch.delenv(recent_init.PUBDIR_ENV, raising=False)
    root = admitted / 'epoch'
    receipt = run_init(root, capsys)
    assert receipt['predecessor_discovery'] == {'path': None, 'outcome': 'no_publish_location'}
    assert_continuity_unknown(root, receipt, 'no_publish_location')


def test_corrupt_overview_records_unknown_continuity(admitted, pubdir, capsys):
    root = admitted / 'epoch'
    pubdir.mkdir()
    (pubdir / 'overview.json').write_text('{"schema_version": "labelwatch.recent-ov')
    assert_continuity_unknown(root, run_init(root, capsys), 'unreadable')


def test_overview_without_generation_records_unknown_continuity(admitted, pubdir, capsys):
    root = admitted / 'epoch'
    init(root, T0)
    published = publish_overview(root, pubdir, T0 + datetime.timedelta(hours=1), capsys)
    published['acquisition'] = None
    (pubdir / 'overview.json').write_text(json.dumps(published))
    lose(root)
    assert_continuity_unknown(root, run_init(root, capsys), 'no_generation')


def test_published_overview_flag_overrides_configured_location(admitted, pubdir, tmp_path, capsys):
    root = admitted / 'epoch'
    first = init(root, T0)
    other = tmp_path / 'elsewhere'
    publish_overview(root, other, T0 + datetime.timedelta(hours=1), capsys)
    lose(root)
    receipt = run_init(root, capsys, '--published-overview', str(other / 'overview.json'))
    assert receipt['predecessor']['generation_id'] == first['generation_id']
    assert receipt['predecessor_discovery']['path'] == str(other / 'overview.json')


@pytest.mark.parametrize('flag', ['--first-install', '--continuity-break', '--predecessor-receipt'])
def test_explicit_flag_overrides_discovery(admitted, pubdir, tmp_path, capsys, flag):
    root = admitted / 'epoch'
    first = init(root, T0)
    receipt_path = tmp_path / 'receipt.json'
    receipt_path.write_text(json.dumps(first))
    publish_overview(root, pubdir, T0 + datetime.timedelta(hours=1), capsys)
    lose(root)
    args = [flag, str(receipt_path)] if flag == '--predecessor-receipt' else [flag]
    receipt = run_init(root, capsys, *args)
    assert receipt['predecessor_discovery'] is None
    assert receipt['predecessor']['basis'] == {'--first-install': 'first_install', '--continuity-break': 'continuity_break',
                                                '--predecessor-receipt': 'predecessor_receipt'}[flag]


@pytest.mark.parametrize('flags', [
    ['--first-install', '--continuity-break'],
    ['--first-install', '--predecessor-receipt', 'r.json'],
    ['--continuity-break', '--predecessor-receipt', 'r.json'],
    ['--first-install', '--published-overview', 'o.json'],
    ['--continuity-break', '--published-overview', 'o.json'],
])
def test_conflicting_flags_are_refused(admitted, pubdir, capsys, flags):
    root = admitted / 'epoch'
    with pytest.raises(SystemExit) as refused:
        recent_init.main(['--root', str(root), '--now', iso(now()), *flags])
    assert refused.value.code == 2 and not root.exists()


def test_compatibility_store_without_predecessor_key_is_unchanged(admitted):
    legacy = RecentStore.create(admitted / 'legacy', iso(T0))
    assert legacy.predecessor()['basis'] == 'legacy_default'
    out, _ = pages(legacy.root, T0 + datetime.timedelta(hours=1))
    assert NO_GAPS in out['/'] and 'Continuity break' not in out['/']
    assert CONTINUITY_UNKNOWN not in ''.join(out.values())
    assert 'ended on 2026-10-06' in out['/previous-epoch']
