"""Review r1 (2026-10-08): failing tests that demonstrate defects; not fixes."""
import json
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage  # noqa: E402
from storage import connect, db  # noqa: E402
from recent_storage import RecentStore  # noqa: E402
from labelwatch.recent_collector import Collector, Limits  # noqa: E402

A = 'did:plc:labelera'


@pytest.fixture
def store(tmp_path, monkeypatch):
    root = tmp_path / 'runtime'; root.mkdir()
    evidence = tmp_path / 'evidence'; evidence.mkdir()
    (evidence / 'ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive': str(root / 'store/archive')}))
    monkeypatch.setattr(storage, 'ROOT', root)
    return RecentStore.create(root / 'store', '2026-09-01T00:00:00Z')


def meta(store, key):
    with closing(connect(store.state, readonly=True)) as c:
        return db.get_meta(c, key)


def test_gap_overflow_is_cleared_once_its_gaps_expire(store):
    """129 gaps on day one overflow the 128-row table; after they all leave the
    30-day window the store still discloses the whole window as unknown forever."""
    base = datetime(2026, 9, 1, 1, tzinfo=timezone.utc)
    for n in range(129):
        lo = base + timedelta(seconds=10 * n)
        store.record_gap(lo.isoformat(), (lo + timedelta(seconds=1)).isoformat(), 'source_unavailable')
    assert meta(store, 'q:recent_gap_overflow') == '1'
    store.advance_day('2026-10-05T00:00:00Z')
    with closing(connect(store.state, readonly=True)) as c:
        assert c.execute('SELECT COUNT(*) FROM q_recent_gaps').fetchone()[0] == 0
    assert meta(store, 'q:recent_gap_overflow') == '0'


class Transport:
    def request(self, operation, params, **limits):
        return json.dumps({'labels': [{'src': A, 'uri': 'did:plc:subject', 'val': 'x',
                                       'cts': '2026-09-01T23:59:00Z'}]}).encode()


def test_pre_maintenance_refusal_is_not_recorded_as_source_unavailable(store):
    """Between 00:00 and the 00:05 maintenance run the store refuses ingestion;
    the collector records that local refusal as a source outage."""
    store.remember_sources([{'did': A, 'endpoint': 'https://labeler.example'}])
    now = datetime(2026, 9, 2, 0, 1, tzinfo=timezone.utc)
    result = Collector(store, Transport(), now=lambda: now, limits=Limits(pages=1)).tick()
    assert result['results'][0]['status'] == 'refused'
    with closing(connect(store.state, readonly=True)) as c:
        reasons = [r[0] for r in c.execute('SELECT reason FROM q_recent_gaps')]
    assert reasons and 'source_unavailable' not in reasons


@pytest.mark.xfail(strict=True, reason="other lane: publication/reserve owners fix this, not the collector lane")
def test_world_writable_publication_directory_is_refused(tmp_path):
    """The brief requires an explicit 0755 directory; a 0777 directory lets any
    local account swap overview.json between publications, yet it is accepted."""
    from labelwatch import recent_overview
    pubdir = tmp_path / 'pub'; pubdir.mkdir(); pubdir.chmod(0o777)
    envelope = recent_overview.held(datetime(2026, 10, 8, tzinfo=timezone.utc), signature='review')
    with pytest.raises(recent_overview.OverviewRefused):
        recent_overview.publish(pubdir, envelope, ancestors=False)


@pytest.mark.xfail(strict=True, reason="other lane: publication/reserve owners fix this, not the collector lane")
def test_override_ack_is_bound_to_the_weakened_policy(tmp_path, monkeypatch):
    """The ack is sha256(recorded policy) only: once present in the env file it
    silently admits any further weakening, down to a 1-byte reserve."""
    import hashlib
    admitted = tmp_path / 'admitted'; admitted.mkdir()
    for key in (storage.RESERVES_FILE_ENV, storage.OVERRIDE_ENV):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(storage.ADMITTED_ENV, str(admitted))
    monkeypatch.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': 1000}]))
    monkeypatch.setenv(storage.INODES_ENV, '1')
    store = RecentStore.create(admitted / 'store', '2026-10-08T00:00:00Z')
    with closing(connect(store.state, readonly=True)) as c:
        record = db.get_meta(c, storage.RESERVES_KEY)
    monkeypatch.setenv(storage.OVERRIDE_ENV, hashlib.sha256(record.encode()).hexdigest())
    monkeypatch.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': 500}]))
    RecentStore(store.root)  # the reviewed weakening
    monkeypatch.setenv(storage.RESERVES_ENV, json.dumps([{'mount': '/', 'bytes': 1}]))
    with pytest.raises(RuntimeError, match='weaker'):
        RecentStore(store.root)  # an unreviewed further weakening
