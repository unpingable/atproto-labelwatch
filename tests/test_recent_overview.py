"""Aggregate estate overview: schema, privacy boundary, atomic world-readable publication."""
import json
import os
import re
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage  # noqa: E402
from storage import custody  # noqa: E402
from recent_storage import RecentStore  # noqa: E402
import recent_publish  # noqa: E402
from labelwatch import recent_overview  # noqa: E402

A, B, C = 'did:plc:labelera', 'did:plc:labelerb', 'did:web:labelerc.example'
SUBJECT = 'did:plc:privatesubject'


def row(labeler, value, subject=SUBJECT):
    raw = {'src': labeler, 'uri': subject, 'val': value, 'cts': '1999-01-01T00:00:00Z'}
    return (labeler, labeler, subject, None, value, 0, None, None, '1999-01-01T00:00:00Z',
            custody.canonical_identity(raw), subject)


@pytest.fixture
def store(tmp_path, monkeypatch):
    root = tmp_path / 'runtime'; root.mkdir()
    evidence = tmp_path / 'evidence'; evidence.mkdir()
    (evidence / 'ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive': str(root / 'store/archive')}))
    monkeypatch.setattr(storage, 'ROOT', root)
    s = RecentStore.create(root / 'store', '2026-09-01T00:00:00Z')
    s.remember_sources([{'did': A}, {'did': B}, {'did': C}])
    s.ingest([row(A, 'early')], 'fixture', 'c1', '2026-09-01T00:01:00Z')
    s.ingest([row(A, 'late'), row(B, 'late')], 'fixture', 'c2', '2026-09-01T13:00:00Z')
    s.advance_day('2026-09-02T00:00:00Z')  # 2026-09-01 becomes a retired owner read from parquet
    s.ingest([row(B, 'next'), row(C, 'next')], 'fixture', 'c3', '2026-09-02T12:00:00Z')
    s.record_gap('2026-09-02T01:00:00Z', '2026-09-02T02:00:00Z', 'source_unavailable')
    return s


NOW = datetime(2026, 9, 8, 12, 30, tzinfo=timezone.utc)


def test_envelope_schema_counts_and_top_labelers(store):
    stats = recent_publish.read_store(store, NOW)
    env = recent_overview.overview(stats, NOW, signature='abc1234')
    assert set(env) == recent_overview.ENVELOPE_KEYS
    assert env['schema_version'] == 'labelwatch.recent-overview.v1' and env['window_days'] == 30
    assert env['hold'] is False and env['state'] == {'current_label_state': 'unknown'}
    assert env['build_signature'] == 'abc1234'
    assert env['acquisition']['started_at'] == '2026-09-01T00:00:00.000000Z'
    assert env['acquisition']['basis'] == 'local_acceptance_clock'
    # 7d cut 2026-09-01T12:30Z splits the archived first day by observation clock.
    assert env['counts'] == {'events_24h': 0, 'events_7d': 4, 'events_window': 5,
                             'labelers_active_7d': 3, 'labelers_active_window': 3}
    assert env['top_labelers_7d'] == [{'labeler_did': B, 'count': 2}, {'labeler_did': A, 'count': 1},
                                      {'labeler_did': C, 'count': 1}]
    cov = env['coverage']
    assert cov['status'] == 'warming_up' and cov['observed_days'] == 7
    assert cov['warmup'] == {'start': '2026-08-09T12:30:00.000000Z', 'end': '2026-09-01T00:00:00.000000Z',
                             'reason': 'not_observed'}
    assert cov['gaps'] == [{'start': '2026-09-02T01:00:00.000000Z', 'end': '2026-09-02T02:00:00.000000Z',
                            'reason': 'source_unavailable'}]
    assert cov['sources'] == {'seeded': 3, 'established': 0, 'probing': None, 'refused': None,
                              'pre_epoch_skipped': None}
    assert cov['observed_through'] == '2026-09-02T12:00:00.000000Z'
    assert stats['last_maintenance']['generation'] == 1


def test_no_private_fields_or_subject_dids(store):
    env = recent_overview.overview(recent_publish.read_store(store, NOW), NOW)
    body = json.dumps(env)
    dids = set(re.findall(r'did:[a-z0-9]+:[A-Za-z0-9._:%-]+', body))
    assert dids == {r['labeler_did'] for r in env['top_labelers_7d']}
    for private in (SUBJECT, 'at://', 'early', 'late', 'next', 'quarantin', 'probe', 'endpoint', 'cursor', str(store.root)):
        assert private not in body
    leaked = dict(env, top_labelers_7d=[{'labeler_did': SUBJECT, 'count': 1, 'uri': 'x'}])
    with pytest.raises(recent_overview.OverviewRefused):
        recent_overview.validate(leaked)
    leaked = json.loads(body); leaked['coverage']['gaps'].append({'start': 'x', 'end': 'y', 'reason': SUBJECT})
    with pytest.raises(recent_overview.OverviewRefused):
        recent_overview.validate(leaked)


def test_steady_state_has_no_warmup_and_reports_gaps():
    stats = {'acquisition': {'generation_id': '12345678-1234-4234-9234-123456789012',
                             'started_at': '2026-08-01T00:00:00Z', 'basis': 'local_acceptance_clock'},
             'counts': dict.fromkeys(recent_overview.COUNT_KEYS, 0), 'gaps': [], 'sources': {},
             'top_labelers_7d': [], 'observed_through': None}
    env = recent_overview.overview(stats, NOW, signature='x')
    assert env['coverage']['warmup'] is None and env['coverage']['observed_days'] == 30
    assert env['coverage']['status'] == 'no_recorded_gaps'
    stats['gap_overflow'] = True
    env = recent_overview.overview(stats, NOW, signature='x')
    assert env['coverage']['status'] == 'gapped' and env['coverage']['gap_overflow'] is True
    assert env['coverage']['gaps'] == [{'start': '2026-08-09T12:30:00.000000Z', 'end': '2026-09-08T12:30:00.000000Z',
                                        'reason': 'unknown'}]


def test_atomic_replace_and_modes(tmp_path, store):
    pubdir = tmp_path / 'pub'
    old = os.umask(0o077)  # a restrictive service umask must not leak into publication
    try:
        first = recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)
        inode = first.stat().st_ino
        env = recent_overview.overview(recent_publish.read_store(store, NOW), NOW)
        target = recent_overview.publish(pubdir, env, ancestors=False)
    finally:
        os.umask(old)
    assert stat.S_IMODE(pubdir.stat().st_mode) == 0o755
    assert stat.S_IMODE(target.stat().st_mode) == 0o644
    assert target.stat().st_ino != inode  # replaced, not rewritten in place
    assert json.loads(target.read_text()) == env
    assert sorted(p.name for p in pubdir.iterdir()) == ['overview.json']


def test_refuses_untraversable_directory_without_partial_publish(tmp_path, store, capsys):
    pubdir = tmp_path / 'pub'
    recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)
    before = (pubdir / 'overview.json').read_bytes()
    os.chmod(pubdir, 0o700)
    with pytest.raises(recent_overview.OverviewRefused, match='world-traversable'):
        recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)
    code = recent_publish.main(['--store', str(store.root), '--pubdir', str(pubdir), '--now', NOW.isoformat()])
    assert code == 2 and '"published": false' in capsys.readouterr().out
    os.chmod(pubdir, 0o755)
    assert (pubdir / 'overview.json').read_bytes() == before
    assert sorted(p.name for p in pubdir.iterdir()) == ['overview.json']


def test_refuses_untraversable_ancestor(tmp_path):
    parent = tmp_path / 'private'; parent.mkdir(mode=0o700); os.chmod(parent, 0o700)
    with pytest.raises(recent_overview.OverviewRefused, match='ancestor'):
        recent_overview.publish(parent / 'pub', recent_overview.held(NOW))
    assert not (parent / 'pub' / 'overview.json').exists()


def test_refuses_existing_private_file_mode(tmp_path):
    pubdir = tmp_path / 'pub'
    recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)
    os.chmod(pubdir / 'overview.json', 0o600)
    with pytest.raises(recent_overview.OverviewRefused, match='0644'):
        recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)


def test_hold_artifact_via_command(tmp_path, monkeypatch, capsys):
    pubdir = tmp_path / 'pub'
    publish = recent_overview.publish
    monkeypatch.setattr(recent_overview, 'publish', lambda d, e: publish(d, e, ancestors=False))
    assert recent_publish.main(['--pubdir', str(pubdir), '--hold', '--now', '2026-10-08T00:00:00Z']) == 0
    env = json.loads((pubdir / 'overview.json').read_text())
    assert env['hold'] is True and env['counts'] == {} and env['top_labelers_7d'] == []
    assert env['acquisition'] is None and env['coverage']['status'] == 'held'
    assert env['generated_at'] == '2026-10-08T00:00:00.000000Z'
    assert set(env) == recent_overview.ENVELOPE_KEYS
    with pytest.raises(recent_overview.OverviewRefused):
        recent_overview.validate(dict(env, counts={'events_7d': 3}))


def test_maintenance_recovery_pending_refuses(store):
    from storage import connect
    with connect(store.state) as c:
        c.execute("INSERT INTO q_recent_transition VALUES (1,'{}')")
    with pytest.raises(RuntimeError, match='maintenance'):
        recent_publish.read_store(store, NOW)


def test_hold_and_activation_markers_publish_held_artifact(tmp_path, monkeypatch, store):
    publish = recent_overview.publish
    monkeypatch.setattr(recent_overview, 'publish', lambda d, e: publish(d, e, ancestors=False))
    pubdir, hold, active = tmp_path / 'pub', tmp_path / 'safe-hold', tmp_path / 'activation'
    argv = ['--store', str(store.root), '--pubdir', str(pubdir), '--now', NOW.isoformat(),
            '--hold-marker', str(hold), '--activation-marker', str(active)]
    assert recent_publish.main(argv) == 0  # not activated: held
    assert json.loads((pubdir / 'overview.json').read_text())['hold'] is True
    active.touch()
    assert recent_publish.main(argv) == 0
    assert json.loads((pubdir / 'overview.json').read_text())['hold'] is False
    hold.touch()
    assert recent_publish.main(argv) == 0
    assert json.loads((pubdir / 'overview.json').read_text())['hold'] is True
