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
    # Unstarted sources: the collector says coverage is unknown, and so does the overview.
    assert cov['status'] == 'unknown' and cov['observed_days'] == 7
    assert cov['warmup'] == {'start': '2026-08-09T12:30:00.000000Z', 'end': '2026-09-01T00:00:00.000000Z',
                             'reason': 'not_observed'}
    assert cov['gaps'] == [{'start': '2026-09-02T01:00:00.000000Z', 'end': '2026-09-02T02:00:00.000000Z',
                            'reason': 'source_unavailable'}]
    assert cov['sources'] == {'enrolled': 3, 'seeded': 3, 'discovered': 0, 'pending_attempts': 0, 'unstarted': 3, 'probing': 0, 'established': 0, 'refused': 0,
                              'pre_epoch_skipped': 0, 'undated_skipped': 0, 'late_skipped': 0, 'cursor_order_unverified': 0, 'undated_only': 0, 'provider_rejects': 0, 'unavailable': 0,
                              'future_skipped': 0}
    assert cov['observed_through'] is None  # ingest() fixtures accept no collector page
    assert stats['last_maintenance']['generation'] == 1


def test_no_private_fields_or_subject_dids(store):
    env = recent_overview.overview(recent_publish.read_store(store, NOW), NOW)
    body = json.dumps(env)
    dids = set(re.findall(r'did:[a-z0-9]+:[A-Za-z0-9._:%-]+', body))
    assert dids == {r['labeler_did'] for r in env['top_labelers_7d']}
    for private in (SUBJECT, 'at://', '"early"', '"late"', '"next"', 'quarantin', 'probe', 'endpoint', 'provider_cursor', 'opaque', str(store.root)):
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
             'counts': dict.fromkeys(recent_overview.COUNT_KEYS, 0), 'gaps': [],
             'sources': dict(dict.fromkeys(recent_overview.SOURCE_KEYS, 0), coverage='established_sources_only'),
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
    with pytest.raises(recent_overview.OverviewRefused, match='writable'):
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


def refuse(store, did, reason='opaque_cursor', at='2026-09-02T12:30:00Z'):
    token = store.begin_source_attempt(did, at)
    return store.bootstrap_step(did, token, at, {'phase': 'unstarted', 'probes': 0, 'lo': None, 'hi': None},
                                'refused', reason=reason)


def test_refused_source_is_counted_publicly_and_its_reason_kept_private(store):
    assert refuse(store, C)['reason'] == 'bootstrap_refused:opaque_cursor'
    stats = recent_publish.read_store(store, NOW)
    env = recent_overview.overview(stats, NOW, signature='x')
    assert env['coverage']['sources'] == {'enrolled': 3, 'seeded': 3, 'discovered': 0, 'pending_attempts': 0, 'unstarted': 2, 'probing': 0, 'established': 0,
                                          'refused': 1, 'pre_epoch_skipped': 0, 'undated_skipped': 0, 'late_skipped': 0, 'cursor_order_unverified': 0, 'undated_only': 0, 'provider_rejects': 0, 'unavailable': 0,
                              'future_skipped': 0}
    body = json.dumps(env)
    assert 'opaque_cursor' not in body and 'bootstrap_refused' not in body and 'reason": "bootstrap' not in body
    assert stats['refused_sources'] == [{'did': C, 'reason': 'bootstrap_refused:opaque_cursor'}]
    from test_recent_site import site, get
    _, h = site(store)
    health = json.loads(get(h, '/health')[2])
    assert health['coverage']['refused_sources'] == [{'did': C, 'reason': 'bootstrap_refused:opaque_cursor'}]
    assert health['coverage']['sources']['refused'] == 1
    landing = get(h, '/')[2].decode()
    assert 'opaque_cursor' not in landing
    assert 'Refused</th><td>1</td>' in landing


def test_observed_through_is_the_latest_accepted_page(store):
    store.accept_page([], source=B, request_cursor=None, next_cursor='7', observed_at='2026-09-02T12:00:00Z',
                      skipped={'pre_epoch': 2})
    env = recent_overview.overview(recent_publish.read_store(store, NOW), NOW, signature='x')
    assert env['coverage']['observed_through'] == '2026-09-02T12:00:00.000000Z'
    assert env['coverage']['sources']['pre_epoch_skipped'] == 2


def test_group_or_world_writable_directory_is_refused(tmp_path):
    for mode in (0o777, 0o775, 0o757):
        pubdir = tmp_path / ('pub-%o' % mode); pubdir.mkdir(); os.chmod(pubdir, mode)
        with pytest.raises(recent_overview.OverviewRefused, match='writable'):
            recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)
        assert not (pubdir / 'overview.json').exists()
    ok = tmp_path / 'pub-750'; ok.mkdir(); os.chmod(ok, 0o751)
    with pytest.raises(recent_overview.OverviewRefused):
        recent_overview.publish(ok, recent_overview.held(NOW), ancestors=False)  # o+x without o+r


def test_stale_temporaries_are_swept(tmp_path):
    pubdir = tmp_path / 'pub'
    recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)
    (pubdir / '.overview.tmp-99999').write_text('partial')
    recent_overview.publish(pubdir, recent_overview.held(NOW), ancestors=False)
    assert sorted(p.name for p in pubdir.iterdir()) == ['overview.json']


def test_status_read_does_not_take_the_writer_lock(store):
    from storage import lock
    with lock(store.root):  # a collector round holding the writer lease
        stats = recent_publish.read_store(store, NOW, timeout=0.5, budget=5)
    assert stats['counts']['events_7d'] == 4


def test_order_lateness_and_reject_disclosures_are_counts_publicly_and_detail_privately(store):
    from storage import connect
    with connect(store.state) as c:
        c.execute("UPDATE q_recent_sources SET cursor_order_unverified=1, cursor_order_evidence=?, late_skipped=5 WHERE did=?",
                  (json.dumps({'regressed_at': 'secret-evidence'}), A))
        c.execute("UPDATE q_recent_sources SET undated_skipped=3, accepted_rows=0, provider_rejects=2 WHERE did=?", (B,))
    stats = recent_publish.read_store(store, NOW)
    env = recent_overview.overview(stats, NOW, signature='x')
    src = env['coverage']['sources']
    assert (src['late_skipped'], src['cursor_order_unverified'], src['undated_only'], src['provider_rejects'],
            src['undated_skipped']) == (5, 1, 1, 1, 3)
    body = json.dumps(env)
    assert 'secret-evidence' not in body and 'evidence' not in body
    assert set(re.findall(r'did:[a-z0-9]+:[A-Za-z0-9._:%-]+', body)) <= {r['labeler_did'] for r in env['top_labelers_7d']}
    from test_recent_site import site, get
    _, h = site(store)
    health = json.loads(get(h, '/health')[2])['coverage']
    assert health['cursor_order_unverified_sources'] == [{'did': A, 'evidence': {'regressed_at': 'secret-evidence'}}]
    assert health['undated_only_sources'] == [B]
    assert health['provider_rejects_sources'] == [{'did': B, 'provider_rejects': 2}]
    landing = get(h, '/')[2].decode()
    assert 'Sources whose page order could not be verified: 1 (their coverage is reported as unknown)' in landing
    assert 'Rows skipped as late (older than 48 h at acceptance): 5' in landing
    assert 'secret-evidence' not in landing
    about = get(h, '/about')[2].decode()
    assert '<code>late_skipped</code>' in about and '<code>cursor_order_unverified</code>' in about


def test_unavailable_sources_counted_publicly_and_detailed_for_operators(store):
    from storage import connect
    with connect(store.state) as c:
        c.execute("UPDATE q_recent_sources SET unavailable_since=?, last_failure_at=?, last_failure_reason=? WHERE did=?",
                  ('2026-09-02T10:00:00.000000Z', '2026-09-02T11:00:00.000000Z', 'transport_timeout-secret', C))
    env = recent_overview.overview(recent_publish.read_store(store, NOW), NOW, signature='x')
    assert env['coverage']['sources']['unavailable'] == 1
    assert 'transport_timeout-secret' not in json.dumps(env)
    from test_recent_site import site, get
    _, h = site(store)
    health = json.loads(get(h, '/health')[2])['coverage']
    assert health['unavailable_sources'] == [{'did': C, 'since': '2026-09-02T10:00:00.000000Z',
                                              'last_failure_at': '2026-09-02T11:00:00.000000Z',
                                              'reason': 'transport_timeout-secret'}]
    landing = get(h, '/')[2].decode()
    assert 'Sources currently unavailable (transient failures, retried with backoff): 1' in landing
    assert 'transport_timeout-secret' not in landing


def test_status_carries_the_collector_coverage_verdict(store, monkeypatch):
    stats = recent_publish.read_store(store, NOW)
    assert stats['sources']['coverage'] == 'unknown'
    env = recent_overview.overview(stats, NOW, signature='x')
    assert env['coverage']['status'] == 'unknown'
    assert env['coverage']['warmup'] is not None and env['coverage']['gaps']  # still disclosed
    assert 'coverage' not in env['coverage']['sources']
    later = datetime(2026, 10, 5, tzinfo=timezone.utc)  # window fully inside the epoch
    stats['sources']['coverage'] = 'established_sources_only'
    assert recent_overview.overview(stats, later, signature='x')['coverage']['status'] == 'no_recorded_gaps'
    stats['sources']['coverage'] = 'unknown'
    env = recent_overview.overview(stats, later, signature='x')
    assert env['coverage']['status'] == 'unknown' and env['coverage']['warmup'] is None
    stats['sources']['future_skipped'] = 4
    assert recent_overview.overview(stats, later, signature='x')['coverage']['sources']['future_skipped'] == 4


def test_seeded_and_discovered_are_reported_separately(store, monkeypatch):
    summary = store.collector_bootstrap_summary
    monkeypatch.setattr(store, 'collector_bootstrap_summary', lambda: {**summary(), 'seeded': 1, 'discovered': 2})
    env = recent_overview.overview(recent_publish.read_store(store, NOW), NOW, signature='x')
    src = env['coverage']['sources']
    assert (src['enrolled'], src['seeded'], src['discovered']) == (3, 1, 2)
    from test_recent_site import site, get
    _, h = site(store)
    assert 'Enrolled 3 = seeded 1 + discovered 2' in get(h, '/')[2].decode()


def test_pending_attempts_and_establishment_times_reach_operator_view(store):
    token = store.begin_source_attempt(A, '2026-09-02T12:30:00Z')  # an attempt left open
    env = recent_overview.overview(recent_publish.read_store(store, NOW), NOW, signature='x')
    assert env['coverage']['sources']['pending_attempts'] == 1
    assert A not in json.dumps(env['coverage'])
    from test_recent_site import site, get
    _, h = site(store)
    health = json.loads(get(h, '/health')[2])['coverage']
    assert health['pending_attempts_sources_total'] == 1 and A in json.dumps(health['pending_attempts_sources'])
    assert set(health['established_at']) == {'earliest', 'latest'}
    assert 'established_at' not in json.dumps(env)
    assert token
