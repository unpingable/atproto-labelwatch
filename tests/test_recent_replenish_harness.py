"""Oracle logic of the C3/C4 qualification harnesses (not the store itself)."""
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import pytest
import storage
import recent_replenish as rr
from recent_concurrent import compare

UTC = dt.timezone.utc
HEX = lambda n: format(n, '064x')


def test_digest_is_order_independent_and_sees_loss_and_duplicates():
    ids = [HEX(n * 7919 + 3) for n in range(50)]
    a = rr.Digest().update(ids)
    assert a == rr.Digest().update(reversed(ids))
    assert a != rr.Digest().update(ids[:-1])                 # lost
    assert a != rr.Digest().update(ids + ids[:1])            # duplicated
    assert a != rr.Digest().update(ids[:-1] + [HEX(10**6)])  # substituted, same count
    assert rr.Digest().update(ids[:20]).merge(rr.Digest().update(ids[20:])) == a


def test_cursor_chain_is_per_source_and_continuous():
    sources = 4
    chain = {}
    for g in range(40):
        request, following = rr.page_cursors(g, sources)
        assert request == chain.get(g % sources)
        chain[g % sources] = following
        assert rr.committed_through(following) == g


def test_resume_point_requires_a_committed_prefix():
    s, first, last = 4, 100, 120
    cursors = lambda through: {k: (str(max(g for g in range(-s, through + 1) if g % s == k) + 1)
                                   if max(g for g in range(-s, through + 1) if g % s == k) >= 0 else None)
                               for k in range(s)}
    assert rr.resume_point(cursors(99), first, last, s) == 100      # nothing today
    assert rr.resume_point(cursors(109), first, last, s) == 110     # mid-day
    assert rr.resume_point(cursors(119), first, last, s) == 120     # complete
    holed = cursors(109)
    holed[3] = str(115 + 1)                                          # source 3 ahead: 111 missing
    with pytest.raises(AssertionError, match='prefix'):
        rr.resume_point(holed, first, last, s)
    beyond = cursors(119)
    beyond[0] = str(124 + 1)
    with pytest.raises(AssertionError):
        rr.resume_point(beyond, first, last, s)


def test_authored_time_is_unique_and_inside_the_late_allowance():
    t0, pages = rr.DEFAULT_T0, 14000
    seen = set()
    for g in range(0, 2 * pages, 37):
        for slot in (0, 99):
            cts = rr.authored(t0, pages, g, slot)
            observed = rr.nominal(t0, pages, g) + dt.timedelta(seconds=120)  # restart floor
            assert rr.within_allowance(observed, cts, t0)
            assert cts not in seen
            seen.add(cts)
    late = rr.LATE_EVERY - 1
    assert rr.nominal(t0, pages, late) - rr.authored(t0, pages, late, 0) == rr.LAG_LATE
    assert not rr.within_allowance(t0, t0 - rr.LATE_ALLOWANCE, t0)
    assert not rr.within_allowance(t0, t0 + dt.timedelta(seconds=1), t0)   # authored after observed


def test_kill_point_grammar():
    points = rr.parse_kill_points(['ingest:4:0.5', 'ingest:5:0.25:after_recent_ingest_commit',
                                   'maintain:7:after_recent_authority', 'maintain:8:timer:2.5'])
    assert [p['mode'] for p in points] == ['parent_sigkill', 'phase', 'phase', 'parent_sigkill']
    for bad in (['ingest:4:1.0'], ['ingest:4:0.5:no_such_phase'], ['maintain:7:nope'], ['flush:1:2'],
                ['ingest:4:0.1', 'ingest:4:0.2']):
        with pytest.raises(ValueError):
            rr.parse_kill_points(bad)


def test_plateau_and_gap_cover():
    assert rr.plateau([100, 104, 98, 101, 103, 99, 100, 102], .1)['pass']
    assert not rr.plateau([100, 110, 120, 130, 140, 150, 160, 170], .1)['pass']
    gaps = [('2026-11-01T00:00:00Z', '2026-11-01T00:05:00Z', 'maintenance')]
    assert rr.covered(gaps, '2026-11-01T00:01:00Z', '2026-11-01T00:05:00Z', 'maintenance')
    assert not rr.covered(gaps, '2026-11-01T00:01:00Z', '2026-11-01T00:06:00Z')
    assert not rr.covered(gaps, '2026-11-01T00:01:00Z', '2026-11-01T00:02:00Z', 'unknown')


def test_vessel_digest_counts_duplicates(tmp_path):
    path = tmp_path / 'v.sqlite'
    with sqlite3.connect(path) as c:
        c.execute('PRAGMA page_size=4096')
        c.execute('CREATE TABLE label_events(id INTEGER PRIMARY KEY, event_hash TEXT)')
        c.executemany('INSERT INTO label_events(event_hash) VALUES (?)', [(HEX(1),), (HEX(2),), (HEX(2),)])
    digest, distinct = rr.vessel_digest(path)
    assert (digest.count, distinct) == (3, 2)
    assert digest != rr.Digest().update([HEX(1), HEX(2)])


def test_concurrent_verdicts_detect_torn_and_unstable_reads():
    good = {'refusal': None, 'count': 5, 'digest': 'a', 'manifest_stable': True}
    assert compare(good, dict(good), 5) == []
    assert 'torn or unstable snapshot' in compare({**good, 'digest': 'b'}, good, 5)
    assert 'count differs from generator oracle' in compare(good, good, 6)
    assert 'manifest changed across pages' in compare({**good, 'manifest_stable': False}, good, 5)
    assert compare({'refusal': 'snapshot_expired_or_unavailable'}, good, 5) == [
        'refused_under_contention:snapshot_expired_or_unavailable']
    assert compare(good, {'refusal': 'snapshot_row_limit'}, 5) == ['refusal differs from quiescent read']
    refused = {'refusal': 'snapshot_row_limit'}
    assert compare(refused, refused, 1001, row_limit=1000) == []
    assert compare(refused, refused, 10, row_limit=1000) == ['row-limit refusal below the limit']


# -------------------------------------------------------------- end to end

@pytest.fixture
def campaign(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    root = tmp_path / 'runtime'
    root.mkdir()
    (tmp_path / 'evidence').mkdir()
    (tmp_path / 'evidence/ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive': str(root)}))
    monkeypatch.setattr(storage, 'ROOT', root)
    n = 3000
    rows = {'id': list(range(1, n + 1)), 'labeler_did': ['did:plc:lab'] * n, 'src': ['did:plc:lab'] * n,
            'uri': ['at://did:plc:t%d/app.bsky.feed.post/%d' % (i % 37, i) for i in range(n)],
            'cid': [None] * n, 'val': ['spam' if i % 3 else 'porn' for i in range(n)], 'neg': [0] * n,
            'exp': [None] * n, 'sig': [None] * n, 'ts': ['2026-09-28T00:00:00Z'] * n,
            'event_hash': [HEX(i) for i in range(n)], 'target_did': ['did:plc:t%d' % (i % 37) for i in range(n)]}
    spec = tmp_path / 'specimen.parquet'
    pq.write_table(pa.table(rows), spec, row_group_size=500)
    (tmp_path / 'SPECIMEN.json').write_text(json.dumps({'parquet_sha256': storage.sha(spec)}))
    return root, spec


def args_for(root, spec, *extra):
    return ['--occurrence', str(root / 'c3'), '--specimen', str(spec), '--days', '3', '--prefill-days', '1',
            '--events-per-day', '400', '--sources', '3', '--ceiling-bytes', str(2 * 1024**3),
            '--budget-seconds', '300', '--require-plateau', '0', *extra]


def test_c3_end_to_end_recovers_from_real_kills(campaign):
    root, spec = campaign
    rr.main(args_for(root, spec, '--kill-points', 'ingest:2:0.5:after_recent_ingest_commit',
                     'maintain:3:after_recent_plan'))
    result = json.loads((root / 'c3/RESULT.json').read_text())
    assert (result['accepted'], result['lost'], result['duplicated']) == (1600, 0, 0)
    ingest_kill, maintain_kill = result['kills']
    assert ingest_kill['exit'] == maintain_kill['exit'] == -9
    assert ingest_kill['pending_rows_at_reopen'] == 100 and ingest_kill['pending_after_first_write'] == 0
    assert ingest_kill['redelivered_last_page']['inserted'] == 0
    assert maintain_kill['reads_refused_before_recovery'] and maintain_kill['recent_transition_after'] == 0
    assert all(x['disclosed'] for x in result['gap_checks'])
    assert json.loads((root / 'c3/TERMINAL.json').read_text())['result'] == 'PASS'


def test_c3_harness_fails_closed_on_a_lost_row(campaign, monkeypatch):
    root, spec = campaign
    real = rr.vessel_digest

    def lossy(path):
        digest, distinct = real(path)
        return rr.Digest(digest.count - 1, digest.total, digest.xor), distinct - 1
    monkeypatch.setattr(rr, 'vessel_digest', lossy)
    with pytest.raises(AssertionError, match='oracle mismatch'):
        rr.main(args_for(root, spec))
    assert json.loads((root / 'c3/TERMINAL.json').read_text())['result'] == 'FAIL'


def test_c3_harness_refuses_an_unexercised_kill_point(campaign):
    root, spec = campaign
    with pytest.raises(RuntimeError, match='not exercised'):
        rr.main(args_for(root, spec, '--kill-points', 'maintain:1:during_recent_unlink'))
