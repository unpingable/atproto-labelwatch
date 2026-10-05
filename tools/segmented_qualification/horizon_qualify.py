"""Direct horizon hard-gate qualification using the existing candidate.

Small counterexamples precede any current-volume allocation. These specimens
are lifetime/metadata tests, not current-volume performance acceptance.
"""
from pathlib import Path
import datetime as dt
import json
import os
import resource
import statistics
import sys
import time

from storage import Store, atomic, connect, db, retention, sha
from qualify import event
from coverage_qualify import main as coverage_regression
import tier


def snapshot(s):
    c = connect(s.state, readonly=True)
    counts = {name: c.execute('SELECT COUNT(*) FROM ' + name).fetchone()[0]
              for name in ('q_hot_keys', 'quarantined_events', 'labeler_evidence', 'q_segments', 'labelers')}
    counts['future_keys'] = c.execute("SELECT COUNT(*) FROM q_hot_keys WHERE ts >= '2100'").fetchone()[0]
    counts['page_bytes'] = c.execute('PRAGMA page_count').fetchone()[0] * 4096
    counts['freelist_bytes'] = c.execute('PRAGMA freelist_count').fetchone()[0] * 4096
    counts['cursor'] = db.get_cursor(c, 'horizon-fixture')
    c.close()
    return counts


def lifetime(out):
    out.mkdir()
    start = dt.date(2026, 9, 28)
    s = Store.create(out / 'store', floor='2026-08-19T00:00:00Z')
    archive = out / 'archive'; archive.mkdir()
    active = start
    checkpoints = []
    width = 256
    for day in range(96):
        today = start + dt.timedelta(days=day)
        period = start + dt.timedelta(days=(day // 7) * 7)
        if period != active:
            s.rotate(period.isoformat()); s.archive(active.isoformat(), archive); s.retire(active.isoformat())
            active = period
        normal = [event(day * width + i + 1, today.isoformat() + 'T12:00:00Z') for i in range(width)]
        future = [event(1000000 + day * width + i, '2100-01-01T12:00:00Z') for i in range(width)]
        old = [event(2000000 + day * width + i, '1990-01-01T12:00:00Z') for i in range(width)]
        assert s.ingest(normal, 'horizon-fixture', 'normal-' + str(day))['inserted'] == width
        assert s.ingest(future, 'horizon-fixture', 'future-' + str(day))['inserted'] == width
        assert s.ingest(old, 'horizon-fixture', 'old-' + str(day))['inserted'] == 0
        # Exactly the existing floor/expiry implementation; no policy invented.
        floor = today - dt.timedelta(days=39)
        s.advance_floor(floor.isoformat() + 'T00:00:00Z')
        if day in (6, 39, 46, 79, 95):
            checkpoint = {'day_count': day + 1, 'accepted': (day + 1) * width * 2,
                          'below_floor_offers': (day + 1) * width, **snapshot(s)}
            checkpoints.append(checkpoint); atomic(out / 'PROGRESS.json', checkpoint)
            print(json.dumps(checkpoint), flush=True)
    # Retired Parquet remains the only payload for this event. Correct dedupe
    # needs its global key even though arrival is older than the live window.
    replay = event(1000000, '2100-01-01T12:00:00Z')
    assert s.ingest([replay], 'horizon-fixture', 'replay-preserved-key')['inserted'] == 0
    before = snapshot(s)
    c = connect(s.state, readonly=True)
    assert retention.quarantined_for_subject(c, 'did:plc:fixturesubject') == 96 * width
    c.close()
    # Local substitution only: show the consequence of expiry by arrival age.
    c = connect(s.state); c.execute('DELETE FROM q_hot_keys WHERE event_hash=?', (replay[9],)); c.commit(); c.close()
    admitted = s.ingest([replay], 'horizon-fixture', 'replay-after-key-removal')['inserted']
    assert admitted == 1, 'If replay is bounded elsewhere, this counterexample is false'
    with tier.TierSession(s, archive, include_below_floor=True) as q:
        copies = q.execute('SELECT COUNT(*) FROM label_events WHERE event_hash=?', (replay[9],)).fetchone()[0]
    assert copies == 2
    restored = Store(s.root)
    assert restored.snapshot()['cursor']['ingest_cursor:horizon-fixture'] == 'replay-after-key-removal'
    result = {'result': 'COUNTEREXAMPLE', 'days': 96, 'events_accepted_before_substitution': 49152,
              'below_floor_unique_events': 24576, 'per_class_events_per_day': width,
              'checkpoints': checkpoints, 'before_key_substitution': before,
              'after_key_substitution_replay_inserted': admitted,
              'historical_payload_copies_after_substitution': copies,
              'normalization_contract': 'Existing normalize_label accepts supplied authored ts; no upper skew admission.',
              'mutable_quarantine_contract': 'Existing full-event rows keyed by event_hash/reason retain and mutate seen_count and last delivery metadata; no pruning in floor advancement.',
              'scope': 'Finite deterministic lifetime counterexample, fixed labeler/source/subject; NOT a current-volume capacity specimen.',
              'production_mutations': []}
    atomic(out / 'RESULT.json', result)
    return result


def catalog(out, source):
    out.mkdir(); archive = out / 'archive'; archive.mkdir()
    payload = source / 'archive' / '2026-09-28.parquet'
    checksum = sha(payload)
    original_sha = tier.sha
    calls = []
    def counted_sha(path):
        calls.append({'bytes': Path(path).stat().st_size, 'path': str(path)})
        return original_sha(path)
    tier.sha = counted_sha
    measurements = []
    created = 0
    start = dt.date(1900, 1, 1)
    try:
        for count in (128, 1024, 8192):
            while created < count:
                identity = (start + dt.timedelta(days=created * 7)).isoformat()
                os.link(payload, archive / (identity + '.parquet'))
                atomic(archive / (identity + '.receipt.json'), {'identity': identity, 'parquet_sha256': checksum})
                created += 1
            calls.clear(); t = time.perf_counter(); admitted = tier.VerifiedCatalog(archive); startup = time.perf_counter() - t
            startup_calls = len(calls); startup_bytes = sum(v['bytes'] for v in calls)
            times = []
            for _ in range(7):
                t = time.perf_counter(); values = admitted.files(); times.append(time.perf_counter() - t)
                assert len(values) == count
            # Existing API has no incremental admission: a fresh catalog is the
            # available direct path after publishing a new receipt.
            identity = (start + dt.timedelta(days=created * 7)).isoformat()
            os.link(payload, archive / (identity + '.parquet'))
            atomic(archive / (identity + '.receipt.json'), {'identity': identity, 'parquet_sha256': checksum}); created += 1
            calls.clear(); t = time.perf_counter(); successor = tier.VerifiedCatalog(archive); new_seconds = time.perf_counter() - t
            measurements.append({'count': count, 'startup_seconds': startup, 'startup_payload_hash_calls': startup_calls,
                                 'startup_payload_bytes_read': startup_bytes, 'files_p50_seconds': statistics.median(times),
                                 'files_p95_seconds': max(times), 'new_segment_admission_seconds': new_seconds,
                                 'new_segment_admission_payload_hash_calls': len(calls),
                                 'new_segment_admission_payload_bytes_read': sum(v['bytes'] for v in calls),
                                 'catalog_metadata_bytes': sum(f.stat().st_size for f in archive.glob('*.receipt.json')),
                                 'rss_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024})
            assert len(successor.entries) == count + 1
            print(json.dumps(measurements[-1]), flush=True)
    finally: tier.sha = original_sha
    result = {'result': 'MEASURED_NOT_ACCEPTED', 'measurements': measurements,
              'payload_provenance': 'Hard links to one unchanged verified 1-row regression Parquet; metadata-cardinality fixture only, no years of event history claimed.',
              'asymptotic': 'Catalog constructor hashes every receipt artifact: O(n + total historical payload bytes); files() performs O(n) path stats. No indexed time-range/retirement/identity API exists.',
              'links': payload.stat().st_nlink, 'production_mutations': []}
    atomic(out / 'RESULT.json', result)
    return result


if __name__ == '__main__':
    out = Path(sys.argv[1]); terminal = Path(sys.argv[2]); out.mkdir()
    try:
        coverage_regression(out / 'coverage')
        global_result = lifetime(out / 'lifetime')
        catalog_result = catalog(out / 'catalog', out / 'coverage')
        atomic(out / 'RESULT.json', {'result': 'HARD_GATE_COUNTEREXAMPLE', 'global': global_result,
                                    'catalog': catalog_result, 'historical_coverage_regression': json.loads((out / 'coverage/RESULT.json').read_text()),
                                    'full_volume_horizon_run': 'NOT_RUN: direct correctness-required lifetime bound fails before macro allocation.',
                                    'rss_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                                    'production_mutations': []})
    except BaseException as e:
        atomic(terminal, {'result': 'FAILED', 'error': repr(e), 'at': dt.datetime.now(dt.timezone.utc).isoformat()}); raise
    else:
        atomic(terminal, {'result': 'PASS_COUNTEREXAMPLE_REPRODUCTION', 'at': dt.datetime.now(dt.timezone.utc).isoformat()})
