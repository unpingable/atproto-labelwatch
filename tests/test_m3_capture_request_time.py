"""Exact request spelling across the app → typed native receipt boundary."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import runpy
import tarfile

import pytest


def capture():
    return runpy.run_path(str(Path(__file__).resolve().parents[1] / 'scripts/m3_fixture_capture.py'))


@pytest.mark.parametrize(('source', 'expected'), [
    ('2026-09-09T08:27:13+00:00', '2026-09-09T08:27:13Z'),
    ('2026-09-09T08:27:13.000000+00:00', '2026-09-09T08:27:13Z'),
    ('2026-09-09T08:27:13.123000+00:00', '2026-09-09T08:27:13.123Z'),
    ('2026-09-09T08:27:13.123400+00:00', '2026-09-09T08:27:13.123400Z'),
    ('2026-09-09T08:27:13.991194+00:00', '2026-09-09T08:27:13.991194Z'),
    ('2026-09-09T08:27:13.971921Z', '2026-09-09T08:27:13.971921Z'),
    ('2026-09-09T00:27:13.120000+01:00', '2026-09-08T23:27:13.120Z'),
    ('2026-09-09T23:27:13.123400-01:00', '2026-09-10T00:27:13.123400Z'),
])
def test_exact_native_utc_spelling_without_precision_loss(source, expected):
    result = capture()['_request_time'](source)
    assert result == expected
    assert datetime.fromisoformat(result.replace('Z', '+00:00')) == datetime.fromisoformat(source.replace('Z', '+00:00'))


@pytest.mark.parametrize('value', ['2026-09-09T08:27:13.123456789Z',
    '2026-09-09T08:27:13+25:00', '2026-09-09T08:27:13',
    datetime(2026, 9, 9, 8, 27, 13), '2026-02-30T08:27:13Z'])
def test_finer_precision_or_non_fixture_time_is_refused_not_truncated(value):
    with pytest.raises(ValueError):
        capture()['_request_time'](value)


def test_live_clock_emission_matches_exact_microseconds():
    value = datetime(2026, 9, 9, 8, 27, 13, 123400, tzinfo=timezone.utc)
    assert capture()['_request_time'](value) == '2026-09-09T08:27:13.123400Z'


@pytest.mark.skipif(not os.environ.get('M3_CAPTURE_RETAINED_TAR') or not os.environ.get('M3_NQ_BIN'),
    reason='explicit retained qualification inputs and exact native binary required')
def test_real_retained_cleanup_request_requalifies_with_exact_receipt(tmp_path):
    module = capture()
    prefix = 'var/lib/constellation-m3/later-cases/cleanup-after_cleanup_authorized/03-cleanup-native/'
    with tarfile.open(os.environ['M3_CAPTURE_RETAINED_TAR']) as archive:
        request = json.load(archive.extractfile(prefix + 'cleanup-request.json'))
        receipt = json.load(archive.extractfile(prefix + 'cleanup-receipt.json'))
        raw_source = archive.extractfile(prefix + 'cleanup-source.json').read()
    assert request != receipt['request']  # Preserves the original false refusal.
    request['evaluated_at'] = module['_request_time'](request['evaluated_at'])
    request['held_request']['evaluated_at'] = module['_request_time'](request['held_request']['evaluated_at'])
    assert request == receipt['request']
    source_path, request_path = tmp_path / 'source.json', tmp_path / 'request.json'
    source_path.write_bytes(raw_source)
    request_path.write_text(json.dumps(request))
    actual = module['_native'](Path(os.environ['M3_NQ_BIN']), os.environ['M3_NQ_SHA256'],
        'labelwatch-cleanup', source_path, request_path)
    assert actual == receipt  # Exact same source/instant/receipt, not approximate equality.
    assert actual['request'] == request and actual['disposition'] == 'ESTABLISHED'
    request['evaluated_at'] = request['evaluated_at'].replace('991194', '991195')
    assert request != actual['request']  # A real one-microsecond change still differs.
