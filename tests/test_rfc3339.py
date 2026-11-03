"""The shared timestamp parser accepts the same strings on CPython 3.10 and 3.12."""
from datetime import datetime, timezone

import pytest

from labelwatch.rfc3339 import parse_utc
from labelwatch.recent_collector import _source_time
from labelwatch.recent_observations import timestamp, Refused
from labelwatch.recent_overview import utc


@pytest.mark.parametrize('value,expected', [
    ('2026-10-07T02:53:59.278503138Z', datetime(2026, 10, 7, 2, 53, 59, 278503, tzinfo=timezone.utc)),   # nanoseconds (real labelers)
    ('2026-10-07T02:53:59.9999999Z', datetime(2026, 10, 7, 2, 53, 59, 999999, tzinfo=timezone.utc)),     # truncated, never rounded up
    ('2026-10-07T02:53:59.1Z', datetime(2026, 10, 7, 2, 53, 59, 100000, tzinfo=timezone.utc)),
    ('2026-10-07T02:53:59.12Z', datetime(2026, 10, 7, 2, 53, 59, 120000, tzinfo=timezone.utc)),
    ('2026-10-07T02:53:59Z', datetime(2026, 10, 7, 2, 53, 59, tzinfo=timezone.utc)),
    ('2026-10-07T02:53:59.123+02:00', datetime(2026, 10, 7, 0, 53, 59, 123000, tzinfo=timezone.utc)),
    ('2026-10-07 02:53:59.123456Z', datetime(2026, 10, 7, 2, 53, 59, 123456, tzinfo=timezone.utc)),
])
def test_fractions_of_any_length_parse_identically(value, expected):
    assert parse_utc(value) == expected
    assert _source_time(value) == expected
    assert timestamp(value) == expected
    assert utc(value) == expected


@pytest.mark.parametrize('value', ['2026-10-07T02:53:59.278503138', '2026-10-07', 'yesterday', '', None, 12, '2026-13-01T00:00:00Z',
                                   '2026-10-07T02:53:59+0000', '2026-10-07T02:53:59+05', '2026-10-07T02:53:59+00:00.5', '2026-10-07T02:53:59.123Z extra'])
def test_missing_offset_or_garbage_is_refused_everywhere(value):
    assert parse_utc(value) is None
    assert _source_time(value) is None
    with pytest.raises(Refused):
        timestamp(value)


def test_storage_clock_accepts_nanosecond_utc_and_refuses_offsets():
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools' / 'segmented_qualification'))
    from recent_storage import clock
    assert clock('2026-10-07T02:53:59.278503138Z') == datetime(2026, 10, 7, 2, 53, 59, 278503, tzinfo=timezone.utc)
    assert clock('2026-10-07T02:53:59Z') == datetime(2026, 10, 7, 2, 53, 59, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        clock('2026-10-07T02:53:59+02:00')
    with pytest.raises(ValueError):
        clock('2026-10-07T02:53:59')


def test_naive_timestamp_gets_the_timezone_refusal_on_every_interpreter():
    with pytest.raises(Refused, match='timestamp_requires_timezone'):
        timestamp('2026-10-07T02:53:59.278503138')
    with pytest.raises(Refused, match='invalid_timestamp'):
        timestamp('2026-10-07T02:53:59+0000')
