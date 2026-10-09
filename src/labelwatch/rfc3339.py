"""One RFC 3339 / ISO 8601 timestamp parser for the recent observatory.

``datetime.fromisoformat`` differs between the qualification interpreter
(CPython 3.12 accepts any number of fractional digits) and the production host
(CPython 3.10 accepts exactly 0, 3 or 6). Labelers emit nanosecond ``cts``
values such as ``2026-10-07T02:53:59.278503138Z``, which 3.10 would refuse and
the collector would then drop as undated. This parser normalises the fraction
to microseconds (truncating, never rounding into the next second) before
delegating, so every interpreter accepts the same set of strings.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

_FRACTION = re.compile(r'^(?P<head>\d{4}-\d{2}-\d{2}[T t]\d{2}:\d{2}:\d{2})(?P<frac>[.,]\d+)?(?P<tail>.*)$')


def parse_utc(value):
    """Return an aware UTC datetime, or None when ``value`` is not an RFC 3339
    timestamp with an explicit offset. Never raises."""
    if not isinstance(value, str):
        return None
    m = _FRACTION.match(value.strip())
    if not m:
        return None
    frac = m.group('frac') or ''
    if frac:
        digits = frac[1:]
        frac = '.' + (digits[:6] if len(digits) >= 6 else digits.ljust(6, '0'))
    tail = m.group('tail')
    if tail in ('Z', 'z'):
        tail = '+00:00'
    try:
        parsed = datetime.fromisoformat(m.group('head').replace(' ', 'T') + frac + tail)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)
