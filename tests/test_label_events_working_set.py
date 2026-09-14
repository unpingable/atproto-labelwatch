from __future__ import annotations

import pytest

from tools.label_events_working_set import _days


def test_days_returns_complete_half_open_interval() -> None:
    assert _days("2026-02-27", "2026-03-02") == [
        "2026-02-27",
        "2026-02-28",
        "2026-03-01",
    ]


@pytest.mark.parametrize(
    ("start", "end"),
    [("2026-03-02", "2026-03-02"), ("2026-03-03", "2026-03-02")],
)
def test_days_refuses_empty_or_reversed_interval(start: str, end: str) -> None:
    with pytest.raises(ValueError, match="start must precede"):
        _days(start, end)
