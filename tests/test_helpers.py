"""Tests for src/maxc_cli/helpers.py utility functions."""

from datetime import datetime, timezone, timedelta

from maxc_cli.helpers import _dt_to_iso


def test_dt_to_iso_preserves_naive_as_local_wallclock():
    # PyODPS returns naive datetimes from instance.start_time. They are
    # wall-clock local time, not UTC. Tagging them as UTC shifts the
    # displayed timestamp by the local offset, which is the C1 bug.
    naive = datetime(2026, 5, 21, 14, 30, 0)
    out = _dt_to_iso(naive)
    # The wall-clock minute/hour must survive intact.
    assert "14:30:00" in out
    # And the result must carry the LOCAL offset, not be silently
    # re-stamped as UTC (unless the host actually IS UTC).
    import time
    local_offset_seconds = -time.timezone if time.daylight == 0 else -time.altzone
    if local_offset_seconds != 0:
        assert "+00:00" not in out, (
            "naive PyODPS datetime must not be re-stamped as UTC on a "
            "non-UTC host (got %r)" % out
        )


def test_dt_to_iso_passes_through_aware():
    aware = datetime(2026, 5, 21, 14, 30, 0, tzinfo=timezone(timedelta(hours=8)))
    assert _dt_to_iso(aware) == "2026-05-21T14:30:00+08:00"


def test_dt_to_iso_none():
    assert _dt_to_iso(None) is None
