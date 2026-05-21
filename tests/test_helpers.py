"""Tests for src/maxc_cli/helpers.py utility functions."""

from datetime import datetime, timezone, timedelta

from maxc_cli.helpers import _dt_to_iso


def test_dt_to_iso_preserves_naive_as_local_wallclock():
    # PyODPS returns naive datetimes from instance.start_time — they are
    # wall-clock local time, not UTC. The bug (C1) was re-stamping them
    # as UTC, which displayed e.g. Beijing 14:30 as 14:30Z on non-UTC hosts.
    naive = datetime(2026, 5, 21, 14, 30, 0)
    expected = naive.astimezone().isoformat()
    assert _dt_to_iso(naive) == expected


def test_dt_to_iso_passes_through_aware():
    aware = datetime(2026, 5, 21, 14, 30, 0, tzinfo=timezone(timedelta(hours=8)))
    assert _dt_to_iso(aware) == "2026-05-21T14:30:00+08:00"


def test_dt_to_iso_none():
    assert _dt_to_iso(None) is None


def test_json_safe_preserves_naive_datetime_as_local_wallclock():
    from maxc_cli.helpers import json_safe
    naive = datetime(2026, 5, 21, 14, 30, 0)
    expected = naive.astimezone().isoformat()
    assert json_safe(naive) == expected
