"""Tests for src/maxc_cli/backend/data.py private helpers."""

import pytest


def test_safe_abort_swallows_abort_exception():
    # Upload sessions can fail to abort (network blip mid-upload). When
    # that happens the *original* CsvParseError or translated ODPSError
    # must still propagate from the upload handler — _safe_abort exists
    # so the secondary abort failure never masks the real cause.
    from maxc_cli.backend.data import _safe_abort

    class _FlakySession:
        def abort(self):
            raise RuntimeError("network blip mid-abort")

    _safe_abort(_FlakySession())  # must not raise


def test_safe_abort_calls_abort_on_success():
    from maxc_cli.backend.data import _safe_abort

    calls = []

    class _CleanSession:
        def abort(self):
            calls.append("aborted")

    _safe_abort(_CleanSession())
    assert calls == ["aborted"]
