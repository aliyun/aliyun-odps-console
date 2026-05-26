"""PR-5 regressions: query subcommand alias detection + path sanitization.

Pins:
  1. `maxc query cost` (no SQL, no --file, no --stdin) raises a clear
     ValidationError telling the user SQL is missing — instead of silently
     treating the literal token ``cost`` as SQL and falling back to the
     deprecated `--mode` warning code path.
  2. `translate_odps_error` strips the user's $HOME from error messages so
     that an ODPS error referencing a file under the user's home directory
     ("/Users/dingxin/.maxc/...") gets emitted as "~/.maxc/..." in the
     envelope. Without this, scripted logs and shared screenshots leak
     local paths.
"""

from __future__ import annotations

import argparse

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.cli import _resolve_query_mode
from maxc_cli.exceptions import ValidationError
from maxc_cli.helpers import translate_odps_error


def _make_query_args(sql_parts, *, file=None, stdin=False, mode="run") -> argparse.Namespace:
    """Build the Namespace shape `_resolve_query_mode` expects."""
    return argparse.Namespace(
        sql_parts=list(sql_parts),
        file=file,
        stdin=stdin,
        mode=mode,
    )


# ── _resolve_query_mode: alias-only invocation surfaces missing SQL ────────


def test_query_alias_without_sql_raises_validation_error() -> None:
    """`maxc query cost` (no SQL anywhere) currently mis-resolves: ``cost``
    is treated as the SQL text and execution proceeds. The fix is to detect
    the alias regardless of whether SQL follows, so the missing-SQL guard
    later in the pipeline can fire — or to fail here with a clean message."""
    args = _make_query_args(["cost"])
    with pytest.raises(ValidationError) as excinfo:
        _resolve_query_mode(args)
    msg = str(excinfo.value).lower()
    assert "cost" in msg
    assert "sql" in msg


def test_query_alias_with_inline_sql_returns_alias_mode() -> None:
    """The existing happy path must still work: alias + SQL → (mode, [sql])."""
    args = _make_query_args(["cost", "SELECT 1"])
    mode, sql_parts = _resolve_query_mode(args)
    assert mode == "cost"
    assert sql_parts == ["SELECT 1"]


def test_query_alias_with_file_returns_alias_mode() -> None:
    """Alias + --file is the case the master plan called out explicitly."""
    args = _make_query_args(["explain"], file="/tmp/q.sql")
    mode, sql_parts = _resolve_query_mode(args)
    assert mode == "explain"
    assert sql_parts == []


def test_query_alias_with_stdin_returns_alias_mode() -> None:
    args = _make_query_args(["run"], stdin=True)
    mode, sql_parts = _resolve_query_mode(args)
    assert mode == "run"
    assert sql_parts == []


def test_plain_sql_without_alias_is_run_mode() -> None:
    """Regression guard: SQL that happens to start with a non-alias word
    must still be treated as plain SQL in default ``run`` mode."""
    args = _make_query_args(["SELECT * FROM t"])
    mode, sql_parts = _resolve_query_mode(args)
    assert mode == "run"
    assert sql_parts == ["SELECT * FROM t"]


# ── $HOME path sanitization ────────────────────────────────────────────────


def test_translate_odps_error_strips_home_from_message(monkeypatch) -> None:
    """An ODPS error that names a file under the user's HOME ends up in the
    envelope with the absolute path replaced by ``~``. Logs and shared
    screenshots no longer leak the user's local layout."""
    fake_home = "/Users/test_user_xyz"
    monkeypatch.setenv("HOME", fake_home)
    raw_msg = f"failed to read config at {fake_home}/.maxc/config.yaml (SemanticAnalysisException)"

    # Build a synthetic ODPS-like exception (translate_odps_error dispatches
    # on isinstance(ODPSError); a plain Exception falls through to the catch-all
    # BackendConnectionError branch — which still goes through the same message
    # variable, so the sanitization point covers both paths.)
    from odps.errors import ODPSError
    exc = ODPSError(raw_msg)
    err = translate_odps_error(exc)
    assert fake_home not in str(err), (
        f"expected $HOME ({fake_home!r}) to be scrubbed; got: {err!s}"
    )
    assert "~/.maxc/config.yaml" in str(err)


def test_translate_odps_error_preserves_unrelated_paths(monkeypatch) -> None:
    """Sanitization must only replace the user's HOME prefix, not arbitrary
    paths. ``/etc/...`` and ``/tmp/...`` stay intact so debugging info isn't
    accidentally rewritten."""
    monkeypatch.setenv("HOME", "/Users/test_user_xyz")
    from odps.errors import ODPSError
    exc = ODPSError("cannot open /etc/odps.conf (SemanticAnalysisException)")
    err = translate_odps_error(exc)
    assert "/etc/odps.conf" in str(err)
