"""Tests for SettingParser."""

import pytest

from maxc_cli.setting_parser import SettingParser
from maxc_cli.backend.query import _parse_sql_with_hints
from maxc_cli.exceptions import ValidationError


def test_no_set_statements():
    r = SettingParser.parse("SELECT 1 AS one")
    assert r.settings == {}
    assert r.remaining_query.strip() == "SELECT 1 AS one"
    assert r.errors == []


def test_single_set():
    r = SettingParser.parse("SET odps.sql.type.system.odps2=true; SELECT 1")
    assert r.settings == {"odps.sql.type.system.odps2": "true"}
    assert "SELECT 1" in r.remaining_query
    assert r.errors == []


def test_multiple_sets():
    sql = (
        "SET odps.sql.type.system.odps2=true; "
        "SET odps.sql.hive.compatible=true; "
        "SELECT 1"
    )
    r = SettingParser.parse(sql)
    assert r.settings == {
        "odps.sql.type.system.odps2": "true",
        "odps.sql.hive.compatible": "true",
    }
    assert "SELECT 1" in r.remaining_query
    assert r.errors == []


def test_set_with_comment_before():
    sql = "-- a comment\nSET odps.sql.type.system.odps2=true; SELECT 1"
    r = SettingParser.parse(sql)
    assert r.settings == {"odps.sql.type.system.odps2": "true"}
    assert "SELECT 1" in r.remaining_query
    assert r.errors == []


def test_set_with_multiline_comment():
    sql = "/* block */ SET odps.sql.type.system.odps2=true; SELECT 1"
    r = SettingParser.parse(sql)
    assert r.settings == {"odps.sql.type.system.odps2": "true"}
    assert "SELECT 1" in r.remaining_query
    assert r.errors == []


def test_set_missing_semicolon():
    r = SettingParser.parse("SET odps.sql.type.system.odps2=true SELECT 1")
    assert len(r.errors) > 0


def test_set_missing_equals():
    r = SettingParser.parse("SET odps.sql.type.system.odps2; SELECT 1")
    assert len(r.errors) > 0


def test_plain_sql_no_set():
    r = SettingParser.parse("  SELECT * FROM t LIMIT 10  ")
    assert r.settings == {}
    assert "SELECT" in r.remaining_query
    assert r.errors == []


def test_set_with_empty_value():
    r = SettingParser.parse("SET key=; SELECT 1")
    assert r.settings == {"key": ""}
    assert "SELECT 1" in r.remaining_query
    assert r.errors == []


def test_set_preserves_case_in_value():
    r = SettingParser.parse("SET k=SomeValue; SELECT 1")
    assert r.settings["k"] == "SomeValue"


def test_set_with_escaped_semicolon():
    r = SettingParser.parse("SET k=val\\;ue; SELECT 1")
    assert r.settings["k"] == "val;ue"
    assert r.errors == []


# --- _parse_sql_with_hints tests ---


def test_parse_sql_with_hints_default_injects_read_only():
    actual_sql, hints = _parse_sql_with_hints("SELECT 1")
    assert actual_sql == "SELECT 1"
    assert hints == {"odps.sql.read.only": "true"}


def test_parse_sql_with_hints_merges_user_set():
    actual_sql, hints = _parse_sql_with_hints(
        "SET odps.sql.type.system.odps2=true; SELECT 1"
    )
    assert actual_sql == "SELECT 1"
    assert hints == {
        "odps.sql.type.system.odps2": "true",
        "odps.sql.read.only": "true",
    }


def test_parse_sql_with_hints_read_only_cannot_be_overridden():
    actual_sql, hints = _parse_sql_with_hints(
        "SET odps.sql.read.only=false; SELECT 1"
    )
    assert hints["odps.sql.read.only"] == "true"


def test_parse_sql_with_hints_force_skips_read_only():
    actual_sql, hints = _parse_sql_with_hints("CREATE TABLE t (id BIGINT)", force=True)
    assert actual_sql == "CREATE TABLE t (id BIGINT)"
    assert "odps.sql.read.only" not in hints


def test_parse_sql_with_hints_force_preserves_user_sets():
    actual_sql, hints = _parse_sql_with_hints(
        "SET odps.sql.type.system.odps2=true; CREATE TABLE t (id BIGINT)",
        force=True,
    )
    assert actual_sql == "CREATE TABLE t (id BIGINT)"
    assert hints == {"odps.sql.type.system.odps2": "true"}
    assert "odps.sql.read.only" not in hints


def test_parse_sql_with_hints_invalid_set_raises():
    with pytest.raises(ValidationError, match="Invalid SET statement"):
        _parse_sql_with_hints("SET no_semicolon SELECT 1")


# --- translate_odps_error readonly detection tests ---


def test_translate_odps_error_detects_readonly_mode():
    from maxc_cli.helpers import translate_odps_error
    from maxc_cli.exceptions import ReadOnlyError

    try:
        from odps.errors import ODPSError
    except ImportError:
        pytest.skip("pyodps not installed")

    exc = ODPSError(
        "ODPS-0130071:[1,1] Semantic analysis exception - "
        "invalid statement in readonly mode, please 'set odps.sql.read.only=false' and try again"
    )
    result = translate_odps_error(exc)
    assert isinstance(result, ReadOnlyError)
    assert result.error_code == "READ_ONLY_VIOLATION"
    assert "odpscmd" in result.suggestion


def test_translate_odps_error_type_error_not_readonly():
    from maxc_cli.helpers import translate_odps_error
    from maxc_cli.exceptions import ReadOnlyError, SqlError

    try:
        from odps.errors import ODPSError
    except ImportError:
        pytest.skip("pyodps not installed")

    exc = ODPSError(
        "ODPS-0130071:[1,1] Semantic analysis exception - type conversion error"
    )
    result = translate_odps_error(exc)
    assert isinstance(result, SqlError)
    assert not isinstance(result, ReadOnlyError)


def test_cli_readonly_error_has_agent_hints():
    """Verify that READ_ONLY_VIOLATION maps to agent_hints with --force mention."""
    import io
    import json
    from unittest.mock import patch, MagicMock

    from maxc_cli.cli import run

    # Simulate a readonly error from the backend
    from maxc_cli.exceptions import ReadOnlyError

    mock_app = MagicMock()
    mock_app.query.side_effect = ReadOnlyError(
        "invalid statement in readonly mode",
        suggestion="Use odpscmd for write operations.",
    )

    stdout = io.StringIO()

    with patch("maxc_cli.cli.MaxCApp", return_value=mock_app):
        exit_code = run(
            ["query", "CREATE TABLE t (id BIGINT)", "--json"],
            stdout=stdout,
        )

    assert exit_code != 0
    output = json.loads(stdout.getvalue())
    assert output["error"]["code"] == "READ_ONLY_VIOLATION"
    hints = output.get("agent_hints", {})
    next_actions = hints.get("next_actions", [])
    assert any("--force" in action for action in next_actions)
