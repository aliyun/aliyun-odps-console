"""Tests for SettingParser."""

import pytest

from maxc_cli.backend.query import _parse_sql_with_hints
from maxc_cli.exceptions import UnsupportedSqlOperationError, ValidationError
from maxc_cli.setting_parser import SettingParser


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


def test_parse_sql_with_hints_default_no_extra_hints():
    actual_sql, hints, priority = _parse_sql_with_hints("SELECT 1")
    assert actual_sql == "SELECT 1"
    assert hints == {}
    assert priority is None


def test_parse_sql_with_hints_merges_user_set():
    actual_sql, hints, priority = _parse_sql_with_hints(
        "SET odps.sql.type.system.odps2=true; SELECT 1"
    )
    assert actual_sql == "SELECT 1"
    assert hints == {
        "odps.sql.type.system.odps2": "true",
    }
    assert priority is None


def test_parse_sql_with_hints_blocks_write_without_force():
    from maxc_cli.exceptions import WriteOperationRequiresForceError
    with pytest.raises(WriteOperationRequiresForceError):
        _parse_sql_with_hints("INSERT INTO t VALUES (1)")
    with pytest.raises(WriteOperationRequiresForceError):
        _parse_sql_with_hints("CREATE TABLE t (id BIGINT)")
    with pytest.raises(WriteOperationRequiresForceError):
        _parse_sql_with_hints("DROP TABLE t")


def test_parse_sql_with_hints_force_allows_write():
    actual_sql, hints, priority = _parse_sql_with_hints("CREATE TABLE t (id BIGINT)", force=True)
    assert actual_sql == "CREATE TABLE t (id BIGINT)"
    assert hints == {}
    assert priority is None


def test_parse_sql_with_hints_force_preserves_user_sets():
    actual_sql, hints, _priority = _parse_sql_with_hints(
        "SET odps.sql.type.system.odps2=true; CREATE TABLE t (id BIGINT)",
        force=True,
    )
    assert actual_sql == "CREATE TABLE t (id BIGINT)"
    assert hints == {"odps.sql.type.system.odps2": "true"}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; CREATE TABLE t (id BIGINT)",
        "CREATE TABLE t (id BIGINT); SELECT 1",
    ],
)
def test_parse_sql_with_hints_blocks_write_anywhere_in_script(sql):
    from maxc_cli.exceptions import WriteOperationRequiresForceError

    with pytest.raises(WriteOperationRequiresForceError):
        _parse_sql_with_hints(sql)


def test_parse_sql_with_hints_ignores_semicolon_inside_string():
    _, hints, _ = _parse_sql_with_hints("SELECT 'a;b' AS value")
    assert "odps.sql.submit.mode" not in hints


def test_parse_sql_with_hints_enables_script_mode_for_top_level_semicolon():
    _, hints, _ = _parse_sql_with_hints("SELECT 'a;b'; SELECT 2")
    assert hints["odps.sql.submit.mode"] == "script"


@pytest.mark.parametrize(
    "sql",
    [
        "IF (true) INSERT INTO t SELECT * FROM source",
        "BEGIN; IF (true) UPDATE t SET value = 1; END IF",
        "CLONE TABLE source TO target IF EXISTS OVERWRITE",
        "RESTORE TABLE source TO VERSION AS OF 3",
        "KILL 20260830123456789gabcdef",
        "ALIAS resource_name AS resource_alias",
        "MSCK REPAIR TABLE external_table ADD PARTITIONS",
        "UNLOAD FROM (SELECT * FROM source) INTO LOCATION 'oss://bucket/path'",
        "SETPROJECT odps.sql.allow.fullscan=true",
        (
            "FROM source "
            "INSERT INTO t1 SELECT id WHERE kind = 1 "
            "INSERT INTO t2 SELECT id WHERE kind = 2"
        ),
        (
            "WITH c AS (SELECT * FROM source) FROM c "
            "INSERT INTO t1 SELECT id INSERT INTO t2 SELECT id"
        ),
        "IF (c1) IF (c2) INSERT INTO t SELECT * FROM source",
        "BEGIN IF (true) INSERT INTO t SELECT * FROM source",
        "IF (true) INSERT INTO t SELECT CASE WHEN 1=1 THEN 1 ELSE 0 END",
    ],
)
def test_parse_sql_with_hints_blocks_nested_script_writes(sql):
    from maxc_cli.exceptions import WriteOperationRequiresForceError

    with pytest.raises(WriteOperationRequiresForceError):
        _parse_sql_with_hints(sql)


def test_parse_sql_with_hints_force_allows_multi_insert():
    sql = (
        "FROM source "
        "INSERT INTO t1 SELECT id WHERE kind = 1 "
        "INSERT INTO t2 SELECT id WHERE kind = 2"
    )
    actual_sql, _, _ = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql


@pytest.mark.parametrize(
    "sql",
    [
        "WITH c AS (SELECT * FROM update_log) SELECT * FROM c",
        "WITH alias AS (SELECT 1 AS value) SELECT * FROM alias",
        "WITH c AS (SELECT value AS kill FROM source) SELECT * FROM c",
        "WITH c AS (SELECT load(value) AS value FROM source) SELECT * FROM c",
    ],
)
def test_cte_identifiers_and_udfs_are_not_write_operations(sql):
    actual_sql, _, _ = _parse_sql_with_hints(sql)
    assert actual_sql == sql


def test_cte_insert_still_requires_force():
    from maxc_cli.exceptions import WriteOperationRequiresForceError

    sql = "WITH c AS (SELECT * FROM source) INSERT INTO target SELECT * FROM c"
    with pytest.raises(WriteOperationRequiresForceError):
        _parse_sql_with_hints(sql)


def test_bare_setproject_inspection_is_read_only():
    actual_sql, _, _ = _parse_sql_with_hints("SETPROJECT;")
    assert actual_sql == "SETPROJECT;"


def test_script_assignment_requires_explicit_maintainer_force():
    sql = "IF (cond) @t := SELECT 1 AS id;"
    with pytest.raises(UnsupportedSqlOperationError):
        _parse_sql_with_hints(sql)
    actual_sql, _, _ = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql


def test_code_embedded_temporary_function_requires_explicit_maintainer_force():
    sql = (
        "CREATE TEMPORARY FUNCTION foo AS 'com.example.Foo' USING\n"
        "#CODE ('lang'='JAVA')\n"
        "public class Foo { public Long evaluate(Long v) { return v + 1; } }\n"
        "#END CODE;"
    )
    with pytest.raises(UnsupportedSqlOperationError):
        _parse_sql_with_hints(sql)
    actual_sql, _, _ = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql


@pytest.mark.parametrize(
    "sql",
    [
        "PAI -name xgboost -DoutputTableName=out",
        "EXECUTE IMMEDIATE 'DROP TABLE target'",
        "CALL update_catalog()",
        "CLEAR EXPIRED GRANTS",
        "PUT POLICY policy_name",
        "FUTURE_DIALECT_COMMAND target",
    ],
)
def test_unknown_or_unproven_sql_operations_fail_closed(sql):
    with pytest.raises(UnsupportedSqlOperationError, match="not proven read-only"):
        _parse_sql_with_hints(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SHOW TABLES",
        "DESC project.schema.table",
        "DESCRIBE project.schema.table",
        "EXPLAIN SELECT * FROM project.schema.table",
        "WITH c AS (SELECT 1 AS id) SELECT * FROM c",
        "SETPROJECT;",
    ],
)
def test_proven_read_only_sql_operations_are_allowed(sql):
    actual_sql, _, _ = _parse_sql_with_hints(sql)
    assert actual_sql == sql


def test_permanent_sql_function_still_requires_force():
    from maxc_cli.exceptions import WriteOperationRequiresForceError

    with pytest.raises(WriteOperationRequiresForceError):
        _parse_sql_with_hints("CREATE SQL FUNCTION foo AS 'com.example.Foo'")


def test_parse_sql_with_hints_invalid_set_raises():
    with pytest.raises(ValidationError, match="Invalid SET statement"):
        _parse_sql_with_hints("SET no_semicolon SELECT 1")


def test_parse_sql_with_hints_extracts_priority():
    actual_sql, hints, priority = _parse_sql_with_hints(
        "SET odps.instance.priority=3; SELECT 1"
    )
    assert actual_sql == "SELECT 1"
    assert priority == 3
    # priority must be stripped from the hints dict — it's a run_sql kwarg, not a SQL hint.
    assert "odps.instance.priority" not in hints


def test_parse_sql_with_hints_priority_case_insensitive_key():
    _, hints, priority = _parse_sql_with_hints(
        "SET ODPS.Instance.Priority=5; SELECT 1"
    )
    assert priority == 5
    assert hints == {}


def test_parse_sql_with_hints_priority_invalid_raises():
    with pytest.raises(ValidationError, match="odps.instance.priority"):
        _parse_sql_with_hints("SET odps.instance.priority=high; SELECT 1")


def test_parse_sql_with_hints_priority_coexists_with_other_hints():
    _, hints, priority = _parse_sql_with_hints(
        "SET odps.instance.priority=1; "
        "SET odps.sql.type.system.odps2=true; "
        "SELECT 1"
    )
    assert priority == 1
    assert hints == {"odps.sql.type.system.odps2": "true"}


# --- translate_odps_error readonly detection tests ---


def test_translate_odps_error_detects_readonly_mode():
    from maxc_cli.exceptions import ReadOnlyError
    from maxc_cli.helpers import translate_odps_error

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
    from maxc_cli.exceptions import ReadOnlyError, SqlError
    from maxc_cli.helpers import translate_odps_error

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
    """Verify that client-side write detection returns WRITE_OPERATION_REQUIRES_FORCE with hints."""
    import io
    import json

    from maxc_cli.cli import run

    stdout = io.StringIO()

    exit_code = run(
        ["query", "CREATE TABLE t (id BIGINT)", "--json"],
        stdout=stdout,
    )

    assert exit_code != 0
    output = json.loads(stdout.getvalue())
    assert output["error"]["code"] == "WRITE_OPERATION_REQUIRES_FORCE"
    assert output["error"]["recoverable"] is False
    assert "--force" not in output["error"].get("suggestion", "")
    assert "outside" in output["error"].get("suggestion", "")
