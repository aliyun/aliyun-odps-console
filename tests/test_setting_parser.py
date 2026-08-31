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


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO target SELECT * FROM source",
        "INSERT OVERWRITE TABLE target SELECT * FROM source",
        "UPDATE target SET value = 1 WHERE id = 7",
        "UPDATE target SET label = 3, owner = 'team', authorization = 'approved' WHERE id = 7",
        "DELETE FROM target WHERE id = 7",
        "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET value = source.value",
        "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET label = source.label, owner = source.owner, authorization = source.authorization",
        "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT VALUES(source.id, source.value)",
        "CREATE EXTERNAL TABLE ext (id BIGINT)",
        "CREATE OBJECT TABLE objects LOCATION 'oss://bucket/path'",
        "CREATE ICEBERG TABLE lake (id BIGINT) WITH CONNECTION conn OPTIONS(location='oss://bucket/path')",
        "CREATE SNAPSHOT TABLE target_snapshot CLONE target",
        "CREATE OR REPLACE SNAPSHOT TABLE target_snapshot CLONE target",
        "CREATE SNAPSHOT TABLE expiring_snapshot CLONE target OPTIONS(description='verified', expiration_timestamp=TIMESTAMP '2099-01-01 00:00:00')",
        "CREATE OR REPLACE TABLE target (id BIGINT)",
        "CREATE OR REPLACE VIEW latest AS SELECT * FROM source",
        "CREATE SQL FUNCTION foo AS 'com.example.Foo'",
        "CREATE SCHEMA analytics",
        "CREATE EXTERNAL SCHEMA ext_schema WITH fs_hive ON 'default'",
        "ALTER SCHEMA analytics SET COMMENT 'verified schema'",
        "ALTER SCHEMA analytics SET COMMENT 'a--b /* literal */' /* trailing */",
        "DROP SCHEMA IF EXISTS old_analytics",
        "ALTER TABLE target ADD COLUMNS (value STRING)",
        "ALTER TABLE target COMPACT MAJOR",
        "ALTER VIEW old_view RENAME TO new_view",
        "ALTER VIEW `project`.`schema`.`old_view` RENAME TO `project`.`schema`.`new_view`",
        "ALTER VIEW old_view /* CHANGEOWNER TO attacker */ RENAME TO new_view",
        "ALTER SNAPSHOT TABLE target_snapshot SET OPTIONS(description='verified snapshot', expiration_timestamp=TIMESTAMP '2099-01-01 00:00:00')",
        "ALTER /* harmless */ SNAPSHOT TABLE target_snapshot SET OPTIONS(description='verified snapshot')",
        "ALTER SNAPSHOT TABLE IF EXISTS target_snapshot SET OPTIONS(description='verified snapshot')",
        "DROP VIEW old_view",
        "DROP TABLE IF EXISTS old_table",
        "DROP TABLE /* GRANT SELECT */ old_table",
        "DROP SNAPSHOT TABLE target_snapshot",
        "DROP FUNCTION old_function",
        "DROP MATERIALIZED VIEW IF EXISTS old_mv PURGE",
        "PURGE TABLE target",
        "TRUNCATE TABLE target",
        "MSCK REPAIR TABLE external_table ADD PARTITIONS",
        "ANALYZE TABLE target COMPUTE STATISTICS",
        "LOAD INTO TABLE target FROM LOCATION 'oss://bucket/path' STORED BY 'com.aliyun.odps.CsvStorageHandler'",
        "LOAD OVERWRITE TABLE target FROM LOCATION 'oss://bucket/path' STORED BY 'com.aliyun.odps.CsvStorageHandler'",
        "UNLOAD FROM (SELECT * FROM source) INTO LOCATION 'oss://bucket/path'",
    ],
)
def test_parse_sql_with_hints_force_allows_recognized_data_plane_mutation(sql):
    actual_sql, _, _ = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql


@pytest.mark.parametrize(
    "sql",
    [
        'INSERT INTO TABLE target VALUES (R"("a;b")")',
        "INSERT INTO TABLE target VALUES (R'(\"a;b\" and \'quoted\')')",
        'INSERT INTO TABLE target VALUES (r"(lowercase; raw \' quote)")',
    ],
)
def test_force_treats_maxcompute_raw_literal_content_as_one_value(sql):
    actual_sql, hints, priority = _parse_sql_with_hints(sql, force=True)

    assert actual_sql == sql
    assert hints == {}
    assert priority is None


@pytest.mark.parametrize(
    "sql",
    [
        (
            'SET odps.sql.submit.mode=script; '
            'INSERT INTO TABLE target VALUES (R"(foo"bar)"); '
            'DROP TABLE hidden;'
        ),
        (
            "SET odps.sql.submit.mode=script; "
            "INSERT INTO TABLE target VALUES (R'(foo'bar)'); "
            "DROP TABLE hidden;"
        ),
    ],
)
def test_force_raw_literal_cannot_hide_a_second_statement(sql):
    with pytest.raises(ValidationError, match="exactly one executable SQL statement"):
        _parse_sql_with_hints(sql, force=True)


def test_force_allows_admin_words_inside_quoted_table_comment():
    sql = "ALTER TABLE target SET COMMENT 'CHANGEOWNER OWNER TO SET LABEL'"
    actual_sql, _, _ = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql


@pytest.mark.parametrize(
    "sql",
    [
        (
            "CREATE SQL FUNCTION my_sum(@a BIGINT, @b BIGINT) "
            "RETURNS @my_sum BIGINT AS BEGIN "
            "@temp := @a + @b; @my_sum := @temp + 1; END;"
        ),
        (
            "CREATE SQL FUNCTION literal_value(@a STRING) "
            "RETURNS @literal_value STRING AS BEGIN "
            "@temp := 'literal'; @literal_value := @temp; END;"
        ),
        (
            "CREATE SQL FUNCTION call_expression(@a STRING) "
            "RETURNS @call_expression STRING AS BEGIN "
            "@call_expression := load(@a); END;"
        ),
        (
            "CREATE SQL FUNCTION trim_expression(@a STRING) "
            "RETURNS @trim_expression STRING AS BEGIN "
            "@trim_expression := TRIM(BOTH 'x' FROM @a); END;"
        ),
        (
            "CREATE SQL FUNCTION extract_expression(@a TIMESTAMP) "
            "RETURNS @extract_expression BIGINT AS BEGIN "
            "@extract_expression := EXTRACT(YEAR FROM @a); END;"
        ),
        (
            "CREATE VIEW IF NOT EXISTS pv2 (@sale_date STRING, @region STRING) "
            "AS BEGIN "
            "@srcp := SELECT * FROM src WHERE ds=@sale_date; "
            "@pv2 := SELECT * FROM @srcp WHERE region=@region; END;"
        ),
    ],
)
def test_force_allows_one_compound_view_or_sql_function_ddl(sql):
    actual_sql, hints, _priority = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql
    assert hints["odps.sql.submit.mode"] == "script"


def test_force_compound_ddl_rejects_embedded_unrelated_command():
    sql = (
        "CREATE VIEW pv AS BEGIN "
        "@pv := SELECT * FROM source; DROP TABLE source; END;"
    )
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


@pytest.mark.parametrize(
    "rhs",
    [
        "EXECUTE IMMEDIATE 'DROP TABLE target'",
        "CALL update_catalog()",
        "PAI -name xgboost",
        "(DELETE FROM target WHERE id=1)",
        "SELECT 1",
        "WITH c AS (SELECT 1) SELECT * FROM c",
    ],
)
def test_force_compound_sql_function_rejects_command_shaped_rhs(rhs):
    sql = (
        "CREATE SQL FUNCTION f(@a BIGINT) RETURNS @f BIGINT AS BEGIN "
        f"@f := {rhs}; END;"
    )
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


def test_force_compound_view_rejects_with_wrapped_mutation():
    sql = (
        "CREATE VIEW pv AS BEGIN "
        "@pv := WITH c AS (SELECT 1 AS id) "
        "INSERT INTO target SELECT id FROM c; END;"
    )
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


def test_force_compound_ddl_rejects_trailing_statement_after_end():
    sql = (
        "CREATE SQL FUNCTION f(@a BIGINT) AS BEGIN @f := @a + 1; END; "
        "DROP TABLE source;"
    )
    with pytest.raises(ValidationError, match="exactly one executable SQL statement"):
        _parse_sql_with_hints(sql, force=True)


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE VIEW v AS SELECT authorization, changeowner FROM source",
        "CREATE TABLE t AS SELECT authorization, changeowner FROM source",
        "ALTER TABLE target ADD COLUMNS (authorization STRING, changeowner STRING)",
    ],
)
def test_force_allows_admin_like_column_names_in_data_plane_ddl(sql):
    actual_sql, _, _ = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql


def test_force_rejects_unsupported_legacy_load_data_shape():
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(
            "LOAD DATA INPATH 'oss://bucket/path' INTO TABLE target",
            force=True,
        )


def test_parse_sql_with_hints_force_preserves_user_sets():
    actual_sql, hints, _priority = _parse_sql_with_hints(
        "SET odps.sql.type.system.odps2=true; CREATE TABLE t (id BIGINT)",
        force=True,
    )
    assert actual_sql == "CREATE TABLE t (id BIGINT)"
    assert hints == {"odps.sql.type.system.odps2": "true"}


@pytest.mark.parametrize(
    "setting_key",
    [
        "CheckPermissionUsingACL",
        "CHECKPERMISSIONUSINGPOLICY",
        "ObjectCreatorHasAccessPermission",
        "objectcreatorhasgrantpermission",
        "LabelSecurity",
        "ProjectProtection",
        "odps.output.field.formatter",
        "ODPS.ISOLATION.SESSION.ENABLE",
        "odps.forbid.fetch.result.by.bearertoken",
        "odps.security.enabledownloadprivilege",
        "odps.security.ip.whitelist",
        "odps.security.ip.whitelist.services",
        "odps.security.vpc.whitelist",
    ],
)
@pytest.mark.parametrize(
    ("statement", "force"),
    [
        ("SELECT 1", False),
        ("CREATE TABLE t (id BIGINT)", True),
    ],
)
def test_project_security_and_masking_set_hints_are_always_blocked(
    setting_key,
    statement,
    force,
):
    with pytest.raises(
        UnsupportedSqlOperationError,
        match="controls project security or data masking",
    ):
        _parse_sql_with_hints(
            f"SET {setting_key}=false; {statement}",
            force=force,
        )


def test_force_rejects_unreviewed_set_hint_but_select_preserves_compatibility():
    sql = "SET future.vendor.execution.control=true; SELECT 1"
    actual_sql, hints, _priority = _parse_sql_with_hints(sql)
    assert actual_sql == "SELECT 1"
    assert hints == {"future.vendor.execution.control": "true"}

    with pytest.raises(
        UnsupportedSqlOperationError,
        match="not an audited execution hint for mutating SQL",
    ):
        _parse_sql_with_hints(
            "SET future.vendor.execution.control=true; "
            "CREATE TABLE t (id BIGINT)",
            force=True,
        )


def test_blocked_set_name_in_value_does_not_trigger_key_check():
    actual_sql, hints, _priority = _parse_sql_with_hints(
        "SET odps.sql.type.system.odps2='ProjectProtection'; SELECT 1"
    )
    assert actual_sql == "SELECT 1"
    assert hints == {"odps.sql.type.system.odps2": "'ProjectProtection'"}


@pytest.mark.parametrize(
    ("setting_key", "statement"),
    [
        ("odps.namespace.schema", "CREATE SCHEMA analytics"),
        (
            "odps.sql.allow.namespace.schema",
            "INSERT INTO analytics.target SELECT * FROM analytics.source",
        ),
        ("odps.sql.bigquery.compatible", "CREATE TABLE analytics (value BIGINT)"),
    ],
)
def test_force_allows_audited_execution_hint(setting_key, statement):
    actual_sql, hints, _priority = _parse_sql_with_hints(
        f"SET {setting_key}=true; {statement}",
        force=True,
    )
    assert actual_sql == statement
    assert hints == {setting_key: "true"}


def test_force_allows_delta_insert_deduplication_hint():
    sql = (
        "SET odps.sql.insert.acidtable.deduplicate.enable=true; "
        "INSERT INTO TABLE target VALUES (1)"
    )
    actual_sql, hints, _priority = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == "INSERT INTO TABLE target VALUES (1)"
    assert hints == {
        "odps.sql.insert.acidtable.deduplicate.enable": "true",
    }


def test_parse_sql_with_hints_force_rejects_mixed_statements():
    with pytest.raises(ValidationError, match="exactly one executable SQL statement"):
        _parse_sql_with_hints(
            "SELECT 1; CREATE TABLE t (id BIGINT)",
            force=True,
        )


def test_parse_sql_with_hints_force_still_validates_set_syntax():
    with pytest.raises(ValidationError, match="Invalid SET statement"):
        _parse_sql_with_hints(
            "SET odps.sql.type.system.odps2=true CREATE TABLE t (id BIGINT)",
            force=True,
        )


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


def test_parse_sql_with_hints_force_rejects_multi_target_insert():
    sql = (
        "FROM source "
        "INSERT INTO t1 SELECT id WHERE kind = 1 "
        "INSERT INTO t2 SELECT id WHERE kind = 2"
    )
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


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


def test_script_assignment_is_not_a_public_force_operation():
    sql = "IF (cond) @t := SELECT 1 AS id;"
    with pytest.raises(UnsupportedSqlOperationError):
        _parse_sql_with_hints(sql)
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


def test_code_embedded_temporary_function_is_not_a_public_force_operation():
    sql = (
        "CREATE TEMPORARY FUNCTION foo AS 'com.example.Foo' USING\n"
        "#CODE ('lang'='JAVA')\n"
        "public class Foo { public Long evaluate(Long v) { return v + 1; } }\n"
        "#END CODE;"
    )
    with pytest.raises(UnsupportedSqlOperationError):
        _parse_sql_with_hints(sql)
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


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
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


@pytest.mark.parametrize(
    "sql",
    [
        "GRANT Select ON TABLE t TO USER 'someone'",
        "REVOKE Select ON TABLE t FROM USER 'someone'",
        "KILL 20260830123456789gabcdef",
        "USE another_project",
        "INSTALL PACKAGE package_name",
        "CREATE ROLE analysts",
        "ALTER USER someone IDENTIFIED BY 'new-secret'",
        "DROP POLICY sensitive_policy",
        "CREATE RESOURCE lib.jar",
        "CREATE OR REPLACE PACKAGE package_name",
        "ALTER SYSTEM SET quota = 'other'",
        "ALTER TABLE target CHANGEOWNER TO 'RAM$123:user'",
        "ALTER VIEW target CHANGEOWNER TO 'RAM$123:user'",
        "ALTER SCHEMA analytics OWNER TO someone",
        "ALTER TABLE target SET LABEL 3",
        "CREATE SCHEMA analytics AUTHORIZATION someone",
        "CREATE SCHEMA AUTHORIZATION someone",
        "CREATE PROJECT other_project",
        "ALTER PROJECT other_project SET COMMENT 'changed'",
        "DROP PROJECT other_project",
        "CREATE DATABASE other_database",
        "DROP DATABASE other_database",
        "CREATE SECURITY LABEL sensitive_label",
        "CREATE TENANT other_tenant",
        "ALTER CLUSTER shared_cluster",
        "DROP QUOTA production_quota",
    ],
)
def test_permission_session_and_admin_operations_are_not_public_force_sql(sql):
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


@pytest.mark.parametrize(
    "sql",
    [
        "WITH c AS (SELECT 1 AS id) INSERT INTO target SELECT id FROM c",
        "WITH c AS (SELECT 1 AS id) UPDATE target SET value = c.id FROM c WHERE target.id = c.id",
        "WITH c AS (SELECT 1 AS id) DELETE FROM target WHERE id IN (SELECT id FROM c)",
        "WITH c AS (SELECT 1 AS id) MERGE INTO target USING c ON target.id = c.id WHEN MATCHED THEN UPDATE SET value = c.id",
        "WITH c AS (SELECT 1 AS id) FROM c INSERT INTO TABLE target SELECT id",
    ],
)
def test_cte_dml_is_one_recognized_force_operation(sql):
    actual_sql, _, _ = _parse_sql_with_hints(sql, force=True)
    assert actual_sql == sql


@pytest.mark.parametrize(
    "sql",
    [
        "WITH c AS (SELECT 1) DELETE PROJECT p",
        "WITH c AS (SELECT 1) UPDATE target",
        "WITH c AS (SELECT 1) MERGE PROJECT p",
        "WITH c AS (SELECT 1) REPLACE PROJECT p",
        "WITH c AS (SELECT 1 AS id) REPLACE INTO target SELECT id FROM c",
        "INSERT INTO TABLE t1 SELECT * FROM source INSERT INTO TABLE t2 SELECT * FROM source",
        "WITH c AS (SELECT * FROM source) INSERT INTO TABLE t1 SELECT * FROM c INSERT INTO TABLE t2 SELECT * FROM c",
        "INSERT OVERWRITE TABLE t1 SELECT * FROM source INSERT OVERWRITE TABLE t2 SELECT * FROM source",
        "UPDATE t SET value=1 UPDATE t2 SET value=2",
        "DELETE FROM t DELETE FROM t2",
        "MERGE INTO t USING source ON t.id=source.id WHEN MATCHED THEN UPDATE SET value=1 MERGE INTO t2 USING source ON t2.id=source.id WHEN MATCHED THEN DELETE",
        "UPSERT INTO target SELECT * FROM source",
        "REPLACE INTO target SELECT * FROM source",
        "OPTIMIZE TABLE target",
        "COMPACT TABLE target",
        "VACUUM TABLE target",
        "ALTER VIEW v AS SELECT * FROM source",
        "ALTER VIEW v RENAME TO v2 CHANGEOWNER TO 'RAM$123:user'",
        "/* reviewed */ ALTER VIEW v AS SELECT * FROM source",
        "ALTER SNAPSHOT TABLE target_snapshot SET OPTIONS(access_permissions='everyone')",
        "CREATE SNAPSHOT TABLE target_snapshot CLONE target OPTIONS(access_permissions='everyone')",
        "/* reviewed */ ALTER SNAPSHOT TABLE target_snapshot SET OPTIONS(access_permissions='everyone')",
        "-- reviewed\nALTER SNAPSHOT TABLE target_snapshot SET OPTIONS(access_permissions='everyone')",
        "ALTER SNAPSHOT TABLE target_snapshot SET OPTIONS(description='ok') CHANGEOWNER TO 'RAM$123:user'",
        "PURGE ALL",
        "RENAME TABLE old_table TO new_table",
        "RENAME VIEW old_view TO new_view",
        "CREATE OR REPLACE FUNCTION f AS 'com.example.F'",
        "CREATE OR REPLACE SQL FUNCTION f(@a BIGINT) RETURNS @f BIGINT AS BEGIN @f := @a; END;",
        "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT 1",
        "CREATE OR REPLACE ICEBERG TABLE lake (id BIGINT)",
        "ALTER FUNCTION f RENAME TO f2",
        "DROP SCHEMA analytics CASCADE",
        "DROP TABLE t CASCADE",
        "DROP VIEW v CASCADE",
        "DROP FUNCTION f CASCADE",
        "DROP SNAPSHOT TABLE snapshot_t CASCADE",
        "ALTER SCHEMA analytics RENAME TO renamed",
        "/* reviewed */ ALTER SCHEMA analytics RENAME TO renamed",
        "CREATE TABLE t(id BIGINT) GRANT SELECT ON TABLE t TO USER 'u'",
        "CREATE TABLE t(id BIGINT) REMOVE USER u",
        "CREATE TABLE t(id BIGINT) ADD ACCOUNTPROVIDER RAM",
        "CREATE TABLE t(id BIGINT) REMOVE ACCOUNTPROVIDER RAM",
        "CREATE TABLE t(id BIGINT) ADD TRUSTEDPROJECT p",
        "CREATE TABLE t(id BIGINT) REMOVE TRUSTEDPROJECT p",
        "CREATE TABLE t(id BIGINT) ADD TABLE source TO PACKAGE p",
        "CREATE TABLE t(id BIGINT) REMOVE TABLE source FROM PACKAGE p",
        "CREATE TABLE t(id BIGINT) DELETE PACKAGE p",
        "CREATE TABLE t(id BIGINT) ALTER ROLE r",
        "CREATE VIEW v AS SELECT * FROM source GRANT SELECT ON TABLE v TO USER 'u'",
        "CREATE FUNCTION f AS 'C' USING 'r.jar' ADD FILE secret",
        "DROP TABLE t CREATE ROLE r",
        "DROP FUNCTION f DROP RESOURCE r",
        "ALTER TABLE t ADD COLUMNS(value BIGINT) ADD USER u",
        "CREATE TABLE t(id BIGINT) ADD JAR resource.jar",
        "CREATE TABLE t(id BIGINT) ADD PY resource.py",
        "CREATE TABLE t(id BIGINT) ADD ARCHIVE resource.zip",
        "CREATE TABLE t(id BIGINT) ADD TABLE source AS source_resource",
        "CREATE TABLE t(id BIGINT) ALIAS current_resource=next_resource",
        "INSERT INTO target VALUES(1) SET LABEL 3 TO TABLE target",
        "DELETE FROM target WHERE id=1 SET LABEL 3 TO TABLE target",
        "UPDATE target SET label=1 WHERE id=1 SET LABEL 3 TO TABLE target",
        "MERGE INTO target USING source ON target.id=source.id WHEN MATCHED THEN UPDATE SET label=source.label SET LABEL 3 TO TABLE target",
        "ANALYZE TABLE t COMPUTE STATISTICS INSTALL PACKAGE p",
        "LOAD INTO TABLE t FROM LOCATION 'oss://bucket/path' STORED AS PARQUET INSTALL PACKAGE p",
        "UNLOAD FROM (SELECT * FROM source) INTO LOCATION 'oss://bucket/path' GRANT SELECT ON TABLE source TO USER 'u'",
        "PURGE TABLE t DROP PROJECT p",
        "INSERT INTO target SELECT * FROM source CREATE TABLE hidden(id BIGINT)",
        "UPDATE target SET value=1 DROP TABLE hidden",
        "DELETE FROM target ALTER TABLE hidden DROP COLUMN value",
        "MERGE INTO target USING source ON target.id=source.id WHEN MATCHED THEN UPDATE SET value=1 TRUNCATE TABLE hidden",
        "CREATE TABLE target(id BIGINT) INSERT INTO hidden VALUES(1)",
        "ALTER TABLE target ADD COLUMNS(value BIGINT) UPDATE hidden SET value=1",
        "TRUNCATE TABLE target DELETE FROM hidden",
        "LOAD INTO TABLE target FROM LOCATION 'oss://bucket/path' STORED AS PARQUET INSERT INTO hidden VALUES(1)",
        "UPDATE target",
        "INSERT OVERWRITE target SELECT * FROM source",
        "INSERT OVERWRITE DIRECTORY 'oss://bucket/path' SELECT * FROM source",
        "INSERT INTO DIRECTORY 'oss://bucket/path' SELECT * FROM source",
        "WITH c AS (SELECT 1) INSERT OVERWRITE DIRECTORY 'oss://bucket/path' SELECT * FROM c",
        (
            "WITH c AS (SELECT * FROM source) FROM c "
            "INSERT INTO TABLE t1 SELECT id "
            "INSERT OVERWRITE TABLE t2 SELECT id"
        ),
        (
            "WITH a AS (SELECT * FROM source), b AS (SELECT * FROM a) FROM b "
            "INSERT INTO TABLE t1 SELECT id "
            "INSERT OVERWRITE TABLE t2 SELECT id"
        ),
    ],
)
def test_force_rejects_unrecognized_direct_or_wrapped_dml_shape(sql):
    with pytest.raises(UnsupportedSqlOperationError, match="one recognized DDL/DML"):
        _parse_sql_with_hints(sql, force=True)


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
    assert "--force" in result.suggestion
    assert "cannot bypass" in result.suggestion
    assert "exact DDL/DML" in result.suggestion


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


def test_cli_readonly_error_has_agent_hints(tmp_path):
    """Verify that client-side write detection returns WRITE_OPERATION_REQUIRES_FORCE with hints."""
    import io
    import json

    from maxc_cli.cli import run

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: test_project\n"
        f"state_dir: {tmp_path / 'state'}\n"
        f"cache_dir: {tmp_path / 'cache'}\n"
        "auth:\n"
        "  provider: access_key\n"
        "  access_id: test_access_id\n"
        "  secret_access_key: test_secret\n"
        "  project: test_project\n"
        "  endpoint: https://service.example.invalid/api\n",
        encoding="utf-8",
    )
    stdout = io.StringIO()

    exit_code = run(
        [
            "--config",
            str(config_path),
            "query",
            "CREATE TABLE t (id BIGINT)",
            "--json",
        ],
        cwd=tmp_path,
        stdout=stdout,
    )

    assert exit_code != 0
    output = json.loads(stdout.getvalue())
    assert output["error"]["code"] == "WRITE_OPERATION_REQUIRES_FORCE"
    assert output["error"]["recoverable"] is False
    assert "--force" in output["error"].get("suggestion", "")
    assert "not authorization" in output["error"].get("suggestion", "")
    assert any(
        "explicitly authorized" in warning
        for warning in output["agent_hints"].get("warnings", [])
    )


def test_cli_force_admin_error_does_not_recommend_force_retry(tmp_path):
    """Administrative SQL stays blocked and recovery must not suggest --force."""
    import io
    import json

    from maxc_cli.cli import run

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: test_project\n"
        f"state_dir: {tmp_path / 'state'}\n"
        f"cache_dir: {tmp_path / 'cache'}\n"
        "auth:\n"
        "  provider: access_key\n"
        "  access_id: test_access_id\n"
        "  secret_access_key: test_secret\n"
        "  project: test_project\n"
        "  endpoint: https://service.example.invalid/api\n",
        encoding="utf-8",
    )
    stdout = io.StringIO()

    exit_code = run(
        [
            "--config",
            str(config_path),
            "query",
            "ALTER SYSTEM SET quota = 'other'",
            "--force",
            "--json",
        ],
        cwd=tmp_path,
        stdout=stdout,
    )

    assert exit_code != 0
    output = json.loads(stdout.getvalue())
    assert output["error"]["code"] == "UNSUPPORTED_SQL_OPERATION"
    assert "dedicated approved workflow" in output["error"]["suggestion"]
    recovery_steps = output["error"]["recovery_steps"]
    assert any("--force does not enable" in step for step in recovery_steps)
    assert not any("submit" in step.lower() and "--force" in step for step in recovery_steps)
    assert any(
        "--force does not enable" in warning
        for warning in output["agent_hints"].get("warnings", [])
    )
