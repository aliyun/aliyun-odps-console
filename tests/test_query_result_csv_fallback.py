"""Regression tests for OdpsBackend._instance_to_query_result.

When the pyodps instance tunnel times out, pyodps falls back to a
CsvRecordReader that lacks a `.schema` attribute (its column metadata
is parsed lazily from the CSV header and stored in `_csv_columns`).

The backend used to misread this as "DDL/DML — no schema" and silently
return an empty result, dropping all rows. These tests pin the corrected
behavior.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from odps.readers import CsvRecordReader

from maxc_cli.backend.odps import OdpsBackend


class _FakeInstance:
    """Minimal stand-in for an ODPS instance object."""

    id = "fake_instance_id"
    start_time = None
    end_time = None

    def __init__(self, reader, *, warn_on_open: str | None = None) -> None:
        self._reader = reader
        self._warn_on_open = warn_on_open

    def open_reader(self):
        if self._warn_on_open:
            import warnings as _w
            _w.warn(self._warn_on_open, UserWarning, stacklevel=2)
        return self._reader

    def get_task_cost(self):
        return None


class _StubBackend:
    """Bind just the methods under test without invoking OdpsBackend.__init__."""

    project = "test_project"
    _instance_to_query_result = OdpsBackend._instance_to_query_result
    _task_cost = OdpsBackend._task_cost


def _run(
    reader,
    *,
    sql: str = "SELECT a, b FROM t",
    max_rows: int = 100,
    offset: int = 0,
    warn_on_open: str | None = None,
):
    instance = _FakeInstance(reader, warn_on_open=warn_on_open)
    return _StubBackend()._instance_to_query_result(
        instance,
        project="test_project",
        max_rows=max_rows,
        sql=sql,
        elapsed_ms=0,
        offset=offset,
    )


def test_csv_fallback_reader_yields_rows_and_schema() -> None:
    """The data-loss bug: tunnel-timeout fallback returns CsvRecordReader,
    which has no `.schema` attribute. Backend must derive schema from
    `_csv_columns` and return the actual rows instead of an empty result.
    """
    csv_text = "chain_id,sales_cnt\n96052793,42\n"
    reader = CsvRecordReader(schema=None, stream=csv_text)

    result = _run(reader, sql="SELECT chain_id, sales_cnt FROM t WHERE chain_id=96052793")

    assert [col["name"] for col in result.schema] == ["chain_id", "sales_cnt"]
    assert result.rows == [{"chain_id": "96052793", "sales_cnt": "42"}]
    assert result.returned_rows == 1
    assert result.total_rows == 1
    assert result.has_more is False


def test_csv_fallback_reader_handles_multiple_rows() -> None:
    csv_text = "a,b,c\n1,x,foo\n2,y,bar\n3,z,baz\n"
    reader = CsvRecordReader(schema=None, stream=csv_text)

    result = _run(reader)

    assert [col["name"] for col in result.schema] == ["a", "b", "c"]
    assert result.rows == [
        {"a": "1", "b": "x", "c": "foo"},
        {"a": "2", "b": "y", "c": "bar"},
        {"a": "3", "b": "z", "c": "baz"},
    ]
    assert result.returned_rows == 3


def test_csv_fallback_reader_respects_offset_and_max_rows() -> None:
    csv_text = "n\n0\n1\n2\n3\n4\n"
    reader = CsvRecordReader(schema=None, stream=csv_text)

    result = _run(reader, max_rows=2, offset=1)

    assert [col["name"] for col in result.schema] == ["n"]
    assert result.rows == [{"n": "1"}, {"n": "2"}]
    assert result.returned_rows == 2


def test_tunnel_reader_path_still_uses_reader_schema() -> None:
    """Tunnel-success path: reader exposes a populated `.schema.columns`.
    Schema must come from there (with real types), not from `_csv_columns`.
    """

    class _Column:
        def __init__(self, name: str, type_: str) -> None:
            self.name = name
            self.type = type_

    class _Schema:
        columns = [_Column("id", "bigint"), _Column("name", "string")]

    class _Record:
        def __init__(self, values):
            self.values = values

    class _TunnelReader:
        schema = _Schema()
        count = 2

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __iter__(self):
            return iter([_Record([1, "alice"]), _Record([2, "bob"])])

    result = _run(_TunnelReader())

    assert result.schema == [
        {"name": "id", "type": "bigint", "comment": ""},
        {"name": "name", "type": "string", "comment": ""},
    ]
    assert result.rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    assert result.total_rows == 2
    assert result.returned_rows == 2


@pytest.mark.parametrize(
    ("sql", "operation"),
    [
        ("CREATE TABLE IF NOT EXISTS t (id BIGINT)", "CREATE"),
        ("SET odps.sql.type.system.odps2=true; CREATE TABLE t (id BIGINT)", "CREATE"),
        ("CLONE TABLE source TO target IF EXISTS OVERWRITE", "CLONE"),
        ("RESTORE TABLE source TO VERSION AS OF 3", "RESTORE"),
        ("KILL 20260830123456789gabcdef", "KILL"),
        ("ALIAS resource_name AS resource_alias", "ALIAS"),
        ("MSCK REPAIR TABLE external_table ADD PARTITIONS", "MSCK"),
        ("UNLOAD FROM (SELECT * FROM source) INTO LOCATION 'oss://bucket/path'", "UNLOAD"),
        ("SETPROJECT odps.sql.allow.fullscan=true", "SETPROJECT"),
    ],
)
def test_write_statement_does_not_open_result_reader(sql: str, operation: str) -> None:
    """A successful DDL job has no result set, so PyODPS must not open a reader."""

    class _WriteInstance(_FakeInstance):
        def open_reader(self):
            raise AssertionError("result reader must not be opened for a write statement")

    instance = _WriteInstance(reader=None)
    result = _StubBackend()._instance_to_query_result(
        instance,
        project="test_project",
        max_rows=100,
        sql=sql,
        elapsed_ms=12,
    )

    assert result.rows == []
    assert result.schema == []
    assert result.total_rows == 0
    assert result.returned_rows == 0
    assert result.extra_metadata["result_kind"] == "statement"
    assert result.extra_metadata["statement_operation"] == operation


def test_script_with_select_before_cleanup_write_still_reads_result() -> None:
    reader = CsvRecordReader(schema=None, stream="value\n1\n")
    result = _run(
        reader,
        sql=(
            "CREATE TABLE IF NOT EXISTS t (id BIGINT); "
            "SELECT 1 AS value; DROP TABLE t"
        ),
    )

    assert result.rows == [{"value": "1"}]
    assert "result_kind" not in result.extra_metadata


def test_script_with_cte_select_before_cleanup_write_still_reads_result() -> None:
    reader = CsvRecordReader(schema=None, stream="value\n1\n")
    result = _run(
        reader,
        sql=(
            "CREATE TABLE t (value BIGINT); "
            "WITH c AS (SELECT 1 AS value) SELECT * FROM c; "
            "DROP TABLE t"
        ),
    )

    assert result.rows == [{"value": "1"}]
    assert "result_kind" not in result.extra_metadata


@pytest.mark.parametrize(
    "sql",
    [
        "WITH c AS (SELECT value FROM update_log) SELECT * FROM c",
        "WITH alias AS (SELECT 1 AS value) SELECT * FROM alias",
        "WITH c AS (SELECT value AS kill FROM source) SELECT * FROM c",
        "WITH c AS (SELECT load(value) AS value FROM source) SELECT * FROM c",
    ],
)
def test_cte_identifiers_and_udfs_still_read_result(sql: str) -> None:
    reader = CsvRecordReader(schema=None, stream="value\n1\n")
    result = _run(reader, sql=sql)

    assert result.rows == [{"value": "1"}]
    assert "result_kind" not in result.extra_metadata


def test_script_result_branch_takes_precedence_over_write_branch() -> None:
    reader = CsvRecordReader(schema=None, stream="value\n1\n")
    result = _run(
        reader,
        sql=(
            "IF (true) SELECT 1 AS value; "
            "ELSE INSERT INTO target SELECT * FROM source;"
        ),
    )

    assert result.rows == [{"value": "1"}]
    assert "result_kind" not in result.extra_metadata


@pytest.mark.parametrize(
    "sql",
    [
        "IF (true) INSERT INTO t SELECT * FROM source",
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
def test_resultless_script_writes_do_not_open_reader(sql: str) -> None:
    class _ScriptWriteInstance(_FakeInstance):
        def open_reader(self):
            raise AssertionError("result reader must not be opened for script writes")

    result = _StubBackend()._instance_to_query_result(
        _ScriptWriteInstance(reader=None),
        project="test_project",
        max_rows=100,
        sql=sql,
        elapsed_ms=12,
    )

    assert result.rows == []
    assert result.extra_metadata["result_kind"] == "statement"
    assert result.extra_metadata["statement_operation"] == "INSERT"


@pytest.mark.parametrize(
    "sql",
    [
        "@i BIGINT; @i := 1;",
        "@t TABLE(id BIGINT); @t := SELECT 1 AS id;",
        "IF (cond) @t := SELECT 1 AS id;",
        "FUNCTION my_add(@a BIGINT) AS @a + 1;",
        (
            "CREATE TEMPORARY FUNCTION foo AS 'com.example.Foo' USING\n"
            "#CODE ('lang'='JAVA')\n"
            "public class Foo { public Long evaluate(Long v) { return v + 1; } }\n"
            "#END CODE;"
        ),
    ],
)
def test_script_local_declarations_do_not_open_reader(sql: str) -> None:
    class _ScriptDeclarationInstance(_FakeInstance):
        def open_reader(self):
            raise AssertionError("result reader must not be opened for script declarations")

    result = _StubBackend()._instance_to_query_result(
        _ScriptDeclarationInstance(reader=None),
        project="test_project",
        max_rows=100,
        sql=sql,
        elapsed_ms=12,
    )

    assert result.rows == []
    assert result.extra_metadata["result_kind"] == "statement"
    assert result.extra_metadata["statement_operation"] == "SCRIPT"


@pytest.mark.parametrize(
    "sql",
    [
        "@t TABLE(id BIGINT); @t := SELECT 1 AS id; SELECT * FROM @t;",
        "FUNCTION my_add(@a BIGINT) AS @a + 1; SELECT my_add(1) AS value;",
        (
            "CREATE TEMPORARY FUNCTION foo AS 'com.example.Foo' USING\n"
            "#CODE ('lang'='JAVA')\n"
            "public class Foo { public Long evaluate(Long v) { return v + 1; } }\n"
            "#END CODE; SELECT foo(1) AS value;"
        ),
    ],
)
def test_script_local_declarations_with_output_still_open_reader(sql: str) -> None:
    reader = CsvRecordReader(schema=None, stream="value\n1\n")
    result = _run(reader, sql=sql)

    assert result.rows == [{"value": "1"}]
    assert "result_kind" not in result.extra_metadata


def test_if_select_with_case_expression_still_opens_reader() -> None:
    reader = CsvRecordReader(schema=None, stream="value\n1\n")
    result = _run(
        reader,
        sql="IF (true) SELECT CASE WHEN 1=1 THEN 1 ELSE 0 END AS value;",
    )

    assert result.rows == [{"value": "1"}]
    assert "result_kind" not in result.extra_metadata


def test_missing_sql_uses_authoritative_non_select_marker() -> None:
    class _NonSelectInstance(_FakeInstance):
        _is_select = False

        def open_reader(self):
            raise AssertionError("result reader must not be opened for a non-select instance")

    result = _StubBackend()._instance_to_query_result(
        _NonSelectInstance(reader=None),
        project="test_project",
        max_rows=100,
        sql="",
        elapsed_ms=12,
    )

    assert result.rows == []
    assert result.extra_metadata["result_kind"] == "statement"
    assert "statement_operation" not in result.extra_metadata


def test_missing_sql_without_non_select_marker_still_opens_reader() -> None:
    class _UnknownInstance(_FakeInstance):
        def open_reader(self):
            raise RuntimeError("reader failed for unknown statement")

    with pytest.raises(Exception, match="reader failed for unknown statement"):
        _StubBackend()._instance_to_query_result(
            _UnknownInstance(reader=None),
            project="test_project",
            max_rows=100,
            sql="",
            elapsed_ms=12,
        )


def test_query_like_statement_still_opens_result_reader() -> None:
    """Do not turn reader failures for query-like statements into false success."""

    class _FailingInstance(_FakeInstance):
        def open_reader(self):
            raise RuntimeError("reader failed")

    instance = _FailingInstance(reader=None)
    with pytest.raises(Exception, match="reader failed"):
        _StubBackend()._instance_to_query_result(
            instance,
            project="test_project",
            max_rows=100,
            sql="SHOW TABLES",
            elapsed_ms=12,
        )


# ---------------------------------------------------------------------------
# Fallback-warning capture: surface pyodps's "tunnel fallback" UserWarning
# into the envelope so agents can see that the result was truncated.
# ---------------------------------------------------------------------------

PYODPS_TIMEOUT_WARNING = (
    "Instance tunnel timed out, will fallback to restricted approach. "
    "10000 records will be limited. You may try merging small files on your "
    "source table. See https://example.invalid/ for more information."
)

PYODPS_UNSUPPORTED_WARNING = (
    "Instance tunnel not supported, will fallback to restricted approach. "
    "10000 records will be limited. See https://example.invalid/ for more."
)

PYODPS_PROTECTION_WARNING = (
    "Project or data under protection, 10000 records will be limited. "
    "Raw error message:\nsome protection error\nSee https://example.invalid/."
)


def test_tunnel_timeout_warning_surfaces_in_result_warnings() -> None:
    """Tunnel-timeout UserWarning emitted during open_reader() must end up
    in `result.warnings` so the envelope's `agent_hints.warnings` shows it.
    """
    reader = CsvRecordReader(schema=None, stream="a,b\n1,foo\n")
    result = _run(reader, warn_on_open=PYODPS_TIMEOUT_WARNING)

    assert any("tunnel timed out" in w.lower() for w in result.warnings), result.warnings
    assert any("10000" in w for w in result.warnings), result.warnings
    # Rows still correctly extracted on top of warning capture
    assert result.rows == [{"a": "1", "b": "foo"}]


def test_tunnel_unsupported_warning_surfaces_in_result_warnings() -> None:
    reader = CsvRecordReader(schema=None, stream="a\n1\n")
    result = _run(reader, warn_on_open=PYODPS_UNSUPPORTED_WARNING)

    assert any("not supported" in w.lower() for w in result.warnings), result.warnings


def test_protection_warning_surfaces_in_result_warnings() -> None:
    reader = CsvRecordReader(schema=None, stream="a\n1\n")
    result = _run(reader, warn_on_open=PYODPS_PROTECTION_WARNING)

    assert any("protection" in w.lower() for w in result.warnings), result.warnings


def test_no_warning_means_empty_result_warnings() -> None:
    """When no fallback warning is emitted, `result.warnings` stays empty."""
    reader = CsvRecordReader(schema=None, stream="a\n1\n")
    result = _run(reader)
    assert result.warnings == []


def test_unrelated_warning_is_not_treated_as_fallback() -> None:
    """A random UserWarning (e.g. deprecation noise from another lib) must
    NOT pollute result.warnings — only known fallback patterns count.
    """
    reader = CsvRecordReader(schema=None, stream="a\n1\n")
    result = _run(reader, warn_on_open="some unrelated noisy warning")
    assert result.warnings == []


def test_fallback_warning_is_still_emitted_to_stderr(capsys) -> None:
    """Capturing the warning into the envelope must NOT silence it on stderr —
    human users running without --json should still see pyodps's diagnostic.
    """
    import warnings as _w

    reader = CsvRecordReader(schema=None, stream="a\n1\n")

    # Force showwarning to write to stderr (pytest's `capsys` reads stderr).
    # Use the default filter so warnings propagate.
    with _w.catch_warnings():
        _w.simplefilter("always")
        _run(reader, warn_on_open=PYODPS_TIMEOUT_WARNING)

    captured = capsys.readouterr()
    assert "Instance tunnel timed out" in captured.err
