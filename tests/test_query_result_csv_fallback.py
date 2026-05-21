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
