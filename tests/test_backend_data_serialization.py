"""Tests for serialization & upload robustness in DataMixin.

Pins:
  1. `_serialize_value` JSON-safes every type that ODPS read_table can yield:
     datetime/date (already handled), Decimal, bytes/bytearray.
     Without this, sampling a table with a DECIMAL column blew up at JSON
     time with ``Object of type Decimal is not JSON serializable``.
  2. Upload to a partitioned table passes ``create_partition=True`` to the
     tunnel session so a fresh partition value doesn't require a separate
     ``ALTER TABLE ... ADD PARTITION`` round-trip first.
  3. Sampling a view surfaces a clear `data sample` error instead of letting
     the tunnel layer's cryptic "tunnel does not support views" propagate.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.backend.data import _serialize_value
from maxc_cli.config import TableColumn, TableDefinition
from maxc_cli.exceptions import ValidationError

# ── _serialize_value JSON-safety ───────────────────────────────────────────


def test_serialize_decimal_yields_json_safe_string() -> None:
    """Decimal — common from DECIMAL(38, 18) columns. We can't keep it as a
    Python Decimal because the stdlib json encoder rejects it; we also don't
    want to lose precision via float. String preserves precision exactly."""
    value = Decimal("3.141592653589793238")
    serialized = _serialize_value(value)
    assert json.dumps(serialized) == '"3.141592653589793238"'


def test_serialize_bytes_yields_json_safe_string() -> None:
    """ODPS BINARY columns yield bytes. Latin-1 decode round-trips any byte
    sequence safely (every byte maps to a codepoint), and the result is a
    plain JSON string the agent can re-encode if needed."""
    serialized = _serialize_value(b"\x00\x01\xff")
    json.dumps(serialized)


def test_serialize_bytearray_yields_json_safe_string() -> None:
    serialized = _serialize_value(bytearray(b"hello"))
    json.dumps(serialized)


def test_serialize_datetime_iso_unchanged() -> None:
    """Regression guard for the pre-existing datetime branch."""
    serialized = _serialize_value(datetime(2026, 5, 21, 12, 0, 0))
    assert serialized == "2026-05-21T12:00:00"


def test_serialize_date_iso_unchanged() -> None:
    serialized = _serialize_value(date(2026, 5, 21))
    assert serialized == "2026-05-21"


def test_serialize_passthrough_for_primitives() -> None:
    """Strings, ints, floats, None, bools are already JSON-safe — don't wrap them."""
    for raw in ("abc", 42, 3.14, None, True, False):
        assert _serialize_value(raw) is raw or _serialize_value(raw) == raw


# ── Upload auto-creates partition ──────────────────────────────────────────


class _FakeRecord(dict):
    """Stand-in for a tunnel record: a writable mapping is enough."""


class _FakeRecordWriter:
    def __init__(self) -> None:
        self.records: list[dict] = []
        self.closed = False

    def write(self, record: dict) -> None:
        self.records.append(dict(record))

    def close(self) -> None:
        self.closed = True


class _FakeUploadSession:
    """Tunnel-like recorder. Captures the create_upload_session kwargs so
    we can assert the upload path passed create_partition=True."""

    def __init__(self) -> None:
        self._writers: list[_FakeRecordWriter] = []
        self.committed_blocks: list[int] | None = None

    def new_record(self) -> _FakeRecord:
        return _FakeRecord()

    def open_record_writer(self, block_id: int) -> _FakeRecordWriter:
        writer = _FakeRecordWriter()
        self._writers.append(writer)
        return writer

    def commit(self, block_ids: list[int]) -> None:
        self.committed_blocks = list(block_ids)

    def abort(self) -> None:
        pass


class _FakeTunnel:
    """Records the kwargs handed to create_upload_session."""

    def __init__(self) -> None:
        self.create_calls: list[dict] = []

    def create_upload_session(self, table_name, **kwargs):
        self.create_calls.append({"table_name": table_name, **kwargs})
        return _FakeUploadSession()


class _UploadHarness:
    """Bind DataMixin upload methods without invoking OdpsBackend.__init__."""

    def __init__(self, definition: TableDefinition, tunnel: _FakeTunnel) -> None:
        self._definition = definition
        self._tunnel = tunnel
        self.project = "test_project"
        from maxc_cli.backend.data import DataMixin
        self._upload = DataMixin.upload_table.__get__(self, _UploadHarness)

    def describe_table(self, *args, **kwargs) -> TableDefinition:
        return self._definition

    def _table_tunnel(self) -> _FakeTunnel:
        return self._tunnel

    def upload(self, *args, **kwargs):
        return self._upload(*args, **kwargs)


def test_upload_partitioned_passes_create_partition_true(tmp_path) -> None:
    """A fresh partition value should not require a separate ALTER TABLE
    ADD PARTITION step. The tunnel API accepts create_partition=True which
    creates-if-missing and is idempotent for existing partitions."""
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id,name\n1,foo\n2,bar\n", encoding="utf-8")

    definition = TableDefinition(
        name="dx_test",
        description="",
        columns=[
            TableColumn(name="id", type="bigint"),
            TableColumn(name="name", type="string"),
            TableColumn(name="ds", type="string"),
        ],
        partition_columns=[TableColumn(name="ds", type="string")],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)
    harness.upload(
        "dx_test",
        str(csv_path),
        partition="ds=20260521",
    )
    assert len(tunnel.create_calls) == 1
    call = tunnel.create_calls[0]
    assert call["partition_spec"] == "ds=20260521"
    assert call["create_partition"] is True


def test_upload_unpartitioned_does_not_request_create_partition(tmp_path) -> None:
    """Non-partitioned tables must not receive create_partition=True — passing
    it would be at best confusing in logs, at worst rejected by the server."""
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id,name\n1,foo\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[
            TableColumn(name="id", type="bigint"),
            TableColumn(name="name", type="string"),
        ],
        partition_columns=[],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)
    harness.upload("dx_flat", str(csv_path))
    assert len(tunnel.create_calls) == 1
    call = tunnel.create_calls[0]
    assert call["partition_spec"] is None
    assert "create_partition" not in call or call["create_partition"] is False


# ── Sampling a view surfaces a clean error ────────────────────────────────


class _SampleHarness:
    """Bind DataMixin.sample_table without invoking __init__. The describe
    path is stubbed to return a VIRTUAL_VIEW so we can pin the early-exit
    behavior without ever hitting read_table."""

    def __init__(self, definition: TableDefinition) -> None:
        self._definition = definition
        self.project = "test_project"
        self.read_table_called = False
        from maxc_cli.backend.data import DataMixin
        self._sample = DataMixin.sample_table.__get__(self, _SampleHarness)

        class _Client:
            def read_table(_self, *args, **kwargs):
                raise AssertionError(
                    "read_table must not be invoked for a virtual view; sample_table "
                    "should reject with a clear error first."
                )

        self.client = _Client()

    def describe_table(self, *args, **kwargs):
        return self._definition

    def _resolve_partition_for_sample(self, definition, partition, *, project):
        return partition, []

    def sample(self, *args, **kwargs):
        return self._sample(*args, **kwargs)


def test_sample_view_raises_validation_error_not_tunnel_blowup() -> None:
    """Sampling a view via read_table fails inside the tunnel layer with an
    opaque ``tunnel does not support views`` error. Catch it early and surface
    a clear, actionable message instead so agents know to issue a SELECT."""
    view_def = TableDefinition(
        name="my_view",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
        table_type="VIRTUAL_VIEW",
    )
    harness = _SampleHarness(view_def)
    with pytest.raises(ValidationError) as excinfo:
        harness.sample("my_view", rows=10)
    msg = str(excinfo.value).lower()
    suggestion = (excinfo.value.suggestion or "").lower()
    assert "view" in msg
    assert "select" in suggestion  # the actionable suggestion lives on the field
