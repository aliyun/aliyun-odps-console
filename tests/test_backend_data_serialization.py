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
import os
from datetime import date, datetime
from decimal import Decimal

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.backend.data import _serialize_value
from maxc_cli.config import TableColumn, TableDefinition
from maxc_cli.exceptions import (
    BackendConnectionError,
    CsvParseError,
    UploadCommitOutcomeUnknownError,
    ValidationError,
)

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
        self.aborted = False

    def new_record(self) -> _FakeRecord:
        return _FakeRecord()

    def open_record_writer(self, block_id: int) -> _FakeRecordWriter:
        writer = _FakeRecordWriter()
        self._writers.append(writer)
        return writer

    def commit(self, block_ids: list[int]) -> None:
        self.committed_blocks = list(block_ids)

    def abort(self) -> None:
        self.aborted = True


class _FakeTunnel:
    """Records the kwargs handed to create_upload_session."""

    def __init__(self) -> None:
        self.create_calls: list[dict] = []
        self.session = _FakeUploadSession()

    def create_upload_session(self, table_name, **kwargs):
        self.create_calls.append({"table_name": table_name, **kwargs})
        return self.session


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

    def _table_tunnel(self, project: str | None = None) -> _FakeTunnel:
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
        create_partition=True,
    )
    assert len(tunnel.create_calls) == 1
    call = tunnel.create_calls[0]
    assert call["partition_spec"] == "ds=20260521"
    assert call["create_partition"] is True


@pytest.mark.parametrize("delimiter", ["", "||"])
def test_upload_invalid_delimiter_fails_before_describe_or_session(
    tmp_path, delimiter
) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)
    harness.describe_table = lambda *_args, **_kwargs: pytest.fail(
        "invalid delimiter reached describe_table"
    )

    with pytest.raises(ValidationError, match="exactly one character"):
        harness.upload("dx_flat", str(csv_path), delimiter=delimiter)

    assert tunnel.create_calls == []


def test_upload_missing_file_fails_before_describe_or_session(tmp_path) -> None:
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)
    harness.describe_table = lambda *_args, **_kwargs: pytest.fail(
        "missing file reached describe_table"
    )

    with pytest.raises(ValidationError, match="does not exist"):
        harness.upload("dx_flat", str(tmp_path / "missing.csv"))

    assert tunnel.create_calls == []


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


@pytest.mark.parametrize(
    ("header", "row"),
    [
        ("ignored,id,name", "skip,1,Alice"),
        ("id,ignored,name", "1,skip,Alice"),
    ],
)
def test_upload_extra_header_columns_preserve_source_indexes(
    tmp_path, header: str, row: str
) -> None:
    """Ignored CSV columns must not shift values into a target column."""
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text(f"{header}\n{row}\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[
            TableColumn(name="id", type="bigint"),
            TableColumn(name="name", type="string"),
        ],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)

    result = harness.upload("dx_flat", str(csv_path))

    assert tunnel.session._writers[0].records == [{"id": 1, "name": "Alice"}]
    assert result["warnings"] == ["CSV header has extra columns ignored: ['ignored']"]


def test_upload_dry_run_validates_every_row_without_opening_tunnel(tmp_path) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[
            TableColumn(name="id", type="bigint"),
            TableColumn(name="name", type="string"),
        ],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)

    result = harness.upload("dx_flat", str(csv_path), dry_run=True)

    assert tunnel.create_calls == []
    assert result["rows_written"] == 0
    assert result["rows_found"] == 2
    assert result["column_mapping"] == ["id", "name"]
    assert result["validation"] == {
        "table_schema": True,
        "csv_structure": True,
        "row_widths": True,
        "mapped_value_types": True,
        "upload_session_created": False,
    }
    assert "no upload session was created" in result["warnings"][-1]


def test_upload_dry_run_rejects_bad_mapped_value_without_opening_tunnel(
    tmp_path,
) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id,name\n1,Alice\nnot-an-int,Bob\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[
            TableColumn(name="id", type="bigint"),
            TableColumn(name="name", type="string"),
        ],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)

    with pytest.raises(CsvParseError) as error:
        harness.upload("dx_flat", str(csv_path), dry_run=True)

    assert error.value.line == 3
    assert error.value.column == "id"
    assert tunnel.create_calls == []


@pytest.mark.parametrize("dry_run", [False, True])
def test_upload_and_dry_run_reject_the_same_row_width_mismatch(
    tmp_path, dry_run: bool
) -> None:
    csv_path = tmp_path / f"rows-{dry_run}.csv"
    csv_path.write_text("id,name\n1,Alice\n2\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[
            TableColumn(name="id", type="bigint"),
            TableColumn(name="name", type="string"),
        ],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)

    with pytest.raises(CsvParseError, match="expected 2 columns, got 1") as error:
        harness.upload("dx_flat", str(csv_path), dry_run=dry_run)

    assert error.value.line == 3
    assert tunnel.create_calls == []
    assert tunnel.session.aborted is False


def test_upload_commits_the_owner_private_snapshot_when_source_changes(
    tmp_path,
    monkeypatch,
) -> None:
    """Bytes validated locally must be the exact bytes sent to Tunnel."""
    from maxc_cli.backend import data as data_module

    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    original_stat = csv_path.stat()
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)
    original_validate = data_module._validate_upload_csv_file
    observed_snapshot = None
    observed_modes = None

    def mutate_source_after_validation(file_path, **kwargs):
        nonlocal observed_modes, observed_snapshot
        observed_snapshot = file_path
        observed_modes = (
            file_path.parent.stat().st_mode & 0o777,
            file_path.stat().st_mode & 0o777,
        )
        result = original_validate(file_path, **kwargs)
        csv_path.write_text("id\n9\n", encoding="utf-8")
        os.utime(
            csv_path,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )
        return result

    monkeypatch.setattr(
        data_module,
        "_validate_upload_csv_file",
        mutate_source_after_validation,
    )

    result = harness.upload("dx_flat", str(csv_path))

    assert result["rows_written"] == 1
    assert tunnel.session._writers[0].records == [{"id": 1}]
    assert csv_path.read_text(encoding="utf-8") == "id\n9\n"
    assert observed_snapshot is not None
    assert observed_snapshot != csv_path
    assert not observed_snapshot.exists()
    if os.name == "posix":
        assert observed_modes == (0o700, 0o600)


@pytest.mark.parametrize(
    "cleanup_exception",
    [OSError("simulated cleanup failure"), KeyboardInterrupt()],
    ids=["os-error", "keyboard-interrupt"],
)
def test_upload_cleanup_failure_after_commit_is_only_a_warning(
    tmp_path,
    monkeypatch,
    cleanup_exception: BaseException,
) -> None:
    """A committed append must never look retryable because temp cleanup failed."""
    from maxc_cli.backend import data as data_module

    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
    )
    tunnel = _FakeTunnel()
    harness = _UploadHarness(definition, tunnel)
    original_snapshot = data_module._create_upload_snapshot

    class _CleanupFailure:
        def __init__(self, wrapped) -> None:
            self._wrapped = wrapped

        def cleanup(self) -> None:
            self._wrapped.cleanup()
            raise cleanup_exception

    def failing_cleanup_snapshot(file_path):
        snapshot_dir, snapshot_path = original_snapshot(file_path)
        return _CleanupFailure(snapshot_dir), snapshot_path

    monkeypatch.setattr(
        data_module,
        "_create_upload_snapshot",
        failing_cleanup_snapshot,
    )

    result = harness.upload("dx_flat", str(csv_path))

    assert tunnel.session.committed_blocks == [0]
    assert tunnel.session._writers[0].records == [{"id": 1}]
    assert any(
        "committed successfully" in warning
        and "must not be uploaded again" in warning
        for warning in result["warnings"]
    )


def test_upload_writer_failure_closes_writer_and_uses_optional_abort(tmp_path) -> None:
    class _FailingWriter(_FakeRecordWriter):
        def write(self, record: dict) -> None:
            raise RuntimeError("writer transport failed")

    class _FailingSession(_FakeUploadSession):
        def open_record_writer(self, block_id: int) -> _FakeRecordWriter:
            writer = _FailingWriter()
            self._writers.append(writer)
            return writer

    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
    )
    tunnel = _FakeTunnel()
    tunnel.session = _FailingSession()
    harness = _UploadHarness(definition, tunnel)

    with pytest.raises(BackendConnectionError, match="writer transport failed") as exc_info:
        harness.upload("dx_flat", str(csv_path))

    assert tunnel.session.aborted is True
    assert tunnel.session._writers[0].closed is True
    assert tunnel.session.committed_blocks is None
    assert exc_info.value.context == {
        "upload_session_created": True,
        "partition_may_remain": False,
        "remote_commit_state": "not_attempted",
        "duplicate_write_risk": False,
        "uncommitted_rows_visible": False,
        "upload_session_cleanup": "client_abort",
    }


def test_upload_commit_failure_is_reported_as_unknown_without_blind_abort(tmp_path) -> None:
    class _FailingCommitSession(_FakeUploadSession):
        def commit(self, block_ids: list[int]) -> None:
            raise RuntimeError("commit transport failed")

    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
    )
    tunnel = _FakeTunnel()
    tunnel.session = _FailingCommitSession()
    harness = _UploadHarness(definition, tunnel)

    with pytest.raises(BackendConnectionError, match="commit transport failed") as exc_info:
        harness.upload("dx_flat", str(csv_path))

    assert tunnel.session.aborted is False
    assert tunnel.session._writers[0].closed is True
    assert exc_info.value.recoverable is False
    assert exc_info.value.context == {
        "upload_session_created": True,
        "partition_may_remain": False,
        "remote_commit_state": "unknown",
        "duplicate_write_risk": True,
        "upload_session_cleanup": "not_attempted_after_commit_request",
    }
    assert "Do not retry" in str(exc_info.value.suggestion)


def test_upload_interrupt_during_commit_returns_typed_unknown_outcome(tmp_path) -> None:
    class _InterruptedCommitSession(_FakeUploadSession):
        def commit(self, block_ids: list[int]) -> None:
            self.committed_blocks = list(block_ids)
            raise KeyboardInterrupt()

    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id\n1\n", encoding="utf-8")
    definition = TableDefinition(
        name="dx_flat",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
    )
    tunnel = _FakeTunnel()
    tunnel.session = _InterruptedCommitSession()
    harness = _UploadHarness(definition, tunnel)

    with pytest.raises(UploadCommitOutcomeUnknownError) as exc_info:
        harness.upload("dx_flat", str(csv_path))

    assert tunnel.session.committed_blocks == [0]
    assert tunnel.session.aborted is False
    assert exc_info.value.exit_code == 130
    assert exc_info.value.context["remote_commit_state"] == "unknown"
    assert exc_info.value.context["duplicate_write_risk"] is True
    assert "Do not retry" in str(exc_info.value.suggestion)


class _FakeDownloadSession:
    def __init__(self, rows: list[dict], *, fail_after: int | None = None) -> None:
        self._rows = rows
        self._fail_after = fail_after
        self.count = len(rows)

    def open_record_reader(self, start: int, count: int):
        def _records():
            for index, row in enumerate(self._rows[start : start + count]):
                if self._fail_after is not None and index >= self._fail_after:
                    raise RuntimeError("download transport failed")
                yield row

        return _records()


class _FakeDownloadTunnel:
    def __init__(self, session: _FakeDownloadSession) -> None:
        self.session = session

    def create_download_session(self, *args, **kwargs) -> _FakeDownloadSession:
        return self.session


class _DownloadHarness:
    project = "test_project"

    def __init__(self, session: _FakeDownloadSession) -> None:
        self._session = session
        from maxc_cli.backend.data import DataMixin

        self._download = DataMixin.download_table.__get__(self, _DownloadHarness)

    def describe_table(self, *args, **kwargs) -> TableDefinition:
        return TableDefinition(
            name="dx_flat",
            description="",
            columns=[
                TableColumn(name="id", type="bigint"),
                TableColumn(name="name", type="string"),
            ],
        )

    def _table_tunnel(self, project: str | None = None) -> _FakeDownloadTunnel:
        return _FakeDownloadTunnel(self._session)

    def download(self, *args, **kwargs):
        return self._download(*args, **kwargs)


def test_download_failure_preserves_existing_target_and_cleans_temp(tmp_path) -> None:
    output = tmp_path / "rows.csv"
    output.write_text("keep this file\n", encoding="utf-8")
    harness = _DownloadHarness(
        _FakeDownloadSession(
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            fail_after=1,
        )
    )

    with pytest.raises(BackendConnectionError, match="download transport failed"):
        harness.download("dx_flat", str(output), overwrite=True)

    assert output.read_text(encoding="utf-8") == "keep this file\n"
    assert list(tmp_path.glob(".rows.csv.*.tmp")) == []


def test_download_rejects_existing_target_without_overwrite(tmp_path) -> None:
    output = tmp_path / "rows.csv"
    output.write_text("old contents\n", encoding="utf-8")
    harness = _DownloadHarness(
        _FakeDownloadSession([{"id": 1, "name": "Alice"}])
    )

    with pytest.raises(ValidationError, match="already exists"):
        harness.download("dx_flat", str(output))

    assert output.read_text(encoding="utf-8") == "old contents\n"
    assert list(tmp_path.glob(".rows.csv.*.tmp")) == []


def test_download_missing_parent_fails_before_describe_or_session(tmp_path) -> None:
    harness = _DownloadHarness(_FakeDownloadSession([{"id": 1, "name": "Alice"}]))
    harness.describe_table = lambda *_args, **_kwargs: pytest.fail(
        "invalid output path reached describe_table"
    )

    with pytest.raises(ValidationError, match="directory does not exist"):
        harness.download("dx_flat", str(tmp_path / "missing" / "rows.csv"))


def test_download_invalid_delimiter_fails_before_describe_or_session(tmp_path) -> None:
    harness = _DownloadHarness(_FakeDownloadSession([{"id": 1, "name": "Alice"}]))
    harness.describe_table = lambda *_args, **_kwargs: pytest.fail(
        "invalid delimiter reached describe_table"
    )

    with pytest.raises(ValidationError, match="exactly one character"):
        harness.download("dx_flat", str(tmp_path / "rows.csv"), delimiter="||")


def test_download_race_does_not_clobber_concurrently_created_target(
    tmp_path, monkeypatch
) -> None:
    """The final no-overwrite publish must be atomic, not check-then-replace."""
    import os

    output = tmp_path / "rows.csv"
    harness = _DownloadHarness(
        _FakeDownloadSession([{"id": 1, "name": "Alice"}])
    )
    original_link = os.link

    def link_after_competitor_wins(source, destination, *args, **kwargs):
        output.write_text("competitor contents\n", encoding="utf-8")
        return original_link(source, destination, *args, **kwargs)

    monkeypatch.setattr(os, "link", link_after_competitor_wins)

    with pytest.raises(ValidationError, match="already exists"):
        harness.download("dx_flat", str(output), overwrite=False)

    assert output.read_text(encoding="utf-8") == "competitor contents\n"
    assert list(tmp_path.glob(".rows.csv.*.tmp")) == []


def test_download_success_atomically_replaces_existing_target_with_overwrite(tmp_path) -> None:
    output = tmp_path / "rows.csv"
    output.write_text("old contents\n", encoding="utf-8")
    harness = _DownloadHarness(
        _FakeDownloadSession([{"id": 1, "name": "Alice"}])
    )

    result = harness.download("dx_flat", str(output), overwrite=True)

    assert output.read_text(encoding="utf-8") == "id,name\n1,Alice\n"
    assert result["output_path"] == str(output.absolute())
    assert list(tmp_path.glob(".rows.csv.*.tmp")) == []


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


def test_view_recovery_command_preserves_explicit_scope_and_agent_ua(
    monkeypatch,
) -> None:
    from maxc_cli.odps_runtime import set_agent_user_agent

    view_def = TableDefinition(
        name="my_view",
        description="",
        columns=[TableColumn(name="id", type="bigint")],
        table_type="VIRTUAL_VIEW",
    )
    harness = _SampleHarness(view_def)
    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    set_agent_user_agent("AlibabaCloud-Agent-Skills/test/session123")
    try:
        with pytest.raises(ValidationError) as excinfo:
            harness.sample(
                "my_view",
                rows=10,
                project="other_project",
                schema="sales",
            )
    finally:
        set_agent_user_agent(None)

    suggestion = excinfo.value.suggestion or ""
    assert "aliyun maxc" in suggestion
    assert "--user-agent AlibabaCloud-Agent-Skills/test/session123" in suggestion
    assert "SELECT * FROM sales.my_view LIMIT 10" in suggestion
    assert "--project other_project" in suggestion
