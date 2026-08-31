"""Contract tests for CSV/NDJSON stdout and query file output."""

from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit

import maxc_cli.cli as cli_module
from maxc_cli.cli import run
from maxc_cli.exceptions import SqlError
from maxc_cli.models import AgentHints, Envelope, action


def _rendered_action(payload: dict, action_id: str) -> dict:
    return next(
        item
        for item in payload["agent_hints"]["actions"]
        if item["id"] == action_id
    )


def _auth_config() -> SimpleNamespace:
    external = SimpleNamespace(is_configured=lambda: False)
    ncs = SimpleNamespace(is_configured=lambda: False)
    return SimpleNamespace(
        access_id="FAKE",
        secret_access_key="FAKE",
        external=external,
        ncs=ncs,
    )


class _FakeCliApp:
    def __init__(
        self,
        query_envelope: Envelope | None = None,
        job_result_envelope: Envelope | None = None,
    ) -> None:
        self.config = SimpleNamespace(
            auth=_auth_config(),
            default_project="test_project",
            default_format="json",
        )
        self.query_envelope = query_envelope or _query_envelope()
        self.job_result_envelope = job_result_envelope or _query_envelope(
            command="job.result"
        )
        self.query_calls = 0
        self.job_result_calls = 0
        self.log_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def query(self, **_kwargs) -> Envelope:
        self.query_calls += 1
        return self.query_envelope

    def job_result(self, *_args, **_kwargs) -> Envelope:
        self.job_result_calls += 1
        return self.job_result_envelope

    def meta_list_tables(self, **_kwargs) -> Envelope:
        return Envelope(
            command="meta.list-tables",
            status="success",
            data={
                "tables": [
                    {"table_name": "orders", "qualified_name": "sales.orders"},
                    {"table_name": "users", "qualified_name": "sales.users"},
                ],
                "total": 2,
                "has_more": False,
            },
            metadata={"project": "test_project"},
        )

    def log(self, *args, **kwargs) -> None:
        self.log_calls.append((args, kwargs))


class _RaisingQueryApp(_FakeCliApp):
    def query(self, **_kwargs) -> Envelope:
        self.query_calls += 1
        raise SqlError("invalid SQL")


class _CrashingQueryApp(_FakeCliApp):
    def query(self, **_kwargs) -> Envelope:
        self.query_calls += 1
        raise RuntimeError("unexpected query failure")


def _query_envelope(
    *,
    command: str = "query",
    rows: list[dict[str, object]] | None = None,
    status: str = "success",
    result_kind: str | None = None,
) -> Envelope:
    actual_rows = rows if rows is not None else [
        {"id": 1, "note": "comma, newline\nvalue"},
        {"id": 2, "note": 'say "hello"'},
    ]
    metadata: dict[str, object] = {
        "project": "test_project",
        "sql_executed": "SELECT id, note FROM t",
    }
    if result_kind is not None:
        metadata["result_kind"] = result_kind
    return Envelope(
        command=command,
        status=status,
        data={
            "rows": actual_rows,
            "schema": [
                {"name": "id", "type": "bigint"},
                {"name": "note", "type": "string"},
            ],
            "total_rows": len(actual_rows),
            "returned_rows": len(actual_rows),
            "has_more": False,
            "next_cursor": None,
        },
        metadata=metadata,
        agent_hints=AgentHints(),
    )


def _run_with_app(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    app: _FakeCliApp,
    argv: list[str],
) -> tuple[int, str, str]:
    monkeypatch.setattr(cli_module, "MaxCApp", lambda **_kwargs: app)
    stdout = StringIO(newline="")
    stderr = StringIO()
    code = run(argv, cwd=tmp_path, stdout=stdout, stderr=stderr)
    return code, stdout.getvalue(), stderr.getvalue()


@pytest.mark.parametrize("output_format", ["csv", "ndjson"])
def test_non_record_command_returns_format_native_error_instead_of_empty_stdout(
    tmp_path: Path,
    output_format: str,
) -> None:
    stdout = StringIO(newline="")
    stderr = StringIO()

    code = run(
        ["--format", output_format, "agent", "context"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code == 2
    assert stderr.getvalue() == ""
    assert stdout.getvalue().strip()
    if output_format == "ndjson":
        payload = json.loads(stdout.getvalue())
        assert payload["status"] == "failure"
        assert payload["error"]["code"] == "OUTPUT_FORMAT_ERROR"
        assert payload["agent_hints"] is None
    else:
        rows = list(csv.DictReader(StringIO(stdout.getvalue())))
        assert rows[0]["status"] == "failure"
        assert rows[0]["error_code"] == "OUTPUT_FORMAT_ERROR"


@pytest.mark.parametrize("output_format", ["csv", "ndjson"])
def test_argument_error_uses_requested_record_format_and_exit_two(
    tmp_path: Path,
    output_format: str,
) -> None:
    stdout = StringIO(newline="")
    stderr = StringIO()

    code = run(
        [
            "--format",
            output_format,
            "auth",
            "login",
            "--secret-access-key",
            "TOP-SECRET",
            "--not-a-real-flag",
        ],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code == 2
    assert stderr.getvalue() == ""
    assert "TOP-SECRET" not in stdout.getvalue()
    if output_format == "ndjson":
        payload = json.loads(stdout.getvalue())
        assert payload["error"]["code"] == "ARGUMENT_ERROR"
    else:
        rows = list(csv.DictReader(StringIO(stdout.getvalue())))
        assert rows[0]["error_code"] == "ARGUMENT_ERROR"


@pytest.mark.parametrize("output_format", ["csv", "ndjson"])
def test_list_commands_emit_their_record_collection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    app = _FakeCliApp()

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--format", output_format, "meta", "list-tables"],
    )

    assert code == 0
    assert stderr == ""
    if output_format == "ndjson":
        rows = [json.loads(line) for line in stdout.splitlines()]
    else:
        rows = list(csv.DictReader(StringIO(stdout)))
    assert [row["table_name"] for row in rows] == ["orders", "users"]


@pytest.mark.parametrize(
    ("command", "data", "expected_key", "expected_value"),
    [
        ("query", {"rows": [{"id": 1}], "schema": [{"name": "id"}]}, "id", 1),
        ("job.wait", {"rows": [{"id": 1}], "schema": [{"name": "id"}]}, "id", 1),
        ("job.result", {"rows": [{"id": 1}], "schema": [{"name": "id"}]}, "id", 1),
        ("job.list", {"jobs": [{"job_id": "j1"}]}, "job_id", "j1"),
        ("meta.list-projects", {"projects": [{"name": "p1"}]}, "name", "p1"),
        ("meta.list-schemas", {"schemas": [{"name": "s1"}]}, "name", "s1"),
        ("meta.list-tables", {"tables": [{"table_name": "t1"}]}, "table_name", "t1"),
        ("meta.partitions", {"partitions": ["ds=1"]}, "partition", "ds=1"),
        ("meta.search", {"matches": [{"table_name": "t1"}]}, "table_name", "t1"),
        (
            "meta.search-columns",
            {"matches": [{"column_name": "id"}]},
            "column_name",
            "id",
        ),
        (
            "meta.semantic.list-missing",
            {"tables": [{"table_name": "t1"}]},
            "table_name",
            "t1",
        ),
        ("data.sample", {"rows": [{"id": 1}], "schema": [{"name": "id"}]}, "id", 1),
        (
            "agent.skill.list",
            {"installed": [{"platform": "codex"}]},
            "platform",
            "codex",
        ),
    ],
)
def test_ndjson_collection_matrix(
    command: str,
    data: dict[str, object],
    expected_key: str,
    expected_value: object,
) -> None:
    envelope = Envelope(command=command, status="success", data=data)
    args = SimpleNamespace(format="ndjson", json=False)
    stdout = StringIO()

    cli_module._emit_envelope(
        envelope,
        args=args,
        stdout=stdout,
        default_format="json",
    )

    rows = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert rows == [{expected_key: expected_value}]


def test_empty_query_csv_emits_schema_header(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp(_query_envelope(rows=[]))

    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--format", "csv", "query", "SELECT id, note FROM t WHERE 1 = 0"],
    )

    assert code == 0
    assert stdout == "id,note\n"


@pytest.mark.parametrize(
    ("command", "data", "expected_header"),
    [
        (
            "meta.search",
            {"matches": []},
            "table_name,description,score,matched_columns\n",
        ),
        (
            "meta.search-columns",
            {"matches": []},
            "table_name,column_name,column_type,column_comment,score\n",
        ),
    ],
)
def test_empty_search_csv_emits_stable_header(
    command: str,
    data: dict[str, object],
    expected_header: str,
) -> None:
    envelope = Envelope(command=command, status="success", data=data)
    stdout = StringIO(newline="")

    cli_module._emit_record_format(envelope, "csv", stdout)

    assert stdout.getvalue() == expected_header


def test_preparse_format_detection_stops_at_posix_terminator() -> None:
    assert cli_module._argv_requested_output_format(
        ["query", "--", "SELECT", "--format", "csv"]
    ) is None


@pytest.mark.parametrize(
    ("output_format", "suffix"),
    [("json", ".json"), ("csv", ".csv"), ("ndjson", ".ndjson")],
)
def test_query_file_and_stdout_match_when_formats_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
    suffix: str,
) -> None:
    app = _FakeCliApp()
    output_path = tmp_path / f"result{suffix}"

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        [
            "--format",
            output_format,
            "query",
            "SELECT id, note FROM t",
            "--output",
            str(output_path),
            "--output-format",
            output_format,
        ],
    )

    assert code == 0
    assert stderr == ""
    assert output_path.read_text(encoding="utf-8") == stdout
    if output_format == "json":
        payload = json.loads(stdout)
        assert payload["status"] == "success"
        assert payload["metadata"]["output_path"] == str(output_path)
        assert payload["metadata"]["output_written"] is True


def test_output_format_requires_output_and_does_not_execute_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["query", "SELECT 1", "--output-format", "csv", "--json"],
    )

    assert code == 2
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["error"]["code"] == "OUTPUT_FORMAT_ERROR"
    assert "requires --output" in payload["error"]["message"]
    assert app.query_calls == 0


@pytest.mark.parametrize("mode", ["cost", "explain"])
def test_query_analysis_rejects_record_formats_before_backend_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    app = _FakeCliApp()
    construction_calls: list[dict[str, object]] = []

    def app_factory(**kwargs):
        construction_calls.append(kwargs)
        return app

    monkeypatch.setattr(cli_module, "MaxCApp", app_factory)
    stdout = StringIO()
    stderr = StringIO()

    code = run(
        ["--format", "ndjson", "query", mode, "SELECT 1"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code == 2
    assert stderr.getvalue() == ""
    payload = json.loads(stdout.getvalue())
    assert payload["error"]["code"] == "OUTPUT_FORMAT_ERROR"
    assert app.query_calls == 0
    assert construction_calls
    assert all(call.get("load_backend") is False for call in construction_calls)


def test_query_dry_run_rejects_record_format_before_backend_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()

    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--format", "csv", "query", "SELECT 1", "--dry-run"],
    )

    assert code == 2
    rows = list(csv.DictReader(StringIO(stdout)))
    assert rows[0]["error_code"] == "OUTPUT_FORMAT_ERROR"
    assert app.query_calls == 0


@pytest.mark.parametrize("output_format", ["csv", "ndjson"])
def test_raised_query_error_uses_requested_format_and_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    app = _RaisingQueryApp()

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--format", output_format, "query", "not valid SQL"],
    )

    assert code == 4
    assert stderr == ""
    assert stdout.strip()
    if output_format == "ndjson":
        payload = json.loads(stdout)
        assert payload["error"]["code"] == "SQL_ERROR"
    else:
        rows = list(csv.DictReader(StringIO(stdout)))
        assert rows[0]["error_code"] == "SQL_ERROR"


@pytest.mark.parametrize("output_format", ["csv", "ndjson"])
def test_unexpected_query_error_uses_requested_format_and_exit_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    app = _CrashingQueryApp()

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--format", output_format, "query", "SELECT 1"],
    )

    assert code == 1
    assert stderr == ""
    if output_format == "ndjson":
        assert json.loads(stdout)["error"]["code"] == "INTERNAL_ERROR"
    else:
        rows = list(csv.DictReader(StringIO(stdout)))
        assert rows[0]["error_code"] == "INTERNAL_ERROR"


@pytest.mark.parametrize("output_format", ["csv", "ndjson"])
def test_failure_envelope_does_not_overwrite_query_output_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    failure = Envelope(
        command="query",
        status="failure",
        error=SqlError("invalid SQL").to_payload(),
    )
    app = _FakeCliApp(failure)
    output_path = tmp_path / f"existing.{output_format}"
    output_path.write_text("keep me\n", encoding="utf-8")

    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        [
            "--format",
            output_format,
            "query",
            "not valid SQL",
            "--output",
            str(output_path),
            "--output-format",
            output_format,
            "--overwrite",
        ],
    )

    assert code == 4
    assert stdout.strip()
    assert output_path.read_text(encoding="utf-8") == "keep me\n"


def test_existing_output_is_rejected_before_query_without_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()
    output_path = tmp_path / "existing.json"
    output_path.write_text("keep me\n", encoding="utf-8")

    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--json", "query", "SELECT 1", "--output", str(output_path)],
    )

    payload = json.loads(stdout)
    assert code == 1
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert "--overwrite" in payload["error"]["suggestion"]
    assert app.query_calls == 0
    assert output_path.read_text(encoding="utf-8") == "keep me\n"


def test_failed_output_replace_preserves_existing_file_and_remote_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()
    output_path = tmp_path / "existing.json"
    output_path.write_text("keep me\n", encoding="utf-8")

    def fail_replace(_source, _destination):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(cli_module.os, "replace", fail_replace)
    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        [
            "--json",
            "query",
            "SELECT 1",
            "--output",
            str(output_path),
            "--overwrite",
        ],
    )

    payload = json.loads(stdout)
    assert code == 1
    assert payload["error"]["code"] == "OUTPUT_WRITE_FAILED"
    assert payload["metadata"]["remote_execution_succeeded"] is True
    assert payload["metadata"]["output_written"] is False
    assert payload["data"]["result"]["rows"]
    assert "Do not rerun" in payload["error"]["suggestion"]
    assert output_path.read_text(encoding="utf-8") == "keep me\n"
    assert list(tmp_path.glob(".existing.json.*.tmp")) == []
    assert app.log_calls[-1][0][:2] == ("query", "failure")
    assert app.log_calls[-1][1]["error"]["code"] == "OUTPUT_WRITE_FAILED"


def test_concurrent_output_creation_is_not_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()
    output_path = tmp_path / "result.json"

    def race_link(_source, destination):
        Path(destination).write_text("created concurrently\n", encoding="utf-8")
        raise FileExistsError("destination appeared after preflight")

    monkeypatch.setattr(cli_module.os, "link", race_link)
    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--json", "query", "SELECT 1", "--output", str(output_path)],
    )

    payload = json.loads(stdout)
    assert code == 1
    assert app.query_calls == 1
    assert payload["error"]["code"] == "OUTPUT_WRITE_FAILED"
    assert payload["metadata"]["remote_execution_succeeded"] is True
    assert output_path.read_text(encoding="utf-8") == "created concurrently\n"
    assert list(tmp_path.glob(".result.json.*.tmp")) == []
    assert app.log_calls[-1][0][:2] == ("query", "failure")
    assert app.log_calls[-1][1]["error"]["code"] == "OUTPUT_WRITE_FAILED"


def test_job_result_output_failure_appends_final_failure_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()
    output_path = tmp_path / "job-result.json"

    def fail_link(_source, _destination):
        raise OSError("simulated publication failure")

    monkeypatch.setattr(cli_module.os, "link", fail_link)
    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        [
            "--json",
            "job",
            "result",
            "job-123",
            "--output",
            str(output_path),
        ],
    )

    payload = json.loads(stdout)
    assert code == 1
    assert app.job_result_calls == 1
    assert payload["error"]["code"] == "OUTPUT_WRITE_FAILED"
    assert not output_path.exists()
    assert app.log_calls[-1][0][:2] == ("job.result", "failure")
    assert app.log_calls[-1][1]["error"]["code"] == "OUTPUT_WRITE_FAILED"


@pytest.mark.parametrize("output_format", ["csv", "ndjson"])
def test_statement_result_emits_success_control_record_not_blank_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    app = _FakeCliApp(_query_envelope(rows=[], result_kind="statement"))

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--format", output_format, "query", "CREATE TABLE t (id BIGINT)", "--force"],
    )

    assert code == 0
    assert stderr == ""
    assert stdout.strip()
    if output_format == "ndjson":
        payload = json.loads(stdout)
        assert payload["status"] == "success"
        assert payload["metadata"]["result_kind"] == "statement"
    else:
        rows = list(csv.DictReader(StringIO(stdout)))
        assert rows[0]["status"] == "success"


def _pending_job_envelope(command: str = "query") -> Envelope:
    metadata = {"job_id": "job-123", "project": "test_project"}
    return Envelope(
        command=command,
        status="pending",
        data={"job_id": "job-123"},
        metadata=metadata,
        agent_hints=AgentHints(
            actions=[
                action(
                    "job.result",
                    data={"job_id": "job-123", "max_rows": 100},
                    metadata=metadata,
                )
            ]
        ),
    )


def test_pending_query_defers_output_without_writing_control_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp(_pending_job_envelope())
    output_path = tmp_path / "result with space.csv"

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        [
            "--json",
            "query",
            "SELECT 1",
            "--wait",
            "0",
            "--output",
            str(output_path),
            "--output-format",
            "csv",
            "--overwrite",
        ],
    )

    payload = json.loads(stdout)
    assert code == 0
    assert stderr == ""
    assert not output_path.exists()
    assert payload["status"] == "pending"
    assert payload["metadata"]["output_written"] is False
    assert payload["metadata"]["output_deferred"] is True
    assert "next_actions" not in payload["agent_hints"]
    action_payload = _rendered_action(payload, "job.result")
    assert action_payload["executable"] is False
    assert action_payload["placeholders"]["user_agent"] == "<user_agent>"
    next_action = action_payload["command"]
    assert "job result job-123" in next_action
    assert f"--output '{output_path}'" in next_action
    assert "--output-format csv" in next_action
    assert "--overwrite" in next_action
    assert "query SELECT" not in next_action


def test_job_result_writes_successful_rows_to_requested_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()
    output_path = tmp_path / "result.csv"

    code, stdout, stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        [
            "--json",
            "job",
            "result",
            "job-123",
            "--output",
            str(output_path),
            "--output-format",
            "csv",
        ],
    )

    payload = json.loads(stdout)
    assert code == 0
    assert stderr == ""
    assert app.job_result_calls == 1
    rows = list(csv.DictReader(StringIO(output_path.read_text(encoding="utf-8"))))
    assert [row["id"] for row in rows] == ["1", "2"]
    assert payload["metadata"]["output_written"] is True
    assert payload["metadata"]["output_deferred"] is False


def test_pending_job_result_preserves_output_intent_without_creating_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp(job_result_envelope=_pending_job_envelope("job.result"))
    output_path = tmp_path / "later.ndjson"

    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        [
            "--json",
            "job",
            "result",
            "job-123",
            "--output",
            str(output_path),
            "--output-format",
            "ndjson",
        ],
    )

    payload = json.loads(stdout)
    assert code == 0
    assert app.job_result_calls == 1
    assert not output_path.exists()
    assert payload["metadata"]["output_deferred"] is True
    assert "next_actions" not in payload["agent_hints"]
    action_payload = _rendered_action(payload, "job.result")
    assert action_payload["executable"] is False
    assert action_payload["placeholders"]["user_agent"] == "<user_agent>"
    next_action = action_payload["command"]
    assert "job result job-123" in next_action
    assert f"--output {output_path}" in next_action
    assert "--output-format ndjson" in next_action


def test_job_result_output_preflight_rejects_existing_file_before_remote_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()
    output_path = tmp_path / "existing.json"
    output_path.write_text("keep\n", encoding="utf-8")

    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--json", "job", "result", "job-123", "--output", str(output_path)],
    )

    payload = json.loads(stdout)
    assert code == 1
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert app.job_result_calls == 0
    assert output_path.read_text(encoding="utf-8") == "keep\n"


def test_job_result_output_format_requires_output_before_remote_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _FakeCliApp()

    code, stdout, _stderr = _run_with_app(
        tmp_path,
        monkeypatch,
        app,
        ["--json", "job", "result", "job-123", "--output-format", "csv"],
    )

    payload = json.loads(stdout)
    assert code == 2
    assert payload["error"]["code"] == "OUTPUT_FORMAT_ERROR"
    assert app.job_result_calls == 0
