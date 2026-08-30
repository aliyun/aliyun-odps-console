"""Regression tests for machine-readable CLI safety and output contracts."""

from __future__ import annotations

import csv
import json
from copy import deepcopy
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit

import maxc_cli.cli as cli_module
from maxc_cli.app import MaxCApp
from maxc_cli.cli import _build_permission_denied_hints, _emit_csv, _render_human, run
from maxc_cli.config import TableColumn, TableDefinition
from maxc_cli.exceptions import ErrorPayload, UnsupportedSqlOperationError
from maxc_cli.models import AgentHints, Envelope, JobInfo, SuggestedAction


def _make_app(tmp_path: Path) -> MaxCApp:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: test_project\n"
        "state_dir: ./state\n"
        "cache_dir: ./cache\n"
        "auth:\n"
        "  access_id: FAKE\n"
        "  secret_access_key: FAKE\n"
        "  project: test_project\n"
        "masking:\n"
        "  enabled: true\n"
        "  sensitive_patterns:\n"
        "    - bank_account\n",
        encoding="utf-8",
    )
    return MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)


def test_authenticated_backend_construction_does_not_create_cache(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config-with-endpoint.yaml"
    cache_dir = tmp_path / "cache"
    config_path.write_text(
        "default_project: test_project\n"
        f"cache_dir: {cache_dir}\n"
        f"state_dir: {tmp_path / 'state'}\n"
        "auth:\n"
        "  access_id: FAKE\n"
        "  secret_access_key: FAKE\n"
        "  project: test_project\n"
        "  endpoint: https://service.example.invalid/api\n",
        encoding="utf-8",
    )

    app = MaxCApp(cwd=tmp_path, config_path=config_path)

    assert app.backend is not None
    assert app._cache is None
    assert not cache_dir.exists()


def test_access_key_whoami_lazy_backend_does_not_create_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    cache_dir = tmp_path / "cache"
    config_path.write_text(
        "default_project: test_project\n"
        f"cache_dir: {cache_dir}\n"
        f"state_dir: {tmp_path / 'state'}\n"
        "auth:\n"
        "  access_id: FAKE\n"
        "  secret_access_key: FAKE\n"
        "  project: test_project\n"
        "  endpoint: https://service.example.invalid/api\n",
        encoding="utf-8",
    )
    observed_cache = None

    class _WhoamiBackend:
        supports_remote_jobs = True

        def __init__(self, _config, *, cache=None) -> None:
            nonlocal observed_cache
            observed_cache = cache
            self.resolved_auth = SimpleNamespace(
                auth_type="access_key",
                identity_source="config_file",
                suppressed_env_vars=[],
            )

        def whoami_info(self, *, project):
            return (
                {
                    "authenticated": True,
                    "configured": True,
                    "validation_status": "verified",
                    "backend": "odps",
                    "project": project,
                },
                [],
            )

    monkeypatch.setattr("maxc_cli.app.OdpsBackend", _WhoamiBackend)
    app = MaxCApp(cwd=tmp_path, config_path=config_path, load_backend=False)

    envelope = app.auth_whoami()

    assert envelope.status == "success"
    assert observed_cache is app._lazy_cache
    assert app._cache is None
    assert not cache_dir.exists()


def test_local_sql_rejection_schema_enrichment_does_not_create_cache(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    cache_dir = tmp_path / "cache"
    config_path.write_text(
        "default_project: test_project\n"
        f"cache_dir: {cache_dir}\n"
        f"state_dir: {tmp_path / 'state'}\n"
        "auth:\n"
        "  access_id: FAKE\n"
        "  secret_access_key: FAKE\n"
        "  project: test_project\n"
        "  endpoint: https://service.example.invalid/api\n",
        encoding="utf-8",
    )
    stdout = StringIO()

    code = run(
        ["--config", str(config_path), "query", "DROP TABLE t", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )

    assert code != 0
    assert json.loads(stdout.getvalue())["status"] == "failure"
    assert not cache_dir.exists()


class _NeverSubmitUnknownSqlBackend:
    def __init__(self) -> None:
        self.submit_calls = 0

    def submit_query(self, *_args, **_kwargs):
        self.submit_calls += 1
        raise AssertionError("unsupported SQL reached the backend")


@pytest.mark.parametrize(
    "sql",
    [
        "PAI -name xgboost -DoutputTableName=out",
        "EXECUTE IMMEDIATE 'DROP TABLE target'",
        "CALL update_catalog()",
        "PUT POLICY policy_name",
    ],
)
@pytest.mark.parametrize("entry_point", ["query", "job.submit"])
def test_unproven_sql_is_blocked_before_remote_submission(
    tmp_path: Path,
    sql: str,
    entry_point: str,
) -> None:
    app = _make_app(tmp_path)
    backend = _NeverSubmitUnknownSqlBackend()
    app.backend = backend
    app.remote_jobs = True

    with pytest.raises(UnsupportedSqlOperationError):
        if entry_point == "query":
            app.query(command="query", sql=sql, wait=0)
        else:
            app.submit_job(sql=sql)

    assert backend.submit_calls == 0


@pytest.mark.parametrize(
    ("sensitive_args", "secrets"),
    [
        (["--secret-access-key", "SECRET-SEPARATE"], ["SECRET-SEPARATE"]),
        (["--access-key-secret=SECRET-INLINE"], ["SECRET-INLINE"]),
        (["--security-token", "STS-TOKEN"], ["STS-TOKEN"]),
        (["--access-id=ACCESS-ID"], ["ACCESS-ID"]),
        (["--login-continuation", "a" * 64], ["a" * 64]),
        (["--oauth-continuation", "b" * 64], ["b" * 64]),
        (["--password", "UNKNOWN-PASSWORD"], ["UNKNOWN-PASSWORD"]),
    ],
)
def test_argument_error_json_redacts_sensitive_argv_values(
    tmp_path: Path,
    sensitive_args: list[str],
    secrets: list[str],
) -> None:
    stdout = StringIO()
    stderr = StringIO()

    code = run(
        [
            "auth",
            "login",
            *sensitive_args,
            "--definitely-not-a-real-flag",
            "--json",
        ],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code == 2
    assert stderr.getvalue() == ""
    raw_payload = stdout.getvalue()
    payload = json.loads(raw_payload)
    assert payload["error"]["code"] == "ARGUMENT_ERROR"
    assert "<redacted>" in raw_payload
    for secret in secrets:
        assert secret not in raw_payload


def test_argument_error_json_redacts_external_process_command(tmp_path: Path) -> None:
    process_command = "credential-helper --token PROCESS-TOKEN"
    stdout = StringIO()

    code = run(
        [
            "auth",
            "login-external",
            "--process-command",
            process_command,
            "--definitely-not-a-real-flag",
            "--json",
        ],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )

    assert code == 2
    assert process_command not in stdout.getvalue()
    assert "PROCESS-TOKEN" not in stdout.getvalue()


@pytest.mark.parametrize("data", [None, {"job_id": "job-123"}])
def test_human_query_failure_preserves_real_error_and_resume_context(data) -> None:
    envelope = Envelope(
        command="query",
        status="failure",
        data=data,
        metadata={
            "job_id": "job-123",
            "project": "project-a",
            "logview": "https://logview.example.test/job-123",
        },
        error=ErrorPayload(
            code="BACKEND_CONNECTION_ERROR",
            message="connection lost after submission",
            suggestion="Resume the existing job.",
            recoverable=True,
        ),
        agent_hints=AgentHints(
            actions=[
                SuggestedAction(
                    id="job.status",
                    title="Show job status",
                    command="maxc job status job-123 --json",
                )
            ],
        ),
    )

    rendered = _render_human(envelope)

    assert "BACKEND_CONNECTION_ERROR" in rendered
    assert "connection lost after submission" in rendered
    assert "job-123" in rendered
    assert "maxc job status job-123 --json" in rendered
    assert "INTERNAL_ERROR" not in rendered
    assert "(no rows)" not in rendered


class _SensitiveSampleBackend:
    def __init__(self) -> None:
        self.table = TableDefinition(
            name="customers",
            description="",
            columns=[
                TableColumn("email", "string"),
                TableColumn("phone", "string"),
                TableColumn("bank_account", "string"),
                TableColumn("name", "string"),
            ],
        )
        self.rows = [
            {
                "email": "alice@example.com",
                "phone": "13812345678",
                "bank_account": "6222020202020202",
                "name": "Alice",
            },
            {
                "email": "bob@example.com",
                "phone": "13987654321",
                "bank_account": "6222030303030303",
                "name": "Bob",
            },
        ]

    def sample_table(self, *_args, **_kwargs):
        schema = [
            {"name": column.name, "type": column.type, "comment": column.comment}
            for column in self.table.columns
        ]
        return self.table, deepcopy(self.rows), {
            "schema": schema,
            "applied_partition": None,
            "selected_columns": [column.name for column in self.table.columns],
            "warnings": [],
        }


def test_data_sample_masks_sensitive_rows(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    app.backend = _SensitiveSampleBackend()

    payload = app.data_sample("customers").to_dict()
    serialized = json.dumps(payload)
    rows = payload["data"]["sample"]["rows"]

    assert rows[0]["email"] == "a***@example.com"
    assert rows[0]["phone"] == "138****5678"
    assert rows[0]["bank_account"] == "***"
    assert rows[0]["name"] == "Alice"
    assert "alice@example.com" not in serialized
    assert "13812345678" not in serialized
    assert "6222020202020202" not in serialized
    assert "Sensitive columns masked:" in payload["agent_hints"]["warnings"][0]


def test_data_profile_masks_sample_and_top_values(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    app.backend = _SensitiveSampleBackend()

    payload = app.data_profile("customers").to_dict()
    serialized = json.dumps(payload)
    profiles = {
        column["name"]: column
        for column in payload["data"]["profile"]["columns"]
    }

    assert profiles["email"]["sample_values"] == [
        "a***@example.com",
        "b***@example.com",
    ]
    assert [entry["value"] for entry in profiles["email"]["top_values_in_sample"]] == [
        "a***@example.com",
        "b***@example.com",
    ]
    assert profiles["bank_account"]["sample_values"] == ["***", "***"]
    assert profiles["bank_account"]["top_values_in_sample"] == [
        {"value": "***", "count": 2}
    ]
    assert "alice@example.com" not in serialized
    assert "13812345678" not in serialized
    assert "6222020202020202" not in serialized
    assert "Sensitive columns masked:" in payload["agent_hints"]["warnings"][0]


def _create_pending_local_job(app: MaxCApp) -> str:
    job = app._ensure_job_store().create_job(
        sql="SELECT 1",
        project="test_project",
        result={
            "data": {
                "rows": [{"value": 1}],
                "schema": [{"name": "value", "type": "bigint"}],
                "total_rows": 1,
                "returned_rows": 1,
                "has_more": False,
                "next_cursor": None,
            },
            "metadata": {"project": "test_project"},
            "agent_hints": {"warnings": []},
        },
    )
    return job["job_id"]


def test_successful_cancel_is_a_successful_command_and_cli_exit_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _make_app(tmp_path)
    job_id = _create_pending_local_job(app)

    monkeypatch.setattr(cli_module, "MaxCApp", lambda **_kwargs: app)
    stdout = StringIO()
    code = run(
        ["job", "cancel", job_id, "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )
    payload = json.loads(stdout.getvalue())

    assert code == 0
    assert payload["status"] == "success"
    assert payload["error"] is None
    assert payload["data"]["job"]["cancelled"] is True


def test_remote_cancel_resource_failure_does_not_mark_command_failed(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    app.remote_jobs = True
    app.backend = SimpleNamespace(
        cancel_job=lambda _job_id, *, project: JobInfo(
            job_id="remote-job",
            status="failure",
            project=project,
            progress=0,
            stage="cancel_requested",
            failure_reason="Cancellation has been requested.",
        )
    )

    envelope = app.cancel_job("remote-job")

    assert envelope.status == "success"
    assert envelope.error is None
    assert envelope.data == {
        "job_id": "remote-job",
        "cancelled": False,
        "cancel_requested": True,
        "already_terminal": False,
        "outcome": "cancel_requested",
        "job_status": "running",
    }


def test_pending_job_actions_do_not_offer_unreadable_result(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    info = JobInfo(
        job_id="job-1",
        status="running",
        project="p",
        progress=50,
    )

    envelope = app._job_info_envelope("job.status", info)

    assert envelope.status == "pending"
    assert [item.id for item in envelope.agent_hints.actions] == [
        "job.wait",
        "job.status",
    ]


def test_unknown_job_status_is_not_reported_as_success(tmp_path: Path) -> None:
    app = _make_app(tmp_path)
    info = JobInfo(
        job_id="job-1",
        status="future_backend_state",
        project="p",
        progress=0,
    )

    envelope = app._job_info_envelope("job.status", info)

    assert envelope.status == "pending"
    assert "unknown job status" in envelope.agent_hints.warnings[0]
    assert all(action.id != "job.result" for action in envelope.agent_hints.actions)


def test_permission_denied_hints_do_not_invent_an_environment_project() -> None:
    app = SimpleNamespace(config=SimpleNamespace(default_project="customer_project"))

    hints = _build_permission_denied_hints(app)
    payload = hints.to_dict()

    assert payload["action_ids"] == ["auth.whoami", "auth.can-i"]
    assert all("customer_project_dev" not in command for command in payload["next_actions"])
    assert "does not identify an alternative project" in payload["warnings"][0]


class _JobWaitApp:
    def __init__(self, envelope: Envelope) -> None:
        external = SimpleNamespace(is_configured=lambda: False)
        ncs = SimpleNamespace(is_configured=lambda: False)
        self.config = SimpleNamespace(
            auth=SimpleNamespace(
                access_id="FAKE",
                secret_access_key="FAKE",
                external=external,
                ncs=ncs,
            ),
            default_project="test_project",
        )
        self.envelope = envelope

    def job_wait(
        self,
        _job_id: str,
        *,
        timeout: int | None = None,
        project: str | None = None,
    ):
        return self.envelope, []


def _run_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    envelope: Envelope,
) -> tuple[int, dict[str, object]]:
    app = _JobWaitApp(envelope)
    monkeypatch.setattr(cli_module, "MaxCApp", lambda **_kwargs: app)
    stdout = StringIO()
    code = run(
        ["job", "wait", "job-1", "--timeout", "1", "--stream", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )
    lines = stdout.getvalue().splitlines()
    assert len(lines) == 1
    return code, json.loads(lines[0])


def test_job_wait_stream_emits_failure_and_nonzero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = Envelope(
        command="job.wait",
        status="failure",
        data={"job_id": "job-1"},
        metadata={"job_id": "job-1", "project": "test_project"},
        error=ErrorPayload(
            code="BACKEND_CONNECTION_ERROR",
            message="connection lost",
            suggestion="Check network.",
            recoverable=True,
        ),
        agent_hints=AgentHints(warnings=["Backend unavailable."]),
    )

    code, event = _run_stream(tmp_path, monkeypatch, envelope)

    assert code == 1
    assert event["type"] == "failed"
    assert event["status"] == "failure"
    assert event["job_id"] == "job-1"
    assert event["error"]["code"] == "BACKEND_CONNECTION_ERROR"


def test_job_wait_stream_emits_ndjson_when_initial_lookup_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from maxc_cli.exceptions import BackendConnectionError

    class _RaisingJobWaitApp(_JobWaitApp):
        def log(self, *_args, **_kwargs):
            return None

        def job_wait(self, *_args, **_kwargs):
            raise BackendConnectionError(
                "connection lost before the first status response",
                suggestion="Retry status for the same job.",
            )

    app = _RaisingJobWaitApp(Envelope(command="job.wait", status="failure"))
    monkeypatch.setattr(cli_module, "MaxCApp", lambda **_kwargs: app)
    stdout = StringIO()
    stderr = StringIO()

    code = run(
        ["job", "wait", "job-early", "--project", "project-a", "--stream"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    events = stdout.getvalue().splitlines()
    assert code == 1
    assert stderr.getvalue() == ""
    assert len(events) == 1
    event = json.loads(events[0])
    assert event["type"] == "failed"
    assert event["job_id"] == "job-early"
    assert event["metadata"]["project"] == "project-a"
    assert event["error"]["code"] == "BACKEND_CONNECTION_ERROR"
    assert any(
        item["id"] == "job.status"
        and "job-early" in item["command"]
        for item in event["agent_hints"]["actions"]
    )


@pytest.mark.parametrize(
    ("subcommand", "extra_args"),
    [
        ("status", []),
        ("wait", []),
        ("result", []),
        ("cancel", []),
        ("diagnose", []),
    ],
)
def test_job_command_early_failure_preserves_job_scope_and_job_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    subcommand: str,
    extra_args: list[str],
) -> None:
    from maxc_cli.exceptions import BackendConnectionError

    class _RaisingJobApp(_JobWaitApp):
        def log(self, *_args, **_kwargs):
            return None

        def _raise(self):
            raise BackendConnectionError("network unavailable")

        def job_status(self, *_args, **_kwargs):
            return self._raise()

        def job_wait(self, *_args, **_kwargs):
            return self._raise()

        def job_result(self, *_args, **_kwargs):
            return self._raise()

        def cancel_job(self, *_args, **_kwargs):
            return self._raise()

        def job_diagnose(self, *_args, **_kwargs):
            return self._raise()

    app = _RaisingJobApp(Envelope(command="job.wait", status="failure"))
    monkeypatch.setattr(cli_module, "MaxCApp", lambda **_kwargs: app)
    stdout = StringIO()

    code = run(
        [
            "job", subcommand, "job-123", "--project", "project-a",
            *extra_args, "--json",
        ],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )

    payload = json.loads(stdout.getvalue())
    assert code == 1
    data_key = "diagnosis" if subcommand == "diagnose" else "job"
    assert payload["data"][data_key]["job_id"] == "job-123"
    assert payload["metadata"] == {"job_id": "job-123", "project": "project-a"}
    assert payload["error"]["code"] == "BACKEND_CONNECTION_ERROR"
    assert payload["agent_hints"]["action_ids"] == [
        "job.status", "job.wait", "job.diagnose"
    ]


def test_job_not_found_uses_job_family_recovery_instead_of_metadata_search(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from maxc_cli.exceptions import NotFoundError

    class _MissingJobApp(_JobWaitApp):
        def log(self, *_args, **_kwargs):
            return None

        def job_status(self, *_args, **_kwargs):
            raise NotFoundError("job does not exist")

    app = _MissingJobApp(Envelope(command="job.status", status="failure"))
    monkeypatch.setattr(cli_module, "MaxCApp", lambda **_kwargs: app)
    stdout = StringIO()

    code = run(
        ["job", "status", "job-missing", "--project", "project-a", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )

    payload = json.loads(stdout.getvalue())
    assert code == 1
    assert payload["agent_hints"]["action_ids"] == ["job.list"]
    assert all("meta " not in step for step in payload["error"]["recovery_steps"])
    assert "job list" in payload["error"]["recovery_steps"][0]


def test_job_wait_stream_emits_pending_timeout_and_zero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = Envelope(
        command="job.wait",
        status="pending",
        data={"job_id": "job-1"},
        metadata={
            "job_id": "job-1",
            "project": "test_project",
            "wait_seconds": 1,
        },
        agent_hints=AgentHints(insights=["Job still running after 1s."]),
    )

    code, event = _run_stream(tmp_path, monkeypatch, envelope)

    assert code == 0
    assert event["type"] == "pending"
    assert event["status"] == "pending"
    assert event["job_id"] == "job-1"
    assert event["metadata"]["wait_seconds"] == 1


def test_job_wait_stream_distinguishes_cancelled_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = Envelope(
        command="job.wait",
        status="success",
        data={"job_id": "job-1", "status": "cancelled", "progress": 100},
        metadata={"job_id": "job-1", "project": "test_project"},
        agent_hints=AgentHints(),
    )

    code, event = _run_stream(tmp_path, monkeypatch, envelope)

    assert code == 0
    assert event["type"] == "cancelled"
    assert event["status"] == "success"
    assert event["data"]["job"]["status"] == "cancelled"


def test_csv_emitter_quotes_commas_newlines_and_quotes() -> None:
    stdout = StringIO(newline="")
    rows = [
        {
            "comma": "a,b",
            "newline": "line 1\nline 2",
            "quote": 'say "hello"',
            "null": None,
        }
    ]

    _emit_csv(rows, stdout)

    stdout.seek(0)
    assert list(csv.DictReader(stdout)) == [
        {
            "comma": "a,b",
            "newline": "line 1\nline 2",
            "quote": 'say "hello"',
            "null": "",
        }
    ]
