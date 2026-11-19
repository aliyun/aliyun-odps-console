"""Exercise the CLI diagnosis stream with scripted MCP HTTP responses."""

from __future__ import annotations

import json
from io import BytesIO, StringIO
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from maxc_cli.app import MaxCApp
from maxc_cli.backend import mcp
from maxc_cli.cli import _command_manifest, build_parser, run
from tests.test_cli_mock import clear_odps_env, run_json_command

pytestmark = pytest.mark.unit


class _Output(StringIO):
    def __init__(self):
        super().__init__()
        self.flushed = ""

    def flush(self):
        self.flushed = self.getvalue()


class _EventStream:
    headers = {"Content-Type": "text/event-stream; charset=utf-8"}

    def __init__(self, lines):
        self.lines = lines
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def __iter__(self):
        return iter(self.lines)


@pytest.fixture
def diagnosis_cli(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "user")
    clear_odps_env(monkeypatch)
    config = tmp_path / "config.yaml"
    config.write_text(
        "default_project: current_project\n"
        "default_region: cn-shanghai\n"
        "state_dir: ./state\n"
        "auth:\n"
        "  access_id: fake-id\n"
        "  secret_access_key: fake-secret\n"
        "  endpoint: https://service.example.invalid/api\n",
        encoding="utf-8",
    )
    requests = []
    mints = []
    snapshot_requests = []
    stdout = _Output()
    stderr = StringIO()

    def mint(url, method, **_kwargs):
        mints.append((url, method))
        return SimpleNamespace(content={
            "accessToken": "mcpc_" + "a" * 40,
            "tokenType": "Bearer",
            "expiresIn": 300,
            "scope": ["maxcompute:read", "maxcompute:sql"],
        })

    def diagnose_snapshot(job_id, *, project, session_context):
        snapshot_requests.append((job_id, project, session_context))
        return {"status": "failure", "diagnosis_category": "permission_error"}

    backend = SimpleNamespace(
        supports_remote_jobs=True,
        _catalog_rest=SimpleNamespace(endpoint="https://catalog.example.invalid/api", request=mint),
        diagnose_job=diagnose_snapshot,
    )
    monkeypatch.setattr("maxc_cli.app.OdpsBackend", lambda *_args, **_kwargs: backend)

    def call(respond, *argv, explicit_config=True):
        def open_request(request, **_kwargs):
            requests.append(request)
            return respond(json.loads(request.data))

        monkeypatch.setattr(mcp.urllib.request, "build_opener", lambda: SimpleNamespace(open=open_request))
        flags = ["--config", str(config)] if explicit_config else []
        return run([*flags, "job", "diagnose", *argv], cwd=tmp_path, stdout=stdout, stderr=stderr)

    return SimpleNamespace(
        call=call, config=config, requests=requests, mints=mints,
        snapshot_requests=snapshot_requests, stdout=stdout, stderr=stderr,
    )


def _result(request, **structured):
    return {
        "jsonrpc": "2.0",
        "id": request["id"],
        "result": {"structuredContent": structured},
    }


def _json_response(payload):
    response = BytesIO(json.dumps(payload).encode("utf-8"))
    response.headers = {"Content-Type": "application/json"}
    return response


def _sse_frame(message):
    # Split the object across multiple SSE data fields.
    for line in json.dumps(message, indent=2).splitlines():
        yield f"data: {line}\r\n".encode()
    yield b"\r\n"


@pytest.mark.parametrize("job_id", ["session-instance@7", "session-instance"])
def test_stream_flushes_gateway_progress_before_final_result(diagnosis_cli, tmp_path, job_id):
    cli = diagnosis_cli
    app = MaxCApp(cwd=tmp_path, config_path=cli.config, load_backend=False)
    app._ensure_job_store().save_remote_job_context(job_id, {
        "project": "owning_project",
        "instance_id": "session-instance",
        "session_subquery_id": 7,
        "session_task_name": "AnonymousSQLRTTask",
        "session_project_name": "owning_project",
    })
    diagnosis = {
        "job": {"status": "failed"},
        "answer": {"status": "partial"},
        "evidence": [{"kind": "job_status"}],
        "missing_evidence": [{"kind": "worker_log"}],
    }

    def respond(request):
        assert request["method"] == "tools/call"
        assert request["params"]["name"] == "maxcompute_diagnose_job"
        assert request["params"]["arguments"] == {
            "instance_id": "session-instance",
            "project": "owning_project",
            "subquery_id": "7",
            "task_name": "AnonymousSQLRTTask",
            "region": "cn-shanghai",
        }
        progress_token = request["params"]["_meta"]["progressToken"]
        assert progress_token

        def lines():
            yield b": heartbeat\r\n\r\n"
            yield from _sse_frame({
                "jsonrpc": "2.0",
                "method": "notifications/progress",
                "params": {"progressToken": "other-call", "progress": 1},
            })
            yield from _sse_frame({
                "jsonrpc": "2.0",
                "method": "notifications/progress",
                "params": {
                    "progressToken": progress_token, "progress": 2, "total": 6,
                    "message": "Reading task evidence", "_meta": {"phase": "job_diagnosis.facts"},
                },
            })
            assert json.loads(cli.stdout.flushed) == {
                "type": "progress", "progress": 2, "total": 6,
                "message": "Reading task evidence", "phase": "job_diagnosis.facts",
            }
            yield from _sse_frame(_result(
                request, ok=True, request_id="gateway-request", data=diagnosis,
                warnings=["Some logs are unavailable."],
            ))
            pytest.fail("The client must return on the matching result before waiting for EOF.")

        return _EventStream(lines())

    assert cli.call(respond, job_id, "--stream") == 0
    events = [json.loads(line) for line in cli.stdout.getvalue().splitlines()]
    assert [event["type"] for event in events] == ["progress", "result"]
    terminal = events[-1]
    assert terminal["status"] == "success"
    assert terminal["version"] == "2.0"
    assert terminal["metadata"]["request_id"] == "gateway-request"
    assert terminal["data"]["diagnosis"] == {**diagnosis, "job_id": job_id}
    assert terminal["agent_hints"]["warnings"] == ["Some logs are unavailable."]
    assert len(cli.mints) == len(cli.requests) == 1
    assert cli.requests[0].get_header("Accept") == "application/json, text/event-stream"
    assert cli.stderr.getvalue() == ""


def test_stream_accepts_json_terminal_response(diagnosis_cli):
    cli = diagnosis_cli
    code = cli.call(
        lambda req: _json_response(_result(req, ok=True, data={"answer": {"status": "answered"}})),
        "instance-1", "--project", "explicit", "--stream", "--format", "ndjson",
    )
    assert code == 0
    event = json.loads(cli.stdout.getvalue())
    assert event["type"] == "result"
    assert event["metadata"]["project"] == "explicit"


def test_stream_preserves_gateway_tool_error(diagnosis_cli):
    cli = diagnosis_cli
    code = cli.call(
        lambda req: _json_response(_result(
            req, ok=False, request_id="gateway-request",
            error={"code": "PERMISSION_DENIED", "message": "The job is not accessible."},
        )),
        "instance-1", "--stream",
    )
    assert code != 0
    event = json.loads(cli.stdout.getvalue())
    assert event["type"] == "error"
    assert event["error"]["code"] == "PERMISSION_DENIED"
    assert event["metadata"]["request_id"] == "gateway-request"


def test_stream_fails_on_missing_terminal_response_without_replaying(diagnosis_cli):
    cli = diagnosis_cli

    def respond(request):
        progress_token = request["params"]["_meta"]["progressToken"]
        return _EventStream(_sse_frame({
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {"progressToken": progress_token, "progress": 1},
        }))

    assert cli.call(respond, "instance-1", "--stream", "--json") != 0
    events = [json.loads(line) for line in cli.stdout.getvalue().splitlines()]
    assert [event["type"] for event in events] == ["progress", "error"]
    assert events[-1]["error"]["code"] == "BACKEND_CONNECTION_ERROR"
    assert not events[-1]["error"]["recoverable"]
    assert len(cli.requests) == 1


def test_stream_emits_permission_error_before_first_progress(diagnosis_cli):
    cli = diagnosis_cli

    def respond(_request):
        raise HTTPError("https://mcp.example.invalid/mcp", 403, "Forbidden", {}, BytesIO())

    assert cli.call(respond, "instance-1", "--stream") == 2
    event = json.loads(cli.stdout.getvalue())
    assert event["type"] == "error"
    assert event["error"]["code"] == "PERMISSION_DENIED"
    assert len(cli.requests) == 1


def test_stream_interrupt_closes_response_and_reports_terminal_error(diagnosis_cli):
    cli = diagnosis_cli

    def lines():
        yield b": open\r\n\r\n"
        raise KeyboardInterrupt

    response = _EventStream(lines())
    assert cli.call(lambda _: response, "instance-1", "--stream") == 130
    assert response.closed
    event = json.loads(cli.stdout.getvalue())
    assert event["type"] == "error"
    assert event["error"]["code"] == "DIAGNOSIS_INTERRUPTED"


def test_stream_rejects_endpoint_from_auto_discovered_workspace(diagnosis_cli, tmp_path):
    cli = diagnosis_cli
    user_config = Path.home() / ".maxc" / "config.yaml"
    user_config.parent.mkdir(parents=True)
    user_config.write_text(cli.config.read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / ".maxc.yaml").write_text(
        "mcp:\n  endpoint: https://elsewhere.example.invalid/mcp\n", encoding="utf-8",
    )

    assert cli.call(lambda _: pytest.fail("unexpected request"), "instance-1", "--stream", explicit_config=False) != 0
    event = json.loads(cli.stdout.getvalue())
    assert event["type"] == "error"
    assert event["error"]["code"] == "VALIDATION_ERROR"
    assert cli.mints == cli.requests == []


def test_diagnose_without_stream_uses_original_snapshot(diagnosis_cli, tmp_path):
    cli = diagnosis_cli
    code, envelope, stderr = run_json_command(tmp_path, cli.config, ["job", "diagnose", "instance-1", "--json"])
    assert code == 0
    assert envelope["status"] == "success"
    assert envelope["data"]["diagnosis"]["diagnosis_category"] == "permission_error"
    assert cli.snapshot_requests == [("instance-1", "current_project", None)]
    assert cli.mints == cli.requests == []
    assert stderr == ""


def test_manifest_describes_stream_shape_and_possible_credits():
    manifest = _command_manifest(build_parser())
    entry = next(item for item in manifest["commands"] if item["command"] == "job.diagnose")
    stream = {"arg": "stream", "equals": True}
    assert entry["output"]["rules"][0]["when"] == stream
    assert entry["output"]["rules"][0]["formats"] == ["ndjson"]
    assert any(effect["kind"] == "consume_credits" and effect["when"] == stream for effect in entry["effects"])
    shape = manifest["output_shape_contracts"][entry["output"]["shape_contract"]]
    assert shape["shape_rules"][0]["event_types"] == ["progress", "result", "error"]
