"""Connection recovery with profile STS, without a second authorization."""
import json
import shlex
from io import StringIO

import pytest
import yaml

from maxc_cli import catalog_bootstrap as cb
from maxc_cli.cli import run
from maxc_cli.config import AuthConfig, load_config
from maxc_cli.helpers import resolve_odps_settings
from tests.test_cli_mock import FakeODPS, _StubBackend, clear_odps_env, isolate_home

pytestmark = pytest.mark.unit


@pytest.fixture
def profile(monkeypatch, tmp_path):
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setenv("MAXC_CLI_NAME", "aliyun maxc")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "STS.PROFILE")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "PROFILE_SECRET")
    monkeypatch.setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "PROFILE_TOKEN")
    monkeypatch.setenv("MAXCOMPUTE_REGION", "cn-hangzhou")
    monkeypatch.setattr("odps.ODPS", FakeODPS)
    monkeypatch.setattr(cb, "build_bootstrap_odps", lambda **kw: object())
    monkeypatch.setattr(cb, "list_all_projects", lambda _: [
        cb.ProjectInfo("chosen_project", "cn-shanghai", "owner", True, ""),
    ])
    monkeypatch.setattr("maxc_cli.oauth.start_oauth_flow", lambda **kw: pytest.fail("second OAuth login"))
    return tmp_path / "selected config.yaml"


def invoke(config, argv):
    stdout, stderr = StringIO(), StringIO()
    code = run(["--config", str(config), *argv], cwd=config.parent, stdout=stdout, stderr=stderr)
    return code, stdout.getvalue(), stderr.getvalue()


def test_query_json_returns_connection_action_even_with_tty(profile, monkeypatch):
    profile.write_text("auth: {}\n")
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("JSON must not prompt"))
    code, out, err = invoke(profile, ["query", "SELECT 1", "--json"])
    payload = json.loads(out)
    assert code == 1
    assert payload["error"]["context"]["missing_fields"] == ["project"]
    assert payload["error"]["recoverable"] is True
    command = payload["agent_hints"]["actions"][0]["command"]
    assert "--project <project>" in command
    retry = payload["agent_hints"]["actions"][0]
    assert retry["executable"] is False
    assert retry["effect"] == "remote_compute"
    assert retry["agent_allowed"] is True
    assert str(profile) in shlex.split(command)
    assert "--oauth" not in command
    assert payload["error"]["recovery_steps"]
    assert "PROFILE_SECRET" not in out + err
    assert yaml.safe_load(profile.read_text()) == {"auth": {}}


def test_write_retry_template_retains_confirmation_and_sql(profile):
    profile.write_text("auth: {}\n")
    code, out, _ = invoke(profile, ["query", "DROP TABLE target", "--force", "--json"])
    assert code == 1
    retry = json.loads(out)["agent_hints"]["actions"][0]
    assert retry["executable"] is False
    assert retry["effect"] == "remote_write" and retry["confirmation_required"] is True
    assert "'DROP TABLE target'" in retry["command"] and "--force" in retry["command"]


def test_json_project_actions_round_trip_without_saving_credentials(profile, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("JSON must not prompt"))
    code, out, _ = invoke(profile, ["auth", "login", "--reuse-auth", "--json"])
    payload = json.loads(out)
    assert code == 0 and payload["status"] == "pending"
    command = payload["agent_hints"]["actions"][0]["command"]
    assert "--odps-endpoint" in command and "--reuse-auth" in command
    assert not profile.exists()
    code, out, err = invoke(profile, shlex.split(command)[2:])
    assert code == 0, out + err
    saved = yaml.safe_load(profile.read_text())
    assert saved["auth"]["project"] == "chosen_project"
    assert saved["auth"]["endpoint"] == cb.region_to_endpoint("cn-shanghai")
    assert not ({"provider", "access_id", "secret_access_key", "security_token"} & saved["auth"].keys())
    assert "PROFILE_SECRET" not in profile.read_text() + out + err
    assert "PROFILE_TOKEN" not in profile.read_text() + out + err


@pytest.mark.parametrize("json_mode", [False, True])
def test_missing_project_never_prompts_or_lists_projects(profile, monkeypatch, json_mode):
    profile.write_text("auth: {}\n")
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("data command must not prompt"))
    monkeypatch.setattr(cb, "list_all_projects", lambda _: pytest.fail("data command must not open picker"))
    argv = ["query", "SELECT 1"] + (["--json"] if json_mode else [])
    code, out, err = invoke(profile, argv)
    assert code == 1
    assert "--project <project>" in out + err
    assert yaml.safe_load(profile.read_text()) == {"auth": {}}


@pytest.mark.parametrize("provider", [None, "sts_token", "oauth"])
def test_query_project_flag_is_applied_before_backend_validation(profile, monkeypatch, provider):
    from maxc_cli.auth_providers import resolve_auth_connection
    if provider == "oauth":
        from tests.test_oauth import _oauth_auth
        original = _oauth_auth(project=None, endpoint=cb.region_to_endpoint("cn-hangzhou")).to_mapping()
    elif provider:
        original = {"provider": provider, "access_id": "STS.SAVED",
                    "secret_access_key": "SAVED_SECRET", "security_token": "SAVED_TOKEN",
                    "endpoint": cb.region_to_endpoint("cn-hangzhou")}
    else:
        original = {}
    profile.write_text(yaml.safe_dump({"auth": original}))
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "environment_project")
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("command must not prompt"))
    monkeypatch.setattr(cb, "list_all_projects", lambda _: pytest.fail("unexpected picker"))
    import maxc_cli.app as app_module
    class RoutedQueryBackend(_StubBackend):
        def __init__(self, config, **kw):
            connection = resolve_auth_connection(config)
            assert connection.project == "command_project"
            assert connection.setting_sources["project"] == "command_line"
            assert connection.endpoint == cb.region_to_endpoint("cn-hangzhou")
            assert config.default_project == "command_project"
    monkeypatch.setattr(app_module, "OdpsBackend", RoutedQueryBackend)
    code, out, err = invoke(profile, ["query", "SELECT 1", "--project", "command_project", "--json"])
    assert code == 0, out + err
    assert yaml.safe_load(profile.read_text())["auth"] == original


def test_explicit_connection_setup_keeps_interactive_picker(profile, monkeypatch):
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda *_: "1")
    code, out, err = invoke(profile, ["auth", "login", "--reuse-auth"])
    assert code == 0, out + err
    saved = yaml.safe_load(profile.read_text())
    assert saved["auth"]["project"] == "chosen_project"
    assert saved["auth"]["endpoint"] == cb.region_to_endpoint("cn-shanghai")


@pytest.mark.parametrize("endpoint", [None, "https://private.example/api"])
def test_pinned_project_uses_profile_region_or_explicit_endpoint(profile, monkeypatch, endpoint):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    argv = ["auth", "login", "--reuse-auth", "--project", "p", "--no-validate", "--json"]
    if endpoint:
        argv += ["--odps-endpoint", endpoint]
    code, out, err = invoke(profile, argv)
    assert code == 0, out + err
    assert yaml.safe_load(profile.read_text())["auth"]["endpoint"] == (endpoint or cb.region_to_endpoint("cn-hangzhou"))


def test_unknown_region_returns_manual_connection_template(profile, monkeypatch):
    monkeypatch.setenv("MAXCOMPUTE_REGION", "unknown-1")
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr(cb, "list_all_projects", lambda _: [
        cb.ProjectInfo("p", "unknown-1", None, False, ""),
    ])
    code, out, _ = invoke(profile, ["auth", "login", "--reuse-auth", "--json"])
    payload = json.loads(out)
    assert code == 0 and payload["status"] == "pending"
    item = payload["agent_hints"]["actions"][0]
    assert item["executable"] is False and item["placeholders"]["endpoint"] == "<endpoint>"
    assert "--odps-endpoint <endpoint>" in item["command"]


def test_reuse_auth_preserves_saved_identity_and_suppresses_unrelated_env(profile, monkeypatch):
    original = {"provider": "sts_token", "access_id": "STS.SAVED", "secret_access_key": "SAVED_SECRET", "security_token": "SAVED_TOKEN"}
    profile.write_text(yaml.safe_dump({"auth": original, "other": "preserved"}))
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    code, out, err = invoke(profile, ["auth", "login", "--reuse-auth", "--project", "p", "--region", "cn-beijing", "--no-validate", "--json"])
    assert code == 0, out + err
    saved = yaml.safe_load(profile.read_text())
    assert all(saved["auth"][k] == v for k, v in original.items())
    assert saved["other"] == "preserved"
    assert saved["auth"]["endpoint"] == cb.region_to_endpoint("cn-beijing")


def test_endpoint_derivation_does_not_override_configured_endpoint(monkeypatch, tmp_path):
    clear_odps_env(monkeypatch)
    config = load_config(tmp_path)
    config.auth = AuthConfig(provider="sts_token", region_name="cn-beijing", endpoint="https://private.example/api")
    settings, _, _ = resolve_odps_settings(config)
    assert settings["endpoint"] == "https://private.example/api"


def test_reuse_auth_rejects_identity_replacement(profile):
    code, out, _ = invoke(profile, ["auth", "login", "--reuse-auth", "--oauth", "--json"])
    assert code == 1 and json.loads(out)["error"]["code"] == "VALIDATION_ERROR"


def test_missing_both_fields_non_tty_does_not_prompt(profile, monkeypatch):
    profile.write_text("auth: {}\n")
    monkeypatch.delenv("MAXCOMPUTE_REGION")
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr("builtins.input", lambda *_: pytest.fail("non-TTY must not prompt"))
    code, out, err = invoke(profile, ["query", "SELECT 1", "--json"])
    assert code == 1
    payload = json.loads(out)
    assert payload["error"]["context"]["missing_fields"] == ["project", "endpoint"]
    assert "--project <project>" in payload["error"]["suggestion"]
    assert "PROFILE_SECRET" not in out + err


def test_saved_oauth_is_kept_when_configuring_connection(profile, monkeypatch):
    from tests.test_oauth import _oauth_auth
    auth = _oauth_auth(project=None, endpoint=None)
    original = auth.to_mapping()
    profile.write_text(yaml.safe_dump({"auth": original}))
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    code, out, err = invoke(profile, ["auth", "login", "--reuse-auth", "--project", "p", "--region", "cn-beijing", "--no-validate", "--json"])
    assert code == 0, out + err
    saved = yaml.safe_load(profile.read_text())
    assert saved["auth"]["provider"] == "oauth"
    assert saved["auth"]["oauth"] == original["oauth"]
    assert saved["auth"]["access_id"] == original["access_id"]
    assert saved["auth"]["endpoint"] == cb.region_to_endpoint("cn-beijing")


@pytest.mark.parametrize("target", ["default_project", "another_project"])
def test_mcqa_v1_submission_and_resume_use_the_effective_project(tmp_path, monkeypatch, target):
    from datetime import datetime

    from odps import ODPS
    from odps.errors import NoSuchObject
    from odps.models import Instance
    from odps.models.session import InSessionInstance, SessionInstance

    from maxc_cli.backend.odps import OdpsBackend
    from tests.test_cli_mock import _ExecutionSettings

    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config = load_config(tmp_path)
    config.auth = AuthConfig(access_id="AK", secret_access_key="SK", project="auth_project",
                             endpoint="https://service.example/api")
    config.default_project = "default_project"
    monkeypatch.setattr(ODPS, "get_logview_host", lambda *a, **kw: None)
    monkeypatch.setattr(ODPS, "get_job_insight_host", lambda *a, **kw: None, raising=False)
    submitted_projects = []

    def submit(client, sql, **kwargs):
        assert "project" not in kwargs
        submitted_projects.append(client.project)
        outer = client.get_instance("session-id", project=client.project)
        session = SessionInstance.from_instance(outer, session_task_name="SQLRT", session_project=outer.project)
        return InSessionInstance(name=outer.id, parent=outer.parent, client=outer._client,
                                 session_instance=session, session_project_name=client.project,
                                 session_task_name="SQLRT", session_subquery_id=7, session_is_select=True)

    def reload(instance, **kwargs):
        if instance.project.name != submitted_projects[-1]:
            raise NoSuchObject("instance lookup used the wrong project")
        object.__setattr__(instance, "start_time", datetime(2026, 11, 12))
        object.__setattr__(instance, "end_time", datetime(2026, 11, 12))

    monkeypatch.setattr(ODPS, "run_sql_interactive", submit)
    monkeypatch.setattr(Instance, "reload", reload)
    monkeypatch.setattr(Instance, "get_logview_address", lambda *a, **kw: None)
    monkeypatch.setattr(InSessionInstance, "get_logview_address", lambda *a, **kw: None)
    monkeypatch.setattr("requests.sessions.Session.send", lambda *a, **kw: pytest.fail("unexpected HTTP"))
    monkeypatch.setattr(InSessionInstance, "get_task_detail2", lambda instance, name: {
        "project": instance.project.name, "subquery": instance.subquery_id,
    })
    backend = OdpsBackend(config)
    job = backend.submit_query("SELECT 1", project=target,
                               execution_settings=_ExecutionSettings(version="v1", fallback=False))
    assert submitted_projects == [target]
    assert job.project == job.session_project_name == target
    detail = backend.inspect_job("session-id", project=job.project, section="task-detail",
                                 session_context={"session_task_name": job.session_task_name,
                                                  "session_subquery_id": job.session_subquery_id,
                                                  "session_project_name": job.session_project_name})
    assert detail["detail"] == {"project": target, "subquery": 7}
    assert config.auth.project == "auth_project"


@pytest.mark.parametrize("failure", ["permission", "timeout"])
def test_mcqa_resume_preserves_session_reconstruction_error(tmp_path, monkeypatch, failure):
    import requests
    from odps.errors import NoPermission
    from odps.models.session import SessionInstance

    from maxc_cli.backend.odps import OdpsBackend
    from maxc_cli.exceptions import MaxCError

    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config = load_config(tmp_path)
    config.auth = AuthConfig(access_id="AK", secret_access_key="SK", project="project",
                             endpoint="https://service.example/api")
    config.default_project = "project"
    monkeypatch.setattr("odps.ODPS.get_logview_host", lambda *a, **kw: None)
    monkeypatch.setattr("odps.ODPS.get_job_insight_host", lambda *a, **kw: None, raising=False)
    monkeypatch.setattr("requests.sessions.Session.send", lambda *a, **kw: pytest.fail("unexpected HTTP"))
    cause = NoPermission("session access denied") if failure == "permission" else requests.Timeout("session timed out")

    def fail(*args, **kwargs):
        raise cause

    monkeypatch.setattr("requests.sessions.Session.request", lambda *a, **kw: pytest.fail("unexpected HTTP"))
    monkeypatch.setattr(SessionInstance, "from_instance", fail)
    backend = OdpsBackend(config)
    with pytest.raises(MaxCError) as caught:
        backend.inspect_job("session-id", project="project", section="task-detail",
                            session_context={"session_task_name": "SQLRT", "session_subquery_id": 7})
    assert caught.value.to_payload().code == ("PERMISSION_DENIED" if failure == "permission" else "BACKEND_CONNECTION_ERROR")
    assert caught.value.__cause__ is cause
    assert caught.value.instance_id == "session-id"
    assert str(cause) in caught.value.message


@pytest.fixture
def mcqa_status_backend(tmp_path, monkeypatch):
    from odps import ODPS
    from odps.models import Instance
    from odps.models.session import InSessionInstance

    from maxc_cli.backend.odps import OdpsBackend

    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config = load_config(tmp_path)
    config.auth = AuthConfig(access_id="AK", secret_access_key="SK", project="project",
                             endpoint="https://service.example/api")
    config.default_project = "project"
    monkeypatch.setattr(ODPS, "get_logview_host", lambda *a, **kw: None)
    monkeypatch.setattr(ODPS, "get_job_insight_host", lambda *a, **kw: None, raising=False)
    monkeypatch.setattr("requests.sessions.Session.send", lambda *a, **kw: pytest.fail("unexpected HTTP"))
    monkeypatch.setattr(InSessionInstance, "get_sql_query", lambda *a, **kw: pytest.fail("status fetched SQL source"))
    monkeypatch.setattr(InSessionInstance, "get_logview_address", lambda *a, **kw: pytest.fail("status generated LogView token"))
    calls = []
    payload = {"status": 2, "result": "", "warnings": ""}

    def task_info(instance, task_name, key, **kwargs):
        calls.append((task_name, key))
        assert key == "result_3"
        return json.dumps(payload)

    monkeypatch.setattr(Instance, "get_task_info", task_info)
    backend = OdpsBackend(config)
    context = {"session_task_name": "SQLRT", "session_subquery_id": 3,
               "session_project_name": "project", "session_is_select": True}
    return backend, context, payload, calls


def test_mcqa_status_retains_known_subquery_without_optional_metadata(mcqa_status_backend):
    backend, context, payload, calls = mcqa_status_backend

    info = backend.get_job("session-id", project="project", session_context=context)

    assert info.status == "running"
    assert info.session_subquery_id == 3
    assert calls == [("SQLRT", "result_3")]


@pytest.mark.parametrize("state,status", [(5, "success"), (4, "failure"), (6, "cancelled")])
def test_mcqa_wait_uses_only_subquery_terminal_status(mcqa_status_backend, state, status):
    backend, context, payload, calls = mcqa_status_backend
    payload.update(status=state, result="query failed" if state != 5 else "")

    info = backend.wait_job("session-id", project="project", timeout=1, poll_interval=0,
                            session_context=context)

    assert info.status == status
    assert info.session_subquery_id == 3
    assert info.failure_reason == ("query failed" if state != 5 else None)
    assert calls == [("SQLRT", "result_3")]


@pytest.mark.parametrize("failure", ["timeout", "server"])
def test_mcqa_wait_recovers_transient_status_requests(mcqa_status_backend, monkeypatch, failure):
    from odps.errors import ConnectTimeout, InternalServerError
    from odps.models import Instance

    backend, context, payload, calls = mcqa_status_backend
    payload["status"] = 5
    attempts = []
    cause = ConnectTimeout("status request timed out") if failure == "timeout" else InternalServerError("temporarily unavailable")

    def task_info(instance, task_name, key, **kwargs):
        attempts.append(key)
        if len(attempts) < 3:
            raise cause
        return json.dumps(payload)

    monkeypatch.setattr(Instance, "get_task_info", task_info)
    info = backend.wait_job("session-id", project="project", timeout=1, poll_interval=0,
                            session_context=context)
    assert info.status == "success"
    assert attempts == ["result_3", "result_3", "result_3"]


@pytest.mark.parametrize("max_rows,offset,rows,total,more", [
    (2, 0, [{"x": 1}, {"x": 2}], -1, True),
    (2, 2, [{"x": 3}], 3, False),
    (5, 0, [{"x": 1}, {"x": 2}, {"x": 3}], 3, False),
])
def test_mcqa_result_does_not_fetch_outer_session_cost(mcqa_status_backend, monkeypatch, max_rows, offset, rows, total, more):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from odps.models import Instance
    from odps.models.session import InSessionInstance

    backend, context, payload, calls = mcqa_status_backend
    payload["status"] = 5
    monkeypatch.setattr(InSessionInstance, "get_sql_query", lambda *a, **kw: "SELECT 1 AS x")
    monkeypatch.setattr(InSessionInstance, "get_logview_address", lambda *a, **kw: None)
    monkeypatch.setattr(Instance, "get_task_cost", lambda *a, **kw: pytest.fail("fetched outer session cost"))

    class Reader:
        count = -1
        schema = SimpleNamespace(columns=[SimpleNamespace(name="x", type="bigint")])

        def __iter__(self):
            return iter([SimpleNamespace(values=[value]) for value in (1, 2, 3)])

    monkeypatch.setattr(InSessionInstance, "open_reader", lambda *a, **kw: nullcontext(Reader()))
    result = backend.fetch_job_result("session-id", project="project", max_rows=max_rows, offset=offset,
                                      session_context=context)
    assert result.rows == rows
    assert result.total_rows == total
    assert result.has_more is more
    assert result.session_subquery_id == 3
    assert result.bytes_scanned is None
    assert calls == [("SQLRT", "result_3")]


def test_wait_budget_includes_session_reconstruction(mcqa_status_backend, monkeypatch):
    from maxc_cli.exceptions import JobTimeoutError

    backend, context, payload, calls = mcqa_status_backend
    clock = [0.0]
    get_instance = backend._get_instance

    def reconstruct(*args, **kwargs):
        clock[0] += 2
        return get_instance(*args, **kwargs)

    monkeypatch.setattr(backend, "_get_instance", reconstruct)
    monkeypatch.setattr("maxc_cli.backend.job.monotonic", lambda: clock[0])
    monkeypatch.setattr("maxc_cli.backend.job.sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(JobTimeoutError):
        backend.wait_job("session-id", project="project", timeout=1, poll_interval=1,
                         session_context=context)
    assert calls == []


@pytest.mark.e2e
def test_real_aliyun_wrapper_passes_profile_region_and_endpoint_alias(tmp_path, monkeypatch):
    import os
    import shutil
    import subprocess
    import sys
    from pathlib import Path

    aliyun = shutil.which("aliyun")
    if not aliyun:
        pytest.skip("Alibaba Cloud CLI unavailable")
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1] / "src"))
    source_launcher = tmp_path / "maxc-source"
    source_launcher.write_text(f"#!{sys.executable}\nfrom maxc_cli.__main__ import main\nmain()\n")
    source_launcher.chmod(0o700)
    monkeypatch.setenv("ALIBABA_CLOUD_MAXC_EXEC_PATH", str(source_launcher))
    profile_config = tmp_path / "aliyun.json"
    profile_config.write_text(json.dumps({"current": "Fixture", "profiles": [{
        "name": "Fixture", "mode": "OAuth", "region_id": "cn-hangzhou",
        "access_key_id": "STS.FIXTURE", "access_key_secret": "FIXTURE_SECRET",
        "sts_token": "FIXTURE_TOKEN", "sts_expiration": 4102444800,
        "oauth_site_type": "CN", "output_format": "json", "language": "en",
    }]}))
    target = tmp_path / "maxc.yaml"
    completed = subprocess.run([
        aliyun, "--config-path", str(profile_config), "--profile", "Fixture", "maxc",
        "auth", "login", "--reuse-auth", "--project", "p",
        "--odps-endpoint", "https://private.example/api",
        "--no-validate", "--json", "--config", str(target),
    ], capture_output=True, text=True, env=os.environ.copy(), timeout=20)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["status"] == "success"
    saved = yaml.safe_load(target.read_text())
    assert saved["auth"]["endpoint"] == "https://private.example/api"
    assert saved["auth"]["region_name"] == "cn-hangzhou"
    assert "FIXTURE_SECRET" not in target.read_text() + completed.stdout + completed.stderr
    assert "FIXTURE_TOKEN" not in target.read_text() + completed.stdout + completed.stderr

    # A fresh invocation without a configured project returns a project
    # template rather than trying to authenticate or list Catalog projects.
    empty_target = tmp_path / "empty.yaml"
    empty_target.write_text("auth: {}\n")
    missing = subprocess.run([
        aliyun, "--config-path", str(profile_config), "--profile", "Fixture", "maxc",
        "query", "SELECT 1", "--config", str(empty_target), "--json",
    ], capture_output=True, text=True, env=os.environ.copy(), timeout=20)
    # Alibaba Cloud CLI 3.4.11 swallows a child validation exit code; the
    # Envelope must remain authoritative for failure detection.
    error = json.loads(missing.stdout)
    assert error["status"] == "failure"
    assert error["error"]["context"]["missing_fields"] == ["project"]
    assert "--project <project>" in error["agent_hints"]["actions"][0]["command"]
    assert yaml.safe_load(empty_target.read_text()) == {"auth": {}}
