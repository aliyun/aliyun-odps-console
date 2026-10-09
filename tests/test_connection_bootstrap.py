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
