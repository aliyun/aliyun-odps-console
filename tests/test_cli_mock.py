"""Tests for maxc-cli using FakeODPS mock.

These tests use FakeODPS to mock the ODPS client, allowing testing of
authentication and configuration flows without a real MaxCompute connection.
"""


import json
from io import StringIO
from pathlib import Path

import pytest
import yaml

import maxc_cli.backend as backend_module
from maxc_cli.cli import run


def clear_odps_env(monkeypatch) -> 'None':
    """Clear all ODPS-related environment variables."""
    for aliases in backend_module.ODPS_ENV_ALIASES.values():
        for alias in aliases:
            monkeypatch.delenv(alias, raising=False)


def isolate_home(monkeypatch, tmp_path: 'Path') -> 'None':
    monkeypatch.setenv("HOME", str(tmp_path))


def run_json_command(tmp_path: 'Path', config_path: 'Path', argv: 'list[str]') -> 'tuple[int, dict[str, object], str]':
    """Run a command and return (exit_code, json_payload, stderr)."""
    stdout = StringIO()
    stderr = StringIO()

    code = run(
        ["--config", str(config_path), *argv],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    return code, json.loads(stdout.getvalue()), stderr.getvalue()


class FakeODPS:
    """Mock ODPS client for testing."""

    def __init__(
        self,
        access_id=None,
        secret_access_key: 'str | None' = None,
        project: 'str | None' = None,
        endpoint: 'str | None' = None,
        region_name: 'str | None' = None,
        tunnel_endpoint: 'str | None' = None,
        **_: 'object',
    ) -> 'None':
        if hasattr(access_id, "access_id"):
            account = access_id
            access_id = getattr(account, "access_id", None)
        self.account = type("Account", (), {"access_id": access_id})()
        self.project = project
        self.endpoint = endpoint
        self.region_name = region_name
        self.tunnel_endpoint = tunnel_endpoint

    def get_project(self, project: 'str'):
        """Return mock project with owner."""
        return type("Project", (), {"owner": f"ALIYUN$mock_user_{project}"})()

    def execute_security_query(self, query: 'str', project: 'str | None' = None):
        """Mock security query - returns dict with DisplayName."""
        if query == "whoami":
            target_project = project or self.project
            return {
                "DisplayName": f"ALIYUN$mock_user_{target_project}",
                "ID": "123456789",
                "SourceIP": "127.0.0.1"
            }
        raise NotImplementedError(f"Unknown security query: {query}")


class BrokenWhoamiODPS(FakeODPS):
    """Mock ODPS client that resolves config but fails remote whoami validation."""

    def execute_security_query(self, query: 'str', project: 'str | None' = None):
        if query == "whoami":
            raise OSError("failed to resolve remote whoami endpoint")
        return super().execute_security_query(query, project=project)


# ============================================================
# Auth Login Tests (don't require backend connection)
# ============================================================

def test_auth_login_can_create_new_explicit_config_without_validation(
    tmp_path: 'Path',
    monkeypatch,
) -> 'None':
    """Test auth login creates config file with --no-validate."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config_path = tmp_path / "login.yaml"

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth",
            "login",
            "--access-id",
            "TESTACCESS1234",
            "--secret-access-key",
            "TESTSECRET1234",
            "--project",
            "login_project",
            "--endpoint",
            "http://service.cn-test.maxcompute.aliyun.com/api",
            "--region",
            "cn-test",
            "--no-validate",
            "--json",
        ],
    )

    assert code == 0
    assert payload["command"] == "auth login"
    assert payload["command_id"] == "auth.login"
    assert payload["data"]["persistence"]["saved"] is True
    assert payload["data"]["persistence"]["validated"] is False
    assert payload["data"]["identity"]["identity_source"] == "config_file"
    assert payload["metadata"]["config_path"] == str(config_path.resolve())

    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert saved["auth"]["access_id"] == "TESTACCESS1234"
    assert saved["auth"]["secret_access_key"] == "TESTSECRET1234"
    assert saved["auth"]["project"] == "login_project"
    assert saved["auth"]["endpoint"] == "http://service.cn-test.maxcompute.aliyun.com/api"
    assert saved["auth"]["region_name"] == "cn-test"
    assert saved["default_project"] == "login_project"
    assert saved["default_region"] == "cn-test"


def test_auth_whoami_uses_saved_config_credentials_when_env_missing(
    tmp_path: 'Path',
    monkeypatch,
) -> 'None':
    """Test auth whoami reads from saved config when env vars are missing."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    # Mock ODPS in the odps package where it's imported from
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_format: json
state_dir: .maxc/state
backend:
  type: auto
allowed_operations:
  - SELECT
auth:
  access_id: TESTACCESS1234
  secret_access_key: TESTSECRET1234
  project: config_project
  endpoint: http://service.cn-test.maxcompute.aliyun.com/api
  region_name: cn-test
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "whoami", "--json"],
    )

    assert code == 0
    assert payload["command"] == "auth whoami"
    assert payload["command_id"] == "auth.whoami"
    identity = payload["data"]["identity"]
    assert identity["authenticated"] is True
    assert identity["configured"] is True
    assert identity["validation_status"] == "verified"
    assert identity["backend"] == "odps"
    assert identity["identity_source"] == "config_file"
    assert identity["project"] == "config_project"
    assert identity["region"] == "cn-test"
    assert identity["endpoint"] == "http://service.cn-test.maxcompute.aliyun.com/api"
    assert identity["project_owner"] == "ALIYUN$mock_user_config_project"


# ============================================================
# Backend Creation Tests
# ============================================================

def test_auth_whoami_returns_guidance_without_odps_config(tmp_path: 'Path', monkeypatch) -> 'None':
    """Verify auth whoami returns guidance when auth config is missing."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
backend:
  type: auto
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "whoami", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["identity"]["authenticated"] is False
    assert payload["data"]["identity"]["configured"] is False
    assert payload["data"]["identity"]["validation_status"] == "missing_configuration"
    assert payload["data"]["auth_options"][0]["type"] == "access_key"


def test_auth_whoami_marks_configured_but_unverified_when_remote_check_fails(
    tmp_path: 'Path',
    monkeypatch,
) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps

    monkeypatch.setattr(odps, "ODPS", BrokenWhoamiODPS)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo_project
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
auth:
  access_id: TESTACCESS1234
  secret_access_key: TESTSECRET1234
  project: demo_project
  endpoint: http://service.cn-test.maxcompute.aliyun.com/api
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "whoami", "--json"],
    )

    assert code == 0
    identity = payload["data"]["identity"]
    assert identity["authenticated"] is False
    assert identity["configured"] is True
    assert identity["validation_status"] == "failed"
    assert identity["identity_source"] == "config_file"
    assert payload["agent_hints"]["action_ids"] == ["auth.login", "auth.login-ncs"]
    assert any(
        "failed to resolve remote whoami endpoint" in warning
        for warning in payload["agent_hints"]["warnings"]
    )


def test_auth_login_supports_sts_token_payload(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    config_path = tmp_path / "login-sts.yaml"

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth",
            "login",
            "--access-id",
            "TESTACCESS1234",
            "--secret-access-key",
            "TESTSECRET1234",
            "--security-token",
            "TESTSTS1234",
            "--project",
            "login_project",
            "--endpoint",
            "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-validate",
            "--json",
        ],
    )

    assert code == 0
    assert payload["data"]["identity"]["auth_type"] == "sts_token"
    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert saved["auth"]["provider"] == "sts_token"
    assert saved["auth"]["security_token"] == "TESTSTS1234"


def test_auth_login_ncs_persists_provider_config(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")
    config_path = tmp_path / "login-ncs.yaml"

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth",
            "login-ncs",
            "--account-type",
            "user",
            "--employee-id",
            "123456",
            "--project",
            "login_project",
            "--endpoint",
            "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-validate",
            "--json",
        ],
    )

    assert code == 0
    assert payload["command"] == "auth login-ncs"
    assert payload["data"]["identity"]["auth_type"] == "ncs"
    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert saved["auth"]["provider"] == "ncs"
    assert saved["auth"]["ncs"]["account_type"] == "user"
    assert saved["auth"]["ncs"]["employee_id"] == "123456"


def test_session_show_and_agent_context_work_without_auth(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    session_code, session_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["session", "show", "--json"],
    )
    assert session_code == 0
    assert session_payload["command"] == "session show"
    assert session_payload["data"]["project"]["value"] == "demo"

    context_code, context_payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["agent", "context", "--json"],
    )
    assert context_code == 0
    assert context_payload["command"] == "agent context"
    assert context_payload["data"]["context"]["project"] == "demo"
    assert context_payload["metadata"]["job_mode"] == "unknown"


def test_cache_status_requires_auth_with_structured_failure(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["cache", "status", "--json"],
    )

    assert code == 1
    assert payload["command"] == "cache status"
    assert payload["command_id"] == "cache.status"
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"


def test_session_set_without_values_returns_standard_failure_envelope(tmp_path: 'Path', monkeypatch) -> 'None':
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
default_project: demo
default_format: json
state_dir: .maxc/state
allowed_operations:
  - SELECT
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["session", "set", "--json"],
    )

    assert code == 1
    assert payload["command"] == "session set"
    assert payload["command_id"] == "session.set"
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "VALIDATION_ERROR"


# ============================================================
# Task 1: missing_odps_settings NCS tolerance
# ============================================================

def test_missing_odps_settings_ncs_tolerates_missing_process_command_when_account_fields_present() -> None:
    from maxc_cli.helpers import missing_odps_settings

    # Has account type + identifier but no process_command → should NOT be missing
    settings = {
        "project": "myproj",
        "endpoint": "http://service.cn.maxcompute.aliyun.com/api",
        "ncs_account_type": "user",
        "ncs_employee_id": "123456",
        "ncs_process_command": None,
    }
    assert missing_odps_settings(settings, auth_type="ncs") == []


def test_missing_odps_settings_ncs_reports_missing_when_no_account_fields() -> None:
    from maxc_cli.helpers import missing_odps_settings

    # No process_command AND no account fields → truly missing
    settings = {
        "project": "myproj",
        "endpoint": "http://service.cn.maxcompute.aliyun.com/api",
        "ncs_account_type": None,
        "ncs_employee_id": None,
        "ncs_process_command": None,
    }
    result = missing_odps_settings(settings, auth_type="ncs")
    assert "ncs_process_command" in result
