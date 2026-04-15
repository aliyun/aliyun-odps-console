"""Tests for maxc-cli using FakeODPS mock.

These tests use FakeODPS to mock the ODPS client, allowing testing of
authentication and configuration flows without a real MaxCompute connection.
"""


import json
from io import StringIO
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
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
        # Catalog API stubs — no real catalog in tests
        self.schema = None
        self.app_account = None
        self.namespace = None
        self._rest_client_cls = None
        self._rest_client_kwargs = {}

    @property
    def catalog_endpoint(self):
        return None

    @property
    def catalog_rest(self):
        return None

    def get_project(self, project: 'str'):
        """Return mock project with owner and tenant_id."""
        return type("Project", (), {
            "owner": f"ALIYUN$mock_user_{project}",
            "tenant_id": "000000000000000",
        })()

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


# ============================================================
# Task 2: config_sources in auth whoami and session show
# ============================================================

def test_auth_whoami_metadata_includes_config_sources(tmp_path: 'Path', monkeypatch) -> None:
    """auth whoami metadata should list the active config file paths."""
    import odps as odps_pkg
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: access_key\n"
        "  access_id: TESTID1234\n"
        "  secret_access_key: TESTSECRET1234\n"
        "  project: test_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(odps_pkg, "ODPS", FakeODPS)

    code, payload, _ = run_json_command(tmp_path, config_path, ["auth", "whoami", "--json"])
    assert code == 0
    assert "config_sources" in payload["metadata"]
    assert isinstance(payload["metadata"]["config_sources"], list)
    assert any(str(config_path) in s for s in payload["metadata"]["config_sources"])


def test_session_show_data_includes_config_sources(tmp_path: 'Path', monkeypatch) -> None:
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: demo\n"
        "default_format: json\n"
        "state_dir: .maxc/state\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(tmp_path, config_path, ["session", "show", "--json"])
    assert code == 0
    assert "config_sources" in payload["data"]
    assert isinstance(payload["data"]["config_sources"], list)


# ============================================================
# Task 3: session set warns on auth project mismatch
# ============================================================

def test_session_set_warns_when_project_differs_from_auth_project(tmp_path: 'Path', monkeypatch) -> None:
    """session set should warn when override project differs from auth.project."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: ncs\n"
        "  project: ncs_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n"
        "  ncs:\n"
        "    account_type: user\n"
        "    employee_id: '123456'\n"
        "    process_command: 'ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd'\n"
        "default_project: ncs_project\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["session", "set", "--project", "other_project", "--json"]
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("ncs_project" in w and "other_project" in w for w in warnings), (
        f"Expected a warning about project mismatch, got: {warnings}"
    )


# ============================================================
# Task 4: auth login-ncs interactive mode preserves existing values
# ============================================================

def test_auth_login_ncs_interactive_uses_existing_project_on_empty_input(
    tmp_path: 'Path', monkeypatch
) -> None:
    """Interactive login-ncs preserves existing values when stdin is non-TTY (empty input).

    After the fix, _prompt_text returns `default` when stdin is not a TTY,
    so existing config values survive a --interactive run in non-interactive environments.
    """
    import builtins
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")

    config_path = tmp_path / "existing.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: ncs\n"
        "  project: existing_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n"
        "  ncs:\n"
        "    account_type: user\n"
        "    employee_id: '111'\n"
        "    process_command: 'ncs create credential odpsuser --employee-id 111 -o template -t odpscmd'\n",
        encoding="utf-8",
    )

    # Patch stdin.isatty to return True so _prompt_text enters the interactive branch,
    # then patch builtins.input to return "" (simulating the user pressing Enter).
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(builtins, "input", lambda _: "")

    stdout = StringIO()
    from maxc_cli.cli import run
    code = run(
        ["--config", str(config_path), "auth", "login-ncs", "--interactive", "--no-validate", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )
    payload = json.loads(stdout.getvalue())
    assert code == 0
    # Empty input should keep existing project via the default parameter
    assert payload["data"]["identity"]["project"] == "existing_project"


# ============================================================
# Task 5: Narrow env-var override warning in auth login-ncs
# ============================================================

def test_auth_login_ncs_no_spurious_warning_when_only_credential_env_vars_set(
    tmp_path: 'Path', monkeypatch
) -> None:
    """login-ncs should NOT warn about env vars when only access_id/secret envs are set.

    Those env vars don't override the ncs provider selection, so the warning is misleading.
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")
    # Only set access_id/secret — these don't affect NCS provider
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "some_key")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "some_secret")

    config_path = tmp_path / "ncs.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login-ncs",
            "--account-type", "user",
            "--employee-id", "999",
            "--project", "my_project",
            "--endpoint", "http://service.cn.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert not any("environment variable" in w.lower() or "env" in w.lower() for w in warnings), (
        f"Should not warn about env vars when only credential vars are set: {warnings}"
    )


def test_auth_login_ncs_warns_when_project_or_endpoint_env_var_set(
    tmp_path: 'Path', monkeypatch
) -> None:
    """login-ncs SHOULD warn when MAXCOMPUTE_PROJECT or MAXCOMPUTE_ENDPOINT env vars are set."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "env_override_project")

    config_path = tmp_path / "ncs.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login-ncs",
            "--account-type", "user",
            "--employee-id", "999",
            "--project", "config_project",
            "--endpoint", "http://service.cn.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("env" in w.lower() or "environment" in w.lower() for w in warnings), (
        f"Expected a warning about project/endpoint env var override: {warnings}"
    )


def test_env_vars_suppressed_when_explicit_provider_in_config(
    tmp_path: 'Path', monkeypatch
) -> None:
    """When config has an explicit auth provider, env vars must not override any auth settings."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    # Set env vars that would normally override project/endpoint
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "env_project")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "http://env-endpoint.example.com/api")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env_key")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env_secret")

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
auth:
  provider: access_key
  access_id: config_key
  secret_access_key: config_secret
  project: config_project
  endpoint: http://config-endpoint.example.com/api
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(tmp_path, config_path, ["auth", "whoami", "--json"])

    assert code == 0
    identity = payload["data"]["identity"]
    # Config values must win — env vars must not override
    assert identity["project"] == "config_project", (
        f"Expected config_project but got {identity['project']!r}; env var leaked through"
    )
    assert identity["endpoint"] == "http://config-endpoint.example.com/api", (
        f"Expected config endpoint but got {identity['endpoint']!r}; env var leaked through"
    )
    assert identity["identity_source"] == "config_file"
    # Suppressed env vars must be surfaced in warnings
    warnings = payload["agent_hints"]["warnings"]
    assert any("ignored" in w.lower() or "suppressed" in w.lower() or "ignored" in w.lower() for w in warnings), (
        f"Expected a warning about suppressed env vars: {warnings}"
    )


def test_env_vars_active_when_no_provider_in_config(
    tmp_path: 'Path', monkeypatch
) -> None:
    """When config has no explicit provider, env vars should still provide auth settings."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env_key")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env_secret")
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "env_project")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "http://env-endpoint.example.com/api")

    # Config has no provider field — env vars should take effect
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
backend:
  type: auto
""".strip()
        + "\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(tmp_path, config_path, ["auth", "whoami", "--json"])

    assert code == 0
    identity = payload["data"]["identity"]
    assert identity["project"] == "env_project", (
        f"Expected env_project but got {identity['project']!r}"
    )
    assert identity["identity_source"] in ("environment", "mixed")


# ============================================================
# NCS credential caching and account_type validation tests
# ============================================================

def test_ncs_credential_provider_caches_token_until_expiry() -> None:
    """NcsCredentialProvider must not call ncs again while the cached token is still valid."""
    from datetime import datetime, timedelta, timezone
    from maxc_cli.auth_providers import NcsCredentialProvider

    call_count = 0

    def fake_run(cmd, **kwargs):
        nonlocal call_count
        call_count += 1
        import types
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = types.SimpleNamespace(
            returncode=0,
            stdout=f'{{"AccessKeyId":"AK{call_count}","AccessKeySecret":"SK","SecurityToken":"TOK","Expiration":"{future}"}}',
            stderr="",
        )
        return result

    import unittest.mock as mock
    provider = NcsCredentialProvider(command="fake-ncs", timeout=5)
    with mock.patch("subprocess.run", side_effect=fake_run):
        c1 = provider.get_credentials()
        c2 = provider.get_credentials()

    assert call_count == 1, f"Expected 1 ncs call, got {call_count}"
    assert c1 is c2, "Second call should return the same cached object"


def test_ncs_credential_provider_refreshes_after_expiry() -> None:
    """NcsCredentialProvider must call ncs again once the cached token has expired."""
    from datetime import datetime, timedelta, timezone
    from maxc_cli.auth_providers import NcsCredentialProvider

    call_count = 0

    def fake_run(cmd, **kwargs):
        nonlocal call_count
        call_count += 1
        import types
        # Return an already-expired token so the next call is forced to refresh
        past = (datetime.now(timezone.utc) - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = types.SimpleNamespace(
            returncode=0,
            stdout=f'{{"AccessKeyId":"AK{call_count}","AccessKeySecret":"SK","SecurityToken":"TOK","Expiration":"{past}"}}',
            stderr="",
        )
        return result

    import unittest.mock as mock
    provider = NcsCredentialProvider(command="fake-ncs", timeout=5)
    with mock.patch("subprocess.run", side_effect=fake_run):
        c1 = provider.get_credentials()
        c2 = provider.get_credentials()

    assert call_count == 2, f"Expected 2 ncs calls after expiry, got {call_count}"
    assert c1 is not c2


def test_auth_login_ncs_requires_account_type(tmp_path: 'Path', monkeypatch) -> None:
    """auth login-ncs must reject requests where account_type cannot be determined."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")

    # No existing config, no --account-type flag
    config_path = tmp_path / "empty.yaml"
    config_path.write_text("backend:\n  type: auto\n", encoding="utf-8")

    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login-ncs",
            "--employee-id", "999",
            "--project", "my_project",
            "--endpoint", "http://service.cn.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )
    assert code != 0 or payload["status"] == "error", (
        "Expected error when account_type is missing"
    )
    error_msg = (payload.get("error") or {}).get("message", "")
    assert "account_type" in error_msg.lower(), (
        f"Expected error mentioning account_type, got: {error_msg!r}"
    )


def test_auth_login_clears_session_override(tmp_path: 'Path', monkeypatch) -> None:
    """auth login must delete session_override.yaml so stale project does not shadow new auth."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    # Write a stale session override
    maxc_dir = tmp_path / ".maxc"
    maxc_dir.mkdir(parents=True)
    session_override = maxc_dir / "session_override.yaml"
    session_override.write_text("project: old_project\n", encoding="utf-8")

    config_path = tmp_path / "config.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login",
            "--access-id", "TESTKEY",
            "--secret-access-key", "TESTSECRET",
            "--project", "new_project",
            "--endpoint", "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )

    assert code == 0
    assert not session_override.exists(), "session_override.yaml should have been deleted on auth login"
    warnings = payload["agent_hints"]["warnings"]
    assert any("session override" in w.lower() or "session" in w.lower() for w in warnings), (
        f"Expected a warning about cleared session override: {warnings}"
    )


def test_auth_login_ncs_clears_session_override(tmp_path: 'Path', monkeypatch) -> None:
    """auth login-ncs must also delete session_override.yaml."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")

    maxc_dir = tmp_path / ".maxc"
    maxc_dir.mkdir(parents=True)
    session_override = maxc_dir / "session_override.yaml"
    session_override.write_text("project: old_project\n", encoding="utf-8")

    config_path = tmp_path / "config.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login-ncs",
            "--account-type", "user",
            "--employee-id", "999",
            "--project", "new_project",
            "--endpoint", "http://service.cn-test.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )

    assert code == 0
    assert not session_override.exists(), "session_override.yaml should have been deleted on auth login-ncs"


# ============================================================
# Bootstrap Bug Fixes — Tests
# ============================================================

def test_auth_login_from_env_fails_when_required_env_var_missing(
    tmp_path: 'Path', monkeypatch
) -> None:
    """--from-env must raise a clear error when a required env var is missing."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    # Only set access_id, leave secret/project/endpoint unset
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "TEST_ID")

    config_path = tmp_path / "config.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "login", "--from-env", "--no-validate", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert "--from-env" in payload["error"]["message"]


def test_auth_login_from_env_shows_imported_warning(
    tmp_path: 'Path', monkeypatch
) -> None:
    """--from-env should show 'imported from env' warning, not 'may override' warning."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "TEST_ID")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "TEST_SECRET")
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "test_proj")
    monkeypatch.setenv("MAXCOMPUTE_ENDPOINT", "http://service.cn-test.maxcompute.aliyun.com/api")

    config_path = tmp_path / "config.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["auth", "login", "--from-env", "--no-validate", "--json"],
    )

    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("imported" in w.lower() for w in warnings), (
        f"Expected an 'imported from environment' warning, got: {warnings}"
    )
    assert not any("may override" in w.lower() for w in warnings), (
        f"Should not show 'may override' warning when --from-env is used: {warnings}"
    )


def test_auth_login_ncs_interactive_normalizes_account_type_case(
    tmp_path: 'Path', monkeypatch
) -> None:
    """Interactive login-ncs must normalize account_type to lowercase (e.g. 'User' → 'user')."""
    import builtins
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")

    config_path = tmp_path / "ncs.yaml"

    # Simulate user typing "User" (uppercase) for account_type, then other fields
    input_values = iter(["User", "12345", "my_project", "http://service.cn.maxcompute.aliyun.com/api", "", ""])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(builtins, "input", lambda _: next(input_values))

    stdout = StringIO()
    code = run(
        ["--config", str(config_path), "auth", "login-ncs", "--interactive", "--no-validate", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )
    payload = json.loads(stdout.getvalue())
    assert code == 0

    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert saved["auth"]["ncs"]["account_type"] == "user", (
        f"account_type should be normalized to lowercase, got: {saved['auth']['ncs']['account_type']}"
    )


# ============================================================
# Malformed config.yaml
# ============================================================

def test_malformed_config_yaml_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """A broken YAML config file should produce a structured error, not a raw traceback."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "broken.yaml"
    config_path.write_text("auth:\n  project: [unterminated\n", encoding="utf-8")

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["auth", "whoami", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert "invalid yaml" in payload["error"]["message"].lower()
    assert payload["error"]["suggestion"] is not None


# ============================================================
# Structured error output for ODPS / unexpected exceptions
# ============================================================


class _NoSuchObjectODPS(FakeODPS):
    """Mock ODPS client whose get_table raises NoSuchObject on schema access."""

    def get_table(self, name, *, project=None, schema=None):
        """Return a table object whose table_schema raises NoSuchObject."""
        try:
            from odps.errors import NoSuchObject
        except ImportError:
            pytest.skip("odps package not installed")

        class _ExplodingSchema:
            @property
            def columns(self):
                raise NoSuchObject(f"Table not found - '{name}'")

            @property
            def partitions(self):
                raise NoSuchObject(f"Table not found - '{name}'")

        return type("FakeTable", (), {
            "name": name,
            "table_schema": _ExplodingSchema(),
            "comment": "",
            "owner": None,
            "creation_time": None,
            "last_data_modified_time": None,
            "is_virtual_view": False,
            "size": 0,
            "lifecycle": None,
        })()

    def list_tables(self, project=None):
        return []

    def read_table(self, *a, **kw):
        return []


def _make_config_with_odps(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  access_id: FAKE\n"
        "  secret_access_key: FAKE\n"
        "  project: test_project\n"
        "  endpoint: http://localhost/api\n"
        "backend:\n"
        "  type: auto\n",
        encoding="utf-8",
    )
    return config_path


def test_data_profile_not_found_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """ODPS NoSuchObject from data profile must produce a structured envelope, not a traceback."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _NoSuchObjectODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path, ["data", "profile", "nonexistent_table", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "NOT_FOUND"
    assert "not found" in payload["error"]["message"].lower()


def test_meta_describe_not_found_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """ODPS NoSuchObject from meta describe must produce a structured envelope."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _NoSuchObjectODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path, ["meta", "describe", "nonexistent_table", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "NOT_FOUND"


def test_unexpected_exception_returns_structured_error(
    tmp_path: 'Path', monkeypatch
) -> None:
    """An unexpected non-MaxCError exception with --json must produce a structured INTERNAL_ERROR envelope."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    config_path = _make_config_with_odps(tmp_path)

    # Monkeypatch the handler to raise an unexpected error
    import maxc_cli.cli as cli_module

    def _exploding_handler(*a, **kw):
        raise RuntimeError("something completely unexpected")

    monkeypatch.setattr(cli_module, "_handle_data_profile", _exploding_handler)

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["data", "profile", "some_table", "--json"],
    )

    assert code != 0
    assert payload["status"] == "failure"
    assert payload["error"]["code"] == "INTERNAL_ERROR"
    assert "unexpected" in payload["error"]["message"].lower()


def test_unexpected_exception_renders_markdown_without_json_flag(
    tmp_path: 'Path', monkeypatch
) -> None:
    """Without --json, unexpected exception writes markdown error to stderr."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", FakeODPS)

    config_path = _make_config_with_odps(tmp_path)

    import maxc_cli.cli as cli_module

    def _exploding_handler(*a, **kw):
        raise RuntimeError("something completely unexpected")

    monkeypatch.setattr(cli_module, "_handle_data_profile", _exploding_handler)

    stdout = StringIO()
    stderr = StringIO()
    code = run(
        ["--config", str(config_path), "data", "profile", "some_table"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code != 0
    err_text = stderr.getvalue()
    assert "**Error**" in err_text
    assert "`INTERNAL_ERROR`" in err_text
    assert "unexpected" in err_text.lower()
    assert "**Suggestion**" in err_text
    assert stdout.getvalue().strip() == ""


def test_not_found_error_renders_markdown_without_json_flag(
    tmp_path: 'Path', monkeypatch
) -> None:
    """ODPS NoSuchObject without --json writes markdown error to stderr."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _NoSuchObjectODPS)

    config_path = _make_config_with_odps(tmp_path)

    stdout = StringIO()
    stderr = StringIO()
    code = run(
        ["--config", str(config_path), "data", "profile", "nonexistent_table"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=stderr,
    )

    assert code != 0
    err_text = stderr.getvalue()
    assert "**Error**" in err_text
    assert "`NOT_FOUND`" in err_text
    assert "**Suggestion**" in err_text


# ============================================================
# Schema Passthrough Tests
# ============================================================


class _SchemaAwareODPS(FakeODPS):
    """Mock ODPS client that returns different tables per schema."""

    _SCHEMA_TABLES = {
        None: ["default_table_a", "default_table_b"],
        "california_schools": ["frpm", "satscores", "schools"],
    }

    def list_tables(self, *, project=None, schema=None):
        names = self._SCHEMA_TABLES.get(schema, [])
        return [
            type("FakeTable", (), {"name": n})()
            for n in names
        ]

    def get_table(self, name, *, project=None, schema=None):
        # minimal stub for describe
        return type("FakeTable", (), {
            "name": name,
            "comment": "",
            "table_schema": type("Schema", (), {"columns": [], "partitions": []})(),
            "owner": "test_owner",
            "creation_time": None,
            "last_data_modified_time": None,
            "is_virtual_view": False,
            "size": 0,
            "lifecycle": None,
        })()


def test_meta_list_tables_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta list-tables --schema should list tables from the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "list-tables", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    table_names = [t["table_name"] for t in payload["data"]["tables"]]
    assert sorted(table_names) == ["frpm", "satscores", "schools"]


def test_meta_list_tables_without_schema_uses_default(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta list-tables without --schema should list tables from default schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "list-tables", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    table_names = [t["table_name"] for t in payload["data"]["tables"]]
    assert sorted(table_names) == ["default_table_a", "default_table_b"]


def test_cache_build_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """cache build --schema should list tables from the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["cache", "build", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["data"]["tables_scanned"] == 3
    assert payload["data"]["cached_tables"] == 3


def test_meta_search_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta search --schema should search tables in the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "search", "frpm", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    matches = payload["data"]["search"]["matches"]
    assert len(matches) >= 1
    assert any(m["table_name"] == "frpm" for m in matches)


def test_meta_search_columns_passes_schema_to_backend(
    tmp_path: 'Path', monkeypatch
) -> None:
    """meta search-columns --schema should search in the specified schema."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    import odps
    monkeypatch.setattr(odps, "ODPS", _SchemaAwareODPS)

    config_path = _make_config_with_odps(tmp_path)
    # search for a keyword that won't match (stub tables have no columns),
    # but verify it runs without error and uses the right schema
    code, payload, _ = run_json_command(
        tmp_path, config_path,
        ["meta", "search-columns", "nonexistent", "--schema", "california_schools", "--json"],
    )

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["search"]["matches"] == []
